from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from unittest import mock


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "orquestador"
    / "database_pipeline.py"
)
SPEC = importlib.util.spec_from_file_location("database_pipeline_under_test", MODULE_PATH)
assert SPEC and SPEC.loader
pipeline = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pipeline)


def test_incremental_command_requires_postgres_without_full_sync() -> None:
    command = pipeline.updater_command("incremental")
    assert "--require-postgres" in command
    assert "--force-reclassify" not in command
    assert "--postgres-full" not in command


def test_weekly_command_reclassifies_and_republishes_everything() -> None:
    command = pipeline.updater_command("full")
    assert "--require-postgres" in command
    assert "--force-reclassify" in command
    assert "--postgres-full" in command


def test_analytics_publication_is_mandatory() -> None:
    command = pipeline.analytics_command()
    assert "--publish-postgres" in command
    assert "--require-postgres" in command


def test_pc_analytics_publication_is_mandatory() -> None:
    command = pipeline.pc_analytics_command()
    assert command[1].endswith("build_inteligencia_pc.py")
    assert "--publish-postgres" in command
    assert "--require-postgres" in command
    assert str(pipeline.OPERATIONAL_DB) in command


def test_missing_supabase_dsn_stops_before_any_database_command() -> None:
    emitted: list[tuple[str, str]] = []

    def capture(job_name, status, *_args, **_kwargs):
        emitted.append((job_name, status))

    with (
        mock.patch.dict(os.environ, {}, clear=True),
        mock.patch.object(pipeline, "_run") as run_mock,
        mock.patch.object(pipeline, "emit_component", side_effect=capture),
    ):
        result = pipeline.run_pipeline("incremental")

    assert result == 2
    run_mock.assert_not_called()
    assert emitted == [
        ("db_local", "error"),
        ("supabase_operational", "error"),
        ("analytics", "blocked"),
    ]


def test_old_success_metadata_cannot_mask_a_lock_failure():
    old = {"last_run_started_at": "2026-09-14 00:31:00", "last_local_update_status": "success",
           "last_postgres_sync_status": "success", "last_total_rows": "250845"}
    with mock.patch.dict(os.environ, {"SUPABASE_DB_URL": "test-only"}), \
         mock.patch.object(pipeline, "_run", return_value=1) as runner, \
         mock.patch.object(pipeline, "read_metadata", return_value=old), \
         mock.patch.object(pipeline, "LAST_COMMAND_ERROR", "Otra actualización conserva el bloqueo"), \
         mock.patch.object(pipeline, "emit_component") as emit:
        assert pipeline.run_pipeline("incremental") == 1
    assert runner.call_count == 1
    assert all(call.args[1] == "blocked" for call in emit.call_args_list)
    assert all("bloqueo" in call.args[-1] for call in emit.call_args_list)
    assert all("250845" not in call.args[-1] for call in emit.call_args_list)


def test_current_local_success_survives_postgres_failure():
    from datetime import datetime
    metadata = {"last_run_started_at": datetime.now().isoformat(), "last_local_update_status": "success",
                "last_postgres_sync_status": "error", "last_postgres_sync_error": "connection unavailable"}
    with mock.patch.dict(os.environ, {"SUPABASE_DB_URL": "test-only"}), \
         mock.patch.object(pipeline, "_run", return_value=1) as runner, \
         mock.patch.object(pipeline, "read_metadata", return_value=metadata), \
         mock.patch.object(pipeline, "emit_component") as emit:
        assert pipeline.run_pipeline("incremental") == 1
    assert runner.call_count == 1
    assert [(c.args[0],c.args[1]) for c in emit.call_args_list] == [
        ("db_local", "success"), ("supabase_operational", "error"), ("analytics", "blocked")]
