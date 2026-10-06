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
    assert "--skip-reclassify" in command


def test_weekly_command_reclassifies_and_republishes_everything() -> None:
    command = pipeline.updater_command("full")
    assert "--require-postgres" in command
    assert "--force-reclassify" in command
    assert "--postgres-full" in command
    assert "--skip-reclassify" not in command


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
         mock.patch.object(pipeline, "validate_pc_builder"), \
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
         mock.patch.object(pipeline, "validate_pc_builder"), \
         mock.patch.object(pipeline, "_run", return_value=1) as runner, \
         mock.patch.object(pipeline, "read_metadata", return_value=metadata), \
         mock.patch.object(pipeline, "emit_component") as emit:
        assert pipeline.run_pipeline("incremental") == 1
    assert runner.call_count == 1
    assert [(c.args[0],c.args[1]) for c in emit.call_args_list] == [
        ("db_local", "success"), ("supabase_operational", "error"), ("analytics", "blocked")]


def test_rotation_only_runs_the_publisher_with_existing_scraper_root(tmp_path):
    builder = tmp_path / "scripts/build_inteligencia_pc.py"
    publisher = builder.with_name("update_ct_rotation.py")
    publisher.parent.mkdir()
    publisher.touch()
    with mock.patch.object(pipeline, "pc_analytics_builder", return_value=builder), \
         mock.patch.object(pipeline, "_run", return_value=0) as runner:
        pipeline.refresh_rotation_from_existing_data()
    runner.assert_called_once_with([
        str(pipeline.PYTHON_EXE), str(publisher), "--scrapers-root", str(pipeline.REPO_ROOT),
    ])


def test_rotation_failure_does_not_change_operational_pipeline_result(tmp_path, capsys):
    builder = tmp_path / "scripts/build_inteligencia_pc.py"
    publisher = builder.with_name("update_ct_rotation.py")
    publisher.parent.mkdir()
    publisher.touch()
    with mock.patch.object(pipeline, "pc_analytics_builder", return_value=builder), \
         mock.patch.object(pipeline, "_run", side_effect=OSError("Sheets temporalmente no disponible")):
        pipeline.refresh_rotation_from_existing_data()
    assert "Publicación pendiente" in capsys.readouterr().out


def test_missing_rotation_publisher_does_not_execute_any_process(tmp_path):
    with mock.patch.object(pipeline, "pc_analytics_builder", return_value=tmp_path / "scripts/build_inteligencia_pc.py"), \
         mock.patch.object(pipeline, "_run") as runner:
        pipeline.refresh_rotation_from_existing_data()
    runner.assert_not_called()


def test_rotation_is_published_after_normal_medical_analytics_only():
    from datetime import datetime

    for medical_result in (0, 1):
        metadata = {"last_run_started_at": datetime.now().isoformat(), "last_local_update_status": "success", "last_postgres_sync_status": "success"}
        with mock.patch.dict(os.environ, {"SUPABASE_DB_URL": "test-only"}), \
             mock.patch.object(pipeline, "validate_pc_builder"), \
             mock.patch.object(pipeline, "_run", side_effect=[0, medical_result, 0]) as runner, \
             mock.patch.object(pipeline, "read_metadata", return_value=metadata), \
             mock.patch.object(pipeline, "read_analytics_metadata", return_value={}), \
             mock.patch.object(pipeline, "read_pc_analytics_metadata", return_value={}), \
             mock.patch.object(pipeline, "emit_component"), \
             mock.patch.object(pipeline, "refresh_rotation_from_existing_data") as refresh:
            assert pipeline.run_pipeline("incremental") == medical_result
        assert runner.call_count == 3
        assert refresh.call_count == (1 if medical_result == 0 else 0)
