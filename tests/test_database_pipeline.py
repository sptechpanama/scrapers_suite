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
