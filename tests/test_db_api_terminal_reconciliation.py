from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parents[1] / "db" / "db_api_updater.py"
SPEC = importlib.util.spec_from_file_location(
    "db_api_updater_terminal_reconciliation_test",
    MODULE_PATH,
)
assert SPEC and SPEC.loader
updater = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(updater)


def _record(flow: int, state: str) -> dict[str, object]:
    return {
        "idProcesosContratacionFlujos": flow,
        "idTipoProceso": 6,
        "numProceso": f"2026-0-00-{flow}",
        "prefijo": "CM",
        "_target_state": state,
    }


def test_late_award_lookback_extends_incremental_range() -> None:
    args = updater.build_parser().parse_args(
        ["--to-date", "2026-08-26", "--terminal-lookback-days", "180"]
    )
    with mock.patch.object(
        updater,
        "metadata_get",
        return_value="2026-08-27",
    ):
        start, end = updater.determine_range(args)

    assert start == date(2026, 2, 27)
    assert end == date(2026, 8, 26)


def test_explicit_historical_start_is_not_overridden() -> None:
    args = updater.build_parser().parse_args(
        [
            "--from-date",
            "2024-01-01",
            "--to-date",
            "2026-08-26",
            "--terminal-lookback-days",
            "180",
        ]
    )
    start, end = updater.determine_range(args)
    assert start == date(2024, 1, 1)
    assert end == date(2026, 8, 26)


def test_known_unchanged_act_is_skipped_but_state_change_is_selected(tmp_path) -> None:
    database = tmp_path / "panamacompra.db"
    with mock.patch.object(updater, "DB_PATH", database):
        updater.init_db()
        adjudicated = _record(101, "Adjudicado")
        changed = _record(202, "Adjudicado")
        new_record = _record(303, "Adjudicado")
        with updater.connect_db() as connection:
            connection.executemany(
                "INSERT INTO actos_publicos(enlace,estado) VALUES(?,?)",
                [
                    (updater.process_link(adjudicated), "Adjudicado"),
                    (updater.process_link(changed), "Desierto"),
                ],
            )

        selected, known = updater.filter_new_records(
            [adjudicated, changed, new_record]
        )

    assert [row["idProcesosContratacionFlujos"] for row in selected] == [202, 303]
    assert known == 1


def test_failed_detail_is_retried_even_when_state_is_unchanged(tmp_path) -> None:
    database = tmp_path / "panamacompra.db"
    record = _record(404, "Adjudicado")
    with mock.patch.object(updater, "DB_PATH", database):
        updater.init_db()
        with updater.connect_db() as connection:
            connection.execute(
                "INSERT INTO actos_publicos(enlace,estado) VALUES(?,?)",
                (updater.process_link(record), "Adjudicado"),
            )
            connection.execute(
                """INSERT INTO db_failed_processes
                   (flow_id,process_json,last_error,attempts,updated_at)
                   VALUES(?,?,?,?,?)""",
                (404, "{}", "temporal", 1, "2026-08-26 12:00:00"),
            )

        selected, known = updater.filter_new_records([record])

    assert selected == [record]
    assert known == 0
