from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path


ORCHESTRATOR_DIR = Path(__file__).resolve().parents[1] / "orquestador"
if str(ORCHESTRATOR_DIR) not in sys.path:
    sys.path.insert(0, str(ORCHESTRATOR_DIR))

import main as orchestrator  # noqa: E402


def _job(*, name: str = "clv", days=None, times=None):
    return orchestrator.JobConfig(
        name=name,
        python="python.exe",
        script="worker.py",
        days_of_week=days or ["wed"],
        times=times or ["08:00", "09:00", "10:00"],
    )


def test_operational_jobs_have_a_full_day_startup_recovery_window():
    expected = {
        "clv",
        "clrir",
        "rir1",
        "ctni",
        "otras_fuentes",
        "finance_recurring_autopay",
    }
    for job_name in expected:
        assert orchestrator.resolve_startup_catchup_seconds(job_name) >= 86400


def test_time_sensitive_scrapers_have_priority_over_database_recovery():
    scraper_priority = orchestrator.resolve_cron_job_priority("clv")
    assert scraper_priority == orchestrator.resolve_cron_job_priority("clrir")
    assert scraper_priority == orchestrator.resolve_cron_job_priority("rir1")
    assert scraper_priority < orchestrator.resolve_cron_job_priority("database_daily")
    assert scraper_priority < orchestrator.resolve_cron_job_priority("sunday_db_minsa")


def test_latest_missed_slot_uses_only_the_most_recent_time_today():
    slot = orchestrator._latest_missed_cron_slot(
        _job(),
        now_ref=datetime(2026, 9, 9, 10, 30),  # miercoles
        catchup_seconds=86400,
    )

    assert slot == datetime(2026, 9, 9, 10, 0)


def test_latest_missed_slot_can_recover_the_previous_day():
    slot = orchestrator._latest_missed_cron_slot(
        _job(days=["tue"], times=["23:05"]),
        now_ref=datetime(2026, 9, 9, 0, 15),  # miercoles
        catchup_seconds=86400,
    )

    assert slot == datetime(2026, 9, 8, 23, 5)


def test_catchup_enqueues_one_run_even_when_several_slots_were_missed(monkeypatch):
    enqueued = []
    marked = []
    monkeypatch.setattr(orchestrator, "resolve_startup_catchup_seconds", lambda _name: 86400)
    monkeypatch.setattr(orchestrator, "_job_already_ran_for_slot", lambda *_args: False)
    monkeypatch.setattr(orchestrator, "_cron_slot_already_caught_up", lambda *_args: False)
    monkeypatch.setattr(
        orchestrator,
        "_mark_cron_slot_caught_up",
        lambda name, slot: marked.append((name, slot)),
    )

    orchestrator._maybe_enqueue_latest_cron_catchup(
        _job(),
        enqueue_func=lambda job, source: enqueued.append((job.name, source)),
        now_ref=datetime(2026, 9, 9, 10, 30),
    )

    assert enqueued == [("clv", "cron")]
    assert marked == [("clv", datetime(2026, 9, 9, 10, 0))]


def test_catchup_does_not_repeat_a_slot_that_already_ran(monkeypatch):
    enqueued = []
    monkeypatch.setattr(orchestrator, "resolve_startup_catchup_seconds", lambda _name: 86400)
    monkeypatch.setattr(orchestrator, "_job_already_ran_for_slot", lambda *_args: True)

    orchestrator._maybe_enqueue_latest_cron_catchup(
        _job(),
        enqueue_func=lambda job, source: enqueued.append((job.name, source)),
        now_ref=datetime(2026, 9, 9, 10, 30),
    )

    assert enqueued == []
