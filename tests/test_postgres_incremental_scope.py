from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


spec = importlib.util.spec_from_file_location(
    "updater_incremental_scope", Path(__file__).resolve().parents[1] / "db/db_api_updater.py"
)
updater = importlib.util.module_from_spec(spec)
spec.loader.exec_module(updater)


class RemoteLinks:
    def __init__(self, existing=()):
        self.existing = set(existing)
        self.calls = []
        self.count = 0

    def execute(self, query, params):
        # Link values must remain parameters, including quotes in official URLs.
        assert query == "SELECT COUNT(*) FROM actos_publicos WHERE enlace = ANY(%s)"
        assert len(params) == 1
        self.calls.append(params[0])
        self.count = len(set(params[0]) & self.existing)

    def fetchone(self):
        return (self.count,)


def test_daily_growth_and_updates_do_not_force_full_publication():
    remote = RemoteLinks(["old", "updated"])
    rows = [{"enlace": link} for link in ["updated", "new-a", "new-b"]]
    missing = updater.count_new_postgres_links(remote, rows)
    assert missing == 2
    assert not updater.postgres_reconciliation_required(4, 2, expected_new_rows=missing)


@pytest.mark.parametrize("local,remote,new_rows,required", [
    (251852, 250845, 3, True),  # Failed earlier run left additional unpublished rows.
    (250846, 250845, 1, False), # Normal daily insertion.
    (250845, 250845, 0, False), # Only updates to existing acts.
    (250844, 250845, 0, True),  # Remote contains extra rows.
    (250845, 250845, 1, True),  # Equal counts alone hide a different set of links.
    (250847, 250845, 0, True),  # No current-run rows explain the difference.
])
def test_unexplained_differences_still_require_reconciliation(local, remote, new_rows, required):
    assert updater.postgres_reconciliation_required(
        local, remote, expected_new_rows=new_rows
    ) is required


def test_explicit_weekly_rebuild_always_reconciles():
    assert updater.postgres_reconciliation_required(
        201, 200, requested_full=True, expected_new_rows=1
    )


def test_link_count_is_deduplicated_parameterized_and_bounded():
    links = [f"https://example.test/act/{i}" for i in range(1003)] + ["quote'link"]
    rows = [{"enlace": link} for link in links + links[:10]] + [{"enlace": None}, {"enlace": ""}]
    remote = RemoteLinks(links[:700])
    assert updater.count_new_postgres_links(remote, rows) == 304
    assert [len(batch) for batch in remote.calls] == [1000, 4]


def test_empty_run_does_not_issue_link_queries():
    remote = RemoteLinks()
    assert updater.count_new_postgres_links(remote, []) == 0
    assert not remote.calls
