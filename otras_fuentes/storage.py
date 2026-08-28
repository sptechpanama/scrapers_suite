from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .classifier import should_alert
from .models import Opportunity, SourceFetchResult, stable_hash, utc_now_iso


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS external_sources (
    source TEXT PRIMARY KEY, display_name TEXT NOT NULL DEFAULT '', baseline_completed INTEGER NOT NULL DEFAULT 0,
    last_success_at TEXT, last_error_at TEXT, last_error TEXT, last_count INTEGER NOT NULL DEFAULT 0,
    last_run_id TEXT, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS external_monitor_runs (
    run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT, status TEXT NOT NULL,
    source_count INTEGER NOT NULL DEFAULT 0, success_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0, total_records INTEGER NOT NULL DEFAULT 0,
    new_records INTEGER NOT NULL DEFAULT 0, changed_records INTEGER NOT NULL DEFAULT 0,
    event_count INTEGER NOT NULL DEFAULT 0, postgres_synced INTEGER NOT NULL DEFAULT 0,
    error_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS external_source_runs (
    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, source TEXT NOT NULL, status TEXT NOT NULL,
    started_at TEXT NOT NULL, finished_at TEXT NOT NULL, record_count INTEGER NOT NULL DEFAULT 0,
    new_count INTEGER NOT NULL DEFAULT 0, changed_count INTEGER NOT NULL DEFAULT 0,
    event_count INTEGER NOT NULL DEFAULT 0, coverage TEXT, response_ms INTEGER NOT NULL DEFAULT 0, error TEXT
);
CREATE INDEX IF NOT EXISTS idx_external_source_runs_source ON external_source_runs(source, finished_at DESC);
CREATE TABLE IF NOT EXISTS external_opportunities (
    id TEXT PRIMARY KEY, source TEXT NOT NULL, external_id TEXT NOT NULL, source_group TEXT,
    source_type TEXT, title TEXT NOT NULL, description TEXT, buyer TEXT, country TEXT, province TEXT,
    publication_date TEXT, deadline TEXT, status TEXT, procurement_method TEXT,
    estimated_value REAL, currency TEXT, sector TEXT, registration_required TEXT,
    submission_channel TEXT, eligibility TEXT, source_url TEXT, canonical_url TEXT,
    cross_source_key TEXT, documents_json TEXT, raw_payload_json TEXT, matched_company TEXT,
    matched_keywords_json TEXT, matched_fields_json TEXT, fit_score REAL NOT NULL DEFAULT 0,
    priority TEXT, parser_version TEXT, content_hash TEXT NOT NULL, is_active INTEGER NOT NULL DEFAULT 1,
    first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, last_changed_at TEXT NOT NULL,
    first_run_id TEXT NOT NULL, last_run_id TEXT NOT NULL,
    UNIQUE(source, external_id)
);
CREATE INDEX IF NOT EXISTS idx_external_opportunities_dates ON external_opportunities(publication_date, deadline);
CREATE INDEX IF NOT EXISTS idx_external_opportunities_source ON external_opportunities(source, last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_external_opportunities_cross ON external_opportunities(cross_source_key);
CREATE TABLE IF NOT EXISTS external_opportunity_documents (
    id TEXT PRIMARY KEY, opportunity_id TEXT NOT NULL, title TEXT, url TEXT NOT NULL,
    document_type TEXT, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
    UNIQUE(opportunity_id, url)
);
CREATE TABLE IF NOT EXISTS external_opportunity_versions (
    id TEXT PRIMARY KEY, opportunity_id TEXT NOT NULL, content_hash TEXT NOT NULL,
    captured_at TEXT NOT NULL, run_id TEXT NOT NULL, snapshot_json TEXT NOT NULL,
    UNIQUE(opportunity_id, content_hash)
);
CREATE TABLE IF NOT EXISTS external_alert_events (
    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, opportunity_id TEXT NOT NULL, source TEXT NOT NULL,
    event_type TEXT NOT NULL, title TEXT NOT NULL, matched_company TEXT, priority TEXT,
    fit_score REAL NOT NULL DEFAULT 0, source_url TEXT, deadline TEXT, created_at TEXT NOT NULL,
    notified_at TEXT, UNIQUE(opportunity_id, event_type, id)
);
CREATE INDEX IF NOT EXISTS idx_external_alert_events_run ON external_alert_events(run_id, created_at);
"""


POSTGRES_SCHEMA = SQLITE_SCHEMA.replace("INTEGER NOT NULL DEFAULT 0", "INTEGER NOT NULL DEFAULT 0")


SOURCE_NAMES = {
    "acp": "Autoridad del Canal de Panamá",
    "ensa": "ENSA",
    "idaan": "IDAAN",
    "ena": "ENA Corredores",
    "ungm": "UN Global Marketplace",
    "cruz_roja": "Cruz Roja Panameña",
    "ciudad_saber": "Ciudad del Saber",
}


@dataclass(slots=True)
class IngestStats:
    source: str
    records: int = 0
    new: int = 0
    changed: int = 0
    events: int = 0
    baseline_created: bool = False


class OpportunityStore:
    def __init__(self, connection: Any, dialect: str) -> None:
        self.connection = connection
        self.dialect = dialect
        self.placeholder = "?" if dialect == "sqlite" else "%s"

    @classmethod
    def sqlite(cls, path: str | Path) -> "OpportunityStore":
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(target, timeout=60)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=60000")
        store = cls(connection, "sqlite")
        store.ensure_schema()
        return store

    @classmethod
    def postgres(cls, dsn: str) -> "OpportunityStore":
        import psycopg2

        connection = psycopg2.connect(dsn, connect_timeout=20, application_name="otras_fuentes")
        store = cls(connection, "postgres")
        store.ensure_schema()
        return store

    def close(self) -> None:
        self.connection.close()

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        cursor = self.connection.cursor()
        try:
            yield cursor
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def ensure_schema(self) -> None:
        statements = [part.strip() for part in SQLITE_SCHEMA.split(";") if part.strip()]
        with self.transaction() as cursor:
            for statement in statements:
                cursor.execute(statement)

    def _fetchone(self, cursor: Any, query: str, params: tuple[Any, ...]) -> Any:
        cursor.execute(query, params)
        return cursor.fetchone()

    @staticmethod
    def _row_value(row: Any, index: int, key: str) -> Any:
        if row is None:
            return None
        try:
            return row[key]
        except (TypeError, KeyError, IndexError):
            return row[index]

    def begin_run(self, run_id: str, started_at: str, source_count: int) -> None:
        p = self.placeholder
        with self.transaction() as cursor:
            cursor.execute(
                f"INSERT INTO external_monitor_runs (run_id, started_at, status, source_count) VALUES ({p},{p},{p},{p}) "
                + ("ON CONFLICT(run_id) DO NOTHING"),
                (run_id, started_at, "running", source_count),
            )

    def finish_run(
        self,
        run_id: str,
        *,
        finished_at: str,
        status: str,
        success_count: int,
        error_count: int,
        total_records: int,
        new_records: int,
        changed_records: int,
        event_count: int,
        postgres_synced: bool,
        errors: dict[str, str],
    ) -> None:
        p = self.placeholder
        with self.transaction() as cursor:
            cursor.execute(
                f"UPDATE external_monitor_runs SET finished_at={p},status={p},success_count={p},error_count={p},"
                f"total_records={p},new_records={p},changed_records={p},event_count={p},postgres_synced={p},error_json={p} WHERE run_id={p}",
                (
                    finished_at, status, success_count, error_count, total_records, new_records,
                    changed_records, event_count, int(postgres_synced), json.dumps(errors, ensure_ascii=False), run_id,
                ),
            )

    def ingest_source(
        self,
        run_id: str,
        started_at: str,
        finished_at: str,
        result: SourceFetchResult,
        *,
        emit_events: bool = True,
    ) -> IngestStats:
        stats = IngestStats(source=result.source, records=len(result.opportunities))
        p = self.placeholder
        source_run_id = stable_hash(run_id, result.source, length=32)
        now = finished_at
        with self.transaction() as cursor:
            row = self._fetchone(
                cursor,
                f"SELECT baseline_completed FROM external_sources WHERE source={p}",
                (result.source,),
            )
            baseline_completed = bool(self._row_value(row, 0, "baseline_completed"))

            if result.status != "success":
                cursor.execute(
                    f"INSERT INTO external_sources (source,display_name,baseline_completed,last_error_at,last_error,last_run_id,updated_at) "
                    f"VALUES ({p},{p},{p},{p},{p},{p},{p}) ON CONFLICT(source) DO UPDATE SET "
                    f"last_error_at=excluded.last_error_at,last_error=excluded.last_error,last_run_id=excluded.last_run_id,updated_at=excluded.updated_at",
                    (result.source, SOURCE_NAMES.get(result.source, result.source), int(baseline_completed), now, result.error, run_id, now),
                )
                cursor.execute(
                    f"INSERT INTO external_source_runs (id,run_id,source,status,started_at,finished_at,record_count,new_count,changed_count,event_count,coverage,response_ms,error) "
                    f"VALUES ({','.join([p] * 13)}) ON CONFLICT(id) DO NOTHING",
                    (source_run_id, run_id, result.source, result.status, started_at, finished_at, 0, 0, 0, 0, result.coverage, result.response_ms, result.error),
                )
                return stats

            for opportunity in result.opportunities:
                payload = opportunity.as_storage_dict()
                existing = self._fetchone(
                    cursor,
                    f"SELECT content_hash,first_seen_at,first_run_id,last_changed_at FROM external_opportunities WHERE id={p}",
                    (payload["id"],),
                )
                previous_hash = self._row_value(existing, 0, "content_hash")
                is_new = existing is None
                is_changed = bool(existing is not None and previous_hash != payload["content_hash"])
                if is_new:
                    stats.new += 1
                elif is_changed:
                    stats.changed += 1
                first_seen = self._row_value(existing, 1, "first_seen_at") or now
                first_run = self._row_value(existing, 2, "first_run_id") or run_id
                last_changed = self._row_value(existing, 3, "last_changed_at") or first_seen
                columns = [
                    "id", "source", "external_id", "source_group", "source_type", "title", "description",
                    "buyer", "country", "province", "publication_date", "deadline", "status",
                    "procurement_method", "estimated_value", "currency", "sector", "registration_required",
                    "submission_channel", "eligibility", "source_url", "canonical_url", "cross_source_key",
                    "documents_json", "raw_payload_json", "matched_company", "matched_keywords_json",
                    "matched_fields_json", "fit_score", "priority", "parser_version", "content_hash", "is_active",
                    "first_seen_at", "last_seen_at", "last_changed_at", "first_run_id", "last_run_id",
                ]
                values = [payload.get(column) for column in columns[:33]] + [
                    first_seen, now, now if (is_new or is_changed) else last_changed, first_run, run_id
                ]
                updates = ",".join(
                    f"{column}=excluded.{column}" for column in columns[1:] if column not in {"first_seen_at", "first_run_id"}
                )
                cursor.execute(
                    f"INSERT INTO external_opportunities ({','.join(columns)}) VALUES ({','.join([p] * len(columns))}) "
                    f"ON CONFLICT(id) DO UPDATE SET {updates}",
                    tuple(values),
                )

                snapshot_id = stable_hash(payload["id"], payload["content_hash"], length=32)
                cursor.execute(
                    f"INSERT INTO external_opportunity_versions (id,opportunity_id,content_hash,captured_at,run_id,snapshot_json) "
                    f"VALUES ({','.join([p] * 6)}) ON CONFLICT(id) DO NOTHING",
                    (snapshot_id, payload["id"], payload["content_hash"], now, run_id, json.dumps(payload, ensure_ascii=False, default=str)),
                )
                for document in opportunity.documents:
                    doc_id = stable_hash(payload["id"], document.url, length=32)
                    cursor.execute(
                        f"INSERT INTO external_opportunity_documents (id,opportunity_id,title,url,document_type,first_seen_at,last_seen_at) "
                        f"VALUES ({','.join([p] * 7)}) ON CONFLICT(id) DO UPDATE SET title=excluded.title,document_type=excluded.document_type,last_seen_at=excluded.last_seen_at",
                        (doc_id, payload["id"], document.title, document.url, document.document_type, now, now),
                    )

                if emit_events and baseline_completed and (is_new or is_changed) and should_alert(opportunity):
                    event_type = "new" if is_new else "updated"
                    event_id = stable_hash(payload["id"], event_type, payload["content_hash"], length=32)
                    cursor.execute(
                        f"INSERT INTO external_alert_events (id,run_id,opportunity_id,source,event_type,title,matched_company,priority,fit_score,source_url,deadline,created_at) "
                        f"VALUES ({','.join([p] * 12)}) ON CONFLICT(id) DO NOTHING",
                        (event_id, run_id, payload["id"], result.source, event_type, opportunity.title,
                         opportunity.matched_company, opportunity.priority, opportunity.fit_score,
                         opportunity.source_url, opportunity.deadline, now),
                    )
                    if cursor.rowcount:
                        stats.events += 1

            cursor.execute(
                f"INSERT INTO external_sources (source,display_name,baseline_completed,last_success_at,last_error,last_count,last_run_id,updated_at) "
                f"VALUES ({p},{p},1,{p},'',{p},{p},{p}) ON CONFLICT(source) DO UPDATE SET "
                f"display_name=excluded.display_name,baseline_completed=1,last_success_at=excluded.last_success_at,last_error='',"
                f"last_count=excluded.last_count,last_run_id=excluded.last_run_id,updated_at=excluded.updated_at",
                (result.source, SOURCE_NAMES.get(result.source, result.source), now, stats.records, run_id, now),
            )
            cursor.execute(
                f"INSERT INTO external_source_runs (id,run_id,source,status,started_at,finished_at,record_count,new_count,changed_count,event_count,coverage,response_ms,error) "
                f"VALUES ({','.join([p] * 13)}) ON CONFLICT(id) DO NOTHING",
                (source_run_id, run_id, result.source, result.status, started_at, finished_at, stats.records,
                 stats.new, stats.changed, stats.events, result.coverage, result.response_ms, result.error),
            )
            stats.baseline_created = not baseline_completed
        return stats

    def events_for_run(self, run_id: str) -> list[dict[str, Any]]:
        p = self.placeholder
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                f"SELECT id,source,event_type,title,matched_company,priority,fit_score,source_url,deadline,created_at "
                f"FROM external_alert_events WHERE run_id={p} ORDER BY fit_score DESC,created_at",
                (run_id,),
            )
            columns = [item[0] for item in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()


def default_sqlite_path() -> Path:
    configured = os.environ.get("OTRAS_FUENTES_DB_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[1] / "data" / "otras_fuentes" / "otras_fuentes.db"


def postgres_dsn() -> str:
    return (os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL") or "").strip()
