# -*- coding: utf-8 -*-
"""Recupera evidencia oficial de precios por renglón para fichas RIR sin requisitos.

El proceso es incremental y reanudable: solo marca una fila con la versión vigente
cuando Panamá Compra respondió sin errores. No modifica clasificaciones, montos
globales ni fichas detectadas existentes.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import concurrent.futures
import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DB_MODULE_DIR = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(DB_MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(DB_MODULE_DIR))

import db_api_updater as updater  # noqa: E402


DEFAULT_SOURCE_DB = REPO_ROOT / "data" / "db" / "panamacompra.db"
DEFAULT_ANALYTICS_DB = REPO_ROOT / "data" / "db" / "inteligencia_proveedores.db"


@dataclass(frozen=True)
class TargetAct:
    enlace: str
    flow_id: int
    tipo_proceso: int


@dataclass(frozen=True)
class FetchResult:
    target: TargetAct
    payload_json: str
    status: str
    items_json: str = ""
    error: str = ""


def log(tag: str, message: str) -> None:
    stamp = datetime.now(updater.PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} | {tag:<10} | {message}", flush=True)


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return bool(
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
    )


def eligible_links(analytics_db: Path) -> list[str]:
    """Devuelve los actos asociados a las fichas clasificadas NO CT / NO RS."""

    if not analytics_db.exists():
        raise FileNotFoundError(f"No existe la capa analítica: {analytics_db}")
    connection = sqlite3.connect(
        f"file:{analytics_db.as_posix()}?mode=ro", uri=True, timeout=60
    )
    try:
        required = {"intel_actos_fichas", "intel_ficha_metadata"}
        missing = sorted(table for table in required if not _table_exists(connection, table))
        if missing:
            raise RuntimeError(
                "La capa analítica no contiene las tablas requeridas: " + ", ".join(missing)
            )
        return [
            str(row[0]).strip()
            for row in connection.execute(
                """
                SELECT DISTINCT iaf.enlace
                FROM intel_actos_fichas iaf
                INNER JOIN intel_ficha_metadata meta ON meta.ficha = iaf.ficha
                WHERE lower(trim(COALESCE(meta.tiene_ct, ''))) = 'no'
                  AND lower(trim(COALESCE(meta.registro_sanitario, ''))) = 'no'
                  AND trim(COALESCE(iaf.enlace, '')) <> ''
                ORDER BY iaf.enlace
                """
            )
        ]
    finally:
        connection.close()


def ensure_source_columns(connection: sqlite3.Connection) -> None:
    columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(actos_publicos)")}
    for column, sql_type in (
        ("items_json", "TEXT"),
        ("ofertas_items_json", "TEXT"),
        ("ofertas_items_version", "TEXT"),
        ("ofertas_items_estado", "TEXT"),
    ):
        if column not in columns:
            connection.execute(
                f'ALTER TABLE actos_publicos ADD COLUMN "{column}" {sql_type}'
            )
    connection.commit()


def source_ids_from_link(enlace: str) -> tuple[int, int] | None:
    """Recupera los IDs oficiales del token reversible usado por PanamaCompra."""

    try:
        token = str(enlace or "").split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
        encoded = token[::-1]
        encoded += "=" * ((4 - len(encoded) % 4) % 4)
        payload = json.loads(base64.b64decode(encoded).decode("utf-8"))
        flow_id = int(payload["i"])
        tipo_proceso = int(payload["tp"])
        if flow_id <= 0 or tipo_proceso <= 0:
            return None
        return flow_id, tipo_proceso
    except (
        binascii.Error,
        KeyError,
        TypeError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        return None


def target_acts(
    source_db: Path,
    links: Sequence[str],
    *,
    refresh: bool = False,
    limit: int = 0,
) -> tuple[list[TargetAct], int]:
    if not source_db.exists():
        raise FileNotFoundError(f"No existe la base operacional: {source_db}")
    connection = sqlite3.connect(source_db, timeout=120)
    try:
        connection.execute("PRAGMA busy_timeout=120000")
        ensure_source_columns(connection)
        records: list[TargetAct] = []
        recovered_ids: list[tuple[str, str, str]] = []
        skipped = 0
        for offset in range(0, len(links), 700):
            chunk = list(links[offset : offset + 700])
            if not chunk:
                continue
            placeholders = ",".join("?" for _ in chunk)
            query = f"""
                SELECT enlace, source_flow_id, source_tipo_proceso,
                       COALESCE(ofertas_items_version, ''),
                       COALESCE(ofertas_items_estado, '')
                FROM actos_publicos
                WHERE enlace IN ({placeholders})
            """
            for row in connection.execute(query, chunk):
                enlace = str(row[0] or "").strip()
                try:
                    flow_id = int(float(str(row[1] or "").strip()))
                    tipo_proceso = int(float(str(row[2] or "").strip()))
                except (TypeError, ValueError):
                    recovered = source_ids_from_link(enlace)
                    if recovered is None:
                        skipped += 1
                        continue
                    flow_id, tipo_proceso = recovered
                    recovered_ids.append((str(flow_id), str(tipo_proceso), enlace))
                current = str(row[3] or "").strip()
                status = str(row[4] or "").strip()
                if (
                    not refresh
                    and current == updater.RESULT_ENRICHMENT_VERSION
                    and status not in {"parcial_actas", "error_temporal"}
                ):
                    skipped += 1
                    continue
                records.append(TargetAct(enlace, flow_id, tipo_proceso))
        if recovered_ids:
            connection.executemany(
                """UPDATE actos_publicos
                      SET source_flow_id=?, source_tipo_proceso=?
                    WHERE enlace=?""",
                recovered_ids,
            )
            connection.commit()
            log("RECOVER", f"IDs oficiales recuperados desde URL: {len(recovered_ids):,}")
        records.sort(key=lambda item: (item.flow_id, item.enlace))
        if limit > 0:
            records = records[:limit]
        return records, skipped
    finally:
        connection.close()


def fetch_offer_evidence(target: TargetAct) -> FetchResult:
    try:
        detail_url = updater.DETAIL_ENDPOINT.format(
            tipo=target.tipo_proceso, flujo=target.flow_id
        )
        detail = updater.request_json("GET", detail_url).get("result") or {}
        components = detail.get("pageComponentes") or []
        if not isinstance(components, list):
            raise RuntimeError("El detalle oficial no devolvió pageComponentes")
        requested_items: list[dict[str, Any]] = []
        for component in components:
            component_type = updater.normalize_text(component.get("tipo")).replace(" ", "")
            if component_type not in {"componentitems", "componentitemspliego"}:
                continue
            requested_items.extend(
                updater._item_detail_record(item)
                for item in updater._component_rows(component)
            )
        result_components, _routes, failures = updater._official_result_pages(
            components,
            tipo=target.tipo_proceso,
            flow=target.flow_id,
        )
        offers = updater._offer_item_details([*components, *result_components])
        if failures:
            return FetchResult(
                target=target,
                payload_json=json.dumps(offers, ensure_ascii=False, separators=(",", ":")),
                status="parcial_actas",
                items_json=json.dumps(
                    requested_items, ensure_ascii=False, separators=(",", ":")
                ),
                error=f"{failures} documento(s) oficial(es) con error temporal",
            )
        status = "completo" if offers else "completo_sin_ofertas_por_renglon"
        return FetchResult(
            target=target,
            payload_json=json.dumps(offers, ensure_ascii=False, separators=(",", ":")),
            status=status,
            items_json=json.dumps(
                requested_items, ensure_ascii=False, separators=(",", ":")
            ),
        )
    except Exception as exc:
        return FetchResult(
            target=target,
            payload_json="",
            status="error_temporal",
            error=str(exc),
        )


def persist_results(
    source_db: Path, results: Sequence[FetchResult]
) -> tuple[int, int, int]:
    completed = partial = failed = 0
    connection = sqlite3.connect(source_db, timeout=120)
    try:
        connection.execute("PRAGMA busy_timeout=120000")
        for result in results:
            if result.status == "error_temporal":
                connection.execute(
                    "UPDATE actos_publicos SET ofertas_items_estado=? WHERE enlace=?",
                    (f"error_temporal: {result.error[:500]}", result.target.enlace),
                )
                failed += 1
                continue
            version = (
                updater.RESULT_ENRICHMENT_VERSION
                if result.status != "parcial_actas"
                else ""
            )
            connection.execute(
                """
                UPDATE actos_publicos
                   SET ofertas_items_json=?, ofertas_items_version=?,
                       ofertas_items_estado=?,
                       items_json=CASE
                           WHEN ? NOT IN ('', '[]') THEN ?
                           ELSE items_json
                       END
                 WHERE enlace=?
                """,
                (
                    result.payload_json,
                    version,
                    result.status,
                    result.items_json,
                    result.items_json,
                    result.target.enlace,
                ),
            )
            if result.status == "parcial_actas":
                partial += 1
            else:
                completed += 1
        connection.commit()
    finally:
        connection.close()
    return completed, partial, failed


def verify(source_db: Path, eligible: Sequence[str]) -> dict[str, int]:
    connection = sqlite3.connect(
        f"file:{source_db.as_posix()}?mode=ro", uri=True, timeout=120
    )
    try:
        totals = {
            "eligible_links": len(eligible),
            "current_version": 0,
            "with_line_offers": 0,
            "partial": 0,
            "failed": 0,
            "invalid_json": 0,
        }
        for offset in range(0, len(eligible), 700):
            chunk = list(eligible[offset : offset + 700])
            if not chunk:
                continue
            placeholders = ",".join("?" for _ in chunk)
            for version, status, raw_json in connection.execute(
                f"""
                SELECT COALESCE(ofertas_items_version, ''),
                       COALESCE(ofertas_items_estado, ''),
                       COALESCE(ofertas_items_json, '')
                FROM actos_publicos WHERE enlace IN ({placeholders})
                """,
                chunk,
            ):
                if str(version) == updater.RESULT_ENRICHMENT_VERSION:
                    totals["current_version"] += 1
                status_text = str(status or "")
                if status_text.startswith("parcial"):
                    totals["partial"] += 1
                if status_text.startswith("error"):
                    totals["failed"] += 1
                raw = str(raw_json or "")
                if raw:
                    try:
                        parsed = json.loads(raw)
                        if isinstance(parsed, list) and parsed:
                            totals["with_line_offers"] += 1
                    except json.JSONDecodeError:
                        totals["invalid_json"] += 1
        if totals["invalid_json"]:
            raise RuntimeError(
                f"Se encontraron {totals['invalid_json']} evidencias JSON inválidas"
            )
        return totals
    finally:
        connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Recupera precios por renglón para fichas RIR sin CT ni RS"
    )
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--analytics-db", type=Path, default=DEFAULT_ANALYTICS_DB)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--limit", type=int, default=0, help="Solo para pruebas")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Devuelve error si queda alguna consulta temporal pendiente",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    started = time.perf_counter()
    eligible = eligible_links(args.analytics_db)
    targets, skipped = target_acts(
        args.source_db,
        eligible,
        refresh=bool(args.refresh),
        limit=max(0, int(args.limit)),
    )
    log(
        "SCOPE",
        f"actos elegibles={len(eligible):,} | pendientes={len(targets):,} | ya completos/omitidos={skipped:,}",
    )
    totals = {"completed": 0, "partial": 0, "failed": 0}
    batch: list[FetchResult] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        future_map = {pool.submit(fetch_offer_evidence, target): target for target in targets}
        for index, future in enumerate(concurrent.futures.as_completed(future_map), start=1):
            result = future.result()
            batch.append(result)
            if len(batch) >= max(1, args.batch_size) or index == len(targets):
                completed, partial, failed = persist_results(args.source_db, batch)
                totals["completed"] += completed
                totals["partial"] += partial
                totals["failed"] += failed
                batch.clear()
            if index % 100 == 0 or index == len(targets):
                elapsed = time.perf_counter() - started
                log(
                    "PROGRESS",
                    f"{index:,}/{len(targets):,} | completos={totals['completed']:,} "
                    f"parciales={totals['partial']:,} fallos={totals['failed']:,} | {elapsed:.1f}s",
                )
    verification = verify(args.source_db, eligible)
    log("VERIFY", json.dumps(verification, ensure_ascii=False, sort_keys=True))
    print(
        "RESULT_JSON="
        + json.dumps(
            {**totals, **verification, "version": updater.RESULT_ENRICHMENT_VERSION},
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    if args.strict and (totals["failed"] or totals["partial"]):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
