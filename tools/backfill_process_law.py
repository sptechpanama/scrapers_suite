from __future__ import annotations

"""Completa la ley oficial y separa actos Ley 419 sin ficha detectada.

Por seguridad el modo predeterminado es simulacion. ``--apply`` habilita las
escrituras; cada movimiento usa copiar -> releer/verificar -> eliminar y deja
un respaldo JSON local antes de modificar Google Sheets.
"""

import argparse
import concurrent.futures
import json
import os
import random
import re
import sqlite3
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter


REPO_ROOT = Path(__file__).resolve().parents[1]
COMMON_DIR = REPO_ROOT / "common"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from common.process_law import (  # noqa: E402
    PROCESS_LAW_COLUMN,
    PROCESS_LAW_UNKNOWN,
    has_detected_ficha,
    is_law_419,
    process_law_from_components,
)
from tools.backfill_purchase_unit_details import (  # noqa: E402
    DEFAULT_SPREADSHEET_ID,
    NEW_API_ENDPOINT,
    API_TEMPLATE,
    build_sheets_service,
    col_letter,
    decode_route_payload,
    ensure_sheet_column_capacity,
    execute_with_backoff,
    load_sheet_grid_metadata,
    read_sheet,
)


MIGRATIONS = {
    "cl_abiertas": "cl_abiertas_419_sfd",
    "cl_prog_sin_ficha": "cl_prog_419_sfd",
    "ap_sin_ficha": "ap_419_sfd",
}
TARGET_SHEETS = (
    "cl_abiertas",
    "cl_abiertas_rir_sin_requisitos",
    "cl_abiertas_rir_con_ct",
    "cl_abiertas_419_sfd",
    "cl_abiertas_ct_rir",
    "cl_prog_sin_ficha",
    "cl_prog_sin_requisitos",
    "cl_prog_con_ct",
    "cl_prog_419_sfd",
    "cl_prog_ct_rir",
    "ap_sin_ficha",
    "ap_sin_requisitos",
    "ap_con_ct",
    "ap_419_sfd",
    "ap_ct_rir",
    "cl_prioritarios",
    "cl_descartes",
)
DEFAULT_CHECKPOINT = REPO_ROOT / "data" / "backfill" / "process_law.json"
DEFAULT_BACKUP_DIR = REPO_ROOT / "data" / "backfill" / "process_law_backups"
_thread_local = threading.local()


def log(tag: str, message: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {tag:<10} | {message}", flush=True)


def normalize_header(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def normalize_link(value: object) -> str:
    return str(value or "").strip().rstrip("/")


def row_value(row: list[Any], index: int | None) -> str:
    if index is None or index < 0 or index >= len(row):
        return ""
    return str(row[index] or "").strip()


def _session() -> requests.Session:
    session = getattr(_thread_local, "process_law_session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=0)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mozilla/5.0 process-law-backfill/1.0",
            }
        )
        _thread_local.process_law_session = session
    return session


def fetch_process_law(url: str, attempts: int = 5) -> str:
    flow, process_type = decode_route_payload(url)
    endpoint = API_TEMPLATE.format(tipo=process_type, flujo=flow)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = _session().get(endpoint, timeout=(10, 45))
            response.raise_for_status()
            payload = response.json()
            if int(payload.get("status") or 0) != 1:
                response = _session().post(
                    NEW_API_ENDPOINT,
                    json={
                        "idTipoProceso": process_type,
                        "idProcesosContratacionFlujos": flow,
                    },
                    timeout=(10, 45),
                )
                response.raise_for_status()
                payload = response.json()
            if int(payload.get("status") or 0) != 1:
                raise RuntimeError(
                    f"API status invalido: {payload.get('message') or payload.get('result')}"
                )
            result = payload.get("result") or {}
            return process_law_from_components(result.get("pageComponentes") or [])
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(min(15.0, (1.6**attempt) + random.random()))
    raise RuntimeError(f"{type(last_error).__name__}: {last_error}")


def load_checkpoint(path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("results"), dict):
                data.setdefault("errors", {})
                return data
        except Exception:
            pass
    return {"version": 1, "results": {}, "errors": {}}


def save_checkpoint(path: Path, checkpoint: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint["updated_at"] = datetime.now().isoformat(timespec="seconds")
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(checkpoint, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temporary.replace(path)


def capture_laws(
    links: list[str],
    checkpoint_path: Path,
    *,
    workers: int,
    retry_errors: bool,
) -> tuple[dict[str, str], dict[str, str]]:
    checkpoint = load_checkpoint(checkpoint_path)
    results: dict[str, str] = checkpoint["results"]
    errors: dict[str, str] = checkpoint["errors"]
    pending = [
        link
        for link in links
        if link not in results and (retry_errors or link not in errors)
    ]
    log(
        "CAPTURE",
        f"enlaces={len(links):,} | cache={len(results):,} | pendientes={len(pending):,}",
    )
    completed = 0
    lock = threading.Lock()

    def persist(link: str, law: str | None, error: str = "") -> None:
        nonlocal completed
        with lock:
            if law is not None:
                results[link] = law
                errors.pop(link, None)
            else:
                errors[link] = error
            completed += 1
            if completed % 25 == 0 or completed == len(pending):
                save_checkpoint(checkpoint_path, checkpoint)
                log(
                    "PROGRESS",
                    f"{completed:,}/{len(pending):,} | resueltos={len(results):,} | fallos={len(errors):,}",
                )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_map = {executor.submit(fetch_process_law, link): link for link in pending}
        for future in concurrent.futures.as_completed(future_map):
            link = future_map[future]
            try:
                persist(link, future.result())
            except Exception as exc:
                persist(link, None, f"{type(exc).__name__}: {exc}")
    save_checkpoint(checkpoint_path, checkpoint)
    return results, errors


def ensure_target_sheets(service, spreadsheet_id: str) -> dict[str, dict[str, int]]:
    metadata = load_sheet_grid_metadata(service, spreadsheet_id)
    missing = [sheet for sheet in MIGRATIONS.values() if sheet not in metadata]
    if missing:
        request = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": sheet,
                                "gridProperties": {"rowCount": 4000, "columnCount": 40},
                            }
                        }
                    }
                    for sheet in missing
                ]
            },
        )
        execute_with_backoff(request, label="crear hojas Ley 419")
        log("SHEETS", f"creadas: {', '.join(missing)}")
        metadata = load_sheet_grid_metadata(service, spreadsheet_id)
    return metadata


def extract_links(rows: list[list[str]]) -> list[str]:
    if not rows:
        return []
    indexes = {normalize_header(value): index for index, value in enumerate(rows[0])}
    link_index = indexes.get("enlace")
    if link_index is None:
        return []
    return [
        link
        for row in rows[1:]
        if (link := row_value(row, link_index))
    ]


def backup_sheets(
    service,
    spreadsheet_id: str,
    sheets: list[str],
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = output_dir / f"process_law_sheets_{stamp}.json"
    payload = {
        "spreadsheet_id": spreadsheet_id,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "sheets": {sheet: read_sheet(service, spreadsheet_id, sheet) for sheet in sheets},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    log("BACKUP", str(path))
    return path


def update_law_column(
    service,
    spreadsheet_id: str,
    sheet: str,
    rows: list[list[str]],
    results: dict[str, str],
    grid_metadata: dict[str, dict[str, int]],
    *,
    apply: bool,
) -> dict[str, int]:
    if not rows:
        return {"rows": 0, "resolved": 0, "changed": 0}
    header = [str(value or "").strip() for value in rows[0]]
    indexes = {normalize_header(value): index for index, value in enumerate(header)}
    link_index = indexes.get("enlace")
    if link_index is None:
        return {"rows": len(rows) - 1, "resolved": 0, "changed": 0}
    law_index = indexes.get(normalize_header(PROCESS_LAW_COLUMN))
    if law_index is None:
        law_index = len(header)
    values = [[PROCESS_LAW_COLUMN]]
    resolved = 0
    changed = 0
    for row in rows[1:]:
        link = row_value(row, link_index)
        old = row_value(row, indexes.get(normalize_header(PROCESS_LAW_COLUMN)))
        law = results.get(link)
        new = str(law or old or PROCESS_LAW_UNKNOWN)
        values.append([new])
        if law is not None:
            resolved += 1
        if new != old:
            changed += 1
    if apply and changed:
        ensure_sheet_column_capacity(
            service,
            spreadsheet_id,
            sheet,
            law_index + 1,
            grid_metadata,
        )
        column = col_letter(law_index + 1)
        request = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet}'!{column}1:{column}{len(values)}",
            valueInputOption="RAW",
            body={"values": values},
        )
        execute_with_backoff(request, label=f"ley {sheet}")
    return {"rows": len(rows) - 1, "resolved": resolved, "changed": changed}


def _header_indexes(header: list[Any]) -> dict[str, int]:
    return {normalize_header(value): index for index, value in enumerate(header)}


def migration_candidates(rows: list[list[str]]) -> list[tuple[int, str]]:
    """Devuelve (fila 1-based, enlace) solo para 419 confirmado y sin ficha."""

    if not rows:
        return []
    indexes = _header_indexes(rows[0])
    link_index = indexes.get("enlace")
    ficha_index = indexes.get("ficha_detectada")
    law_index = indexes.get(normalize_header(PROCESS_LAW_COLUMN))
    if link_index is None or law_index is None:
        return []
    selected: list[tuple[int, str]] = []
    for row_number, row in enumerate(rows[1:], start=2):
        law = row_value(row, law_index)
        ficha = row_value(row, ficha_index)
        link = row_value(row, link_index)
        if link and is_law_419(law) and not has_detected_ficha(ficha):
            selected.append((row_number, link))
    return selected


def _align_row(source_header: list[str], source_row: list[str], target_header: list[str]) -> list[str]:
    source_indexes = _header_indexes(source_header)
    return [row_value(source_row, source_indexes.get(normalize_header(column))) for column in target_header]


def _merged_target_header(
    source_header: list[str], target_header: list[str]
) -> list[str]:
    """Conserva el esquema destino y agrega cualquier columna nueva del origen."""

    merged = list(target_header)
    known = set(_header_indexes(merged))
    for column in source_header:
        normalized = normalize_header(column)
        if normalized and normalized not in known:
            merged.append(column)
            known.add(normalized)
    return merged


def _append_rows(service, spreadsheet_id: str, sheet: str, rows: list[list[str]]) -> None:
    if not rows:
        return
    request = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows},
    )
    execute_with_backoff(request, label=f"copiar a {sheet}")


def _delete_rows(service, spreadsheet_id: str, sheet_id: int, row_numbers: list[int]) -> None:
    requests = [
        {
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": row_number - 1,
                    "endIndex": row_number,
                }
            }
        }
        for row_number in sorted(set(row_numbers), reverse=True)
    ]
    for start in range(0, len(requests), 200):
        request = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests[start : start + 200]},
        )
        execute_with_backoff(request, label="eliminar filas verificadas")


def migrate_sheet(
    service,
    spreadsheet_id: str,
    source: str,
    target: str,
    grid_metadata: dict[str, dict[str, int]],
    *,
    apply: bool,
) -> dict[str, int]:
    source_rows = read_sheet(service, spreadsheet_id, source)
    candidates = migration_candidates(source_rows)
    if not source_rows or not candidates:
        return {"candidates": len(candidates), "copied": 0, "deleted": 0}

    source_header = [str(value or "").strip() for value in source_rows[0]]
    target_rows = read_sheet(service, spreadsheet_id, target)
    if target_rows:
        target_header = [str(value or "").strip() for value in target_rows[0]]
        merged_header = _merged_target_header(source_header, target_header)
        if merged_header != target_header:
            target_header = merged_header
            if apply:
                ensure_sheet_column_capacity(
                    service,
                    spreadsheet_id,
                    target,
                    len(target_header),
                    grid_metadata,
                )
                request = service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{target}'!A1:{col_letter(len(target_header))}1",
                    valueInputOption="RAW",
                    body={"values": [target_header]},
                )
                execute_with_backoff(request, label=f"ampliar encabezado {target}")
                target_rows[0] = target_header
    else:
        target_header = source_header
        if apply:
            ensure_sheet_column_capacity(
                service,
                spreadsheet_id,
                target,
                len(target_header),
                grid_metadata,
            )
            request = service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{target}'!A1",
                valueInputOption="RAW",
                body={"values": [target_header]},
            )
            execute_with_backoff(request, label=f"encabezado {target}")
            target_rows = [target_header]

    target_indexes = _header_indexes(target_header)
    target_link_index = target_indexes.get("enlace")
    if target_link_index is None:
        raise RuntimeError(f"{target}: encabezado sin columna enlace")
    existing = {
        normalize_link(row_value(row, target_link_index))
        for row in target_rows[1:]
        if row_value(row, target_link_index)
    }
    source_by_number = {
        row_number: source_rows[row_number - 1]
        for row_number, _link in candidates
    }
    rows_to_append: list[list[str]] = []
    for row_number, link in candidates:
        key = normalize_link(link)
        if key in existing:
            continue
        rows_to_append.append(
            _align_row(source_header, source_by_number[row_number], target_header)
        )
        existing.add(key)

    if not apply:
        return {
            "candidates": len(candidates),
            "copied": len(rows_to_append),
            "deleted": 0,
        }

    _append_rows(service, spreadsheet_id, target, rows_to_append)
    verified_rows = read_sheet(service, spreadsheet_id, target)
    verified_indexes = _header_indexes(verified_rows[0] if verified_rows else [])
    verified_link_index = verified_indexes.get("enlace")
    verified = {
        normalize_link(row_value(row, verified_link_index))
        for row in verified_rows[1:]
        if row_value(row, verified_link_index)
    }
    delete_numbers = [
        row_number
        for row_number, link in candidates
        if normalize_link(link) in verified
    ]
    if len(delete_numbers) != len(candidates):
        raise RuntimeError(
            f"{source}->{target}: verificacion incompleta; "
            f"{len(delete_numbers)}/{len(candidates)} filas confirmadas. No se elimina nada."
        )
    _delete_rows(
        service,
        spreadsheet_id,
        int(grid_metadata[source]["sheet_id"]),
        delete_numbers,
    )
    return {
        "candidates": len(candidates),
        "copied": len(rows_to_append),
        "deleted": len(delete_numbers),
    }


def update_local_database(results: dict[str, str], *, apply: bool) -> int:
    from db import db_api_updater

    updates = [
        (law, link)
        for link, law in results.items()
        if law and law != PROCESS_LAW_UNKNOWN
    ]
    if not apply:
        return len(updates)
    if not updates or not db_api_updater.DB_PATH.exists():
        return 0
    db_api_updater.init_db()
    with sqlite3.connect(db_api_updater.DB_PATH, timeout=60) as connection:
        connection.execute("PRAGMA busy_timeout=60000")
        before = connection.total_changes
        connection.executemany(
            "UPDATE actos_publicos SET ley_proceso=? WHERE enlace=?",
            updates,
        )
        changed = int(connection.total_changes - before)
        connection.commit()
    return changed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--spreadsheet-id",
        default=os.environ.get("PC_SHEETS_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
    )
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT)
    parser.add_argument("--backup-dir", type=Path, default=DEFAULT_BACKUP_DIR)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--max-links", type=int, default=0)
    parser.add_argument("--retry-errors", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--sync-postgres", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    service = build_sheets_service()
    metadata = ensure_target_sheets(service, args.spreadsheet_id) if args.apply else load_sheet_grid_metadata(service, args.spreadsheet_id)
    available = [sheet for sheet in TARGET_SHEETS if sheet in metadata]
    snapshots = {
        sheet: read_sheet(service, args.spreadsheet_id, sheet) for sheet in available
    }
    if args.apply:
        backup_sheets(service, args.spreadsheet_id, available, args.backup_dir)

    links = list(
        dict.fromkeys(
            link
            for sheet in available
            for link in extract_links(snapshots.get(sheet) or [])
        )
    )
    if args.max_links > 0:
        links = links[: args.max_links]
    results, errors = capture_laws(
        links,
        args.checkpoint,
        workers=args.workers,
        retry_errors=args.retry_errors,
    )

    for sheet in available:
        stats = update_law_column(
            service,
            args.spreadsheet_id,
            sheet,
            snapshots[sheet],
            results,
            metadata,
            apply=args.apply,
        )
        log(
            "WRITE" if args.apply else "DRYRUN",
            f"{sheet}: filas={stats['rows']:,} | resueltas={stats['resolved']:,} | cambios={stats['changed']:,}",
        )

    if args.apply:
        # Las decisiones de movimiento se calculan sobre la lectura posterior a
        # completar la columna, nunca sobre la instantanea anterior.
        for source, target in MIGRATIONS.items():
            stats = migrate_sheet(
                service,
                args.spreadsheet_id,
                source,
                target,
                metadata,
                apply=True,
            )
            log(
                "MIGRATE",
                f"{source}->{target}: candidatos={stats['candidates']:,} | "
                f"copiados={stats['copied']:,} | eliminados={stats['deleted']:,}",
            )
    else:
        # Proyecta la matriz con los resultados capturados sin tocar Sheets.
        for source, target in MIGRATIONS.items():
            rows = [list(row) for row in snapshots.get(source) or []]
            if rows:
                header = list(rows[0])
                indexes = _header_indexes(header)
                law_index = indexes.get(normalize_header(PROCESS_LAW_COLUMN))
                if law_index is None:
                    law_index = len(header)
                    header.append(PROCESS_LAW_COLUMN)
                    rows[0] = header
                link_index = _header_indexes(header).get("enlace")
                for row in rows[1:]:
                    while len(row) <= law_index:
                        row.append("")
                    link = row_value(row, link_index)
                    row[law_index] = results.get(link) or row[law_index] or PROCESS_LAW_UNKNOWN
            candidates = migration_candidates(rows)
            log("DRYRUN", f"{source}->{target}: movería {len(candidates):,} fila(s)")

    db_changes = update_local_database(results, apply=args.apply)
    log("DATABASE", f"filas locales con ley aplicable={db_changes:,}")
    if args.apply and args.sync_postgres:
        from db import db_api_updater

        synced = db_api_updater.sync_postgres(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), full=True
        )
        if not synced:
            raise RuntimeError("La sincronizacion completa con Supabase no se confirmó")

    resolved = sum(link in results for link in links)
    failures = sum(link in errors for link in links)
    law_419 = sum(is_law_419(results.get(link)) for link in links)
    log(
        "DONE",
        f"modo={'APPLY' if args.apply else 'DRY-RUN'} | enlaces={len(links):,} | "
        f"resueltos={resolved:,} | Ley419={law_419:,} | fallos={failures:,}",
    )
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
