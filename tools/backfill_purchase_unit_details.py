from __future__ import annotations

"""Completa contacto y unidad compradora en las hojas operativas de PanamaCompra.

La lectura usa la API publica V3 y el identificador contenido en cada enlace.
La escritura se limita a columnas: nunca agrega, elimina ni reordena actos.
"""

import argparse
import base64
import concurrent.futures
import json
import os
import random
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import requests
from requests.adapters import HTTPAdapter


REPO_ROOT = Path(__file__).resolve().parents[1]
COMMON_DIR = REPO_ROOT / "common"
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from purchase_unit_details import (  # noqa: E402
    CONTACT_EMAIL_COLUMN,
    CONTACT_NAME_COLUMN,
    CONTACT_PHONE_COLUMN,
    CONTACT_ROLE_COLUMN,
    DEPENDENCY_COLUMN,
    PROVINCE_COLUMN,
    PURCHASE_UNIT_COLUMN,
    PURCHASE_UNIT_DETAIL_COLUMNS,
    has_purchase_unit_details,
    parse_purchase_unit_components,
)


DEFAULT_SPREADSHEET_ID = "17hOfP-vMdJ4D7xym1cUp7vAcd8XJPErpY3V-9Ui2tCo"
DEFAULT_CHECKPOINT = REPO_ROOT / "data" / "backfill" / "purchase_unit_details.json"
API_TEMPLATE = (
    "https://apisv3.panamacompra.gob.pa/procesos-configuracion/"
    "pagina-componentes-publico/{tipo}/procesoVistaPliego/{flujo}"
)
NEW_API_ENDPOINT = (
    "https://apisv3.panamacompra.gob.pa/ps/documentos-proceso/"
    "pliego-general/publico/get-page"
)
TARGET_SHEETS = (
    "ap_sin_requisitos",
    "ap_con_ct",
    "ap_sin_ficha",
    "ap_ct_rir",
    "cl_prog_sin_ficha",
    "cl_prog_sin_requisitos",
    "cl_prog_con_ct",
    "cl_prog_ct_rir",
    "cl_abiertas",
    "cl_abiertas_rir_sin_requisitos",
    "cl_abiertas_rir_con_ct",
    "cl_abiertas_ct_rir",
    "cl_prioritarios",
    "cl_descartes",
)
WRITE_COLUMNS = PURCHASE_UNIT_DETAIL_COLUMNS
_thread_local = threading.local()


def log(tag: str, message: str) -> None:
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {tag:<10} | {message}", flush=True)


def normalize_header(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def col_letter(index_1b: int) -> str:
    result = ""
    value = int(index_1b)
    while value:
        value, remainder = divmod(value - 1, 26)
        result = chr(65 + remainder) + result
    return result


def decode_route_payload(url: str) -> tuple[int, int]:
    fragment = unquote(urlparse(str(url or "")).fragment or "")
    token = fragment.rstrip("/").split("/")[-1]
    if not token:
        raise ValueError("enlace sin token de ruta")
    encoded = token[::-1]
    encoded += "=" * ((4 - len(encoded) % 4) % 4)
    payload = json.loads(base64.b64decode(encoded).decode("utf-8"))
    flow = int(payload["i"])
    process_type = int(payload["tp"])
    if flow <= 0 or process_type <= 0:
        raise ValueError("token sin flujo/tipo valido")
    return flow, process_type


def _session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=0)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mozilla/5.0 purchase-unit-backfill/1.0",
            }
        )
        _thread_local.session = session
    return session


def fetch_details(url: str, attempts: int = 5) -> dict[str, str]:
    flow, process_type = decode_route_payload(url)
    endpoint = API_TEMPLATE.format(tipo=process_type, flujo=flow)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = _session().get(endpoint, timeout=(10, 45))
            response.raise_for_status()
            payload = response.json()
            if int(payload.get("status") or 0) != 1:
                # Algunos procesos recientes usan el nuevo documento de pliego.
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
                raise RuntimeError(f"API status invalido: {payload.get('message') or payload.get('result')}")
            result = payload.get("result") or {}
            details = parse_purchase_unit_components(result.get("pageComponentes") or [])
            if not has_purchase_unit_details(details):
                raise RuntimeError("detalle sin bloques de contacto/unidad")
            details["entidad"] = str(details.get("entidad") or "")
            return details
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(min(12.0, (1.4**attempt) + random.random()))
    raise RuntimeError(f"{type(last_error).__name__}: {last_error}")


def resolve_credentials() -> Path:
    for name in (
        "PC_SHEETS_SERVICE_ACCOUNT_FILE",
        "CLV_SERVICE_ACCOUNT_FILE",
        "CLRIR_SERVICE_ACCOUNT_FILE",
        "RIR1_SERVICE_ACCOUNT_FILE",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ):
        value = os.environ.get(name)
        if value and Path(value).expanduser().exists():
            return Path(value).expanduser()
    legacy = Path(r"C:\Users\rodri\cl\serious-app-417920-eed299fa06b5.json")
    if legacy.exists():
        return legacy
    candidate = REPO_ROOT / "credentials" / "service-account.json"
    if candidate.exists():
        return candidate
    raise FileNotFoundError("No se encontro la cuenta de servicio de Google Sheets")


def build_sheets_service():
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    credentials = Credentials.from_service_account_file(
        str(resolve_credentials()),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def read_sheet(service, spreadsheet_id: str, sheet: str) -> list[list[str]]:
    response = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=f"'{sheet}'!A1:ZZ")
        .execute()
    )
    return response.get("values") or []


def available_target_sheets(service, spreadsheet_id: str) -> list[str]:
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    available = {
        item.get("properties", {}).get("title")
        for item in metadata.get("sheets") or []
    }
    return [sheet for sheet in TARGET_SHEETS if sheet in available]


def extract_links(rows: list[list[str]]) -> list[str]:
    if not rows:
        return []
    header_map = {normalize_header(value): index for index, value in enumerate(rows[0])}
    link_index = header_map.get("enlace")
    if link_index is None:
        return []
    return [
        str(row[link_index]).strip()
        for row in rows[1:]
        if link_index < len(row) and str(row[link_index]).strip()
    ]


def load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "results": {}, "errors": {}}
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
    temporary.write_text(json.dumps(checkpoint, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def scrape_links(
    links: list[str],
    checkpoint_path: Path,
    *,
    workers: int,
    retry_errors: bool,
) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    checkpoint = load_checkpoint(checkpoint_path)
    results: dict[str, dict[str, str]] = checkpoint["results"]
    errors: dict[str, str] = checkpoint["errors"]
    pending = [url for url in links if url not in results and (retry_errors or url not in errors)]
    log("CAPTURE", f"unicos={len(links):,} | cache={len(results):,} | pendientes={len(pending):,}")
    completed = 0
    lock = threading.Lock()

    def persist_result(url: str, details: dict[str, str] | None, error: str = "") -> None:
        nonlocal completed
        with lock:
            if details is not None:
                results[url] = details
                errors.pop(url, None)
            else:
                errors[url] = error
            completed += 1
            if completed % 25 == 0 or completed == len(pending):
                save_checkpoint(checkpoint_path, checkpoint)
                log(
                    "PROGRESS",
                    f"{completed:,}/{len(pending):,} | correctos={len(results):,} | fallos={len(errors):,}",
                )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        future_map = {executor.submit(fetch_details, url): url for url in pending}
        for future in concurrent.futures.as_completed(future_map):
            url = future_map[future]
            try:
                persist_result(url, future.result())
            except Exception as exc:
                persist_result(url, None, f"{type(exc).__name__}: {exc}")
    save_checkpoint(checkpoint_path, checkpoint)
    return results, errors


def _old_value(row: list[str], index: int | None) -> str:
    if index is None or index >= len(row):
        return ""
    return str(row[index] or "")


def update_sheet_columns(
    service,
    spreadsheet_id: str,
    sheet: str,
    results: dict[str, dict[str, str]],
    *,
    dry_run: bool,
) -> dict[str, int]:
    rows = read_sheet(service, spreadsheet_id, sheet)
    if not rows:
        return {"rows": 0, "matched": 0, "changed": 0}
    header = [str(value or "").strip() for value in rows[0]]
    header_map = {normalize_header(value): index for index, value in enumerate(header)}
    link_index = header_map.get("enlace")
    if link_index is None:
        return {"rows": max(0, len(rows) - 1), "matched": 0, "changed": 0}

    target_keys = list(WRITE_COLUMNS)
    previous_indexes: dict[str, int | None] = {
        column: header_map.get(normalize_header(column)) for column in target_keys
    }
    for column in target_keys:
        key = normalize_header(column)
        if key not in header_map:
            header_map[key] = len(header)
            header.append(column)

    updates: list[dict[str, Any]] = []
    matched = 0
    changed = 0
    for column in target_keys:
        target_index = header_map[normalize_header(column)]
        previous_index = previous_indexes[column]
        values = [[column]]
        for row in rows[1:]:
            url = _old_value(row, link_index).strip()
            old = _old_value(row, previous_index)
            details = results.get(url)
            new = str((details or {}).get(column) or old)
            values.append([new])
            if details and column == target_keys[0]:
                matched += 1
            if new != old:
                changed += 1
        letter = col_letter(target_index + 1)
        updates.append({"range": f"'{sheet}'!{letter}1:{letter}{len(values)}", "values": values})

    aliases = {"entidad": "entidad", "unidad solicitante": PURCHASE_UNIT_COLUMN}
    for existing_name, detail_name in aliases.items():
        target_index = header_map.get(normalize_header(existing_name))
        if target_index is None:
            continue
        values = [[header[target_index]]]
        for row in rows[1:]:
            url = _old_value(row, link_index).strip()
            old = _old_value(row, target_index)
            new = str((results.get(url) or {}).get(detail_name) or old)
            values.append([new])
            if new != old:
                changed += 1
        letter = col_letter(target_index + 1)
        updates.append({"range": f"'{sheet}'!{letter}1:{letter}{len(values)}", "values": values})

    if updates and not dry_run:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": updates},
        ).execute()
    return {"rows": max(0, len(rows) - 1), "matched": matched, "changed": changed}


def verify_sheet(service, spreadsheet_id: str, sheet: str) -> dict[str, int]:
    rows = read_sheet(service, spreadsheet_id, sheet)
    if not rows:
        return {"rows": 0, "complete_contact": 0, "complete_entity": 0}
    header_map = {normalize_header(value): index for index, value in enumerate(rows[0])}

    def present(row: list[str], column: str) -> bool:
        index = header_map.get(normalize_header(column))
        return bool(_old_value(row, index).strip())

    data = rows[1:]
    return {
        "rows": len(data),
        "complete_contact": sum(
            present(row, CONTACT_NAME_COLUMN) and present(row, CONTACT_EMAIL_COLUMN) for row in data
        ),
        "complete_entity": sum(
            present(row, DEPENDENCY_COLUMN)
            and present(row, PURCHASE_UNIT_COLUMN)
            and present(row, PROVINCE_COLUMN)
            for row in data
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--spreadsheet-id",
        default=os.environ.get("PC_SHEETS_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
    )
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--max-links", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--retry-errors", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    service = build_sheets_service()
    sheets = available_target_sheets(service, args.spreadsheet_id)
    all_links: list[str] = []
    for sheet in sheets:
        links = extract_links(read_sheet(service, args.spreadsheet_id, sheet))
        all_links.extend(links)
        log("SHEET", f"{sheet}: {len(links):,} enlaces")
    unique_links = list(dict.fromkeys(all_links))
    if args.max_links > 0:
        unique_links = unique_links[: args.max_links]
    results, errors = scrape_links(
        unique_links,
        args.checkpoint,
        workers=args.workers,
        retry_errors=args.retry_errors,
    )

    total_changed = 0
    for sheet in sheets:
        stats = update_sheet_columns(
            service,
            args.spreadsheet_id,
            sheet,
            results,
            dry_run=args.dry_run,
        )
        total_changed += stats["changed"]
        log(
            "WRITE" if not args.dry_run else "DRYRUN",
            f"{sheet}: filas={stats['rows']:,} | enlaces resueltos={stats['matched']:,} | "
            f"celdas nuevas={stats['changed']:,}",
        )

    if not args.dry_run:
        for sheet in sheets:
            stats = verify_sheet(service, args.spreadsheet_id, sheet)
            log(
                "VERIFY",
                f"{sheet}: contacto nombre+correo={stats['complete_contact']:,}/{stats['rows']:,} | "
                f"unidad completa={stats['complete_entity']:,}/{stats['rows']:,}",
            )
    failed = sum(url in errors for url in unique_links)
    log(
        "DONE",
        f"enlaces={len(unique_links):,} | resueltos={sum(url in results for url in unique_links):,} | "
        f"fallos={failed:,} | celdas cambiadas={total_changed:,}",
    )
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
