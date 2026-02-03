import logging
import os
import random
import re
import socket
import ssl
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

from google.auth.exceptions import TransportError
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from httplib2 import HttpLib2Error

BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent
SERVICE_ACCOUNT_FILE = Path(
    os.environ.get(
        "ORQUESTADOR_GOOGLE_SERVICE_ACCOUNT",
        REPO_ROOT / "credentials" / "service-account.json",
    )
)
SPREADSHEET_ID = os.environ.get(
    "ORQUESTADOR_SPREADSHEET_ID",
    "1-2sgJPhSPzP65HLeGSvxDBtfNczhiDiZhdEbyy6lia0",
)
CONFIG_SHEET_NAME = os.environ.get("ORQUESTADOR_CONFIG_SHEET", "pc_config")
STATE_SHEET_NAME = os.environ.get("ORQUESTADOR_STATE_SHEET", "pc_state")
MANUAL_SHEET_NAME = os.environ.get("ORQUESTADOR_MANUAL_SHEET", "pc_manual")

CONFIG_HEADERS = ["name", "python", "script", "days", "times", "enabled"]
STATE_HEADERS = [
    "job_name",
    "status",
    "started_at",
    "finished_at",
    "duration_seconds",
    "duration_display",
    "detail",
]
MANUAL_HEADERS = [
    "id",
    "job",
    "requested_by",
    "requested_at",
    "status",
    "notes",
    "payload",
    "result_file_id",
    "result_file_url",
    "result_file_name",
    "result_error",
]

SPANISH_DAY_MAP: Dict[str, str] = {
    "lunes": "mon",
    "martes": "tue",
    "miercoles": "wed",
    "miércoles": "wed",
    "jueves": "thu",
    "viernes": "fri",
    "sabado": "sat",
    "sábado": "sat",
    "domingo": "sun",
}
ENGLISH_TO_SPANISH_DAY: Dict[str, str] = {
    "mon": "lunes",
    "tue": "martes",
    "wed": "miércoles",
    "thu": "jueves",
    "fri": "viernes",
    "sat": "sábado",
    "sun": "domingo",
}
VALID_DAY_ABBRS = set(SPANISH_DAY_MAP.values())
DAY_SPLIT_RE = re.compile(r"[\s,;/]+")
TIME_SPLIT_RE = re.compile(r"[\s,;/]+")

_credentials: Credentials | None = None
_credentials_lock = threading.Lock()
_service_local = threading.local()
_service_build_lock = threading.Lock()
_sheet_titles: set[str] | None = None
_sheet_titles_lock = threading.Lock()


def _get_credentials() -> Credentials:
    global _credentials
    if _credentials is not None:
        return _credentials
    with _credentials_lock:
        if _credentials is None:
            _credentials = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_FILE))
    return _credentials


def _reset_thread_service() -> None:
    if hasattr(_service_local, "service"):
        delattr(_service_local, "service")


def _build_service() -> Any:
    creds = _get_credentials()
    with _service_build_lock:
        return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get_service(force_refresh: bool = False) -> Any:
    service = getattr(_service_local, "service", None)
    if service is None or force_refresh:
        service = _build_service()
        _service_local.service = service
    return service


def _call_with_backoff(action, label: str, max_attempts: int = 5, base_delay: float = 1.5):
    network_errors = (
        TimeoutError,
        socket.timeout,
        ssl.SSLError,
        TransportError,
        HttpLib2Error,
    )
    for attempt in range(max_attempts):
        try:
            return action()
        except HttpError as err:
            status = getattr(err.resp, "status", None)
            if status not in {429, 500, 503} or attempt == max_attempts - 1:
                raise
            wait = base_delay * (2**attempt) + random.uniform(0, 0.5)
            logging.warning(
                "Google Sheets %s respondio con status %s; reintento %s/%s en %.1fs",
                label,
                status,
                attempt + 1,
                max_attempts,
                wait,
            )
            time.sleep(wait)
        except network_errors as err:
            _reset_thread_service()
            if attempt == max_attempts - 1:
                raise
            wait = base_delay * (2**attempt) + random.uniform(0, 0.5)
            logging.warning(
                "Google Sheets %s arrojo %s; reintento %s/%s en %.1fs",
                label,
                type(err).__name__,
                attempt + 1,
                max_attempts,
                wait,
            )
            time.sleep(wait)

def _column_letter(index: int) -> str:
    result: List[str] = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        result.append(chr(65 + remainder))
    return "".join(reversed(result))


def _refresh_sheet_titles() -> set[str]:
    service = _get_service()
    response = _call_with_backoff(
        lambda: service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute(),
        "spreadsheets.get",
    )
    titles = {sheet["properties"]["title"] for sheet in response.get("sheets", [])}
    global _sheet_titles
    with _sheet_titles_lock:
        _sheet_titles = titles
    return titles


def _ensure_sheet_exists(title: str) -> None:
    with _sheet_titles_lock:
        cached_titles = set(_sheet_titles) if _sheet_titles else None
    if cached_titles is None:
        cached_titles = _refresh_sheet_titles()
    if title in cached_titles:
        return
    service = _get_service()
    body = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
    _call_with_backoff(
        lambda: service.spreadsheets()
        .batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body)
        .execute(),
        f"crear hoja {title}",
    )
    _refresh_sheet_titles()


def _ensure_headers(sheet: str, headers: List[str]) -> None:
    _ensure_sheet_exists(sheet)
    service = _get_service()
    header_range = f"{sheet}!A1:{_column_letter(len(headers))}1"
    existing = _call_with_backoff(
        lambda: service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=header_range)
        .execute(),
        f"leer encabezados {sheet}",
    )
    normalized_existing = [
        cell.strip().lower() for cell in existing.get("values", [[]])[0]
    ] if existing.get("values") else []
    expected = [h.lower() for h in headers]
    if normalized_existing != expected:
        _call_with_backoff(
            lambda: service.spreadsheets()
            .values()
            .update(
                spreadsheetId=SPREADSHEET_ID,
                range=header_range,
                valueInputOption="RAW",
                body={"values": [headers]},
            )
            .execute(),
            f"actualizar encabezados {sheet}",
        )


def _clear_data_rows(sheet: str, columns: int) -> None:
    service = _get_service()
    data_range = f"{sheet}!A2:{_column_letter(columns)}"
    _call_with_backoff(
        lambda: service.spreadsheets()
        .values()
        .clear(spreadsheetId=SPREADSHEET_ID, range=data_range)
        .execute(),
        f"limpiar datos {sheet}",
    )


def _get_values(range_a1: str) -> List[List[str]]:
    service = _get_service()
    response = _call_with_backoff(
        lambda: service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=range_a1)
        .execute(),
        f"get {range_a1}",
    )
    return response.get("values", [])


def _update_values(range_a1: str, values: List[List[str]]) -> None:
    service = _get_service()
    _call_with_backoff(
        lambda: service.spreadsheets()
        .values()
        .update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_a1,
            valueInputOption="RAW",
            body={"values": values},
        )
        .execute(),
        f"update {range_a1}",
    )


def fetch_jobs_from_sheet() -> tuple[List[Dict[str, List[str]]], bool]:
    _ensure_headers(CONFIG_SHEET_NAME, CONFIG_HEADERS)
    data_range = f"{CONFIG_SHEET_NAME}!A2:{_column_letter(len(CONFIG_HEADERS))}"
    rows = _get_values(data_range)
    had_rows = any(any((cell or "").strip() for cell in row) for row in rows)

    jobs: List[Dict[str, List[str]]] = []
    for row_index, row in enumerate(rows, start=2):
        extended = row + [""] * (len(CONFIG_HEADERS) - len(row))
        row_data = dict(zip(CONFIG_HEADERS, extended))

        enabled_value = row_data.get("enabled", "").strip().lower()
        if enabled_value in {"no", "false", "0"}:
            continue

        name = row_data.get("name", "").strip()
        python_path = row_data.get("python", "").strip()
        script_path = row_data.get("script", "").strip()
        if not name or not python_path or not script_path:
            logging.warning(
                "Fila %s en %s sin columnas obligatorias (name/python/script); se ignora",
                row_index,
                CONFIG_SHEET_NAME,
            )
            continue

        days = _parse_days(row_data.get("days", ""), row_index)
        times = _parse_times(row_data.get("times", ""), row_index)

        if not days and not times:
            logging.info(
                "Fila %s en %s configurada como manual-only (sin dias ni horarios).",
                row_index,
                CONFIG_SHEET_NAME,
            )
        elif not days:
            logging.warning(
                "Fila %s en %s sin dias validos; se ignora",
                row_index,
                CONFIG_SHEET_NAME,
            )
            continue
        elif not times:
            logging.warning(
                "Fila %s en %s sin horarios validos; se ignora",
                row_index,
                CONFIG_SHEET_NAME,
            )
            continue

        jobs.append(
            {
                "name": name,
                "python": python_path,
                "script": script_path,
                "days_of_week": days,
                "times": times,
            }
        )

    return jobs, had_rows


def _parse_days(raw_value: str, row_index: int) -> List[str]:
    tokens = [token.strip().lower() for token in DAY_SPLIT_RE.split(raw_value or "") if token.strip()]
    days: List[str] = []
    for token in tokens:
        if token in SPANISH_DAY_MAP:
            normalized = SPANISH_DAY_MAP[token]
        elif token in VALID_DAY_ABBRS:
            normalized = token
        else:
            logging.warning(
                "Fila %s: valor de dia '%s' no reconocido; se ignora",
                row_index,
                token,
            )
            continue
        if normalized not in days:
            days.append(normalized)
    return days


def _parse_times(raw_value: str, row_index: int) -> List[str]:
    tokens = [token.strip() for token in TIME_SPLIT_RE.split(raw_value or "") if token.strip()]
    times: List[str] = []
    for token in tokens:
        match = re.match(r"^(\d{1,2}):(\d{1,2})$", token)
        if not match:
            logging.warning(
                "Fila %s: hora '%s' invalida; formato esperado HH:MM",
                row_index,
                token,
            )
            continue
        hour, minute = int(match.group(1)), int(match.group(2))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            logging.warning(
                "Fila %s: hora '%s' fuera de rango (00:00-23:59); se ignora",
                row_index,
                token,
            )
            continue
        formatted = f"{hour:02d}:{minute:02d}"
        if formatted not in times:
            times.append(formatted)
    return times


def push_state_to_sheet(state: Dict[str, Dict[str, dict]]) -> None:
    _ensure_headers(STATE_SHEET_NAME, STATE_HEADERS)
    last_run = state.get("last_run", {}) if isinstance(state, dict) else {}

    rows: List[List[str]] = []
    for job_name in sorted(last_run.keys()):
        entry = last_run[job_name] or {}
        rows.append(
            [
                job_name,
                str(entry.get("status", "")),
                str(entry.get("started_at", "")),
                str(entry.get("finished_at", "")),
                str(entry.get("duration_seconds", "")),
                str(entry.get("duration_display", "")),
                str(entry.get("detail", "")),
            ]
        )

    _clear_data_rows(STATE_SHEET_NAME, len(STATE_HEADERS))
    header_range = f"{STATE_SHEET_NAME}!A1:{_column_letter(len(STATE_HEADERS))}1"
    _update_values(header_range, [STATE_HEADERS])
    if rows:
        data_range = f"{STATE_SHEET_NAME}!A2"
        _update_values(data_range, rows)


def push_jobs_to_sheet(jobs: List[Dict[str, Any]]) -> None:
    if not jobs:
        return

    _ensure_headers(CONFIG_SHEET_NAME, CONFIG_HEADERS)

    rows: List[List[str]] = []
    for job in jobs:
        days_list = job.get("days_of_week") or job.get("days") or []
        times_list = job.get("times") or []

        days_str_parts: List[str] = []
        for day in days_list:
            normalized = ENGLISH_TO_SPANISH_DAY.get(str(day).lower(), str(day))
            if normalized not in days_str_parts:
                days_str_parts.append(normalized)

        times_str = ", ".join(str(item) for item in times_list)

        rows.append(
            [
                str(job.get("name", "")),
                str(job.get("python", "")),
                str(job.get("script", "")),
                ", ".join(days_str_parts),
                times_str,
                "sí",
            ]
        )

    _clear_data_rows(CONFIG_SHEET_NAME, len(CONFIG_HEADERS))
    data_range = f"{CONFIG_SHEET_NAME}!A2"
    _update_values(data_range, rows)
    logging.info(
        "Configuracion base publicada en Google Sheets (%s / %s)",
        SPREADSHEET_ID,
        CONFIG_SHEET_NAME,
    )


def fetch_manual_requests(
    status_filters: Tuple[str, ...] | None = ("pending",),
) -> List[Dict[str, str]]:
    _ensure_headers(MANUAL_SHEET_NAME, MANUAL_HEADERS)
    data_range = f"{MANUAL_SHEET_NAME}!A2:{_column_letter(len(MANUAL_HEADERS))}"
    rows = _get_values(data_range)

    normalized_filters: set[str] | None = None
    if status_filters is not None:
        normalized_filters = {item.strip().lower() for item in status_filters}

    requests: List[Dict[str, str]] = []
    for row_index, row in enumerate(rows, start=2):
        extended = row + [""] * (len(MANUAL_HEADERS) - len(row))
        row_data = {
            header: (extended[i] or "").strip()
            for i, header in enumerate(MANUAL_HEADERS)
        }
        if not any(row_data.values()):
            continue

        status_value = row_data.get("status", "").lower()
        if normalized_filters is not None and status_value not in normalized_filters:
            continue

        request_entry: Dict[str, str] = {"row": str(row_index)}
        request_entry.update(row_data)
        requests.append(request_entry)

    return requests


def update_manual_request_status(row_index: int, status: str, notes: str | None = None) -> None:
    _ensure_headers(MANUAL_SHEET_NAME, MANUAL_HEADERS)
    status_col = MANUAL_HEADERS.index("status") + 1
    notes_col = MANUAL_HEADERS.index("notes") + 1

    if notes is None:
        range_a1 = f"{MANUAL_SHEET_NAME}!{_column_letter(status_col)}{row_index}"
        _update_values(range_a1, [[status]])
        return

    start_col = min(status_col, notes_col)
    end_col = max(status_col, notes_col)
    range_a1 = (
        f"{MANUAL_SHEET_NAME}!{_column_letter(start_col)}{row_index}"
        f":{_column_letter(end_col)}{row_index}"
    )
    values: List[str] = []
    for col in range(start_col, end_col + 1):
        if col == status_col:
            values.append(status)
        elif col == notes_col:
            values.append(notes)
        else:
            values.append("")
    _update_values(range_a1, [values])


def update_manual_request_result(row_index: int, result: Dict[str, str]) -> None:
    _ensure_headers(MANUAL_SHEET_NAME, MANUAL_HEADERS)
    if row_index is None:
        return

    for key, value in result.items():
        if key not in MANUAL_HEADERS:
            continue
        col_index = MANUAL_HEADERS.index(key) + 1
        range_a1 = f"{MANUAL_SHEET_NAME}!{_column_letter(col_index)}{row_index}"
        _update_values(range_a1, [[value]])
