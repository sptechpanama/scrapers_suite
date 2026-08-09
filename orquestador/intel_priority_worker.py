from __future__ import annotations

"""Procesa de forma reanudable una cartera prioritaria de estudios por ficha."""

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from orquestador.sheets_bridge import (  # noqa: E402
    MANUAL_HEADERS,
    MANUAL_SHEET_NAME,
    _call_with_backoff,
    _column_letter,
    _ensure_headers,
    _get_service,
    _get_values,
    _update_values,
    SPREADSHEET_ID,
)

PORTFOLIO_SHEET = os.environ.get("INTEL_PRIORITY_PORTFOLIO_SHEET", "intel_priority_portfolio")
PORTFOLIO_HEADERS = [
    "batch_id",
    "scope_id",
    "created_at",
    "requested_by",
    "ficha",
    "nombre_ficha",
    "rank_score",
    "rank_monto_ficha_unica",
    "rank_actos_ficha_unica",
    "criterios_seleccion",
    "score_oportunidad",
    "monto_ficha_unica",
    "actos_ficha_unica",
    "estado",
    "intentos",
    "fecha_inicio",
    "fecha_fin",
    "request_id_ficha",
    "error",
]
PRIORITY_JOB_NAME = "intel_estudio_prioritario"
INDIVIDUAL_WORKER = Path(__file__).resolve().with_name("intel_ficha_worker.py")
FINAL_STATES = {"completado", "completado_previo", "fallido"}
PROCESSABLE_STATES = {"pendiente", "error"}


def _clean(value: object) -> str:
    text = str(value if value is not None else "").strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def _int(value: object, default: int = 0) -> int:
    try:
        return int(float(_clean(value)))
    except (TypeError, ValueError):
        return default


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_payload() -> dict[str, Any]:
    raw = _clean(os.environ.get("ORQUESTADOR_MANUAL_PAYLOAD", ""))
    if not raw:
        raise RuntimeError("No se encontró ORQUESTADOR_MANUAL_PAYLOAD.")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError("El payload de la cartera no es un objeto JSON.")
    return parsed


def _load_portfolio(batch_id: str) -> list[dict[str, Any]]:
    _ensure_headers(PORTFOLIO_SHEET, PORTFOLIO_HEADERS)
    last_column = _column_letter(len(PORTFOLIO_HEADERS))
    values = _get_values(f"{PORTFOLIO_SHEET}!A2:{last_column}")
    rows: list[dict[str, Any]] = []
    for row_number, raw in enumerate(values, start=2):
        extended = list(raw) + [""] * (len(PORTFOLIO_HEADERS) - len(raw))
        record = dict(zip(PORTFOLIO_HEADERS, extended[: len(PORTFOLIO_HEADERS)]))
        if _clean(record.get("batch_id")) != batch_id:
            continue
        record["_row_number"] = row_number
        rows.append(record)
    return rows


def _write_portfolio_row(record: dict[str, Any]) -> None:
    row_number = _int(record.get("_row_number"))
    if row_number < 2:
        raise RuntimeError("Fila de cartera inválida.")
    values = [[_clean(record.get(column, "")) for column in PORTFOLIO_HEADERS]]
    _update_values(f"{PORTFOLIO_SHEET}!A{row_number}", values)


def _reset_stale_rows(rows: list[dict[str, Any]], *, stale_hours: int = 6) -> None:
    threshold = datetime.now() - timedelta(hours=max(1, stale_hours))
    for record in rows:
        if _clean(record.get("estado")).lower() != "procesando":
            continue
        try:
            started = datetime.fromisoformat(_clean(record.get("fecha_inicio")))
        except ValueError:
            started = datetime.min
        if started > threshold:
            continue
        record["estado"] = "error"
        record["error"] = "Ejecución anterior interrumpida; reanudada automáticamente."
        record["fecha_fin"] = _now()
        _write_portfolio_row(record)


def _processable(rows: list[dict[str, Any]], *, max_attempts: int) -> list[dict[str, Any]]:
    candidates = [
        row
        for row in rows
        if _clean(row.get("estado")).lower() in PROCESSABLE_STATES
        and _int(row.get("intentos")) < max_attempts
    ]
    return sorted(
        candidates,
        key=lambda row: (
            min(
                [
                    rank
                    for rank in (
                        _int(row.get("rank_score"), 10**9),
                        _int(row.get("rank_monto_ficha_unica"), 10**9),
                        _int(row.get("rank_actos_ficha_unica"), 10**9),
                    )
                    if rank > 0
                ]
                or [10**9]
            ),
            _clean(row.get("ficha")),
        ),
    )


def _individual_payload(parent_payload: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    child_request_id = uuid.uuid4().hex
    return {
        "request_id": child_request_id,
        "parent_request_id": _clean(parent_payload.get("request_id")),
        "batch_id": _clean(parent_payload.get("batch_id")),
        "scope_id": _clean(record.get("scope_id")) or _clean(parent_payload.get("scope_id")),
        "ficha": _clean(record.get("ficha")),
        "nombre_ficha": _clean(record.get("nombre_ficha")),
        "db_path": _clean(parent_payload.get("db_path")),
        "analytics_db_path": _clean(parent_payload.get("analytics_db_path")),
        "filters": parent_payload.get("filters") if isinstance(parent_payload.get("filters"), dict) else {},
        "score_preset": parent_payload.get("score_preset", {}),
        "study_scope": _clean(parent_payload.get("study_scope")) or "analisis_actual",
        "notes": (
            f"Cartera prioritaria {_clean(parent_payload.get('batch_id'))}; "
            f"selección: {_clean(record.get('criterios_seleccion'))}"
        ),
        "requested_from": "cartera_prioritaria_top150",
    }


def _run_individual(
    parent_payload: dict[str, Any],
    record: dict[str, Any],
    *,
    timeout_seconds: int,
) -> tuple[bool, str, str]:
    child_payload = _individual_payload(parent_payload, record)
    child_request_id = _clean(child_payload["request_id"])
    env = os.environ.copy()
    env["ORQUESTADOR_MANUAL_ID"] = child_request_id
    env["ORQUESTADOR_MANUAL_PAYLOAD"] = json.dumps(child_payload, ensure_ascii=False)
    env["INTEL_STUDY_HEADLESS"] = "1"
    env["INTEL_STUDY_APPEND_RESULTS"] = "1"
    cache_root = REPO_ROOT / "data" / "intel_html_cache" / _clean(parent_payload.get("scope_id", "global"))
    cache_root.mkdir(parents=True, exist_ok=True)
    env["INTEL_STUDY_HTML_CACHE_DIR"] = str(cache_root)
    try:
        completed = subprocess.run(
            [sys.executable, str(INDIVIDUAL_WORKER)],
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=max(60, timeout_seconds),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return False, child_request_id, f"Tiempo máximo excedido ({timeout_seconds}s): {exc}"
    output = "\n".join(part for part in (completed.stdout, completed.stderr) if part).strip()
    if completed.returncode != 0:
        return False, child_request_id, output[-3000:] or f"Código de salida {completed.returncode}"
    return True, child_request_id, ""


def _has_pending_continuation(batch_id: str, *, current_request_id: str = "") -> bool:
    _ensure_headers(MANUAL_SHEET_NAME, MANUAL_HEADERS)
    values = _get_values(f"{MANUAL_SHEET_NAME}!A2:{_column_letter(len(MANUAL_HEADERS))}")
    for raw in values:
        extended = list(raw) + [""] * (len(MANUAL_HEADERS) - len(raw))
        record = dict(zip(MANUAL_HEADERS, extended[: len(MANUAL_HEADERS)]))
        if current_request_id and _clean(record.get("id")) == current_request_id:
            continue
        if _clean(record.get("job")) != PRIORITY_JOB_NAME:
            continue
        if _clean(record.get("status")).lower() not in {"pending", "enqueued", "running"}:
            continue
        try:
            payload = json.loads(_clean(record.get("payload")) or "{}")
        except json.JSONDecodeError:
            continue
        if _clean(payload.get("batch_id")) == batch_id:
            return True
    return False


def _append_manual_request(payload: dict[str, Any]) -> str:
    request_id = uuid.uuid4().hex
    next_payload = dict(payload)
    next_payload["request_id"] = request_id
    row = {
        "id": request_id,
        "job": PRIORITY_JOB_NAME,
        "requested_by": _clean(payload.get("requested_by")) or "cartera_prioritaria",
        "requested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "pending",
        "notes": f"Continuación automática de cartera {_clean(payload.get('batch_id'))}",
        "payload": json.dumps(next_payload, ensure_ascii=False),
        "result_file_id": "",
        "result_file_url": "",
        "result_file_name": "",
        "result_error": "",
    }
    values = [[row.get(column, "") for column in MANUAL_HEADERS]]
    service = _get_service()
    _call_with_backoff(
        lambda: service.spreadsheets()
        .values()
        .append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{MANUAL_SHEET_NAME}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        )
        .execute(),
        f"continuación cartera {_clean(payload.get('batch_id'))}",
    )
    return request_id


def _summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        state = _clean(row.get("estado")).lower() or "sin_estado"
        counts[state] = counts.get(state, 0) + 1
    return counts


def main() -> int:
    started = time.perf_counter()
    payload = _load_payload()
    batch_id = _clean(payload.get("batch_id"))
    if not batch_id:
        raise RuntimeError("Payload sin batch_id.")
    batch_size = min(10, max(1, _int(payload.get("batch_size"), 1)))
    max_attempts = min(5, max(1, _int(payload.get("max_attempts"), 3)))
    timeout_seconds = max(60, _int(payload.get("timeout_ficha_seconds"), 3900))

    rows = _load_portfolio(batch_id)
    if not rows:
        raise RuntimeError(f"No se encontró la cartera {batch_id}.")
    _reset_stale_rows(rows, stale_hours=_int(payload.get("stale_hours"), 6))
    rows = _load_portfolio(batch_id)
    candidates = _processable(rows, max_attempts=max_attempts)[:batch_size]
    print(
        f"[intel_priority] batch={batch_id} | total={len(rows)} | "
        f"lote={len(candidates)} | estados={json.dumps(_summary(rows), ensure_ascii=False)}",
        flush=True,
    )

    processed = 0
    for record in candidates:
        ficha = _clean(record.get("ficha"))
        record["estado"] = "procesando"
        record["intentos"] = _int(record.get("intentos")) + 1
        record["fecha_inicio"] = _now()
        record["fecha_fin"] = ""
        record["error"] = ""
        _write_portfolio_row(record)
        print(f"[intel_priority] ficha={ficha} | intento={record['intentos']} | inicio", flush=True)
        ok, child_request_id, error = _run_individual(
            payload,
            record,
            timeout_seconds=timeout_seconds,
        )
        record["request_id_ficha"] = child_request_id
        record["fecha_fin"] = _now()
        if ok:
            record["estado"] = "completado"
            record["error"] = ""
        elif _int(record.get("intentos")) >= max_attempts:
            record["estado"] = "fallido"
            record["error"] = error
        else:
            record["estado"] = "error"
            record["error"] = error
        _write_portfolio_row(record)
        processed += 1
        print(
            f"[intel_priority] ficha={ficha} | estado={record['estado']} | "
            f"duración_total={time.perf_counter()-started:,.1f}s",
            flush=True,
        )

    rows = _load_portfolio(batch_id)
    remaining = _processable(rows, max_attempts=max_attempts)
    continuation_id = ""
    if remaining and not _has_pending_continuation(
        batch_id,
        current_request_id=_clean(payload.get("request_id")),
    ):
        continuation_id = _append_manual_request(payload)
        print(
            f"[intel_priority] continuación={continuation_id} | pendientes={len(remaining)}",
            flush=True,
        )
    result = {
        "ok": True,
        "batch_id": batch_id,
        "processed": processed,
        "remaining": len(remaining),
        "continuation_request_id": continuation_id,
        "states": _summary(rows),
    }
    print("RESULT_JSON=" + json.dumps(result, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
