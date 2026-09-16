"""Last-day reminders for ficha 43358, verified against public PanamaCompra."""
from __future__ import annotations

import base64
import json
import re
import time
import unicodedata
from datetime import datetime, time as clock_time
from zoneinfo import ZoneInfo

import requests

PANAMA = ZoneInfo("America/Panama")
API = "https://apisv3.panamacompra.gob.pa"
FICHA = "43358"


def process_number(value):
    match = re.search(r"\b\d{4}(?:-\d+){3,6}-(?:CL|CM|LP|SLP|LV)-\d+\b", str(value or ""), re.I)
    return match.group(0).upper() if match else ""


def deadline(value):
    """Return Panama deadline + precision. End-of-day is not an official hour."""
    text = str(value or "")
    dates = list(re.finditer(r"\b\d{1,2}[-/]\d{1,2}[-/]\d{4}\b", text))
    if not dates:
        return None, False
    try:
        day = datetime.strptime(dates[-1].group().replace("/", "-"), "%d-%m-%Y")
        # Only times following the LAST date count as closing time.
        times = list(re.finditer(r"\b(\d{1,2}):(\d{2})\s*([AP])\.?\s*M\.?|\b(\d{1,2}):(\d{2})\b",
                                text[dates[-1].end():], re.I))
        if not times:
            return datetime.combine(day.date(), clock_time.max, PANAMA), False
        part = times[-1]
        hour, minute = int(part[1] or part[4]), int(part[2] or part[5])
        meridiem = (part[3] or "").upper()
        if meridiem:
            if not 1 <= hour <= 12:
                return None, False
            hour = hour % 12 + (12 if meridiem == "P" else 0)
        return day.replace(hour=hour, minute=minute, tzinfo=PANAMA), True
    except ValueError:
        return None, False


def _json_request(method, url, **kwargs):
    error = None
    for attempt in range(3):
        try:
            response = requests.request(method, url, timeout=(8, 20), **kwargs)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or payload.get("status") not in (1, "1", True):
                raise ValueError("Public API did not confirm a successful response")
            return payload.get("result")
        except (requests.RequestException, ValueError) as exc:
            error = exc
            if attempt < 2:
                time.sleep(1.5 * 2 ** attempt)
    raise RuntimeError(f"Public verification failed: {type(error).__name__}") from error


def verify_public_entry(entry):
    code = process_number(entry.get("enlace"))
    if not code:
        raise ValueError("No official process number")
    is_cl = "-CL-" in code
    # Public state catalog: 8 abierta, 15 programada, 36 vigente.
    rows = []
    for state in ((8, 15) if is_cl else (36,)):
        result = _json_request("POST", API + "/busqueda/proceso-lista-publico", json={
            "registrosPorPagina": 50, "valorSiguiente": "",
            "filtro": {"idEstado": state, "idTipoProceso": 2 if is_cl else -1,
                       "numProceso": code, "idProvincia": 0},
        })
        if not isinstance(result, dict) or not isinstance(result.get("registros"), list):
            raise ValueError("Unexpected public listing")
        rows = [row for row in result["registros"] if row.get("numProceso") == code and row.get("idEstado") == state]
        if rows:
            break
    if not rows:
        return None  # Never send an actionable reminder for an unverified state.
    row = max(rows, key=lambda item: int(item["idProcesosContratacionFlujos"]))
    flow, kind = int(row["idProcesosContratacionFlujos"]), int(row["idTipoProceso"])
    try:
        detail = _json_request("GET", f"{API}/procesos-configuracion/pagina-componentes-publico/{kind}/procesoVistaPliego/{flow}")
    except RuntimeError:
        detail = _json_request("POST", API + "/ps/documentos-proceso/pliego-general/publico/get-page",
                               json={"idTipoProceso": kind, "idProcesosContratacionFlujos": flow})
    labels = {}
    for component in (detail or {}).get("pageComponentes", []):
        if not str(component.get("tipo", "")).startswith("componentInfo"):
            continue
        for pair in component.get("value") or []:
            if not isinstance(pair, dict):
                continue
            key = "".join(ch for ch in unicodedata.normalize("NFKD", str(pair.get("nombre", ""))).lower()
                          if not unicodedata.combining(ch))
            labels[key] = str(pair.get("value") or "").strip()
    if code not in labels.values():
        raise ValueError("Detail does not belong to requested process")
    date_text = next((v for k, v in labels.items() if k.startswith("fecha y hora presentacion")), "")
    if deadline(date_text)[0] is None:
        raise ValueError("No verifiable presentation deadline")
    route = "solicitud-de-cotizacion" if is_cl else "pliego-de-cargos"
    token = base64.b64encode(json.dumps({"i": flow, "tp": kind}, separators=(",", ":")).encode()).decode().rstrip("=")[::-1]
    return {**entry, "fecha": date_text, "estado_oficial": row.get("nombreRealizado", ""),
            "enlace": f"https://www.panamacompra.gob.pa/Inicio/#/{route}/{code}/{token}"}


def collect_reminders(entries, sent_keys, now, *, verify=verify_public_entry):
    now = now.replace(tzinfo=PANAMA) if now.tzinfo is None else now.astimezone(PANAMA)
    if now.hour < 7:
        return [], []
    selected, errors, processed = [], [], set()
    for entry in entries:
        if not re.search(r"(?<!\d)43358(?!\d)", str(entry.get("ficha_detectada", ""))):
            continue
        code = process_number(entry.get("enlace"))
        source_deadline, _ = deadline(entry.get("fecha"))
        if not code or code in processed or not source_deadline or source_deadline.date() != now.date():
            continue
        processed.add(code)
        key = f"43358|{code}|{now.date().isoformat()}"
        if key in sent_keys:
            continue
        try:
            verified = verify(entry)
            if not verified:
                continue
            cutoff, exact = deadline(verified.get("fecha"))
            if not cutoff or cutoff.date() != now.date() or cutoff <= now:
                continue
            selected.append({**verified, "reminder_key": key, "deadline_exact": exact,
                             "deadline_iso": cutoff.isoformat(), "numero_proceso": code})
        except Exception as exc:
            errors.append({"numero_proceso": code, "error": str(exc)})
    return selected, errors


def message_body(entries):
    lines = ["Hoy es el ultimo dia de presentacion publicado para estos actos de la ficha 43358.",
             "Este recordatorio es adicional al aviso de apertura.", ""]
    for entry in entries:
        lines += [f"Acto: {entry['numero_proceso']}", f"Producto: {entry.get('titulo', '')}",
                  f"Entidad: {entry.get('entidad', '')}", f"Presentacion oficial: {entry['fecha']}",
                  f"Estado verificado: {entry.get('estado_oficial', '')}",
                  f"Precio de referencia: {entry.get('precio_referencia', '')}", f"Enlace: {entry['enlace']}"]
        if not entry["deadline_exact"]:
            lines.append("El portal no indica una hora exacta en este campo. Confirma el cierre en el acto; no asumas que puedes esperar hasta medianoche.")
        lines.append("")
    return "\n".join(lines)
