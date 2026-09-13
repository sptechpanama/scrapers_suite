"""Refresh known public processes before expiry/deduplication, without appending rows."""
from __future__ import annotations

import base64
import json
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests

try:
    from .keyword_watch import parse_reference_amount
except ImportError:  # the scrapers import common modules as top-level modules
    from keyword_watch import parse_reference_amount

API_ROOT = "https://apisv3.panamacompra.gob.pa"


def normalized(value):
    value = unicodedata.normalize("NFKD", str(value or "")).lower()
    return re.sub(r"[^a-z0-9]+", " ", "".join(c for c in value if not unicodedata.combining(c))).strip()


def process_code(url):
    match = re.search(r"\b\d{4}(?:-\d+){3,6}-(?:CL|CM|LP|SCM|SCA|PE|LA|AV)-\d+\b", unquote(str(url or "")), re.I)
    return match.group(0).upper() if match else ""


def route_payload(url):
    parsed = urlparse(str(url))
    if parsed.hostname not in {"www.panamacompra.gob.pa", "panamacompra.gob.pa"}:
        raise ValueError("Enlace ajeno a PanamaCompra")
    token = unquote(parsed.fragment).rstrip("/").split("/")[-1][::-1]
    payload = json.loads(base64.b64decode(token + "=" * (-len(token) % 4)))
    return int(payload["i"]), int(payload["tp"])


def latest_links(urls):
    result = {}
    for url in urls:
        code = process_code(url)
        if not code:
            continue
        try:
            version = route_payload(url)[0]
        except (ValueError, KeyError, UnicodeError):
            continue
        if code not in result or version > route_payload(result[code])[0]:
            result[code] = str(url)
    return result


def parse_official_detail(data, url):
    if not isinstance(data, dict) or data.get("status") not in (1, "1", True):
        raise ValueError("API no devolvio un detalle valido")
    result = data.get("result")
    if not isinstance(result, dict):
        raise ValueError("API sin componentes de detalle")
    labels = {}
    for component in result.get("pageComponentes") or []:
        if not str(component.get("tipo", "")).startswith("componentInfo"):
            continue
        for pair in component.get("value") or []:
            if isinstance(pair, dict) and "nombre" in pair and "value" in pair:
                labels[normalized(pair["nombre"])] = pair["value"]
    code = process_code(url)
    if not code or labels.get("numero de proceso") != code:
        raise ValueError("El detalle oficial no corresponde al numero de acto solicitado")
    deadline = next((str(v).strip() for key, v in labels.items()
                     if key.startswith("fecha y hora presentacion") and v), "")
    dates = re.findall(r"\b\d{2}[-/]\d{2}[-/]\d{4}\b", deadline)
    if not dates:
        raise ValueError("Detalle sin fecha oficial de presentacion verificable")
    datetime.strptime(dates[0].replace("/", "-"), "%d-%m-%Y")
    values = {"enlace": url, "fecha": deadline}
    price = next((labels[k] for k in ("precio de referencia", "precio estimado") if k in labels), None)
    amount = parse_reference_amount(price)
    if amount is not None:
        values["precio referencia"] = amount
    return values


def fetch_official_detail(url, *, attempts=3, sleeper=time.sleep):
    flow, process_type = route_payload(url)
    error = None
    with requests.Session() as session:
        session.headers.update({"User-Agent": "Mozilla/5.0 PanamaCompra-refresh/1.0", "Accept": "application/json"})
        for attempt in range(attempts):
            try:
                response = session.get(
                    f"{API_ROOT}/procesos-configuracion/pagina-componentes-publico/{process_type}/procesoVistaPliego/{flow}",
                    timeout=(8, 25),
                )
                response.raise_for_status()
                data = response.json()
                if data.get("status") not in (1, "1", True):
                    response = session.post(
                        f"{API_ROOT}/ps/documentos-proceso/pliego-general/publico/get-page",
                        json={"idTipoProceso": process_type, "idProcesosContratacionFlujos": flow},
                        timeout=(8, 25),
                    )
                    response.raise_for_status()
                    data = response.json()
                return parse_official_detail(data, url)
            except (requests.RequestException, ValueError, KeyError) as exc:
                error = exc
                if attempt + 1 < attempts:
                    sleeper(1.5 * (2 ** attempt))
    raise RuntimeError(f"No se verifico {process_code(url)}: {error}")


def column_letter(index):
    result = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _rows(snapshot):
    rows = []
    for sheet, values in snapshot.items():
        if not values:
            continue
        columns = {normalized(v): i for i, v in enumerate(values[0])}
        if "enlace" not in columns:
            continue
        for number, cells in enumerate(values[1:], 2):
            record = {k: cells[i] if i < len(cells) else "" for k, i in columns.items()}
            code = process_code(record.get("enlace"))
            if code and normalized(record.get("descartar")) not in {"true", "si", "1", "x"}:
                rows.append((sheet, number, columns, record, code))
    return rows


def refresh_processes(*, listing_links, read_sheets, write_cells, checkpoint_path,
                      fetch=fetch_official_detail, now=None, force=False, workers=3):
    """read_sheets returns a fresh snapshot. write_cells applies only named cells.

    The latest listing URL is authoritative. Same-version acts are checked daily,
    or every two hours near presentation. Failed reads/writes never advance the
    checkpoint and protect affected records from the subsequent date purge.
    """
    now = now or datetime.now()
    checkpoint_path = Path(checkpoint_path)
    try:
        checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        checkpoint = {}
    cache = checkpoint.get("verified", {})
    links = latest_links(listing_links)
    snapshot = read_sheets()
    existing = _rows(snapshot)
    selected = {}
    protected = set()
    for sheet, _, _, row, code in existing:
        if code not in links:
            continue
        latest = links[code]
        previous = cache.get(f"{sheet}|{code}", {})
        try:
            age = now - datetime.fromisoformat(previous.get("checked_at", ""))
        except (ValueError, TypeError):
            age = timedelta(days=365)
        days = re.findall(r"\b\d{2}[-/]\d{2}[-/]\d{4}\b", str(row.get("fecha", "")))
        near = False
        if days:
            try:
                near = datetime.strptime(days[0].replace("/", "-"), "%d-%m-%Y") <= now + timedelta(days=3)
            except ValueError:
                pass
        interval = timedelta(hours=2 if near else 24)
        try:
            same_version = route_payload(row["enlace"]) == route_payload(latest)
        except (ValueError, KeyError, UnicodeError):
            same_version = False
        if force or not same_version or previous.get("url") != latest or age >= interval:
            selected[code] = latest
    report = {"checked": 0, "changed": [], "failed": {}, "protected_codes": protected}
    def get(pair):
        code, url = pair
        try:
            return code, fetch(url), ""
        except Exception as exc:
            return code, None, f"{type(exc).__name__}: {exc}"
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        details = {}
        for code, detail, error in pool.map(get, selected.items()):
            if error:
                report["failed"][code] = error
                protected.add(code)
            else:
                details[code] = detail
    if not details:
        return report
    # Re-read immediately before mutation: row order/headers may have changed in the app.
    fresh = _rows(read_sheets())
    originals = {(sheet, code): row for sheet, _, _, row, code in existing}
    updates = []
    verified = []
    for sheet, number, columns, row, code in fresh:
        detail = details.get(code)
        old = originals.get((sheet, code))
        if detail is None or old is None:
            continue
        if any(str(row.get(key, "")) != str(old.get(key, "")) for key in detail):
            report["failed"][code] = "Fila modificada concurrentemente; se reintentara"
            protected.add(code)
            continue
        changes = {}
        for key, value in detail.items():
            if key not in columns:
                continue
            equal = (parse_reference_amount(row.get(key)) == value) if key == "precio referencia" else str(row.get(key, "")).strip() == str(value).strip()
            if not equal:
                updates.append({"range": f"'{sheet}'!{column_letter(columns[key]+1)}{number}", "values": [[value]]})
                changes[key] = {"before": row.get(key, ""), "after": value}
        if "fecha de actualizacion" in columns:
            updates.append({"range": f"'{sheet}'!{column_letter(columns['fecha de actualizacion']+1)}{number}", "values": [[now.strftime('%Y-%m-%d %H:%M:%S')]]})
        if changes:
            report["changed"].append({"code": code, "sheet": sheet, "changes": changes})
        verified.append((sheet, code, detail))
    if not verified:
        return report
    try:
        if updates:
            write_cells(updates)
        after = {(s, c): r for s, _, _, r, c in _rows(read_sheets())}
        for sheet, code, detail in verified:
            row = after.get((sheet, code), {})
            for key, value in detail.items():
                if key not in originals[(sheet, code)]:
                    continue
                equal = parse_reference_amount(row.get(key)) == value if key == "precio referencia" else str(row.get(key, "")).strip() == str(value).strip()
                if not equal:
                    raise RuntimeError(f"Verificacion posterior fallida: {code}, {key}")
    except Exception as exc:
        for _, code, _ in verified:
            protected.add(code)
            report["failed"][code] = f"Publicacion pendiente: {exc}"
        report["changed"] = []
        return report
    for sheet, code, detail in verified:
        cache[f"{sheet}|{code}"] = {"checked_at": now.isoformat(timespec="seconds"), "url": detail['enlace']}
    checkpoint['verified'] = cache
    checkpoint['changes'] = (checkpoint.get('changes', []) + report['changed'])[-2000:]
    checkpoint['last_success'] = now.isoformat(timespec='seconds')
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    temp = checkpoint_path.with_suffix('.tmp')
    temp.write_text(json.dumps(checkpoint, ensure_ascii=False, indent=2), encoding='utf-8')
    temp.replace(checkpoint_path)
    report['checked'] = len({code for _, code, _ in verified})
    return report


def refresh_google_sheets(service, spreadsheet_id, sheet_names, listing_links, checkpoint_path, *, force=False):
    names = list(dict.fromkeys(sheet_names))
    def read():
        response = service.spreadsheets().values().batchGet(
            spreadsheetId=spreadsheet_id, ranges=[f"'{s}'!A1:ZZ" for s in names],
        ).execute(num_retries=3)
        return {s: r.get('values', []) for s, r in zip(names, response['valueRanges'])}
    def write(data):
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body={'valueInputOption':'RAW', 'data':data},
        ).execute(num_retries=3)
    return refresh_processes(listing_links=listing_links, read_sheets=read, write_cells=write,
                             checkpoint_path=checkpoint_path, force=force)
