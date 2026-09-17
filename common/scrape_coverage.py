"""Verified pagination and recoverable detail failures for PanamaCompra jobs."""
from __future__ import annotations

import json
import base64
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import unquote

PROCESS = re.compile(r"\b\d{4}-\d+-\d+-\d+-\d+-[A-Z]+-\d+\b", re.I)
LIST_ENDPOINT = 'https://apisv3.panamacompra.gob.pa/busqueda/proceso-lista-publico'


def process_key(url):
    match = PROCESS.search(unquote(str(url)))
    return match.group(0).upper() if match else str(url).strip()


def parse_pager(text):
    match = re.search(r"P[aá]gina\s*:?\s*(\d+)\s*/\s*(\d+)\s*(\+)?", text, re.I)
    total = re.search(r"Total\s*:\s*([\d,]+)\s*(\+)?", text, re.I)
    return {
        "page": int(match[1]) if match else None,
        "pages": int(match[2]) if match else None,
        "more": bool(match and match[3]),
        "total": int(total[1].replace(",", "")) if total else None,
        "total_more": bool(total and total[2]),
    }


def recover_listing(listing, page_tools, job, log, *, request=None, normalize_url=str):
    """Recover a stuck public UI with the SAME public state, in bounded windows.

    The public server sometimes times out on a cursor near older active acts.
    Date bisection avoids that cursor; saturated or failed windows are split.
    A failed leaf remains partial and does not imply an empty official result.
    """
    if listing['complete']:
        return listing
    if request is None:
        import requests
        request = requests.post
    state = {'clv': 8, 'clrir': 15, 'rir1': 36}[job]
    if not hasattr(page_tools, 'adjudication_by_url'):
        page_tools.adjudication_by_url = {}
    records, windows, failures = {}, [], []
    calls, consecutive_errors = 0, 0
    size = 5000
    def iso(value): return value.isoformat(timespec='milliseconds').replace('+00:00','Z')
    def fetch(start, end, depth=0):
        nonlocal calls, consecutive_errors
        if calls >= 45 or consecutive_errors >= 6:
            failures.append({'start':iso(start),'end':iso(end),'error':'API indisponible o límite de recuperación alcanzado; reintentar próxima corrida'})
            return
        payload = {'registrosPorPagina':size, 'valorSiguiente':'', 'filtro': {
            'idEstado':state,'idTipoProceso':2 if job!='rir1' else -1,'idProvincia':0,
            'fechaDesde':iso(start),'fechaHasta':iso(end)}}
        error = ''
        rows = None
        for attempt in range(3):
            try:
                calls += 1
                response = request(LIST_ENDPOINT, json=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
                if data.get('status') != 1 or not isinstance(data.get('result'),dict):
                    raise ListingIncomplete('API pública no devolvió un listado exitoso')
                result = data['result']
                rows = result.get('registros')
                if not isinstance(rows,list):
                    raise ListingIncomplete('Respuesta sin registros verificables')
                consecutive_errors = 0
                effective = min(size,int(result.get('registrosPorPagina') or size))
                if len(rows) < effective:
                    for row in rows:
                        if not row.get('numProceso') or not row.get('idProcesosContratacionFlujos') or int(row.get('idEstado',-1))!=state:
                            raise ListingIncomplete('Registro incompleto o de otro estado en API')
                    for row in rows: records[row['numProceso']]=row
                    windows.append({'start':iso(start),'end':iso(end),'rows':len(rows)})
                    log('COVERAGE', f'API estado={state} | {start.date()} -> {end.date()} | registros={len(rows)}')
                    return
                error = f'Ventana saturada: {len(rows)} registros'
                break
            except Exception as exc:
                consecutive_errors += 1
                error = f'{type(exc).__name__}: {exc}'
                if attempt < 2: time.sleep(1.5*(2**attempt))
        if end-start <= timedelta(seconds=1) or depth >= 24:
            failures.append({'start':iso(start),'end':iso(end),'error':error})
            return
        # Boundaries at millisecond precision, no uncovered gap.
        midpoint=start+(end-start)/2
        midpoint=midpoint.replace(microsecond=(midpoint.microsecond//1000)*1000)
        log('COVERAGE', f'API divide ventana: {start.date()} -> {end.date()} ({error})')
        fetch(start,midpoint,depth+1)
        fetch(midpoint+timedelta(milliseconds=1),end,depth+1)

    # CL Programadas uses future opening dates in this API filter. A cutoff
    # at "today" would silently remove most of the scheduled opportunities.
    fetch(datetime(2000,1,1,tzinfo=timezone.utc),datetime(2100,1,1,tzinfo=timezone.utc))
    urls=[]
    for record in records.values():
        payload={'i':int(record['idProcesosContratacionFlujos']),'tp':int(record['idTipoProceso'])}
        if record.get('rutaNueva') is not None: payload['rn']=record['rutaNueva']
        token=base64.b64encode(json.dumps(payload,separators=(',',':')).encode()).decode().rstrip('=')[::-1]
        route='solicitud-de-cotizacion' if record.get('prefijo')=='CL' else 'pliego-de-cargos'
        url=f"https://www.panamacompra.gob.pa/Inicio/#/{route}/{record['numProceso']}/{token}"
        urls.append(url)
        page_tools.adjudication_by_url[normalize_url(url)]=record.get('nombreModalidad') or ''
    # A successful API pass defines the current state; a partial one only adds
    # evidence and must not erase already captured UI links.
    if failures:
        combined={process_key(u):u for u in listing['links']}
        combined.update({process_key(u):u for u in urls})
        urls=list(combined.values())
    log('COVERAGE',f"Recuperación API: {len(urls)} actos únicos; ventanas pendientes={len(failures)}")
    return {**listing,'links':urls,'complete':not failures,
            'error':f'{len(failures)} ventanas API pendientes' if failures else '',
            'ui_error':listing['error'],'recovery_windows':windows,'failed_windows':failures,
            'source':'API pública tras paginación parcial'}


SNAPSHOT_JS = """
const root = document.querySelector('tabla-busqueda-avanzada-v3');
if (!root) return null;
const state = label => {
  const el = root.querySelector('ul.pagination a[aria-label="' + label + '"]');
  return !el ? null : (el.getAttribute('aria-disabled') === 'true' ||
    el.classList.contains('disabled') || el.closest('li').classList.contains('disabled'));
};
return {text:root.innerText, next_disabled:state('Next'), first_disabled:state('First'),
  links:Array.from(document.querySelectorAll(arguments[0])).map(a=>a.href || a.getAttribute('data-uw-original-href')).filter(Boolean),
  rows:Array.from(root.querySelectorAll('table tbody tr')).map(r=>r.innerText),
  states:Array.from(root.querySelectorAll('table tbody tr')).map(r=>(r.cells[2]?.innerText || '').trim())};
"""


class ListingIncomplete(RuntimeError):
    pass


def collect_listing(page_tools, css, log, *, expected_state=None, timeout=35, settle=1.0, max_pages=10000):
    """Click Next once; wait for BOTH a new page number and its actual rows.

    The portal lazily extends `Pagina 2 / 2 +`. Only an explicitly disabled
    Next, without a '+' indicator, confirms the last page. Never use x >= y.
    Partial captures are retained and reported, never presented as complete.
    """
    driver = page_tools.d
    links, seen, pages = [], set(), []

    def wait_page(expected, previous=()):
        until, candidate, since = time.monotonic() + timeout, None, 0
        while time.monotonic() < until:
            page_tools.close_popup()
            raw = driver.execute_script(SNAPSHOT_JS, css)
            if raw:
                raw.update(parse_pager(raw["text"]))
                fingerprint = tuple(process_key(u) for u in raw["links"])
                empty = bool(re.search(r"(?:no se encontraron|sin registros|no hay registros|0 registros)", raw['text'], re.I))
                valid = raw["page"] == expected and bool(fingerprint)
                valid = valid and (not previous or fingerprint != previous)
                if expected_state and (not raw.get('states') or any(s.casefold()!=expected_state.casefold() for s in raw['states'])):
                    valid = False
                # A size change can leave the old ten rows briefly visible.
                if raw["next_disabled"] is False and len(fingerprint) < 50:
                    valid = False
                if expected == 1 and not fingerprint and empty:
                    raw.update(page=1, pages=1, total=0, next_disabled=True, more=False, total_more=False)
                    valid = True
                signature = (raw["page"], fingerprint, raw["next_disabled"], raw["text"])
                if valid:
                    if signature != candidate:
                        candidate, since = signature, time.monotonic()
                    elif time.monotonic() - since >= settle:
                        return raw
                else:
                    candidate = None
            time.sleep(0.15)
        raise ListingIncomplete(f"No se confirmó la página {expected} y sus renglones en {timeout}s")

    try:
        # Fresh navigation should start at one; explicitly reset retained UI state.
        raw = driver.execute_script(SNAPSHOT_JS, css)
        state_matches = raw and (not expected_state or (raw.get('states') and all(s.casefold()==expected_state.casefold() for s in raw['states'])))
        if state_matches and parse_pager(raw['text'])["page"] not in (None, 1):
            driver.execute_script("document.querySelector('tabla-busqueda-avanzada-v3 a[aria-label=First]').click()")
        current = wait_page(1)
        for expected in range(1, max_pages + 1):
            fingerprint = tuple(process_key(u) for u in current['links'])
            # Keep each scraper's modality extraction; this call must match the snapshot.
            captured = page_tools.collect_links()
            if tuple(process_key(u) for u in captured) != fingerprint:
                raise ListingIncomplete(f"La tabla cambió durante la captura de página {expected}")
            added = 0
            for url in captured:
                key = process_key(url)
                if key not in seen:
                    seen.add(key)
                    links.append(url)
                    added += 1
            pages.append({k: current[k] for k in ('page', 'pages', 'more', 'total', 'total_more')})
            pages[-1].update(rows=len(captured), added=added, first=process_key(captured[0]) if captured else '', last=process_key(captured[-1]) if captured else '')
            log('PAGE', f"página={expected} | registros de esta página={len(captured)} | nuevos={added} | acumulados={len(links)} | más páginas={'sí' if current['next_disabled'] is False else 'no/por verificar'}")
            if current['next_disabled'] is True:
                if current['more'] or current['total_more']:
                    # Angular may disable Next while fetching the next batch.
                    until = time.monotonic() + timeout
                    while time.monotonic() < until:
                        time.sleep(0.25)
                        updated = driver.execute_script(SNAPSHOT_JS, css)
                        if not updated:
                            continue
                        updated.update(parse_pager(updated['text']))
                        if updated['next_disabled'] is False or not (updated['more'] or updated['total_more']):
                            current = wait_page(expected)
                            break
                    else:
                        raise ListingIncomplete('Siguiente deshabilitado con total todavía parcial (+) después de esperar la carga')
                if current['next_disabled'] is False:
                    if not page_tools.click_next():
                        raise ListingIncomplete('No se pudo continuar después de cargar el siguiente lote')
                    current = wait_page(expected + 1, fingerprint)
                    continue
                if current['total'] is not None and len(links) != current['total']:
                    raise ListingIncomplete(f"Total oficial={current['total']}; actos únicos capturados={len(links)}")
                return {'links': links, 'pages': pages, 'complete': True, 'error': ''}
            if current['next_disabled'] is None:
                raise ListingIncomplete('No se encontró el control Siguiente; no equivale a final del listado')
            if not page_tools.click_next():
                raise ListingIncomplete('No se pudo avanzar a la siguiente página')
            current = wait_page(expected + 1, fingerprint)
        raise ListingIncomplete(f'Límite de seguridad de {max_pages} páginas alcanzado')
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'
        log('COVERAGE', f'CAPTURA PARCIAL: {error}; se conservan {len(links)} enlaces')
        return {'links': links, 'pages': pages, 'complete': False, 'error': error}


def verify_detail(driver, link, info):
    """Do not persist an error screen or a previous Angular detail as an act."""
    expected = process_key(link)
    text = driver.execute_script('return document.body.innerText') or ''
    if PROCESS.fullmatch(expected) and expected not in text:
        raise ListingIncomplete(f'El detalle no corresponde al acto {expected}')
    for field in ('titulo', 'entidad'):
        value = str(info.get(field) or '').strip().casefold()
        if value in {'', 'no disponible', 'none', 'null'}:
            raise ListingIncomplete(f'Detalle incompleto: falta {field} en {expected}')


def technical_discard(row):
    return any(str(v).strip() == 'skip_timeout_xpath' for v in row)


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(path)


class DetailCoverage:
    """A failure is a durable retry, never a commercial discard.

    Pending links absent from today's listing remain recorded. They are not
    inserted into an active category without confirmation from that listing.
    Successful entries are cleared only after the scraper publishes its output.
    """
    def __init__(self, directory, job, listing):
        self.directory, self.job, self.listing = Path(directory), job, listing
        self.path = self.directory / (job + '_pending_details.json')
        self.pending = json.loads(self.path.read_text(encoding='utf-8')) if self.path.exists() else {}
        self.attempted, self.succeeded, self.failed = set(), set(), {}
        self.current = {process_key(u) for u in listing['links']}
        self.started = datetime.now(timezone.utc).isoformat()
        _write_json(self.directory / (job + '_last_listing.json'), {**listing, 'started': self.started})

    def failure(self, link, error):
        key = process_key(link)
        self.attempted.add(key)
        self.failed[key] = str(error)
        self.pending[key] = {'url': link, 'last_error': str(error), 'last_attempt': datetime.now(timezone.utc).isoformat(), 'attempts': int(self.pending.get(key, {}).get('attempts', 0)) + 1}
        _write_json(self.path, self.pending)

    def success(self, link):
        key = process_key(link)
        self.attempted.add(key)
        self.succeeded.add(key)
        self.failed.pop(key, None)

    def finish(self):
        for key in self.succeeded:
            self.pending.pop(key, None)
        _write_json(self.path, self.pending)
        report = {**self.listing, 'job': self.job, 'started': self.started,
                  'finished': datetime.now(timezone.utc).isoformat(),
                  'attempted': len(self.attempted), 'succeeded': len(self.succeeded),
                  'failed': self.failed, 'pending': self.pending}
        _write_json(self.directory / (self.job + '_last_run.json'), report)
        summary = {k: report[k] for k in ('job', 'complete', 'error', 'attempted', 'succeeded')}
        summary.update(listed=len(self.current), failed=len(self.failed), pending=len(self.pending))
        print('SCRAPE_COVERAGE_JSON=' + json.dumps(summary, ensure_ascii=False), flush=True)


def coverage_problem(stdout):
    """The orchestrator still delivers valid alerts, then reports partial runs."""
    for line in reversed(stdout.splitlines()):
        if line.startswith('SCRAPE_COVERAGE_JSON='):
            data = json.loads(line.split('=', 1)[1])
            if not data['complete'] or data['failed']:
                return f"Captura parcial: {data['listed']} enlaces; {data['failed']} detalles pendientes. {data['error']}".strip()
            return ''
    return ''
