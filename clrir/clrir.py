# -*- coding: utf-8 -*-  # Codificación del archivo en UTF-8 para caracteres latinos
# clv.py — compacto, robusto, con logs claros y totalmente comentado línea por línea
# Ajustes en esta versión:
# - XPaths más robustos (contains + fallback) para asegurar captura de precio/otros campos
# - Sin items_descripcion; se exporta 'descripcion' (general) justo después de los checkboxes
# - Ítems dinámicos: item_1..item_n con el texto real por ítem
# - Orden de columnas: ... ficha_detectada, Prioritario, Descartar, descripcion, item_1..item_n
# - Mantiene toda la operativa (paginación, purga, CT/SR/RS/meds, checkboxes, fechas)

import sys, re, time, unicodedata
from datetime import datetime
from pathlib import Path
import os
from urllib.parse import urlparse, urlunparse, unquote
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"


def _resolve_credentials_file() -> Path:
    env_path = os.environ.get("CLRIR_SERVICE_ACCOUNT_FILE")
    if env_path:
        env_candidate = Path(env_path).expanduser()
        if env_candidate.exists():
            return env_candidate

    legacy_candidate = Path(
        r"C:\Users\rodri\cl\serious-app-417920-eed299fa06b5.json"
    )
    if legacy_candidate.exists():
        return legacy_candidate

    return REPO_ROOT / "credentials" / "service-account.json"


CREDENTIALS_FILE = _resolve_credentials_file()

COMMON_DIR = REPO_ROOT / "common"
if str(COMMON_DIR) not in sys.path:
    sys.path.append(str(COMMON_DIR))

from ficha_utils import detectar_fichas_tokens

# =========================
# CONFIGURACIÓN (CFG)
# =========================
CFG = {
    # ---- Rutas locales (archivos Excel con listas auxiliares) ----
    "xlsx_todas": str(DATA_DIR / "fichas" / "todas_las_fichas.xlsx"),
    "xlsx_con_req": str(DATA_DIR / "fichas" / "fichas_con_requisitos.xlsx"),
    "xlsx_sin_req": str(DATA_DIR / "fichas" / "fichas_sin_requisitos.xlsx"),
    "xlsx_con_ct": str(DATA_DIR / "clrir" / "fichas_con_CT_sin_RS.xlsx"),
    "xlsx_con_rs": str(DATA_DIR / "clrir" / "fichas_con_RS.xlsx"),
    "xlsx_palabras": str(DATA_DIR / "fichas" / "Palabras_Organizadas.xlsx"),
    "xlsx_meds": str(DATA_DIR / "references" / "lista_medicamentos.xlsx"),

    # ---- Google Sheets ----
    "svc_key": str(CREDENTIALS_FILE),
    "spreadsheet_id": "17hOfP-vMdJ4D7xym1cUp7vAcd8XJPErpY3V-9Ui2tCo",
    "sheets_data": ["cl_prog_sin_ficha", "cl_prog_sin_requisitos", "cl_prog_con_ct", "cl_prioritarios"],
    "sheet_desc": "cl_descartes",
    "sheet_prio": "cl_prioritarios",

    # ---- Web (listado y selectores) ----
    "url_list": "https://www.panamacompra.gob.pa/Inicio/#/cotizaciones-en-linea/cotizaciones-en-linea",
    "host": "www.panamacompra.gob.pa",
    "css_links": (
        "tabla-busqueda-avanzada-v3 table tbody "
        "a[href*='/solicitud-de-cotizacion/'], "
        "tabla-busqueda-avanzada-v3 table tbody "
        "a[data-uw-original-href*='/solicitud-de-cotizacion/']"
    ),

    # ---- XPaths del detalle (ficha) ----
    # Aceptan lista de alternativas para mayor robustez
    "xpath_map": {
        "titulo": [
            "//th[normalize-space()='Título']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Título')]/following-sibling::td[1]",
        ],
        "precio": [
            "//th[normalize-space()='Precio estimado']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Precio estimado')]/following-sibling::td[1]",
        ],
        "fecha": [
            "//th[contains(normalize-space(),'Fecha y hora presentación')]/following-sibling::td[1]"
        ],
        "entidad": [
            "//th[normalize-space()='Entidad']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Entidad')]/following-sibling::td[1]",
        ],
        "termino": [
            "//th[normalize-space()='Término de entrega']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Término de entrega')]/following-sibling::td[1]",
        ],
        "unidad": [
            "//th[normalize-space()='Unidad de Compra']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Unidad de Compra')]/following-sibling::td[1]",
        ],
        # Ítems: columna "Descripción" por fila en la tabla de rubros
        "items": [
            "//field-rubros//tbody/tr/td[2][contains(@class,'text-prewrap')]",
            "//field-rubros//tbody//td[@class='text-lg-break text-prewrap']"
        ],
        "desc": [
            "//th[normalize-space()='Descripción']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Descripción')]/following-sibling::td[1]",
        ],
        "pub": [
            "//th[normalize-space()='Fecha de Publicación']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Fecha de Publicación')]/following-sibling::td[1]",
        ],
    },

    "precio_min": 1000.0,  # Umbral de descarte por precio
}

def LOG(tag, msg):
    print(datetime.now().strftime("%H:%M:%S.%f")[:-3], f"| [{tag}] {msg}")
    sys.stdout.flush()

# =========================
# TEXTO / URL (utilidades)
# =========================
def _norm_txt(s):
    s = "" if s is None else str(s)
    s = "".join(ch for ch in unicodedata.normalize("NFD", s.lower().strip()) if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", s)

def normalize_url(u: str) -> str:
    if not u: return ""
    u = u.strip()
    if u.startswith("/"): u = f"https://{CFG['host']}{u}"
    p = urlparse(u)
    netloc = (p.netloc or CFG["host"]).lower()
    if netloc.endswith("panamacompra.gob.pa") and not netloc.startswith("www."): netloc = "www." + netloc
    path = re.sub(r"/{2,}", "/", unquote(p.path)).rstrip("/") or "/"
    frag = re.sub(r"/{2,}", "/", unquote(p.fragment)).rstrip("/").lower()
    return urlunparse(("https", netloc, path, "", "", frag))

# =========================
# CARGA DE LISTAS AUXILIARES
# =========================
def load_list_col(path):
    return pd.read_excel(path, usecols=[0], header=None).squeeze().dropna().astype(str).tolist()

def load_meds(path):
    meds_series = pd.read_excel(path, usecols=[0], header=None).squeeze().dropna().astype(str)
    meds = {_norm_txt(x) for x in meds_series if x and " " not in _norm_txt(x)}
    meds -= {"medicamento", "medicamentos", "producto", "productos"}
    LOG("MEDS", f"{len(meds)} términos 1-palabra")
    return meds

TODAS = load_list_col(CFG["xlsx_todas"])
FICHAS_CON_REQ = load_list_col(CFG["xlsx_con_req"])
FICHAS_SIN_REQ = load_list_col(CFG["xlsx_sin_req"])
FICHAS_CON_CT  = load_list_col(CFG["xlsx_con_ct"])
FICHAS_CON_RS  = load_list_col(CFG["xlsx_con_rs"])
PAL = pd.read_excel(CFG["xlsx_palabras"])
PAL_CONS = PAL['Palabras de Construcción'].dropna().tolist()
PAL_MED  = PAL['Palabras Médicas'].dropna().tolist()
PAL_OTRAS= PAL['Otras Palabras'].dropna().tolist()
MEDS = load_meds(CFG["xlsx_meds"])

# =========================
# GOOGLE SHEETS (mini SDK)
# =========================
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

creds = Credentials.from_service_account_file(CFG["svc_key"])
GSVC = build('sheets','v4',credentials=creds)
SSID = CFG["spreadsheet_id"]

GS_RATE_COOLDOWN = 1.2  # segundos entre lecturas para no sobrepasar 60 rpm
_GS_CACHE = {}
_LAST_GS_CALL = 0.0


def _normalize_range_name(rng):
    if not rng:
        return rng
    rng = rng.strip()
    if "!" not in rng:
        return rng.replace("'", "").replace('"', "")
    sheet, rest = rng.split("!", 1)
    sheet = sheet.strip().strip("'\"")
    return f"{sheet}!{rest}"


def _format_range(sheet, rng="A1:ZZ"):
    sheet = sheet.strip()
    if not sheet:
        return rng
    if " " in sheet and not sheet.startswith("'"):
        sheet = f"'{sheet}'"
    return f"{sheet}!{rng}"


def _throttle_google_calls():
    global _LAST_GS_CALL
    now = time.perf_counter()
    wait = GS_RATE_COOLDOWN - (now - _LAST_GS_CALL)
    if wait > 0:
        time.sleep(wait)
    _LAST_GS_CALL = time.perf_counter()


def _exec_with_retry(label, func, retries=4):
    for attempt in range(retries):
        try:
            _throttle_google_calls()
            return func()
        except HttpError as exc:
            status = getattr(exc.resp, "status", None)
            if status == 429 and attempt < retries - 1:
                backoff = min(10, (attempt + 1) * 2)
                LOG("SHEETS", f"{label}: cuota alcanzada, reintento en {backoff:.1f}s")
                time.sleep(backoff)
                continue
            raise


def gs_get(rng, *, use_cache=True):
    key = _normalize_range_name(rng)
    if use_cache and key in _GS_CACHE:
        return _GS_CACHE[key]

    def _action():
        return (
            GSVC.spreadsheets()
            .values()
            .get(spreadsheetId=SSID, range=rng)
            .execute()
            .get("values", [])
        )

    values = _exec_with_retry(f"get {key}", _action)
    if use_cache:
        _GS_CACHE[key] = values
    return values


def gs_batch_get(ranges):
    normalized = [_normalize_range_name(r) for r in ranges]
    pending = []
    results = {}
    for raw, key in zip(ranges, normalized):
        if key in _GS_CACHE:
            results[key] = _GS_CACHE[key]
        else:
            pending.append((raw, key))
    if pending:
        query_ranges = [raw for raw, _ in pending]

        def _action():
            return (
                GSVC.spreadsheets()
                .values()
                .batchGet(spreadsheetId=SSID, ranges=query_ranges)
                .execute()
            )

        response = _exec_with_retry(f"batchGet {len(query_ranges)}", _action)
        returned = set()
        for rng_values in response.get("valueRanges", []):
            key = _normalize_range_name(rng_values.get("range"))
            values = rng_values.get("values", [])
            _GS_CACHE[key] = values
            results[key] = values
            returned.add(key)
        for _, key in pending:
            if key not in returned:
                _GS_CACHE[key] = []
                results[key] = []
    return results


def _invalidate_cache():
    _GS_CACHE.clear()


def gs_update(rng, values):
    def _action():
        return (
            GSVC.spreadsheets()
            .values()
            .update(
                spreadsheetId=SSID,
                range=rng,
                valueInputOption='RAW',
                body={'values': values},
            )
            .execute()
        )

    result = _exec_with_retry(f"update {rng}", _action)
    _invalidate_cache()
    return result


def gs_append(sheet, rows):
    if not rows:
        return None

    def _action():
        return (
            GSVC.spreadsheets()
            .values()
            .append(
                spreadsheetId=SSID,
                range=f"{sheet}!A1",
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': rows},
            )
            .execute()
        )

    result = _exec_with_retry(f"append {sheet}", _action)
    _invalidate_cache()
    LOG("SHEETS", f"{sheet}: +{len(rows)} filas")
    return result


def gs_meta():
    def _action():
        return GSVC.spreadsheets().get(spreadsheetId=SSID).execute().get("sheets", [])

    return _exec_with_retry("meta", _action)

def gs_sheet_id(title):
    for s in gs_meta():
        if s["properties"]["title"] == title:
            return s["properties"]["sheetId"]
    return None

def find_idx(headers, name):
    if not headers: return None
    try:
        return [str(h).strip().lower() for h in headers].index(name.strip().lower())
    except ValueError:
        return None

def _col_letter(idx):
    res = ""
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        res = chr(65 + rem) + res
    return res or "A"


def ensure_header(sheet, header):
    rows = gs_get(f"{sheet}!A1:ZZ")
    if not rows:
        gs_update(f"{sheet}!A1", [header])
        LOG("SHEETS", f"{sheet}: header asegurado")
        return

    existing = [str(x).strip() for x in rows[0]]
    existing_lower = [x.lower() for x in existing]

    target = []
    seen = set()
    for col in header:
        key = str(col).strip().lower()
        if key and key not in seen:
            target.append(str(col))
            seen.add(key)

    for col, key in zip(existing, existing_lower):
        if key and key not in seen:
            target.append(col)
            seen.add(key)

    if [c.lower() for c in existing] == [c.lower() for c in target]:
        return

    mapping = {key: idx for idx, key in enumerate(existing_lower)}
    width = len(target)
    new_rows = [target]
    for row in rows[1:]:
        padded = list(row) + [''] * max(len(existing) - len(row), 0)
        new_row = [''] * width
        for idx, col in enumerate(target):
            key = str(col).strip().lower()
            src = mapping.get(key)
            if src is not None and src < len(padded):
                new_row[idx] = padded[src]
        new_rows.append(new_row)

    rng = f"{sheet}!A1:{_col_letter(width)}{len(new_rows)}"
    gs_update(rng, new_rows)
    LOG("SHEETS", f"{sheet}: header reordenado y alineado")

def delete_rows(sheet, rows_1b):
    sid = gs_sheet_id(sheet)
    if sid is None or not rows_1b:
        return
    req = [{
        "deleteDimension": {
            "range": {"sheetId": sid, "dimension": "ROWS", "startIndex": r - 1, "endIndex": r}
        }
    } for r in sorted({r for r in rows_1b if r >= 2}, reverse=True)]
    if req:
        GSVC.spreadsheets().batchUpdate(spreadsheetId=SSID, body={"requests": req}).execute()
        LOG("SHEETS", f"{sheet}: -{len(req)} filas")

def reset_checkboxes(sheet):
    vals = gs_get(f"{sheet}!A1:ZZ")
    if not vals:
        LOG("CHK", f"{sheet}: vacía")
        return

    hdr = vals[0]
    iP, iD = find_idx(hdr, 'prioritario'), find_idx(hdr, 'descartar')
    if iP is None or iD is None:
        LOG("CHK", f"{sheet}: faltan columnas")
        return

    sid = gs_sheet_id(sheet)
    last = len(vals)  # filas totales actuales (incluye encabezado)

    # Si solo hay encabezado, no hay filas a las que aplicar validación
    if last <= 1:
        LOG("CHK", f"{sheet}: solo encabezado; omito dataValidation por ahora")
        return

    # Regla de checkbox
    rule = {"condition": {"type": "BOOLEAN"}, "showCustomUi": True}

    # Aplica la validación solo sobre las filas existentes (2..last)
    reqs = [
        {"repeatCell": {
            "range": {
                "sheetId": sid,
                "startRowIndex": 1,           # fila 2 (base 0)
                "endRowIndex": last,          # última fila existente
                "startColumnIndex": iP,
                "endColumnIndex": iP + 1
            },
            "cell": {"dataValidation": rule},
            "fields": "dataValidation"
        }},
        {"repeatCell": {
            "range": {
                "sheetId": sid,
                "startRowIndex": 1,
                "endRowIndex": last,
                "startColumnIndex": iD,
                "endColumnIndex": iD + 1
            },
            "cell": {"dataValidation": rule},
            "fields": "dataValidation"
        }},
    ]

    GSVC.spreadsheets().batchUpdate(spreadsheetId=SSID, body={"requests": reqs}).execute()
    LOG("CHK", f"{sheet}: checkboxes 2..{last}")


def update_fechas_sheet(sheet):
    vals = gs_get(f"{sheet}!A1:ZZ")
    if not vals: return
    n = len(vals) - 1
    if n <= 0: return
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rng = f"{sheet}!A2:A{n+1}"
    gs_update(rng, [[ts] for _ in range(n)])
    LOG("SHEETS", f"{sheet}: Fecha de Actualización (todas) en {n} filas ({rng})")

def read_links_from_sheets(sheets):
    all_links = set()
    by = {}
    ranges = [_format_range(sh) for sh in sheets]
    values_map = gs_batch_get(ranges)
    for sh, rng in zip(sheets, ranges):
        vals = values_map.get(_normalize_range_name(rng), [])
        if not vals:
            by[sh] = []
            LOG("SHEETS", f"{sh}: vacía")
            continue
        i = find_idx(vals[0], 'enlace')
        if i is None:
            by[sh] = []
            LOG("SHEETS", f"{sh}: sin 'enlace'")
            continue
        links = [r[i].strip() for r in vals[1:] if i < len(r) and r[i].strip()]
        by[sh] = links
        all_links.update(links)
        LOG("SHEETS", f"{sh}: {len(links)} enlaces")
    LOG("SHEETS", f"únicos totales={len(all_links)}")
    return by, all_links

def move_rows_by_checkbox(sources, target, col_chk):
    vals_t = gs_get(f"{target}!A1:ZZ")
    hdr_t = vals_t[0] if vals_t else None
    if hdr_t is None and sources:
        vals0 = gs_get(f"{sources[0]}!A1:1")
        hdr_t = vals0[0] if vals0 else []
        ensure_header(target, hdr_t)
    idx_t_enlace = find_idx(hdr_t, 'enlace') if hdr_t else None
    existing = set()
    if hdr_t and vals_t:
        for r in vals_t[1:]:
            if idx_t_enlace is not None and idx_t_enlace < len(r) and r[idx_t_enlace]:
                existing.add(normalize_url(r[idx_t_enlace].strip()))
    total = 0
    for src in sources:
        vals = gs_get(f"{src}!A1:ZZ")
        if not vals:
            continue
        hdr = vals[0]
        iE, iC = find_idx(hdr, 'enlace'), find_idx(hdr, col_chk)
        if iE is None or iC is None:
            continue
        to_app, to_del = [], []
        for r1, row in enumerate(vals[1:], start=2):
            if iC < len(row) and str(row[iC]).strip().upper() == 'TRUE':
                raw = row[iE].strip()
                key = normalize_url(raw)
                if key in existing:
                    to_del.append(r1)
                    continue
                out = (row + [''] * (len(hdr_t) - len(row)))[:len(hdr_t)]
                if idx_t_enlace is not None: out[idx_t_enlace] = raw
                to_app.append(out)
                to_del.append(r1)
                existing.add(key)
        gs_append(target, to_app)
        delete_rows(src, to_del)
        total += len(to_app)
        LOG("MOVE", f"{src} → {target}: {len(to_app)}")
    LOG("MOVE", f"Total movidas a {target}: {total}")

def purge_by_fecha(sheet):
    vals = gs_get(f"{sheet}!A1:ZZ")
    if not vals:
        LOG("PURGE", f"{sheet}: vacía")
        return
    hdr = vals[0]
    iF = find_idx(hdr, 'fecha')
    if iF is None:
        LOG("PURGE", f"{sheet}: SIN 'fecha'")
        return
    ahora, dels, ok, fail = pd.Timestamp.now(), [], 0, 0
    def parse_fecha(s):
        if s is None or not str(s).strip(): return pd.NaT
        t = re.sub(r"\s+a\s+", " a ", str(s).replace("–", "-").replace("—", "-"), flags=re.I)
        md = re.findall(r"\b(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b", t)
        if not md: return pd.to_datetime(t, dayfirst=True, errors="coerce")
        if len(md) >= 2:
            d = pd.to_datetime(md[-1], dayfirst=True, errors="coerce")
            return d + pd.Timedelta(hours=23, minutes=59) if pd.notna(d) else pd.NaT
        base = pd.to_datetime(md[0], dayfirst=True, errors="coerce")
        if pd.isna(base): return pd.NaT
        mt = list(re.finditer(r"\b(\d{1,2})(?::(\d{2}))?\s*([APap]\.?M\.?)?\b", t))
        if mt:
            hh = int(mt[-1].group(1)); mm = int(mt[-1].group(2) or 0); ap = (mt[-1].group(3) or "").replace(".","").upper()
            if ap == "AM" and hh == 12: hh = 0
            if ap == "PM" and hh != 12: hh = (hh % 12) + 12
            return base + pd.Timedelta(hours=hh, minutes=mm)
        return base + pd.Timedelta(hours=23, minutes=59)
    for r1, row in enumerate(vals[1:], start=2):
        if iF < len(row):
            dt = parse_fecha(row[iF])
            ok += int(pd.notna(dt)); fail += int(pd.isna(dt))
            if pd.notna(dt) and dt < ahora:
                dels.append(r1)
    delete_rows(sheet, dels)
    LOG("PURGE", f"{sheet}: actos={len(vals)-1} | ok={ok} | fail={fail} | borrados={len(dels)}")

def purge_all():
    for s in ["cl_prog_sin_ficha","cl_prog_sin_requisitos","cl_prog_con_ct","cl_prioritarios","cl_descartes"]:
        purge_by_fecha(s)


# =========================
# SELENIUM (navegación)
# =========================
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import *
import subprocess

def start_browser():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,768")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--log-level=3")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    srv = Service(ChromeDriverManager().install(), log_output=subprocess.DEVNULL)
    return webdriver.Chrome(service=srv, options=opts)

class PageTools:
    def __init__(self, driver): self.d = driver
    def js_count(self, css):
        try: return self.d.execute_script("return document.querySelectorAll(arguments[0]).length;", css)
        except JavascriptException: return -1
    def wait_css(self, css, to=30):
        return WebDriverWait(self.d, to).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, css)))
    def find_css(self, css): return self.d.find_elements(By.CSS_SELECTOR, css)
    def a_url(self, a):
        try:
            u = a.get_attribute("href") or a.get_attribute("data-uw-original-href")
            return ("https://" + CFG["host"] + u) if (u and u.startswith("/")) else u
        except StaleElementReferenceException:
            return None
    def page_xy(self):
        try:
            el = self.d.find_element(By.XPATH, "//div[contains(@class,'card-body')]//small[contains(.,'Pagina') or contains(.,'Página')]")
            m = re.search(r'(\d+)\s*/\s*(\d+)', el.text)
            return (int(m.group(1)), int(m.group(2))) if m else (None, None)
        except Exception:
            return (None, None)
    def tbody_ref(self):
        try: return self.d.find_element(By.XPATH, "//tabla-busqueda-avanzada-v3//table/tbody")
        except Exception: return None
    def click_next(self):
        try:
            nxt = self.d.find_element(By.XPATH, "//ul[contains(@class,'pagination')]//a[@aria-label='Next']")
            li = nxt.find_element(By.XPATH, "./ancestor::li[1]")
            disabled = ("disabled" in (nxt.get_attribute("class") or "").lower()) or \
                       ("disabled" in (li.get_attribute("class") or "").lower()) or \
                       ((nxt.get_attribute("aria-disabled") or "").lower() == "true")
            LOG("PAGE", f"Next disabled={disabled}")
            if disabled: return False
            try: self.d.execute_script("arguments[0].scrollIntoView();", nxt); time.sleep(0.15); self.d.execute_script("window.scrollBy(0,-160);")
            except: pass
            try: nxt.click()
            except ElementClickInterceptedException: self.d.execute_script("arguments[0].click();", nxt)
            return True
        except NoSuchElementException:
            LOG("PAGE", "No Next."); return False
    def collect_links(self):
        anchors = self.find_css(CFG["css_links"])
        LOG("CAP", f"anchors={len(anchors)} | js={self.js_count(CFG['css_links'])}")
        return [self.a_url(a) for a in anchors if self.a_url(a)]

# =========================
# REGLAS / SCRAPE (detalle)
# =========================
def precio_num(s):
    # Soporta "B/. 224.47", "1.234,56", "224.47"
    txt = str(s) if s is not None else ""
    m = re.search(r'([0-9]{1,3}(?:[.,][0-9]{3})*[.,][0-9]{2}|[0-9]+(?:[.,][0-9]+)?)', txt)
    if not m: return None
    val = m.group(1)
    # normaliza: quita separador de miles y usa punto decimal
    if "," in val and "." in val:
        if val.rfind(",") > val.rfind("."):  # formato 1.234,56
            val = val.replace(".", "").replace(",", ".")
        else:  # formato 1,234.56
            val = val.replace(",", "")
    else:
        val = val.replace(",", ".")
    try: return float(val)
    except: return None

def is_medic(t):
    tt = _norm_txt(t)
    for w in MEDS:
        if re.search(rf'(?<![\w\d]){re.escape(w)}(?![\w\d])', tt):
            return w
    return None

def want_descartar(info):
    p = precio_num(info.get('precio_referencia', ''))
    if p is not None and p < CFG["precio_min"]:
        return True, "precio"
    med = is_medic(info.get('mix', ''))
    if med:
        return True, f"med:{med}"
    fichas_base = info.get('fichas_base') or []
    if any(code in FICHAS_CON_RS for code in fichas_base):
        return True, "RS"
    return False, ""

# Helper robusto: acepta que xp[key] sea str o lista de xpaths
def _first_text_by_xpaths(driver, xps, default="No Disponible"):
    if isinstance(xps, str): xps = [xps]
    for xp in xps:
        els = driver.find_elements(By.XPATH, xp)
        if els:
            txt = els[0].text.strip()
            if txt: return txt
    return default

def scrape(page: PageTools, link: str):
    xp = CFG["xpath_map"]
    page.d.get(link)
    page.d.refresh()
    # Espera a que aparezca el precio (cualquier variante)
    WebDriverWait(page.d, 25).until(
        EC.presence_of_element_located((By.XPATH, xp["precio"][0]))
    )

    titulo  = _first_text_by_xpaths(page.d, xp["titulo"])
    precio  = _first_text_by_xpaths(page.d, xp["precio"])
    fecha   = _first_text_by_xpaths(page.d, xp["fecha"])
    entidad = _first_text_by_xpaths(page.d, xp["entidad"])
    termino = _first_text_by_xpaths(page.d, xp["termino"])
    unidad  = _first_text_by_xpaths(page.d, xp.get("unidad", []), default="")
    desc    = _first_text_by_xpaths(page.d, xp["desc"])
    pub     = _first_text_by_xpaths(page.d, xp["pub"])

    # Ítems: lista de descripciones por fila
    items = []
    for xpath in (xp["items"] if isinstance(xp["items"], list) else [xp["items"]]):
        els = page.d.find_elements(By.XPATH, xpath)
        if els:
            items = [e.text.strip() for e in els]
            break

    publ = (pub.split(' -')[0]) if pub != "No Disponible" else pub
    mix = " ".join([titulo, desc] + items)

    # Clasificamos tokens asegurando primero los numéricos y luego los detectados por nombre con asterisco.
    tokens_detectados = detectar_fichas_tokens(mix)
    tokens_numericos = [tok for tok in tokens_detectados if not tok.startswith("*")]
    tokens_prefijados = [tok for tok in tokens_detectados if tok.startswith("*")]
    ficha_tokens = tokens_numericos + tokens_prefijados

    fichas_base = []
    for token in ficha_tokens:
        base = token.replace("*", "").strip()
        if base and base not in fichas_base:
            fichas_base.append(base)

    fd_ct = next((code for code in fichas_base if code in FICHAS_CON_CT), None)
    fd_sr = next((code for code in fichas_base if code in FICHAS_SIN_REQ), None)
    fd_rs = next((code for code in fichas_base if code in FICHAS_CON_RS), None)

    return {
        "publicacion": publ,
        "enlace": link,
        "titulo": titulo,
        "precio_referencia": precio,   # string original; se convierte luego a número
        "fecha": fecha,
    "entidad": entidad,
    "unidad solicitante": unidad,
    "termino_entrega": termino,
        "descripcion": desc,           # <-- descripción general (va tras checkboxes)
        "items": items,                # <-- lista de ítems
        "mix": mix,
    "has_ct": bool(fd_ct),
    "has_sr": bool(fd_sr),
    "has_rs": bool(fd_rs),
    "ficha_detectada": ", ".join(ficha_tokens) if ficha_tokens else "No Detectada",
    "fichas_base": fichas_base,
    }

def clasifica(info):
    return "rs" if info["has_rs"] else ("ct" if info["has_ct"] else ("sr" if info["has_sr"] else "sf"))

# =========================
# MAIN (flujo completo)
# =========================
def main():
    purge_all()

    _, all_links = read_links_from_sheets(CFG["sheets_data"])
    desc_vals = gs_get(f"{CFG['sheet_desc']}!A1:ZZ")
    descartes = {r[find_idx(desc_vals[0], 'enlace')].strip() for r in desc_vals[1:]} if desc_vals and find_idx(desc_vals[0], 'enlace') is not None else set()

    from selenium.common.exceptions import WebDriverException
    driver = start_browser()
    PT = PageTools(driver)

    # Navegación al listado con reintentos
    for i in range(5):
        try:
            driver.get(CFG["url_list"])
            LOG("NAV", f"intento {i+1}")
            break
        except WebDriverException:
            time.sleep(5)

    time.sleep(1.5)
    driver.execute_script("window.scrollBy(0,400)")
    time.sleep(0.3)

    # Pestaña "Programadass"
    boton_programadas = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, "//label[@for='btnradio2']")))
    driver.execute_script("arguments[0].click();", boton_programadas)
    time.sleep(0.2)

    # 50 por página
    select_registros = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, "//select[contains(@class,'form-select')]")))
    Select(select_registros).select_by_visible_text("50")
    time.sleep(0.3)

    WebDriverWait(driver, 30).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, CFG["css_links"])))

    links, seen = [], set()

    #page_counter = 0
    while True:
        WebDriverWait(driver, 20).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, CFG["css_links"])))
        urls = [u for u in PT.collect_links() if u]
        added = 0
        for u in urls:
            if u not in seen:
                seen.add(u); links.append(u); added += 1
        x, y = PT.page_xy()
        LOG("PAGE", f"+{added} | total={len(links)} | pag={x}/{y}" if x and y else f"+{added} | total={len(links)}")

##        page_counter += 1
##        if page_counter >= 1:
##            LOG("PAGE", "fin por límite de 1 página")
##        break

        last = bool(x and y and x >= y)
        next_disabled = False
        if not last:
            tbody_old = PT.tbody_ref()
            if not PT.click_next():
                next_disabled = True
            else:
                try:
                    if tbody_old is not None:
                        WebDriverWait(driver, 15).until(EC.staleness_of(tbody_old))
                    else:
                        WebDriverWait(driver, 15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, CFG["css_links"])))
                except:
                    time.sleep(1.0)

        if last or next_disabled:
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, CFG["css_links"])))
            urls = [u for u in PT.collect_links() if u]
            add2 = 0
            for u in urls:
                if u not in seen:
                    seen.add(u); links.append(u); add2 += 1
            LOG("PAGE", f"captura final: +{add2} | total={len(links)}")
            LOG("PAGE", "fin de paginación")
            break

    LOG("DONE", f"enlaces extraídos={len(links)}")
    try: driver.quit()
    except: pass

    # De-duplicación y filtro de nuevos
    map_key_raw = {}
    for u in links:
        k = normalize_url(u)
        if k and k not in map_key_raw: map_key_raw[k] = u
    existentes_norm = {normalize_url(x) for x in all_links}
    descartados_norm = {normalize_url(x) for x in descartes}
    nuevos = [k for k in map_key_raw if k not in existentes_norm and k not in descartados_norm]
    LOG("DONE", f"nuevos={len(nuevos)} | existentes={len(existentes_norm)} | descartes={len(descartados_norm)}")

    if not nuevos:
        move_rows_by_checkbox(['cl_prog_sin_ficha','cl_prog_sin_requisitos','cl_prog_con_ct'], CFG["sheet_prio"], "Prioritario")
        move_rows_by_checkbox(['cl_prog_sin_ficha','cl_prog_sin_requisitos','cl_prog_con_ct'], CFG["sheet_desc"], "Descartar")
        for sh in ['cl_prog_sin_ficha','cl_prog_sin_requisitos','cl_prog_con_ct']:
            reset_checkboxes(sh)
            update_fechas_sheet(sh)
        LOG("DONE", "sin nuevos; mantenimiento completo")
        return


    # === SCRAPE DETALLE (sólo para nuevos) ===
    driver = start_browser()
    PT = PageTools(driver)
    datos_ct, datos_sr, datos_sf, datos_rs = [], [], [], []
    for key in nuevos:
        link = map_key_raw[key]
        LOG("SCRAPE", link)
        try:
            info = scrape(PT, link)
        except TimeoutException:
            LOG("SCRAPE", "timeout")
            continue

        # Descarte por precio/RS/med
        desc, mot = want_descartar(info)
        if desc:
            if mot == "precio":
                monto = precio_num(info.get("precio_referencia", ""))
                LOG("SCRAPE", f"DESCARTADO por PRECIO (<{CFG['precio_min']}). precio={monto}")
            elif mot == "RS":
                LOG("SCRAPE", "DESCARTADO por FICHA=RS")
            elif mot.startswith("med:"):
                LOG("SCRAPE", f"DESCARTADO por MEDICAMENTO ({mot.split(':',1)[1]})")
            else:
                LOG("SCRAPE", f"DESCARTADO ({mot})")
            gs_append(CFG["sheet_desc"], [[link, info.get("fecha","")]])
            if mot == "RS": datos_rs.append(info)
            continue

        categoria = clasifica(info)
        if categoria == "ct": datos_ct.append(info)
        elif categoria == "sr": datos_sr.append(info)
        else: datos_sf.append(info)

    try: driver.quit()
    except: pass

    # === SUBIDA A SHEETS ===
    def df_prepare(lst):
        df = pd.DataFrame(lst)
        if df.empty: return df
        df.insert(0, "Fecha de Actualización", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # Precio -> numérico
        if "precio_referencia" in df.columns:
            serie = df["precio_referencia"].apply(precio_num)
            df["precio_referencia"] = pd.to_numeric(serie, errors="coerce")

        # Expandir ítems dinámicamente como item_1..item_n
        max_items = 0
        if "items" in df.columns:
            max_items = max((len(x) if isinstance(x, list) else 0) for x in df["items"])
            for i in range(1, max_items + 1):
                col = f"item_{i}"
                df[col] = df["items"].apply(lambda lst: (lst[i-1] if isinstance(lst, list) and len(lst) >= i else ""))
            df.drop(columns=["items"], inplace=True, errors="ignore")

        df.drop(columns=["fichas_base"], inplace=True, errors="ignore")

        df.fillna("", inplace=True)
        return df

    def append_df(sheet, df):
        base = [
            'Fecha de Actualización',
            'publicacion', 'enlace', 'titulo', 'precio_referencia', 'fecha',
            'entidad', 'unidad solicitante', 'termino_entrega', 'ficha_detectada',
            'Prioritario', 'Descartar',
            'descripcion'  # <-- justo después de los checkboxes
        ]

        if df.empty:
            ensure_header(sheet, base)
            return

        # Orden requerido: ... ficha_detectada, Prioritario, Descartar, descripcion, item_1..item_n
        item_cols = [c for c in df.columns if c.startswith("item_")]
        desired = base + item_cols 

        # Asegura columnas faltantes
        for c in desired:
            if c not in df.columns:
                df[c] = ""

        # Checkboxes vacíos (mantiene validaciones en la hoja)
        if 'Prioritario' in df.columns:
            df['Prioritario'] = df['Prioritario'].replace("", "")
        if 'Descartar' in df.columns:
            df['Descartar'] = df['Descartar'].replace("", "")

        ensure_header(sheet, desired)
        gs_append(sheet, df[desired].values.tolist())

    append_df('cl_prog_con_ct', df_prepare(datos_ct))
    append_df('cl_prog_sin_requisitos', df_prepare(datos_sr))
    append_df('cl_prog_sin_ficha', df_prepare(datos_sf))

    # Movimientos por checkboxes y limpieza final
    move_rows_by_checkbox(['cl_prog_sin_ficha','cl_prog_sin_requisitos','cl_prog_con_ct'], CFG["sheet_prio"], "Prioritario")
    move_rows_by_checkbox(['cl_prog_sin_ficha','cl_prog_sin_requisitos','cl_prog_con_ct'], CFG["sheet_desc"], "Descartar")


    for sh in ['cl_prog_sin_ficha','cl_prog_sin_requisitos','cl_prog_con_ct','cl_prioritarios','cl_descartes']:
        purge_by_fecha(sh)
    for sh in ['cl_prog_sin_ficha','cl_prog_sin_requisitos','cl_prog_con_ct']:
        reset_checkboxes(sh)
        update_fechas_sheet(sh)


    LOG("DONE", f"CT={len(datos_ct)} | SinReq={len(datos_sr)} | SinFicha={len(datos_sf)} | Ignorados_RS={len(datos_rs)}")

if __name__ == "__main__":
    main()     
