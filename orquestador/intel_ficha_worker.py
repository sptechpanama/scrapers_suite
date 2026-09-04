
from __future__ import annotations

import json
import hashlib
import os
import re
import sqlite3
import sys
import time
import uuid
import traceback
import unicodedata
from contextlib import closing
from difflib import SequenceMatcher
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any

import pandas as pd
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover - fallback en entornos sin bs4
    BeautifulSoup = None  # type: ignore[assignment]

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from common.ficha_line_items import (  # noqa: E402
    FichaProfile,
    bind_offers_to_matches,
    build_ficha_profile,
    extract_line_items_from_html,
    extract_offer_lines_from_html,
    match_ficha_to_lines,
)
from common.ficha_utils import get_catalog  # noqa: E402
from sheets_bridge import (
    SPREADSHEET_ID,
    _call_with_backoff,
    _clear_data_rows,
    _column_letter,
    _ensure_headers,
    _get_service,
    _get_values,
    _update_values,
)

PANAMACOMPRA_BASE_URL = "https://www.panamacompra.gob.pa/Inicio/"
FICHA_TOKEN_RE = re.compile(r"\b\d{3,8}\*?\b")
RUNS_SHEET = os.environ.get("INTEL_STUDY_RUNS_SHEET", "intel_study_runs_remote")
DETAIL_SHEET = os.environ.get("INTEL_STUDY_DETAIL_SHEET", "intel_study_detail_remote")
LINE_DETAIL_SHEET = os.environ.get(
    "INTEL_STUDY_LINE_DETAIL_SHEET",
    "intel_study_line_items_remote",
)

RUNS_HEADERS = [
    "request_id","run_id_remote","ficha","nombre_ficha","estado_run","fecha_inicio","fecha_fin",
    "db_source","total_items","total_consultas","consultas_resueltas","notas","updated_at","error",
    "scope_id",
]
DETAIL_HEADERS = [
    "request_id","run_id_remote","detail_id","ficha","nombre_ficha","acto_id","acto_nombre","acto_url",
    "entidad","renglon_texto","proveedor","proveedor_ganador","es_ganador","marca","modelo","pais_origen",
    "cantidad","precio_unitario_participacion","precio_unitario_referencia","fecha_publicacion","fecha_celebracion",
    "fecha_adjudicacion","fecha_orden_compra","dias_acto_a_oc","dias_acto_a_oc_mas_entrega","tipo_flujo",
    "fuente_precio","fuente_fecha","enlace_evidencia","unidad_medida","tiempo_entrega_dias","observaciones",
    "estado_revision","nivel_certeza","requiere_revision","precio_total_acto","enlace_ficha_minsa",
]
LINE_DETAIL_HEADERS = [
    "request_id","run_id_remote","line_detail_id","ficha","nombre_ficha","acto_id",
    "acto_nombre","acto_url","entidad","renglon_id","renglon_numero","renglon_texto",
    "match_method","match_score","match_evidence","match_requires_review","cantidad",
    "unidad_medida","precio_referencia_unitario","precio_referencia_total","proveedor",
    "precio_participacion_unitario","precio_participacion_total","binding_method",
    "binding_score","fuente_renglon","enlace_evidencia","created_at","precio_total_acto","enlace_ficha_minsa",
]
DEBUG_HTML_DIR = Path(r"C:\Users\rodri\scrapers_repo\orquestador\debug_html_intel")


def _log(msg: str, t0: float) -> None:
    print(f"[intel_estudio_ficha] +{time.perf_counter()-t0:,.1f}s | {msg}", flush=True)


def _clean(v: object) -> str:
    t = str(v or "").strip()
    return "" if t.lower() in {"nan", "none", "null", "<na>", "n/a"} else t


def _norm(v: object) -> str:
    t = _clean(v).lower()
    if not t:
        return ""
    # Normalizacion robusta para columnas/valores con acentos y simbolos (°/º).
    t = unicodedata.normalize("NFKD", t)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))
    t = t.replace("°", " ").replace("º", " ").replace("ª", " ")
    t = t.replace("?", " ")
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _num(v: object) -> float:
    t = _clean(v).replace("B/.", "").replace("B/", "").replace("$", "").replace("USD", "")
    t = t.replace(" ", "")
    if "," in t and "." in t:
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "").replace(",", ".")
        else:
            t = t.replace(",", "")
    elif "," in t:
        t = t.replace(",", ".")
    t = re.sub(r"[^0-9.\-]", "", t)
    if t in {"", "-", ".", "-."}:
        return 0.0
    try:
        return float(t)
    except Exception:
        return 0.0


def _date(v: object) -> pd.Timestamp:
    t = _clean(v)
    if not t:
        return pd.NaT
    d = pd.to_datetime(t, errors="coerce", dayfirst=not bool(re.match(r"^\d{4}[\-/]", t)))
    if pd.isna(d):
        d = pd.to_datetime(t, errors="coerce")
    return d


def _extract_tokens(raw: object) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for tk in FICHA_TOKEN_RE.findall(str(raw or "")):
        if tk not in seen:
            seen.add(tk)
            out.append(tk)
    return out


def _abs_url(url: str) -> str:
    u = _clean(url)
    if not u:
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if u.startswith("#/"):
        return PANAMACOMPRA_BASE_URL.rstrip("/") + "/" + u
    if u.startswith("/"):
        return PANAMACOMPRA_BASE_URL.rstrip("/") + u
    return PANAMACOMPRA_BASE_URL.rstrip("/") + "/" + u


def _process_code_from_url(url: str) -> str:
    u = _clean(url)
    if not u:
        return ""
    m = re.search(r"/pliego-de-cargos/([^/]+)/", u, flags=re.I)
    if m:
        return _clean(m.group(1))
    m = re.search(r"/solicitud-de-cotizacion/([^/]+)/", u, flags=re.I)
    if m:
        return _clean(m.group(1))
    return ""


def _lines_from_html(html: str) -> list[str]:
    raw = str(html or "")
    if BeautifulSoup is not None:
        soup = BeautifulSoup(raw, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.extract()
        txt = soup.get_text("\n", strip=True)
        return [re.sub(r"\s+", " ", x).strip() for x in txt.splitlines() if str(x).strip()]

    # Fallback deterministico si bs4 no esta instalado
    txt = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", raw)
    txt = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", txt)
    txt = re.sub(r"(?is)<noscript\b[^>]*>.*?</noscript>", " ", txt)
    txt = re.sub(r"(?is)<br\s*/?>", "\n", txt)
    txt = re.sub(r"(?is)</(p|div|tr|li|h[1-6]|td|th)>", "\n", txt)
    txt = re.sub(r"(?is)<[^>]+>", " ", txt)
    txt = unescape(txt)
    return [re.sub(r"\s+", " ", x).strip() for x in txt.splitlines() if str(x).strip()]


def _date_token(text: str) -> str:
    toks = re.findall(r"\b\d{2}[\-/]\d{2}[\-/]\d{4}\b", text or "")
    if not toks:
        return ""
    parsed: list[tuple[pd.Timestamp, str]] = []
    for tk in toks:
        d = _date(tk)
        if not pd.isna(d):
            parsed.append((d, tk))
    if not parsed:
        return toks[-1]
    parsed.sort(key=lambda x: x[0])
    return parsed[-1][1]


def _pick_date(lines: list[str], labels: list[str]) -> str:
    labels_n = [_norm(x) for x in labels]
    for i, line in enumerate(lines):
        n = _norm(line)
        if any(lbl in n for lbl in labels_n):
            tok = _date_token(line)
            if tok:
                return tok
            if i + 1 < len(lines):
                tok = _date_token(lines[i + 1])
                if tok:
                    return tok
    return ""


def _delivery_days(text: str) -> float:
    m = re.search(r"(\d{1,3})\s*d[ií]as", _norm(text))
    return float(int(m.group(1))) if m else 0.0


def _href(html: str, must_contain: str, exclude: str = "") -> str:
    # Soporta href normal y data-uw-original-href (accesibilidad del sitio).
    pattern = re.compile(r'(?:href|data-uw-original-href)=["\']([^"\']+)["\']', flags=re.I)
    for h in pattern.findall(html or ""):
        hc = _clean(h)
        if not hc:
            continue
        if must_contain.lower() not in hc.lower():
            continue
        if exclude and exclude.lower() in hc.lower():
            continue
        return _abs_url(hc)
    return ""


def _days_between(a: str, b: str) -> float:
    da, db = _date(a), _date(b)
    if pd.isna(da) or pd.isna(db):
        return 0.0
    return float((db - da).days)


def _wait_tipo2_sections(driver: object, html_initial: str, timeout: int = 18) -> str:
    """En actos tipo 2, espera a que carguen bloques de documentos (SPA) antes de extraer links/fechas."""
    try:
        from selenium.webdriver.common.by import By  # type: ignore
    except Exception:
        return html_initial

    def _ready(txt_norm: str) -> bool:
        has_doc_block = (
            "documentos del acto publico" in txt_norm
            or "documentos del acto p blico" in txt_norm
            or "documentos del acto" in txt_norm
        )
        has_arch_block = (
            "archivos de la compra menor" in txt_norm
            or "documentos de la compra menor" in txt_norm
        )
        has_cuadro = (
            "cuadro de propuesta presentada" in txt_norm
            or "cuadro de propuestas" in txt_norm
            or "cuadro de cotizaciones" in txt_norm
        )
        return (has_doc_block and has_arch_block) or (has_doc_block and has_cuadro)

    last_html = str(html_initial or "")
    deadline = time.time() + max(3, timeout)
    while time.time() < deadline:
        try:
            body_text = _norm(driver.find_element(By.TAG_NAME, "body").text)
        except Exception:
            body_text = ""
        try:
            html_now = str(getattr(driver, "page_source", "") or "")
        except Exception:
            html_now = last_html
        if _ready(body_text):
            return html_now
        last_html = html_now or last_html
        time.sleep(0.7)
    return last_html


def _extract_order_date_from_lines(lines: list[str]) -> str:
    best = ""
    best_d = pd.NaT
    for i, ln in enumerate(lines):
        if "orden de compra" not in _norm(ln):
            continue
        for j in range(i, min(i + 5, len(lines))):
            tk = _date_token(lines[j])
            if not tk:
                continue
            d = _date(tk)
            if pd.isna(d):
                continue
            if pd.isna(best_d) or d > best_d:
                best = tk
                best_d = d
    return best

def _build_driver() -> tuple[object | None, str]:
    try:
        from selenium import webdriver  # type: ignore
        opts = webdriver.ChromeOptions()
        opts.add_argument("--start-maximized")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1920,1080")
        headless = _clean(os.getenv("INTEL_STUDY_HEADLESS", "")).lower() in {
            "1", "true", "yes", "si",
        }
        if headless:
            opts.add_argument("--headless=new")
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(60)
        return driver, "ok_headless" if headless else "ok_visible"
    except Exception as exc:
        return None, f"driver_init_error:{exc}"


def _driver_html(driver: object, url: str, timeout: int = 40) -> str:
    cache_dir_raw = _clean(os.getenv("INTEL_STUDY_HTML_CACHE_DIR", ""))
    cache_path: Path | None = None
    if cache_dir_raw and url:
        cache_dir = Path(cache_dir_raw)
        cache_path = cache_dir / f"{hashlib.sha256(url.encode('utf-8')).hexdigest()}.html"
        try:
            if cache_path.exists() and time.time() - cache_path.stat().st_mtime <= 7 * 86400:
                cached = cache_path.read_text(encoding="utf-8", errors="ignore")
                expected = _process_code_from_url(url).lower()
                if len(cached) >= 15000 and (not expected or expected in cached.lower()):
                    return cached
        except OSError:
            cache_path = None

    def _cache(html: str) -> str:
        if cache_path is not None and len(html or "") >= 15000:
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(html, encoding="utf-8", errors="ignore")
            except OSError:
                pass
        return html

    try:
        from selenium.common.exceptions import TimeoutException  # type: ignore
        from selenium.webdriver.common.by import By  # type: ignore
        from selenium.webdriver.support import expected_conditions as EC  # type: ignore
        from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
        expected_proc = _process_code_from_url(url).lower()
        retries = 3
        for attempt in range(1, retries + 1):
            driver.get(url)
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            if expected_proc:
                try:
                    WebDriverWait(driver, min(timeout, 15)).until(
                        lambda d: expected_proc in str(
                            d.execute_script("return document.body ? document.body.innerText : ''")
                        ).lower()
                        or expected_proc in str(
                            d.execute_script("return document.documentElement ? document.documentElement.innerHTML : ''")
                        ).lower()
                    )
                    break
                except TimeoutException:
                    # Fuerza recarga fuerte y reintenta.
                    try:
                        driver.execute_script("window.location.reload(true);")
                    except Exception:
                        pass
                    if attempt >= retries:
                        pass
                    else:
                        time.sleep(1.0)
                        continue
            break

        # En SPA con hash routes, validar que realmente cargó la ruta solicitada.
        expected_fragment = ""
        if "#" in url:
            expected_fragment = url.split("#", 1)[1].strip().lower()
        if expected_fragment:
            try:
                WebDriverWait(driver, min(timeout, 25)).until(
                    lambda d: expected_fragment in str(
                        d.execute_script("return window.location.href || ''")
                    ).lower()
                )
            except TimeoutException:
                pass
        try:
            WebDriverWait(driver, min(timeout, 20)).until(lambda d: d.execute_script("return document.readyState") == "complete")
        except TimeoutException:
            pass

        # Espera activa para SPA: no basta con que exista body.
        deadline = time.time() + timeout
        tokens = (
            "informacion del proponente",
            "aviso de convocatoria",
            "procesos relacionados",
            "documentos de la compra menor",
            "archivos de la compra menor",
            "cuadro de propuesta presentada",
            "cuadro de propuestas",
        )
        last_html = ""
        stable_hits = 0
        while time.time() < deadline:
            html_now = str(getattr(driver, "page_source", "") or "")
            try:
                body_text = _norm(driver.find_element(By.TAG_NAME, "body").text)
            except Exception:
                body_text = _norm(html_now)

            has_token = any(t in body_text for t in tokens)
            has_tables = "<table" in html_now.lower()
            if has_token or (has_tables and len(html_now) >= 45000):
                return _cache(html_now)

            if html_now == last_html:
                stable_hits += 1
            else:
                stable_hits = 0
            last_html = html_now

            if stable_hits >= 4 and len(html_now) >= 15000:
                return _cache(html_now)
            time.sleep(0.8)

        return _cache(str(getattr(driver, "page_source", "") or ""))
    except Exception:
        return ""


def _provider_from_tables(html: str) -> str:
    try:
        tables = pd.read_html(html)
    except Exception:
        return ""
    for t in tables:
        if t.empty or len(t.columns) < 2:
            continue
        c1, c2 = t.columns[0], t.columns[1]
        for _, row in t[[c1, c2]].fillna("").iterrows():
            label = _norm(row[c1])
            value = _clean(row[c2])
            if not value:
                continue
            if "nombre comercial" in label or "razon social" in label:
                return value
    return ""


def _unit_data(html: str, ficha: str) -> dict[str, Any]:
    out: dict[str, Any] = {"unit": 0.0, "ref": 0.0, "qty": 0.0, "um": ""}
    try:
        tables = pd.read_html(html)
    except Exception:
        return out
    for t in tables:
        if t.empty:
            continue
        df = t.copy()
        df.columns = [str(c).strip() for c in df.columns]
        cmap = {_norm(c): c for c in df.columns}
        c_price = next((cmap[k] for k in cmap if "precio unitario" in k), "")
        c_ref = next((cmap[k] for k in cmap if "precio referencia" in k or ("precio" in k and "referencia" in k)), "")
        c_qty = next((cmap[k] for k in cmap if "cantidad" in k), "")
        c_um = next((cmap[k] for k in cmap if "unidad de medida" in k), "")
        c_desc = next((cmap[k] for k in cmap if "descripcion" in k or "especificaciones del comprador" in k), "")
        if not (c_price or c_ref):
            continue
        ridx = 0
        if c_desc and ficha:
            m = df[c_desc].astype(str).str.contains(str(ficha), case=False, regex=False, na=False)
            if m.any():
                ridx = int(df[m].index[0])
        row = df.loc[ridx] if ridx in df.index else df.iloc[0]
        if c_price and out["unit"] <= 0:
            out["unit"] = _num(row.get(c_price, 0))
        if c_ref and out["ref"] <= 0:
            rv = _num(row.get(c_ref, 0))
            qv = _num(row.get(c_qty, 0)) if c_qty else 0.0
            out["ref"] = rv / qv if rv > 0 and qv > 0 else rv
        if c_qty and out["qty"] <= 0:
            out["qty"] = _num(row.get(c_qty, 0))
        if c_um and not out["um"]:
            out["um"] = _clean(row.get(c_um, ""))
    return out


def _cuadro_min_from_driver(driver: object, cuadro_url: str, ficha: str) -> dict[str, Any]:
    out: dict[str, Any] = {"proveedor": "", "unit": 0.0, "qty": 0.0, "um": "", "ev": ""}
    try:
        from selenium.webdriver.common.by import By  # type: ignore
        from selenium.webdriver.support import expected_conditions as EC  # type: ignore
        from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
        driver.get(cuadro_url)
        WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # Espera activa robusta para tabla de propuestas.
        tables = []
        deadline = time.time() + 45
        while time.time() < deadline:
            tables = driver.find_elements(By.CSS_SELECTOR, "table.caption-top")
            if tables:
                break
            time.sleep(0.8)
        if not tables:
            # Un refresh suave puede resolver render tardío.
            try:
                driver.execute_script("window.location.reload();")
                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            except Exception:
                pass
            deadline2 = time.time() + 25
            while time.time() < deadline2:
                tables = driver.find_elements(By.CSS_SELECTOR, "table.caption-top")
                if tables:
                    break
                time.sleep(0.8)

        # Fallback: cualquier tabla que tenga columna "Precio Unitario".
        if not tables:
            any_tables = driver.find_elements(By.CSS_SELECTOR, "table")
            for tb in any_tables:
                headers = [_norm(x.text) for x in tb.find_elements(By.CSS_SELECTOR, "thead th")]
                if any("precio unitario" in h for h in headers):
                    tables.append(tb)

        candidates: list[dict[str, Any]] = []
        for tb in tables:
            provider = ""
            try:
                provider = _clean(tb.find_element(By.XPATH, "./caption//a[1]").text)
            except Exception:
                provider = ""
            headers = [_norm(x.text) for x in tb.find_elements(By.CSS_SELECTOR, "thead th")]
            ip = next((i for i, h in enumerate(headers) if "precio unitario" in h), -1)
            iq = next((i for i, h in enumerate(headers) if "cantidad propuesta" in h or h == "cantidad"), -1)
            iu = next((i for i, h in enumerate(headers) if "unidad de medida" in h), -1)
            idesc = next((i for i, h in enumerate(headers) if "descripcion del bien" in h or "especificaciones del comprador" in h), -1)
            if ip < 0:
                continue
            chosen = None
            for r in tb.find_elements(By.CSS_SELECTOR, "tbody tr"):
                cells = r.find_elements(By.CSS_SELECTOR, "th,td")
                if not cells:
                    continue
                if ficha and idesc >= 0 and idesc < len(cells) and str(ficha) in _clean(cells[idesc].text):
                    chosen = cells
                    break
                if chosen is None:
                    chosen = cells
            if not chosen:
                continue
            unit = _num(chosen[ip].text if ip < len(chosen) else 0)
            qty = _num(chosen[iq].text if iq >= 0 and iq < len(chosen) else 0)
            um = _clean(chosen[iu].text) if iu >= 0 and iu < len(chosen) else ""
            total = 0.0
            for tr in tb.find_elements(By.CSS_SELECTOR, "tfoot tr"):
                if "total" in _norm(tr.text):
                    c = tr.find_elements(By.CSS_SELECTOR, "th,td")
                    if c:
                        total = max(total, _num(c[-1].text))
            if unit > 0:
                candidates.append({"proveedor": provider, "unit": unit, "qty": qty, "um": um, "total": total})
        if not candidates:
            return out
        candidates.sort(key=lambda x: (0 if x["total"] > 0 else 1, x["total"] if x["total"] > 0 else x["unit"]))
        c = candidates[0]
        out.update(c)
        out["ev"] = f"cuadro_min_total|{_clean(c['proveedor'])}|{float(c['total']):.2f}"
        return out
    except Exception:
        return out


def _catalog_map() -> dict[str, dict[str, str]]:
    cands = [
        Path(r"C:\Users\rodri\GEAPP\oferentes_catalogos.xlsx"),
        Path(r"C:\Users\rodri\GEAPP\data\oferentes_catalogos.xlsx"),
        Path(r"C:\Users\rodri\scrapers_repo\data\oferentes_catalogos.xlsx"),
    ]
    src = next((p for p in cands if p.exists()), None)
    if src is None:
        return {}
    try:
        df = pd.read_excel(src)
    except Exception:
        return {}
    cols = [(_norm(c), c) for c in df.columns]

    def _pick_exact(names: list[str]) -> str:
        wanted = {n.strip() for n in names}
        for n, c in cols:
            if n in wanted:
                return c
        return ""

    def _pick_contains(required: list[str], excluded: list[str] | None = None) -> str:
        excl = excluded or []
        for n, c in cols:
            if all(tok in n for tok in required) and not any(bad in n for bad in excl):
                return c
        return ""

    # Prioridades de columnas para evitar confundir "Numero de Oferente" con "Oferente".
    cprov = (
        _pick_exact(["oferente", "proveedor", "razon social", "nombre comercial"])
        or _pick_contains(["oferente"], excluded=["numero"])
        or _pick_contains(["proveedor"])
    )
    cficha = _pick_contains(["ficha"])
    cmarca = _pick_exact(["marca"]) or _pick_contains(["marca"])
    cmodelo = _pick_contains(["modelo"]) or _pick_contains(["catalogo"])
    cpais = (
        _pick_contains(["pais", "origen"])
        or _pick_contains(["origen"])
        or _pick_contains(["procedencia"])
    )
    if not cprov:
        return {}

    def _ficha_digits(v: object) -> str:
        raw = _clean(v)
        if not raw:
            return ""
        m = re.fullmatch(r"\s*(\d+)(?:\.0+)?\s*", raw)
        if m:
            return m.group(1)
        return re.sub(r"\D", "", raw)

    def _score(rec: dict[str, str]) -> int:
        return int(bool(_clean(rec.get("marca", "")))) + int(bool(_clean(rec.get("modelo", "")))) + int(bool(_clean(rec.get("pais_origen", ""))))

    out: dict[str, dict[str, str]] = {}
    for _, r in df.fillna("").iterrows():
        prov = _norm(r.get(cprov, ""))
        if not prov:
            continue
        rec = {
            "marca": _clean(r.get(cmarca, "")) if cmarca else "",
            "modelo": _clean(r.get(cmodelo, "")) if cmodelo else "",
            "pais_origen": _clean(r.get(cpais, "")) if cpais else "",
        }
        rec["__score"] = str(_score(rec))

        fnum = _ficha_digits(r.get(cficha, "")) if cficha else ""
        key = f"{fnum}|{prov}" if fnum else f"|{prov}"
        if key not in out or int(rec["__score"]) > int(out[key].get("__score", "0")):
            out[key] = rec

        # Fallback por proveedor (si luego no coincide ficha exacta).
        pkey = f"prov|{prov}"
        if pkey not in out or int(rec["__score"]) > int(out[pkey].get("__score", "0")):
            out[pkey] = rec
    return out


def _catalog_lookup(cmap: dict[str, dict[str, str]], ficha: str, proveedor: str) -> dict[str, str]:
    def _ficha_digits(v: object) -> str:
        raw = _clean(v)
        if not raw:
            return ""
        m = re.fullmatch(r"\s*(\d+)(?:\.0+)?\s*", raw)
        if m:
            return m.group(1)
        return re.sub(r"\D", "", raw)

    def _name_score(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        if a == b:
            return 1.0
        ta = [x for x in a.split() if x not in {"s", "a", "sa", "de", "del", "la", "y"}]
        tb = [x for x in b.split() if x not in {"s", "a", "sa", "de", "del", "la", "y"}]
        if not ta or not tb:
            return 0.0
        sa, sb = set(ta), set(tb)
        inter = len(sa & sb)
        base = inter / max(len(sa), len(sb))
        seq = SequenceMatcher(None, a, b).ratio()
        if a in b or b in a:
            base += 0.25
        return min(max(base, seq), 1.0)

    p = _norm(proveedor)
    f = _ficha_digits(ficha)
    rec = cmap.get(f"{f}|{p}") or cmap.get(f"|{p}")
    if not rec and p and f:
        # Fuzzy prioritario dentro de la misma ficha.
        best_rec: dict[str, str] | None = None
        best_score = 0.0
        prefix = f"{f}|"
        for k, v in cmap.items():
            if not k.startswith(prefix):
                continue
            pk = k[len(prefix):]
            score = _name_score(p, pk)
            if score > best_score:
                best_score = score
                best_rec = v
        if best_rec is not None and best_score >= 0.5:
            rec = best_rec

    if not rec:
        rec = cmap.get(f"prov|{p}")

    if not rec and p:
        # Fuzzy fallback global por proveedor (solo si no hubo match por ficha).
        best: dict[str, str] | None = None
        best_score = 0.0
        for k, v in cmap.items():
            if not k.startswith("prov|"):
                continue
            pk = k[5:]
            if not pk:
                continue
            score = _name_score(p, pk)
            if score > best_score:
                best = v
                best_score = score
        if best is not None and best_score >= 0.75:
            rec = best
    if not rec:
        return {"marca": "", "modelo": "", "pais_origen": ""}
    return {
        "marca": _clean(rec.get("marca", "")),
        "modelo": _clean(rec.get("modelo", "")),
        "pais_origen": _clean(rec.get("pais_origen", "")),
    }


def _detection_score_for_ficha(raw_json: object, ficha: str, legacy_value: object = "") -> float:
    """Devuelve la mejor confianza de la ficha sin contar varias evidencias dos veces."""
    best = 0.0
    try:
        parsed = json.loads(_clean(raw_json)) if _clean(raw_json) else []
    except Exception:
        parsed = []
    if isinstance(parsed, dict):
        parsed = parsed.get("fichas") or parsed.get("detecciones") or [parsed]
    if isinstance(parsed, list):
        for item in parsed:
            if not isinstance(item, dict):
                continue
            code = re.sub(r"\D", "", str(item.get("code") or item.get("ficha") or ""))
            if code != str(ficha):
                continue
            best = max(best, _num(item.get("score", 0)))
    if best <= 0 and any(re.sub(r"\D", "", token) == str(ficha) for token in _extract_tokens(legacy_value)):
        best = 70.0
    return min(100.0, max(0.0, best))


def _filter_acts_by_payload(df: pd.DataFrame, filters: dict[str, Any] | None, ficha: str) -> pd.DataFrame:
    if df.empty or not filters:
        return df
    out = df.copy()
    date_map = {
        "publicacion": "publicacion",
        "celebracion": "fecha",
        "adjudicacion": "fecha_adjudicacion",
        "actualizacion": "fecha_actualizacion",
    }
    date_column = date_map.get(_clean(filters.get("tipo_fecha", "")).lower(), "publicacion")
    start = pd.to_datetime(_clean(filters.get("fecha_desde", "")), errors="coerce")
    end = pd.to_datetime(_clean(filters.get("fecha_hasta", "")), errors="coerce")
    if date_column in out.columns and (not pd.isna(start) or not pd.isna(end)):
        # Extrae la primera fecha del texto para rangos del tipo "01-01-2026 a 05-01-2026".
        raw_dates = out[date_column].fillna("").astype(str).str.extract(
            r"(\d{1,2}[\-/]\d{1,2}[\-/]\d{4}|\d{4}[\-/]\d{1,2}[\-/]\d{1,2})",
            expand=False,
        )
        iso_mask = raw_dates.fillna("").str.match(r"^\d{4}[\-/]")
        parsed_dates = pd.Series(pd.NaT, index=raw_dates.index, dtype="datetime64[ns]")
        if iso_mask.any():
            parsed_dates.loc[iso_mask] = pd.to_datetime(raw_dates.loc[iso_mask], errors="coerce")
        if (~iso_mask).any():
            parsed_dates.loc[~iso_mask] = pd.to_datetime(
                raw_dates.loc[~iso_mask], errors="coerce", dayfirst=True
            )
        if not pd.isna(start):
            out = out.loc[parsed_dates >= start].copy()
            parsed_dates = parsed_dates.loc[out.index]
        if not pd.isna(end):
            out = out.loc[parsed_dates <= end].copy()

    states = {_norm(value) for value in (filters.get("estados") or []) if _clean(value)}
    if states and "estado" in out.columns:
        out = out[out["estado"].map(_norm).isin(states)].copy()
    entities = {_norm(value) for value in (filters.get("entidades") or []) if _clean(value)}
    if entities and "entidad" in out.columns:
        out = out[out["entidad"].map(_norm).isin(entities)].copy()

    min_amount = _num(filters.get("monto_minimo", 0))
    max_amount = _num(filters.get("monto_maximo", 0))
    amounts = out.get("precio_referencia", pd.Series(0, index=out.index)).map(_num)
    if min_amount > 0:
        out = out.loc[amounts >= min_amount].copy()
        amounts = amounts.loc[out.index]
    if max_amount > 0:
        out = out.loc[amounts <= max_amount].copy()

    min_awarded = _num(filters.get("adjudicado_minimo", 0))
    max_awarded = _num(filters.get("adjudicado_maximo", 0))
    awarded = out.get("total_items_ofertados", pd.Series(0, index=out.index)).map(_num)
    if min_awarded > 0:
        out = out.loc[awarded >= min_awarded].copy()
        awarded = awarded.loc[out.index]
    if max_awarded > 0:
        out = out.loc[awarded <= max_awarded].copy()

    threshold = _num(filters.get("score_minimo", 0))
    if threshold > 0 and "fichas_detectadas_json" in out.columns:
        scores = out.apply(
            lambda row: _detection_score_for_ficha(
                row.get("fichas_detectadas_json", ""), ficha, row.get("ficha_detectada", "")
            ),
            axis=1,
        )
        out = out.loc[scores >= threshold].copy()

    groups = [_norm(value) for value in (filters.get("busqueda") or []) if _clean(value)]
    if groups:
        text_series = (
            out.get("titulo", pd.Series("", index=out.index)).fillna("").astype(str)
            + " "
            + out.get("descripcion", pd.Series("", index=out.index)).fillna("").astype(str)
            + " "
            + out.get("entidad", pd.Series("", index=out.index)).fillna("").astype(str)
        ).map(_norm)
        masks = [text_series.str.contains(group, regex=False, na=False) for group in groups]
        combined = masks[0]
        if _clean(filters.get("modo_busqueda", "OR")).upper() == "AND":
            for mask in masks[1:]:
                combined &= mask
        else:
            for mask in masks[1:]:
                combined |= mask
        out = out.loc[combined].copy()
    return out


def _analytics_refs_for_ficha(
    analytics_db: Path | None,
    ficha: str,
) -> tuple[set[str], set[str]]:
    """Lee la relación normalizada ficha→acto usada por Inteligencia v3.

    La capa analítica es más confiable que volver a buscar el código dentro de
    textos libres. Si no está disponible, el worker conserva su búsqueda
    histórica como respaldo.
    """
    if analytics_db is None or not analytics_db.exists():
        return set(), set()
    try:
        with closing(sqlite3.connect(analytics_db)) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='intel_actos_fichas'"
            ).fetchone()
            if not exists:
                return set(), set()
            rows = conn.execute(
                """
                SELECT CAST(source_id AS TEXT), COALESCE(enlace, '')
                FROM intel_actos_fichas
                WHERE ficha = ?
                """,
                (str(ficha),),
            ).fetchall()
    except (OSError, sqlite3.Error):
        return set(), set()

    source_ids = {_clean(row[0]) for row in rows if _clean(row[0])}
    links = {_clean(row[1]) for row in rows if _clean(row[1])}
    return source_ids, links


def _analytics_metadata_for_ficha(
    analytics_db: Path | None,
    ficha: str,
) -> dict[str, str]:
    """Recupera nombre y URL MINSA de la misma capa analitica usada por la app."""

    empty = {"nombre_ficha": "", "enlace_minsa": ""}
    if analytics_db is None or not analytics_db.exists():
        return empty
    try:
        with closing(sqlite3.connect(analytics_db)) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='intel_ficha_metadata'"
            ).fetchone()
            if not exists:
                return empty
            row = conn.execute(
                """
                SELECT COALESCE(nombre_ficha, ''), COALESCE(enlace_minsa, '')
                FROM intel_ficha_metadata
                WHERE ficha = ?
                LIMIT 1
                """,
                (str(ficha),),
            ).fetchone()
    except (OSError, sqlite3.Error):
        return empty
    if not row:
        return empty
    return {
        "nombre_ficha": _clean(row[0]),
        "enlace_minsa": _clean(row[1]),
    }


def _study_filters_from_payload(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Determina si el estudio usa historial completo o el análisis visible."""
    raw_scope = _norm(payload.get("study_scope", "historico_completo"))
    if raw_scope in {
        "analisis actual",
        "analisis_actual",
        "filtros actuales",
        "filtros_actuales",
    }:
        filters = payload.get("filters")
        return "analisis_actual", filters if isinstance(filters, dict) else {}
    return "historico_completo", {}


def _acts_for_ficha(
    db: Path,
    ficha: str,
    filters: dict[str, Any] | None = None,
    *,
    analytics_db: Path | None = None,
) -> pd.DataFrame:
    f = re.sub(r"\D", "", ficha)
    like = f"%{f}%"
    relation_ids, relation_links = _analytics_refs_for_ficha(analytics_db, f)
    with closing(sqlite3.connect(db)) as conn:
        available_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(actos_publicos)").fetchall()
        }
        awarded_column = (
            "total_items_ofertados"
            if "total_items_ofertados" in available_columns
            else "'' AS total_items_ofertados"
        )
        base_select = f"""
    SELECT id, enlace, titulo, entidad, descripcion, ficha_detectada, fichas_detectadas_json,
           razon_social, nombre_comercial, publicacion, fecha, fecha_adjudicacion, fecha_actualizacion,
           precio_referencia, {awarded_column}, termino_entrega, estado
    FROM actos_publicos
    """
        # Con relación analítica disponible solo buscamos detecciones
        # estructuradas recientes que aún no hayan entrado a esa capa. El
        # barrido de textos grandes (items/source JSON) queda como fallback
        # cuando la analítica no existe, evitando escanear cientos de MB en
        # cada estudio.
        text_candidates = (
            ("ficha_detectada", "fichas_detectadas_json")
            if relation_ids or relation_links
            else (
                "ficha_detectada",
                "fichas_detectadas_json",
                "titulo",
                "descripcion",
                "items_json",
                "source_record_json",
            )
        )
        text_columns = [
            column
            for column in text_candidates
            if column in available_columns
        ]
        frames: list[pd.DataFrame] = []
        if text_columns:
            where_text = " OR ".join(f"{column} LIKE ?" for column in text_columns)
            frames.append(
                pd.read_sql_query(
                    f"{base_select} WHERE {where_text}",
                    conn,
                    params=tuple(like for _ in text_columns),
                )
            )

        # SQLite suele limitar la cantidad de parámetros por sentencia. Se
        # consulta por bloques para cubrir fichas con cientos o miles de actos.
        relation_id_list = sorted(relation_ids)
        for start in range(0, len(relation_id_list), 700):
            chunk = relation_id_list[start : start + 700]
            placeholders = ",".join("?" for _ in chunk)
            frames.append(
                pd.read_sql_query(
                    f"{base_select} WHERE CAST(id AS TEXT) IN ({placeholders})",
                    conn,
                    params=tuple(chunk),
                )
            )
        relation_link_list = sorted(relation_links)
        for start in range(0, len(relation_link_list), 500):
            chunk = relation_link_list[start : start + 500]
            placeholders = ",".join("?" for _ in chunk)
            frames.append(
                pd.read_sql_query(
                    f"{base_select} WHERE enlace IN ({placeholders})",
                    conn,
                    params=tuple(chunk),
                )
            )

    non_empty_frames = [frame for frame in frames if not frame.empty]
    if not non_empty_frames:
        return pd.DataFrame()
    df = pd.concat(non_empty_frames, ignore_index=True)
    df = df.drop_duplicates(subset=["id", "enlace"], keep="first")
    if df.empty:
        return df

    def has_ficha(v: object) -> bool:
        return any(re.sub(r"\D", "", tk) == f for tk in _extract_tokens(v))

    json_match = df["fichas_detectadas_json"].map(
        lambda raw: _detection_score_for_ficha(raw, f, "") > 0
    )
    relation_match = (
        df["id"].astype(str).isin(relation_ids)
        | df["enlace"].fillna("").astype(str).isin(relation_links)
    )
    out = df[
        df["ficha_detectada"].map(has_ficha)
        | json_match
        | relation_match
    ].copy()
    if out.empty:
        txt = df["titulo"].fillna("").astype(str) + " " + df["descripcion"].fillna("").astype(str)
        out = df[txt.str.contains(f, case=False, regex=False, na=False)].copy()
    out = out.drop_duplicates(subset=["id", "enlace"], keep="first")
    out = _filter_acts_by_payload(out, filters, f)
    # Mantiene el nombre utilizado por el resto del worker, corrigiendo su origen real.
    out["fecha_publicacion_db"] = out.get("publicacion", "")
    out.attrs["analytics_relation_ids"] = len(relation_ids)
    out.attrs["analytics_relation_links"] = len(relation_links)
    return out


def _replace_rows(sheet: str, headers: list[str], ficha: str, new_rows: list[list[str]]) -> None:
    _ensure_headers(sheet, headers)
    last_col = _column_letter(len(headers))
    current = _get_values(f"{sheet}!A2:{last_col}")
    idx_f = headers.index("ficha")
    kept: list[list[str]] = []
    for row in current:
        ext = row + [""] * (len(headers) - len(row))
        if _clean(ext[idx_f]) == str(ficha):
            continue
        kept.append(ext[: len(headers)])
    merged = kept + new_rows
    _clear_data_rows(sheet, len(headers))
    if merged:
        _update_values(f"{sheet}!A2", merged)


def _append_rows(sheet: str, headers: list[str], rows: list[list[str]]) -> None:
    if not rows:
        return
    _ensure_headers(sheet, headers)
    service = _get_service()
    _call_with_backoff(
        lambda: service.spreadsheets()
        .values()
        .append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        )
        .execute(),
        f"append {sheet}",
    )


def _publish_rows(sheet: str, headers: list[str], ficha: str, rows: list[list[str]]) -> None:
    append_mode = _clean(os.getenv("INTEL_STUDY_APPEND_RESULTS", "")).lower() in {
        "1", "true", "yes", "si",
    }
    if append_mode:
        _append_rows(sheet, headers, rows)
    else:
        _replace_rows(sheet, headers, ficha, rows)


def _vals(headers: list[str], data: dict[str, Any]) -> list[str]:
    return [str(data.get(h, "") if data.get(h, "") is not None else "") for h in headers]


def _should_debug_no_sheets() -> bool:
    return _clean(os.getenv("INTEL_STUDY_DEBUG_NO_SHEETS", "")).lower() in {"1", "true", "yes", "si"}


def _debug_max_acts() -> int:
    raw = _clean(os.getenv("INTEL_STUDY_DEBUG_MAX_ACTS", ""))
    if not raw:
        return 0
    try:
        return max(0, int(float(raw)))
    except Exception:
        return 0


def _dump_debug_html(ficha: str, acto_id: str, label: str, html: str) -> str:
    try:
        DEBUG_HTML_DIR.mkdir(parents=True, exist_ok=True)
        safe_f = re.sub(r"[^0-9A-Za-z_-]+", "_", str(ficha or "ficha"))
        safe_a = re.sub(r"[^0-9A-Za-z_-]+", "_", str(acto_id or "acto"))
        safe_l = re.sub(r"[^0-9A-Za-z_-]+", "_", str(label or "raw"))
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = DEBUG_HTML_DIR / f"{safe_f}_{safe_a}_{safe_l}_{ts}.html"
        out.write_text(str(html or ""), encoding="utf-8", errors="ignore")
        return str(out)
    except Exception:
        return ""


def _build_study_ficha_profile(ficha: str, nombre: str) -> FichaProfile:
    """Construye el perfil sin convertir una falla de catálogo en falla del estudio."""

    catalog_name = ""
    try:
        catalog_name = _clean(get_catalog().names.get(str(ficha), ""))
    except Exception:
        catalog_name = ""
    resolved_name = catalog_name or _clean(nombre) or f"Ficha {ficha}"
    extra_names = [
        value
        for value in (_clean(nombre), catalog_name)
        if value and _norm(value) != _norm(resolved_name)
    ]
    return build_ficha_profile(ficha, resolved_name, extra_names)


def _line_audit_row(
    *,
    request_id: str,
    run_id: str,
    ficha: str,
    nombre: str,
    acto_id: str,
    acto_nombre: str,
    acto_url: str,
    entidad: str,
    method: str,
    evidence: str,
    created_at: str,
    precio_total_acto: float = 0.0,
    enlace_ficha_minsa: str = "",
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "run_id_remote": run_id,
        "line_detail_id": str(uuid.uuid4()),
        "ficha": ficha,
        "nombre_ficha": nombre,
        "acto_id": acto_id,
        "acto_nombre": acto_nombre,
        "acto_url": acto_url,
        "entidad": entidad,
        "renglon_id": "",
        "renglon_numero": "",
        "renglon_texto": "",
        "match_method": method,
        "match_score": 0.0,
        "match_evidence": evidence,
        "match_requires_review": 1,
        "cantidad": 0.0,
        "unidad_medida": "",
        "precio_referencia_unitario": 0.0,
        "precio_referencia_total": 0.0,
        "proveedor": "",
        "precio_participacion_unitario": 0.0,
        "precio_participacion_total": 0.0,
        "binding_method": "sin_precio_renglon_confirmado",
        "binding_score": 0.0,
        "fuente_renglon": "auditoria",
        "enlace_evidencia": acto_url,
        "created_at": created_at,
        "precio_total_acto": round(float(precio_total_acto or 0.0), 6),
        "enlace_ficha_minsa": _clean(enlace_ficha_minsa),
    }


def _line_detail_rows_for_act(
    *,
    profile: FichaProfile,
    request_id: str,
    run_id: str,
    ficha: str,
    nombre: str,
    acto_id: str,
    acto_nombre: str,
    acto_url: str,
    entidad: str,
    act_html: str,
    offer_html: str,
    default_provider: str,
    evidence_url: str,
    created_at: str,
    precio_total_acto: float = 0.0,
    enlace_ficha_minsa: str = "",
) -> list[dict[str, Any]]:
    """Genera la capa paralela ficha-renglón-oferta sin usar totales del acto."""

    if not _clean(act_html):
        return [
            _line_audit_row(
                request_id=request_id,
                run_id=run_id,
                ficha=ficha,
                nombre=nombre,
                acto_id=acto_id,
                acto_nombre=acto_nombre,
                acto_url=acto_url,
                entidad=entidad,
                method="sin_html_acto",
                evidence="No se obtuvo HTML para identificar renglones.",
                created_at=created_at,
                precio_total_acto=precio_total_acto,
                enlace_ficha_minsa=enlace_ficha_minsa,
            )
        ]

    line_items = extract_line_items_from_html(act_html)
    matches = match_ficha_to_lines(profile, line_items, minimum_score=0.62)
    if not matches:
        return [
            _line_audit_row(
                request_id=request_id,
                run_id=run_id,
                ficha=ficha,
                nombre=nombre,
                acto_id=acto_id,
                acto_nombre=acto_nombre,
                acto_url=acto_url,
                entidad=entidad,
                method="sin_correspondencia_renglon",
                evidence=(
                    f"Se analizaron {len(line_items)} renglones, pero ninguno pudo "
                    "atribuirse con seguridad a la ficha."
                ),
                created_at=created_at,
                precio_total_acto=precio_total_acto,
                enlace_ficha_minsa=enlace_ficha_minsa,
            )
        ]

    offers = extract_offer_lines_from_html(offer_html or act_html)
    bound = bind_offers_to_matches(matches, offers)
    rows: list[dict[str, Any]] = []
    for result in bound:
        line = result.match.line
        offer = result.offer
        reference_total = float(line.reference_total or 0.0)
        if reference_total <= 0 and line.reference_unit_price > 0 and line.quantity > 0:
            reference_total = line.reference_unit_price * line.quantity
        participation_total = float(offer.total or 0.0) if offer else 0.0
        if (
            offer
            and participation_total <= 0
            and offer.unit_price > 0
            and offer.quantity > 0
        ):
            participation_total = offer.unit_price * offer.quantity
        rows.append(
            {
                "request_id": request_id,
                "run_id_remote": run_id,
                "line_detail_id": str(uuid.uuid4()),
                "ficha": ficha,
                "nombre_ficha": profile.name or nombre,
                "acto_id": acto_id,
                "acto_nombre": acto_nombre,
                "acto_url": acto_url,
                "entidad": entidad,
                "renglon_id": line.line_id,
                "renglon_numero": line.line_number,
                "renglon_texto": line.description,
                "match_method": result.match.method,
                "match_score": round(float(result.match.score), 4),
                "match_evidence": result.match.evidence,
                "match_requires_review": 1 if result.match.requires_review else 0,
                "cantidad": round(float(line.quantity or (offer.quantity if offer else 0.0)), 6),
                "unidad_medida": line.unit or (offer.unit if offer else ""),
                "precio_referencia_unitario": round(float(line.reference_unit_price or 0.0), 6),
                "precio_referencia_total": round(reference_total, 6),
                "proveedor": (
                    (_clean(offer.provider) or _clean(default_provider))
                    if offer
                    else _clean(default_provider)
                ),
                "precio_participacion_unitario": (
                    round(float(offer.unit_price), 6) if offer else 0.0
                ),
                "precio_participacion_total": round(participation_total, 6),
                "binding_method": result.binding_method,
                "binding_score": round(float(result.binding_score), 4),
                "fuente_renglon": "html_acto_y_cuadro_propuestas",
                "enlace_evidencia": evidence_url or acto_url,
                "created_at": created_at,
                "precio_total_acto": round(float(precio_total_acto or 0.0), 6),
                "enlace_ficha_minsa": _clean(enlace_ficha_minsa),
            }
        )
    return rows


def _persist_line_amount_rows(
    database_paths: list[Path],
    ficha: str,
    rows: list[dict[str, Any]],
) -> int:
    """Persiste la evidencia ficha-renglón para el siguiente build analítico.

    Es una salida adicional y tolerante a fallos: Sheets conserva el detalle
    visible del estudio, mientras esta tabla permite que los montos confirmados
    por renglón alimenten la inteligencia maestra sin usar el total del acto.
    """

    targets: list[Path] = []
    for raw_path in database_paths:
        path = Path(raw_path)
        if path.exists() and path.resolve() not in {item.resolve() for item in targets}:
            targets.append(path)
    stored = 0
    for path in targets:
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                with closing(sqlite3.connect(path, timeout=30)) as connection:
                    connection.execute("PRAGMA busy_timeout=30000")
                    connection.execute(
                        """
                        CREATE TABLE IF NOT EXISTS intel_ficha_line_amounts (
                            ficha TEXT NOT NULL,
                            acto_url TEXT NOT NULL,
                            acto_id TEXT,
                            line_key TEXT NOT NULL,
                            renglon_id TEXT,
                            renglon_numero TEXT,
                            line_description TEXT,
                            cantidad REAL NOT NULL DEFAULT 0,
                            unidad_medida TEXT,
                            reference_unit REAL NOT NULL DEFAULT 0,
                            reference_total REAL NOT NULL DEFAULT 0,
                            participation_unit REAL NOT NULL DEFAULT 0,
                            participation_total REAL NOT NULL DEFAULT 0,
                            provider TEXT,
                            match_score REAL NOT NULL DEFAULT 0,
                            requires_review INTEGER NOT NULL DEFAULT 1,
                            binding_method TEXT,
                            updated_at TEXT,
                            PRIMARY KEY (ficha, acto_url, line_key, provider)
                        )
                        """
                    )
                    existing_columns = {
                        str(column[1])
                        for column in connection.execute(
                            "PRAGMA table_info(intel_ficha_line_amounts)"
                        )
                    }
                    for column_name, column_type in (
                        ("cantidad", "REAL NOT NULL DEFAULT 0"),
                        ("line_description", "TEXT"),
                        ("unidad_medida", "TEXT"),
                        ("reference_unit", "REAL NOT NULL DEFAULT 0"),
                        ("participation_unit", "REAL NOT NULL DEFAULT 0"),
                    ):
                        if column_name not in existing_columns:
                            connection.execute(
                                f"ALTER TABLE intel_ficha_line_amounts "
                                f"ADD COLUMN {column_name} {column_type}"
                            )
                    connection.execute(
                        "DELETE FROM intel_ficha_line_amounts WHERE ficha = ?",
                        (ficha,),
                    )
                    values: list[tuple[Any, ...]] = []
                    for row in rows:
                        acto_url = _clean(row.get("acto_url"))
                        if not acto_url:
                            continue
                        line_key = (
                            _clean(row.get("renglon_id"))
                            or _clean(row.get("renglon_numero"))
                            or _clean(row.get("line_detail_id"))
                        )
                        if not line_key:
                            continue
                        values.append(
                            (
                                ficha,
                                acto_url,
                                _clean(row.get("acto_id")),
                                line_key,
                                _clean(row.get("renglon_id")),
                                _clean(row.get("renglon_numero")),
                                _clean(row.get("descripcion_renglon")),
                                _num(row.get("cantidad")),
                                _clean(row.get("unidad_medida")),
                                _num(row.get("precio_referencia_unitario")),
                                _num(row.get("precio_referencia_total")),
                                _num(row.get("precio_participacion_unitario")),
                                _num(row.get("precio_participacion_total")),
                                _clean(row.get("proveedor")),
                                _num(row.get("match_score")),
                                int(round(_num(row.get("match_requires_review")))),
                                _clean(row.get("binding_method")),
                                _clean(row.get("created_at")) or datetime.now().isoformat(timespec="seconds"),
                            )
                        )
                    if values:
                        connection.executemany(
                            """
                            INSERT OR REPLACE INTO intel_ficha_line_amounts (
                                ficha, acto_url, acto_id, line_key, renglon_id,
                                renglon_numero, line_description, cantidad, unidad_medida, reference_unit,
                                reference_total, participation_unit, participation_total,
                                provider, match_score, requires_review, binding_method,
                                updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            values,
                        )
                    connection.commit()
                    stored += len(values)
                last_error = None
                break
            except sqlite3.OperationalError as exc:
                last_error = exc
                if attempt < 3:
                    time.sleep(float(attempt))
        if last_error is not None:
            print(
                f"[intel_estudio_ficha] WARN: no se pudo persistir detalle de renglones "
                f"en {path}: {last_error}",
                flush=True,
            )
    return stored


def main() -> int:
    t0 = time.perf_counter()
    _log(f"inicio | spreadsheet={SPREADSHEET_ID}", t0)
    if BeautifulSoup is None:
        _log("bs4 no disponible; usando parser fallback por regex", t0)
    raw = os.environ.get("ORQUESTADOR_MANUAL_PAYLOAD", "").strip()
    if not raw:
        raise RuntimeError("No se encontro ORQUESTADOR_MANUAL_PAYLOAD")
    payload = json.loads(raw)

    request_id = _clean(payload.get("request_id", "")) or _clean(os.environ.get("ORQUESTADOR_MANUAL_ID", ""))
    ficha = re.sub(r"\D", "", str(payload.get("ficha", "")))
    nombre = _clean(payload.get("nombre_ficha", ""))
    notes = _clean(payload.get("notes", ""))
    scope_id = _clean(payload.get("scope_id", ""))
    if not ficha:
        raise RuntimeError("Payload sin ficha valida")

    db_candidates = [
        Path(_clean(payload.get("db_path", ""))) if _clean(payload.get("db_path", "")) else None,
        Path(r"C:\Users\rodri\GEAPP\panamacompra.db"),
        Path(r"C:\Users\rodri\scrapers_repo\data\db\panamacompra.db"),
        Path(r"C:\Users\rodri\OneDrive\cl\panamacompra.db"),
    ]
    db_path = next((p for p in db_candidates if p and p.exists()), None)
    if db_path is None:
        raise RuntimeError("No se encontro panamacompra.db local")

    analytics_candidates = [
        (
            Path(_clean(payload.get("analytics_db_path", "")))
            if _clean(payload.get("analytics_db_path", ""))
            else None
        ),
        Path(r"C:\Users\rodri\scrapers_repo\data\db\inteligencia_proveedores.db"),
    ]
    analytics_db_path = next(
        (path for path in analytics_candidates if path and path.exists()),
        None,
    )
    ficha_metadata = _analytics_metadata_for_ficha(analytics_db_path, ficha)
    if not nombre:
        nombre = ficha_metadata["nombre_ficha"] or f"Ficha {ficha}"
    ficha_minsa_url = (
        _clean(payload.get("enlace_minsa", ""))
        or ficha_metadata["enlace_minsa"]
    )
    study_scope, filters = _study_filters_from_payload(payload)
    _log(
        f"request={request_id or 'sin-id'} | ficha={ficha} | db={db_path} "
        f"| analytics_db={analytics_db_path or 'no disponible'} "
        f"| alcance={study_scope}",
        t0,
    )
    acts = _acts_for_ficha(
        db_path,
        ficha,
        filters,
        analytics_db=analytics_db_path,
    )
    max_acts = _debug_max_acts()
    if max_acts > 0:
        acts = acts.head(max_acts).copy()
        _log(f"DEBUG activo: limitando a {max_acts} actos", t0)
    _log(
        "actos detectados en el alcance solicitado: "
        f"{len(acts)} | referencias_analiticas="
        f"{acts.attrs.get('analytics_relation_ids', 0)} ids/"
        f"{acts.attrs.get('analytics_relation_links', 0)} enlaces "
        f"| filtros={json.dumps(filters, ensure_ascii=False)}",
        t0,
    )

    run_id = str(uuid.uuid4())
    now = datetime.now().isoformat(timespec="seconds")
    if acts.empty:
        run_row = {
            "request_id": request_id,
            "run_id_remote": run_id,
            "ficha": ficha,
            "nombre_ficha": nombre,
            "estado_run": "completada",
            "fecha_inicio": now,
            "fecha_fin": now,
            "db_source": str(db_path),
            "total_items": 0,
            "total_consultas": 0,
            "consultas_resueltas": 0,
            "notas": notes,
            "updated_at": now,
            "error": "Sin actos para la ficha",
            "scope_id": scope_id,
        }
        if _should_debug_no_sheets():
            _log("DEBUG_NO_SHEETS=1 -> run vacio no se escribe a Sheets", t0)
        else:
            _publish_rows(RUNS_SHEET, RUNS_HEADERS, ficha, [_vals(RUNS_HEADERS, run_row)])
            _publish_rows(DETAIL_SHEET, DETAIL_HEADERS, ficha, [])
            _publish_rows(LINE_DETAIL_SHEET, LINE_DETAIL_HEADERS, ficha, [])
        _log("run vacio publicado", t0)
        print(json.dumps({"ok": True, "request_id": request_id, "run_id_remote": run_id, "ficha": ficha}), flush=True)
        return 0

    cmap = _catalog_map()
    _log(f"catalogo claves: {len(cmap)}", t0)
    ficha_profile = _build_study_ficha_profile(ficha, nombre)
    _log(
        f"perfil ficha: {ficha_profile.code} | {ficha_profile.name} "
        f"| medidas_mm={sorted(ficha_profile.dimensions_mm)}",
        t0,
    )

    rows: list[dict[str, Any]] = []
    line_rows: list[dict[str, Any]] = []
    t1 = 0
    t2 = 0
    started = datetime.now().isoformat(timespec="seconds")
    try:
        for i, r in acts.reset_index(drop=True).iterrows():
            acto_id = _clean(r.get("id", ""))
            acto_url = _abs_url(_clean(r.get("enlace", "")))
            acto_nombre = _clean(r.get("titulo", "")) or f"Acto {acto_id}"
            entidad = _clean(r.get("entidad", ""))
            descripcion = _clean(r.get("descripcion", ""))
            razon_social_db = _clean(r.get("razon_social", ""))
            nombre_comercial_db = _clean(r.get("nombre_comercial", ""))
            proveedor_ganador = razon_social_db or nombre_comercial_db
            fecha_pub_db = _clean(r.get("fecha_publicacion_db", ""))
            fecha_adj_db = _clean(r.get("fecha_adjudicacion", ""))
            precio_ref_db = _num(r.get("precio_referencia", 0))
            termino = _clean(r.get("termino_entrega", ""))
            estado_acto = _clean(r.get("estado", ""))
            es_desierto = "desierto" in _norm(estado_acto)

            _log(f"acto {i+1}/{len(acts)} | id={acto_id}", t0)
            driver, mode = _build_driver()
            _log(f"acto id={acto_id} selenium={mode}", t0)
            if driver is None:
                line_rows.extend(
                    _line_detail_rows_for_act(
                        profile=ficha_profile,
                        request_id=request_id,
                        run_id=run_id,
                        ficha=ficha,
                        nombre=nombre,
                        acto_id=acto_id,
                        acto_nombre=acto_nombre,
                        acto_url=acto_url,
                        entidad=entidad,
                        act_html="",
                        offer_html="",
                        default_provider=proveedor_ganador,
                        evidence_url=acto_url,
                        created_at=datetime.now().isoformat(timespec="seconds"),
                        precio_total_acto=precio_ref_db,
                        enlace_ficha_minsa=ficha_minsa_url,
                    )
                )
                rows.append(
                    {
                        "request_id": request_id,
                        "run_id_remote": run_id,
                        "detail_id": str(uuid.uuid4()),
                        "ficha": ficha,
                        "nombre_ficha": nombre,
                        "acto_id": acto_id,
                        "acto_nombre": acto_nombre,
                        "acto_url": acto_url,
                        "entidad": entidad,
                        "renglon_texto": descripcion,
                        "proveedor": proveedor_ganador,
                        "proveedor_ganador": proveedor_ganador,
                        "es_ganador": 1,
                        "marca": "",
                        "modelo": "",
                        "pais_origen": "",
                        "cantidad": 0.0,
                        "precio_unitario_participacion": 0.0,
                        "precio_unitario_referencia": 0.0,
                        "fecha_publicacion": fecha_pub_db,
                        "fecha_celebracion": "",
                        "fecha_adjudicacion": fecha_adj_db,
                        "fecha_orden_compra": "",
                        "dias_acto_a_oc": 0.0,
                        "dias_acto_a_oc_mas_entrega": 0.0,
                        "tipo_flujo": "sin_driver",
                        "fuente_precio": "",
                        "fuente_fecha": "",
                        "enlace_evidencia": acto_url,
                        "unidad_medida": "",
                        "tiempo_entrega_dias": _delivery_days(termino),
                        "observaciones": f"No se pudo iniciar Selenium: {mode}",
                        "estado_revision": "pendiente",
                        "nivel_certeza": 0.2,
                        "requiere_revision": 1,
                        "precio_total_acto": round(precio_ref_db, 6),
                        "enlace_ficha_minsa": ficha_minsa_url,
                    }
                )
                continue

            html = _driver_html(driver, acto_url)
            if not html:
                _log(f"acto id={acto_id} sin html util (url={acto_url})", t0)
                line_rows.extend(
                    _line_detail_rows_for_act(
                        profile=ficha_profile,
                        request_id=request_id,
                        run_id=run_id,
                        ficha=ficha,
                        nombre=nombre,
                        acto_id=acto_id,
                        acto_nombre=acto_nombre,
                        acto_url=acto_url,
                        entidad=entidad,
                        act_html="",
                        offer_html="",
                        default_provider=proveedor_ganador,
                        evidence_url=acto_url,
                        created_at=datetime.now().isoformat(timespec="seconds"),
                        precio_total_acto=precio_ref_db,
                        enlace_ficha_minsa=ficha_minsa_url,
                    )
                )
                rows.append(
                    {
                        "request_id": request_id,
                        "run_id_remote": run_id,
                        "detail_id": str(uuid.uuid4()),
                        "ficha": ficha,
                        "nombre_ficha": nombre,
                        "acto_id": acto_id,
                        "acto_nombre": acto_nombre,
                        "acto_url": acto_url,
                        "entidad": entidad,
                        "renglon_texto": descripcion,
                        "proveedor": proveedor_ganador,
                        "proveedor_ganador": proveedor_ganador,
                        "es_ganador": 1,
                        "marca": "",
                        "modelo": "",
                        "pais_origen": "",
                        "cantidad": 0.0,
                        "precio_unitario_participacion": 0.0,
                        "precio_unitario_referencia": 0.0,
                        "fecha_publicacion": fecha_pub_db,
                        "fecha_celebracion": "",
                        "fecha_adjudicacion": fecha_adj_db,
                        "fecha_orden_compra": "",
                        "dias_acto_a_oc": 0.0,
                        "dias_acto_a_oc_mas_entrega": 0.0,
                        "tipo_flujo": "sin_html",
                        "fuente_precio": "",
                        "fuente_fecha": "",
                        "enlace_evidencia": acto_url,
                        "unidad_medida": "",
                        "tiempo_entrega_dias": _delivery_days(termino),
                        "observaciones": "No se pudo cargar HTML del acto",
                        "estado_revision": "pendiente",
                        "nivel_certeza": 0.2,
                        "requiere_revision": 1,
                        "precio_total_acto": round(precio_ref_db, 6),
                        "enlace_ficha_minsa": ficha_minsa_url,
                    }
                )
                try:
                    driver.quit()
                except Exception:
                    pass
                continue

            expected_proc = _process_code_from_url(acto_url).lower()
            if expected_proc and expected_proc not in html.lower():
                _log(
                    f"acto id={acto_id} WARNING: html no contiene proceso esperado {expected_proc}; posible desalineacion SPA",
                    t0,
                )
            lines = _lines_from_html(html)
            has_info = "informacion del proponente" in _norm(" | ".join(lines))
            if not has_info:
                # En tipo 2, la SPA a veces pinta "Documentos/Archivos" segundos despues.
                html_wait = _wait_tipo2_sections(driver, html, timeout=20)
                if html_wait and len(html_wait) >= len(html):
                    html = html_wait
                    lines = _lines_from_html(html)
                    has_info = "informacion del proponente" in _norm(" | ".join(lines))
            _log(
                f"acto id={acto_id} html_len={len(html)} has_info={has_info} "
                f"table_count_est={html.lower().count('<table')}",
                t0,
            )
            tipo = "tipo_1_info_proponente" if has_info else "tipo_2_cuadro_propuestas"
            t1 += 1 if has_info else 0
            t2 += 0 if has_info else 1

            info = _unit_data(html, ficha)
            offer_html = html
            proveedor = _provider_from_tables(html) if has_info else ""
            fuente_precio = "acto_info_proponente" if has_info and float(info.get("unit", 0)) > 0 else ""
            evidencia = acto_url
            obs = ""

            if not has_info:
                cuadro = _href(html, "/cuadro-de-propuestas/", exclude="/ver-propuesta/")
                if cuadro:
                    cm = _cuadro_min_from_driver(driver, cuadro, ficha)
                    try:
                        offer_html = str(getattr(driver, "page_source", "") or "") or html
                    except Exception:
                        offer_html = html
                    if _clean(cm.get("proveedor", "")):
                        proveedor = _clean(cm.get("proveedor", ""))
                    if float(cm.get("unit", 0)) > 0:
                        info["unit"] = float(cm.get("unit", 0))
                        fuente_precio = "cuadro_propuestas_min_total"
                        evidencia = cuadro
                    if float(cm.get("qty", 0)) > 0:
                        info["qty"] = float(cm.get("qty", 0))
                    if _clean(cm.get("um", "")):
                        info["um"] = _clean(cm.get("um", ""))
                    obs = _clean(cm.get("ev", ""))
                else:
                    _log(f"acto id={acto_id} tipo2 sin enlace cuadro de propuestas", t0)

            if not proveedor:
                proveedor = proveedor_ganador

            fecha_pub = _pick_date(lines, ["Fecha de Publicación", "Fecha de Publicacion"]) or fecha_pub_db
            fecha_adj = _pick_date(lines, ["Fecha de Adjudicación", "Fecha de Adjudicacion"]) or fecha_adj_db
            fecha_cele = ""
            fuente_fecha = ""
            if has_info:
                original = _href(html, "/proceso-original/")
                if original:
                    original_html = _driver_html(driver, original)
                    fecha_cele = _pick_date(_lines_from_html(original_html), [
                        "Fecha y hora de apertura de propuestas",
                        "Fecha y hora presentación de propuestas",
                        "Fecha y hora presentación de cotizaciones",
                        "Fecha de celebración",
                    ])
                    if fecha_cele:
                        fuente_fecha = "proceso_original"
                        evidencia = original
                if not fecha_cele:
                    fecha_cele = _pick_date(lines, [
                        "Fecha y hora de apertura de propuestas",
                        "Fecha y hora presentación de propuestas",
                        "Fecha y hora presentación de cotizaciones",
                        "Fecha de celebración",
                    ])
                    if fecha_cele:
                        fuente_fecha = "acto_fallback"
            else:
                fecha_cele = _pick_date(lines, [
                    "Fecha y hora de apertura de propuestas",
                    "Fecha y hora presentación de propuestas",
                    "Fecha y hora presentación de cotizaciones",
                    "Fecha de celebración",
                ])
                if fecha_cele:
                    fuente_fecha = "acto_apertura"

            fecha_oc = ""
            try:
                for t in pd.read_html(html):
                    if t.empty:
                        continue
                    tf = t.copy()
                    tf.columns = [str(c).strip() for c in tf.columns]
                    cols = {_norm(c): c for c in tf.columns}
                    c_tipo = next((cols[k] for k in cols if k == "tipo"), "")
                    c_desc = next((cols[k] for k in cols if k == "descripcion"), "")
                    c_fecha = next((cols[k] for k in cols if k == "fecha"), "")
                    if not c_fecha:
                        continue
                    for _, rr in tf.fillna("").iterrows():
                        if "orden de compra" not in _norm(f"{rr.get(c_tipo, '')} {rr.get(c_desc, '')}"):
                            continue
                        tk = _date_token(str(rr.get(c_fecha, "")))
                        if tk:
                            if not fecha_oc or (_date(tk) > _date(fecha_oc)):
                                fecha_oc = tk
            except Exception:
                pass
            if not fecha_oc:
                fecha_oc = _extract_order_date_from_lines(lines)
            if not fecha_oc:
                # Fallback regex sobre HTML por si la tabla no parsea bien en pandas.
                for m in re.finditer(r"orden\s+de\s+compra[\s\S]{0,350}?(\d{2}-\d{2}-\d{4})", html or "", flags=re.I):
                    tk = _clean(m.group(1))
                    if tk and (not fecha_oc or _date(tk) > _date(fecha_oc)):
                        fecha_oc = tk

            qty = float(info.get("qty", 0) or 0)
            pref = float(info.get("ref", 0) or 0)
            if pref <= 0 and precio_ref_db > 0 and qty > 0:
                pref = precio_ref_db / max(qty, 1.0)
            entrega = _delivery_days(" | ".join([termino, " ".join(lines)]))
            d_act_oc = _days_between(fecha_cele, fecha_oc)
            d_act_oc_ent = (d_act_oc + entrega) if d_act_oc > 0 and entrega > 0 else max(d_act_oc, 0.0)

            cat = _catalog_lookup(cmap, ficha, proveedor)
            # Fallback por alias: si no encontro por proveedor principal,
            # probar nombre comercial/razon social de la DB (ej. "BTS, PANAMA").
            if not any(_clean(cat.get(k, "")) for k in ("marca", "modelo", "pais_origen")):
                alias_candidates = [nombre_comercial_db, razon_social_db, proveedor_ganador]
                seen_alias: set[str] = set()
                for alias in alias_candidates:
                    alias_txt = _clean(alias)
                    if not alias_txt:
                        continue
                    alias_norm = _norm(alias_txt)
                    if not alias_norm or alias_norm in seen_alias:
                        continue
                    seen_alias.add(alias_norm)
                    cat_try = _catalog_lookup(cmap, ficha, alias_txt)
                    if any(_clean(cat_try.get(k, "")) for k in ("marca", "modelo", "pais_origen")):
                        cat = cat_try
                        obs = (obs + " | " if obs else "") + f"catalogo_via_alias:{alias_txt}"
                        break
            marca_val = _clean(cat.get("marca", ""))
            modelo_val = _clean(cat.get("modelo", ""))
            pais_val = _clean(cat.get("pais_origen", ""))
            unit_num = float(info.get("unit", 0) or 0)
            pref_num = float(pref or 0)
            fecha_oc_out: object = fecha_oc
            unit_out: object = round(unit_num, 6)
            pref_out: object = round(pref_num, 6)
            fuente_precio_out = fuente_precio
            estado_revision = "pendiente"
            nivel_certeza = 0.45
            rev = 1 if (_num(info.get("unit", 0)) <= 0 or not _clean(proveedor)) else 0
            if es_desierto and (unit_num > 0 or _clean(fecha_oc)):
                # Si hay evidencia objetiva de precio/OC, priorizar evidencia sobre estado historico.
                es_desierto = False
                obs = (obs + " | " if obs else "") + "Estado DB='Desierto' pero se detecto evidencia de precio/OC en el acto."
                rev = 0
            if es_desierto:
                if not _clean(proveedor):
                    proveedor = "desierto"
                if not _clean(proveedor_ganador):
                    proveedor_ganador = "desierto"
                if not marca_val:
                    marca_val = "desierto"
                if not modelo_val:
                    modelo_val = "desierto"
                if not pais_val:
                    pais_val = "desierto"
                unit_out = "desierto"
                pref_out = round(pref_num, 6) if pref_num > 0 else "desierto"
                fecha_oc_out = "desierto"
                fuente_precio_out = fuente_precio or "estado_desierto"
                estado_revision = "desierto"
                nivel_certeza = 0.99
                rev = 0
                obs = (obs + " | " if obs else "") + "Acto marcado como desierto en DB."

            _log(
                "acto id={aid} tipo={tipo} prov={prov} unit={unit:.4f} ref={ref:.4f} qty={qty:.4f} "
                "f_cele={fcele} f_oc={foc} rev={rev}".format(
                    aid=acto_id,
                    tipo=tipo,
                    prov=_clean(proveedor) or "-",
                    unit=float(info.get("unit", 0) or 0),
                    ref=float(pref),
                    qty=float(qty),
                    fcele=_clean(fecha_cele) or "-",
                    foc=_clean(fecha_oc) or "-",
                    rev=rev,
                ),
                t0,
            )
            if rev:
                dump_path = _dump_debug_html(ficha, acto_id, "needs_review", html)
                if dump_path:
                    _log(f"acto id={acto_id} debug_html={dump_path}", t0)
            rows.append(
                {
                    "request_id": request_id,
                    "run_id_remote": run_id,
                    "detail_id": str(uuid.uuid4()),
                    "ficha": ficha,
                    "nombre_ficha": nombre,
                    "acto_id": acto_id,
                    "acto_nombre": acto_nombre,
                    "acto_url": acto_url,
                    "entidad": entidad,
                    "renglon_texto": descripcion,
                    "proveedor": proveedor,
                    "proveedor_ganador": proveedor_ganador,
                    "es_ganador": 1 if _norm(proveedor) and _norm(proveedor) == _norm(proveedor_ganador) else 0,
                    "marca": marca_val,
                    "modelo": modelo_val,
                    "pais_origen": pais_val,
                    "cantidad": round(qty, 6),
                    "precio_unitario_participacion": unit_out,
                    "precio_unitario_referencia": pref_out,
                    "fecha_publicacion": fecha_pub,
                    "fecha_celebracion": fecha_cele,
                    "fecha_adjudicacion": fecha_adj,
                    "fecha_orden_compra": fecha_oc_out,
                    "dias_acto_a_oc": round(d_act_oc, 4),
                    "dias_acto_a_oc_mas_entrega": round(d_act_oc_ent, 4),
                    "tipo_flujo": tipo,
                    "fuente_precio": fuente_precio_out,
                    "fuente_fecha": fuente_fecha,
                    "enlace_evidencia": evidencia,
                    "unidad_medida": _clean(info.get("um", "")),
                    "tiempo_entrega_dias": round(entrega, 4),
                    "observaciones": obs,
                    "estado_revision": estado_revision if es_desierto else ("pendiente" if rev else "autocompletado"),
                    "nivel_certeza": nivel_certeza if es_desierto else (0.45 if rev else 0.95),
                    "requiere_revision": rev,
                    "precio_total_acto": round(precio_ref_db, 6),
                    "enlace_ficha_minsa": ficha_minsa_url,
                }
            )
            line_rows.extend(
                _line_detail_rows_for_act(
                    profile=ficha_profile,
                    request_id=request_id,
                    run_id=run_id,
                    ficha=ficha,
                    nombre=nombre,
                    acto_id=acto_id,
                    acto_nombre=acto_nombre,
                    acto_url=acto_url,
                    entidad=entidad,
                    act_html=html,
                    offer_html=offer_html,
                    default_provider=proveedor,
                    evidence_url=evidencia,
                    created_at=datetime.now().isoformat(timespec="seconds"),
                    precio_total_acto=precio_ref_db,
                    enlace_ficha_minsa=ficha_minsa_url,
                )
            )
            saved = rows[-1]
            _log(
                "stored acto id={aid} unit={unit} ref={ref} fecha_oc={foc} estado_rev={est}".format(
                    aid=acto_id,
                    unit=saved.get("precio_unitario_participacion", ""),
                    ref=saved.get("precio_unitario_referencia", ""),
                    foc=saved.get("fecha_orden_compra", ""),
                    est=saved.get("estado_revision", ""),
                ),
                t0,
            )
            try:
                driver.quit()
            except Exception:
                pass
    except Exception as exc:
        _log(f"ERROR durante loop de actos: {exc}", t0)
        _log(traceback.format_exc(), t0)
        raise

    finished = datetime.now().isoformat(timespec="seconds")
    run_row = {
        "request_id": request_id,
        "run_id_remote": run_id,
        "ficha": ficha,
        "nombre_ficha": nombre,
        "estado_run": "completada",
        "fecha_inicio": started,
        "fecha_fin": finished,
        "db_source": str(db_path),
        "total_items": len(rows),
        "total_consultas": 0,
        "consultas_resueltas": 0,
        "notas": notes,
        "updated_at": finished,
        "error": "",
        "scope_id": scope_id,
    }

    persisted_rows = _persist_line_amount_rows(
        [
            db_path,
            REPO_ROOT / "data" / "db" / "panamacompra.db",
        ],
        ficha,
        line_rows,
    )
    _log(f"evidencia de montos por renglón persistida: {persisted_rows}", t0)

    if _should_debug_no_sheets():
        _log("DEBUG_NO_SHEETS=1 -> no se escriben resultados a Sheets", t0)
    else:
        _publish_rows(RUNS_SHEET, RUNS_HEADERS, ficha, [_vals(RUNS_HEADERS, run_row)])
        _publish_rows(DETAIL_SHEET, DETAIL_HEADERS, ficha, [_vals(DETAIL_HEADERS, x) for x in rows])
        _publish_rows(
            LINE_DETAIL_SHEET,
            LINE_DETAIL_HEADERS,
            ficha,
            [_vals(LINE_DETAIL_HEADERS, row) for row in line_rows],
        )
        _log(
            f"publicado en Sheets | detalle={len(rows)} | renglones={len(line_rows)} "
            f"| tipo1={t1} | tipo2={t2}",
            t0,
        )
    print(
        json.dumps(
            {
                "ok": True,
                "request_id": request_id,
                "run_id_remote": run_id,
                "ficha": ficha,
                "total_items": len(rows),
                "total_renglones": len(line_rows),
                "tipo1": t1,
                "tipo2": t2,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[intel_estudio_ficha] ERROR: {exc}", flush=True)
        raise
