
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import uuid
import traceback
import unicodedata
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any

import pandas as pd
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover - fallback en entornos sin bs4
    BeautifulSoup = None  # type: ignore[assignment]

from sheets_bridge import (
    SPREADSHEET_ID,
    _clear_data_rows,
    _column_letter,
    _ensure_headers,
    _get_values,
    _update_values,
)

PANAMACOMPRA_BASE_URL = "https://www.panamacompra.gob.pa/Inicio/"
FICHA_TOKEN_RE = re.compile(r"\b\d{3,8}\*?\b")
RUNS_SHEET = os.environ.get("INTEL_STUDY_RUNS_SHEET", "intel_study_runs_remote")
DETAIL_SHEET = os.environ.get("INTEL_STUDY_DETAIL_SHEET", "intel_study_detail_remote")

RUNS_HEADERS = [
    "request_id","run_id_remote","ficha","nombre_ficha","estado_run","fecha_inicio","fecha_fin",
    "db_source","total_items","total_consultas","consultas_resueltas","notas","updated_at","error",
]
DETAIL_HEADERS = [
    "request_id","run_id_remote","detail_id","ficha","nombre_ficha","acto_id","acto_nombre","acto_url",
    "entidad","renglon_texto","proveedor","proveedor_ganador","es_ganador","marca","modelo","pais_origen",
    "cantidad","precio_unitario_participacion","precio_unitario_referencia","fecha_publicacion","fecha_celebracion",
    "fecha_adjudicacion","fecha_orden_compra","dias_acto_a_oc","dias_acto_a_oc_mas_entrega","tipo_flujo",
    "fuente_precio","fuente_fecha","enlace_evidencia","unidad_medida","tiempo_entrega_dias","observaciones",
    "estado_revision","nivel_certeza","requiere_revision",
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
    d = pd.to_datetime(t, errors="coerce", dayfirst=True)
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
        # Requerimiento: siempre visible (no headless).
        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(60)
        return driver, "ok_visible"
    except Exception as exc:
        return None, f"driver_init_error:{exc}"


def _driver_html(driver: object, url: str, timeout: int = 40) -> str:
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
                return html_now

            if html_now == last_html:
                stable_hits += 1
            else:
                stable_hits = 0
            last_html = html_now

            if stable_hits >= 4 and len(html_now) >= 15000:
                return html_now
            time.sleep(0.8)

        return str(getattr(driver, "page_source", "") or "")
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
        if a in b or b in a:
            base += 0.25
        return min(base, 1.0)

    p = _norm(proveedor)
    f = re.sub(r"\D", "", ficha)
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


def _acts_for_ficha(db: Path, ficha: str) -> pd.DataFrame:
    f = re.sub(r"\D", "", ficha)
    like = f"%{f}%"
    sql = """
    SELECT id, enlace, titulo, entidad, descripcion, ficha_detectada, razon_social, nombre_comercial,
           fecha AS fecha_publicacion_db, fecha_adjudicacion, precio_referencia, termino_entrega, estado
    FROM actos_publicos
    WHERE ficha_detectada LIKE ? OR titulo LIKE ? OR descripcion LIKE ?
    ORDER BY id DESC
    """
    with sqlite3.connect(db) as conn:
        df = pd.read_sql_query(sql, conn, params=(like, like, like))
    if df.empty:
        return df
    def has_ficha(v: object) -> bool:
        return any(re.sub(r"\D", "", tk) == f for tk in _extract_tokens(v))
    out = df[df["ficha_detectada"].map(has_ficha)].copy()
    if out.empty:
        txt = df["titulo"].fillna("").astype(str) + " " + df["descripcion"].fillna("").astype(str)
        out = df[txt.str.contains(f, case=False, regex=False, na=False)].copy()
    return out.drop_duplicates(subset=["id"])


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

    _log(f"request={request_id or 'sin-id'} | ficha={ficha} | db={db_path}", t0)
    acts = _acts_for_ficha(db_path, ficha)
    max_acts = _debug_max_acts()
    if max_acts > 0:
        acts = acts.head(max_acts).copy()
        _log(f"DEBUG activo: limitando a {max_acts} actos", t0)
    _log(f"actos detectados: {len(acts)}", t0)

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
        }
        if _should_debug_no_sheets():
            _log("DEBUG_NO_SHEETS=1 -> run vacio no se escribe a Sheets", t0)
        else:
            _replace_rows(RUNS_SHEET, RUNS_HEADERS, ficha, [_vals(RUNS_HEADERS, run_row)])
            _replace_rows(DETAIL_SHEET, DETAIL_HEADERS, ficha, [])
        _log("run vacio publicado", t0)
        print(json.dumps({"ok": True, "request_id": request_id, "run_id_remote": run_id, "ficha": ficha}), flush=True)
        return 0

    cmap = _catalog_map()
    _log(f"catalogo claves: {len(cmap)}", t0)

    rows: list[dict[str, Any]] = []
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
            proveedor_ganador = _clean(r.get("razon_social", "")) or _clean(r.get("nombre_comercial", ""))
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
                    }
                )
                continue

            html = _driver_html(driver, acto_url)
            if not html:
                _log(f"acto id={acto_id} sin html util (url={acto_url})", t0)
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
            proveedor = _provider_from_tables(html) if has_info else ""
            fuente_precio = "acto_info_proponente" if has_info and float(info.get("unit", 0)) > 0 else ""
            evidencia = acto_url
            obs = ""

            if not has_info:
                cuadro = _href(html, "/cuadro-de-propuestas/", exclude="/ver-propuesta/")
                if cuadro:
                    cm = _cuadro_min_from_driver(driver, cuadro, ficha)
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
                }
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
    }

    if _should_debug_no_sheets():
        _log("DEBUG_NO_SHEETS=1 -> no se escriben resultados a Sheets", t0)
    else:
        _replace_rows(RUNS_SHEET, RUNS_HEADERS, ficha, [_vals(RUNS_HEADERS, run_row)])
        _replace_rows(DETAIL_SHEET, DETAIL_HEADERS, ficha, [_vals(DETAIL_HEADERS, x) for x in rows])
        _log(f"publicado en Sheets | detalle={len(rows)} | tipo1={t1} | tipo2={t2}", t0)
    print(json.dumps({"ok": True, "request_id": request_id, "run_id_remote": run_id, "ficha": ficha, "total_items": len(rows), "tipo1": t1, "tipo2": t2}, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[intel_estudio_ficha] ERROR: {exc}", flush=True)
        raise
