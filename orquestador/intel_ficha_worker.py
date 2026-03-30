
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import uuid
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


def _log(msg: str, t0: float) -> None:
    print(f"[intel_estudio_ficha] +{time.perf_counter()-t0:,.1f}s | {msg}", flush=True)


def _clean(v: object) -> str:
    t = str(v or "").strip()
    return "" if t.lower() in {"nan", "none", "null", "<na>", "n/a"} else t


def _norm(v: object) -> str:
    t = _clean(v).lower()
    for a, b in [("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"), ("ñ", "n")]:
        t = t.replace(a, b)
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
    for h in re.findall(r'href=["\']([^"\']+)["\']', html or "", flags=re.I):
        hc = _clean(h)
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
        from selenium.webdriver.common.by import By  # type: ignore
        from selenium.webdriver.support import expected_conditions as EC  # type: ignore
        from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
        driver.get(url)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
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
        WebDriverWait(driver, 35).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        tables = driver.find_elements(By.CSS_SELECTOR, "table.caption-top")
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
    cmap = {_norm(c): c for c in df.columns}
    cprov = next((cmap[k] for k in cmap if "oferente" in k or "razon social" in k or "nombre comercial" in k), "")
    cficha = next((cmap[k] for k in cmap if "ficha" in k), "")
    cmarca = next((cmap[k] for k in cmap if "marca" in k), "")
    cmodelo = next((cmap[k] for k in cmap if "modelo" in k or "n de catalogo" in k), "")
    cpais = next((cmap[k] for k in cmap if "pais" in k), "")
    if not cprov:
        return {}
    out: dict[str, dict[str, str]] = {}
    for _, r in df.fillna("").iterrows():
        prov = _norm(r.get(cprov, ""))
        if not prov:
            continue
        fnum = re.sub(r"\D", "", _clean(r.get(cficha, ""))) if cficha else ""
        key = f"{fnum}|{prov}" if fnum else f"|{prov}"
        if key in out:
            continue
        out[key] = {"marca": _clean(r.get(cmarca, "")) if cmarca else "", "modelo": _clean(r.get(cmodelo, "")) if cmodelo else "", "pais_origen": _clean(r.get(cpais, "")) if cpais else ""}
    return out


def _catalog_lookup(cmap: dict[str, dict[str, str]], ficha: str, proveedor: str) -> dict[str, str]:
    p = _norm(proveedor)
    f = re.sub(r"\D", "", ficha)
    return cmap.get(f"{f}|{p}") or cmap.get(f"|{p}") or {"marca": "", "modelo": "", "pais_origen": ""}


def _acts_for_ficha(db: Path, ficha: str) -> pd.DataFrame:
    f = re.sub(r"\D", "", ficha)
    like = f"%{f}%"
    sql = """
    SELECT id, enlace, titulo, entidad, descripcion, ficha_detectada, razon_social, nombre_comercial,
           fecha AS fecha_publicacion_db, fecha_adjudicacion, precio_referencia, termino_entrega
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
        _replace_rows(RUNS_SHEET, RUNS_HEADERS, ficha, [_vals(RUNS_HEADERS, run_row)])
        _replace_rows(DETAIL_SHEET, DETAIL_HEADERS, ficha, [])
        _log("run vacio publicado", t0)
        print(json.dumps({"ok": True, "request_id": request_id, "run_id_remote": run_id, "ficha": ficha}), flush=True)
        return 0

    driver, mode = _build_driver()
    _log(f"selenium={mode}", t0)
    if driver is None:
        raise RuntimeError(f"No se pudo iniciar Selenium visible: {mode}")

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

            _log(f"acto {i+1}/{len(acts)} | id={acto_id}", t0)
            html = _driver_html(driver, acto_url)
            if not html:
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
                continue

            lines = _lines_from_html(html)
            has_info = "informacion del proponente" in _norm(" | ".join(lines))
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

            qty = float(info.get("qty", 0) or 0)
            pref = float(info.get("ref", 0) or 0)
            if pref <= 0 and precio_ref_db > 0 and qty > 0:
                pref = precio_ref_db / max(qty, 1.0)
            entrega = _delivery_days(" | ".join([termino, " ".join(lines)]))
            d_act_oc = _days_between(fecha_cele, fecha_oc)
            d_act_oc_ent = (d_act_oc + entrega) if d_act_oc > 0 and entrega > 0 else max(d_act_oc, 0.0)

            cat = _catalog_lookup(cmap, ficha, proveedor)
            rev = 1 if (_num(info.get("unit", 0)) <= 0 or not _clean(proveedor)) else 0
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
                    "marca": _clean(cat.get("marca", "")),
                    "modelo": _clean(cat.get("modelo", "")),
                    "pais_origen": _clean(cat.get("pais_origen", "")),
                    "cantidad": round(qty, 6),
                    "precio_unitario_participacion": round(float(info.get("unit", 0) or 0), 6),
                    "precio_unitario_referencia": round(pref, 6),
                    "fecha_publicacion": fecha_pub,
                    "fecha_celebracion": fecha_cele,
                    "fecha_adjudicacion": fecha_adj,
                    "fecha_orden_compra": fecha_oc,
                    "dias_acto_a_oc": round(d_act_oc, 4),
                    "dias_acto_a_oc_mas_entrega": round(d_act_oc_ent, 4),
                    "tipo_flujo": tipo,
                    "fuente_precio": fuente_precio,
                    "fuente_fecha": fuente_fecha,
                    "enlace_evidencia": evidencia,
                    "unidad_medida": _clean(info.get("um", "")),
                    "tiempo_entrega_dias": round(entrega, 4),
                    "observaciones": obs,
                    "estado_revision": "pendiente" if rev else "autocompletado",
                    "nivel_certeza": 0.45 if rev else 0.95,
                    "requiere_revision": rev,
                }
            )
    finally:
        try:
            driver.quit()
        except Exception:
            pass

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
