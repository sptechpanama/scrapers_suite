# -*- coding: utf-8 -*-
import sys, re, time, unicodedata
from datetime import datetime
from urllib.parse import urlparse, urlunparse, unquote
import pandas as pd
from urllib.parse import urljoin
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import math, time
from googleapiclient.errors import HttpError
import sqlite3
from sqlite3 import Error
import re

DB_PATH = "panamacompra.db"
SAVE_BATCH_SIZE = 100  # Guarda en la base cada 100 actos


# =========================
# CONFIGURACIÓN (CFG)
# =========================
CFG = {
    # ---- Google Sheets ----
    "svc_key": r"C:\Users\rodri\cl\serious-app-417920-eed299fa06b5.json",
    "spreadsheet_id": "17hOfP-vMdJ4D7xym1cUp7vAcd8XJPErpY3V-9Ui2tCo",
    "sheet_db": "DB",

    # ---- Web (listado) ----
    "url_list": "https://www.panamacompra.gob.pa/Inicio/#/busqueda-avanzada",
    "host": "www.panamacompra.gob.pa",

    # anchors de pliego
    "css_links": (
        "tabla-busqueda-avanzada-v3 table tbody "
        "a[href*='/pliego-de-cargos/'], "
        "tabla-busqueda-avanzada-v3 table tbody "
        "a[data-uw-original-href*='/pliego-de-cargos/']"
    ),

    # ---- XPaths del detalle (robustos para ambos formatos) ----
    "xpath_map": {
        "titulo": [
            "//th[normalize-space()='Título']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Título')]/following-sibling::td[1]",
        ],
        # precio puede aparecer como "Precio de referencia" o "Monto de la contratación"
        "precio": [
            "//th[normalize-space()='Precio de referencia' or normalize-space()='Precio de Referencia']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Precio') and contains(normalize-space(),'referencia')]/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Monto de la contratación')]/following-sibling::td[1]",
        ],
        "fecha": [
            "//th[contains(normalize-space(),'Fecha y hora presentación')]/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Fecha y hora de presentación')]/following-sibling::td[1]",
            "//th[normalize-space()='Fecha y hora de apertura de propuestas']/following-sibling::td[1]",
            "//th[normalize-space()='Fecha de Publicación']/following-sibling::td[1]"
        ],
        "entidad": [
            "//th[normalize-space()='Entidad']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Entidad')]/following-sibling::td[1]",
        ],
        "termino": [
            "//th[normalize-space()='Término de entrega']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Término de entrega')]/following-sibling::td[1]",
        ],
        "desc": [
            "//th[normalize-space()='Descripción']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Descripción')]/following-sibling::td[1]",
        ],
        "pub": [
            "//th[normalize-space()='Fecha de Publicación']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Fecha de Publicación')]/following-sibling::td[1]",
        ],
        # Ítems (descripción por fila) en la tabla de rubros
        "items": [
            "//field-rubros//tbody/tr/td[2][contains(@class,'text-prewrap')]",
            "//field-rubros//tbody//td[contains(@class,'text-prewrap')]",
        ],
        # ===== NUEVOS CAMPOS =====
        "razon_social": [
            "//field-templates-basicos[@tipo='tipoInfo']//th[normalize-space()='Razón Social']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Razón Social')]/following-sibling::td[1]",
        ],
        "nombre_comercial": [
            "//field-templates-basicos[@tipo='tipoInfo']//th[normalize-space()='Nombre Comercial']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Nombre Comercial')]/following-sibling::td[1]",
        ],
        # Total de Ítems Ofertados (pie de la tabla de ofertas adjudicadas)
        "total_items_ofertados": [
            "//field-ofertas-adjudicadas-publico//table/tfoot//tr[last()]//td[last()]",
            "//field-ofertas-adjudicadas-publico//tfoot//td[last()]",
        ],

        "unidad_compra": [
            "//th[normalize-space()='Unidad de Compra']/following-sibling::td[1]",
            "//th[contains(normalize-space(),'Unidad de Compra')]/following-sibling::td[1]"
        ],

        "fecha_adjudicacion": [
            "//field-actas-pliego//tr[td[normalize-space()='Motivo de adjudicación']]/td[2]"
        ],
        
    },

    # ---- URLs precargadas por estado (con la consulta que pasaste) ----
    "query_adjudicado": "https://www.panamacompra.gob.pa/Inicio/#/busqueda-avanzada?q=Qf9tnOiEmcw12bjVnIsIiWwADMuADM6ADM6cTMUBzMtQDMtQjMwIjI6ICamJCLioFMwAjLwAjOwAjO3EDVxATLxATL0IDMyIiOiQmZiwSMtojIhJHct92Y0JCLwojIhl2YulmdvJHciwSMxATM6IybkFGdzVmIsIiI6Iibvl2YwlmcjNXZkJye",
    "query_desierto":   "https://www.panamacompra.gob.pa/Inicio/#/busqueda-avanzada?q=0Xf7pjIhJHct92Y1JCLioVO5kjL5UjO5UjOzIDVwMTL0ATL0IDMyIiOigmZiwiIaBDMw4CMwoDMwoTNwQVMw0SMw0CNyAjMiojIkZmIsETL6ISYyBXbvNGdiwCM6ISYpNmbpZ3byBnIsYTM6IybkFGdzVmIsIiI6Iibvl2YwlmcjNXZkJye",
}
 
# =========================
# CARGA DE FICHAS TÉCNICAS (con nombres genéricos)
# =========================
def load_valid_fichas_con_nombres(path_excel):
    import os
    if not os.path.exists(path_excel):
        print(f"[WARN] Archivo no encontrado: {path_excel}")
        return {}, set()

    try:
        df = pd.read_excel(path_excel)
        # Primera columna = número de ficha, segunda = nombre genérico
        fichas_dict = {}
        fichas_set = set()
        for _, row in df.iterrows():
            ficha = str(row.iloc[0]).strip()
            if not ficha or len(ficha) > 6:
                continue
            fichas_set.add(ficha)
            if len(row) > 1:
                nombre = str(row.iloc[1]).strip().lower()
                if nombre:
                    fichas_dict[nombre] = ficha

        print(f"[INIT] {len(fichas_set)} fichas válidas y {len(fichas_dict)} nombres genéricos cargados.")
        return fichas_dict, fichas_set
    except Exception as e:
        print(f"[ERR] Error cargando fichas: {e}")
        return {}, set()

FICHAS_PATH = r"C:\Users\rodri\fichas\fichas-y-nombre.xlsx"
FICHAS_NOMBRES, FICHAS_VALIDAS = load_valid_fichas_con_nombres(FICHAS_PATH)
# Compiladas para fichas numéricas (exactas, 1–6 dígitos)
PATRONES_NUMERICOS = [
    re.compile(rf"(?<![\w\d]){re.escape(f)}(?![\w\d])")
    for f in FICHAS_VALIDAS
]

# Compiladas para fichas por nombre (exactas, case-insensitive)
PATRONES_NOMBRES = {
    nombre: re.compile(rf"(?<![\w\d]){re.escape(nombre)}(?![\w\d])")
    for nombre in FICHAS_NOMBRES
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

def precio_num(s):
    txt = str(s) if s is not None else ""
    m = re.search(r'([0-9]{1,3}(?:[.,][0-9]{3})*[.,][0-9]{2}|[0-9]+(?:[.,][0-9]+)?)', txt)
    if not m: return None
    val = m.group(1)
    if "," in val and "." in val:
        if val.rfind(",") > val.rfind("."):  # 1.234,56
            val = val.replace(".", "").replace(",", ".")
        else:  # 1,234.56
            val = val.replace(",", "")
    else:
        val = val.replace(",", ".")
    try: return float(val)
    except: return None

# =========================
# DETECCIÓN DE FICHA TÉCNICA
# =========================
def detectar_ficha(texto):
    """
    Busca coincidencias exactas de fichas válidas (1–6 dígitos) dentro de un texto,
    evitando falsos positivos (números dentro de otros números).
    """
    if not texto:
        return None
    for ficha in FICHAS_VALIDAS:
        if re.search(rf'(?<![\w\d]){re.escape(ficha)}(?![\w\d])', texto):
            return ficha
    return None

def detectar_fichas_multiples(texto):
    """
    Devuelve todas las fichas válidas detectadas (1–6 dígitos) en un texto.
    Si hay varias, las separa por coma.
    """
    if not texto:
        return ""
    fichas_encontradas = []
    for ficha in FICHAS_VALIDAS:
        if re.search(rf'(?<![\w\d]){re.escape(ficha)}(?![\w\d])', texto):
            fichas_encontradas.append(ficha)
    return ", ".join(sorted(set(fichas_encontradas)))

def detectar_fichas_y_nombres(texto):
    """
    Detecta todas las fichas válidas en un texto:
    - Coincidencia exacta de números (sin prefijo)
    - Coincidencia exacta de nombres (prefijo '*')
    - Si hay duplicados (p. ej. 10004 y *10004), mantiene la versión numérica
    """
    if not texto:
        return ""

    texto_lower = texto.lower()
    fichas_encontradas = set()

    # --- Búsqueda por número exacto ---
    for pat, ficha in zip(PATRONES_NUMERICOS, FICHAS_VALIDAS):
        if pat.search(texto):
            fichas_encontradas.add(ficha)

    # --- Búsqueda por nombre exacto (con prefijo '*') ---
    for nombre, pat in PATRONES_NOMBRES.items():
        if pat.search(texto_lower):
            ficha = FICHAS_NOMBRES[nombre]
            fichas_encontradas.add(f"* {ficha}")

    # --- ✅ Eliminar duplicados (prioriza la versión numérica) ---
    finales = []
    vistos = set()
    # Ordena poniendo primero los numéricos, luego los que tienen '*'
    for f in sorted(fichas_encontradas, key=lambda x: (x.startswith('*'), x.replace('*', '').strip())):
        base = f.replace('*', '').strip()
        if base not in vistos:
            finales.append(f)
            vistos.add(base)

    return ", ".join(finales)



# =========================
# GOOGLE SHEETS (mini SDK)
# =========================
##from google.oauth2.service_account import Credentials
##from googleapiclient.discovery import build
##
##creds = Credentials.from_service_account_file(CFG["svc_key"])
##GSVC = build('sheets','v4',credentials=creds)
##SSID = CFG["spreadsheet_id"]
##
##def gs_get(rng):
##    return GSVC.spreadsheets().values().get(spreadsheetId=SSID, range=rng).execute().get("values", [])
##
##def gs_update(rng, values):
##    GSVC.spreadsheets().values().update(spreadsheetId=SSID, range=rng, valueInputOption='RAW', body={'values': values}).execute()
##
##def _append_chunk(sheet, rows, retries=5, pause=1.5):
##    """
##    Envía un bloque de filas con reintentos tolerantes a errores de red/TLS.
##    """
##    for intento in range(retries):
##        try:
##            GSVC.spreadsheets().values().append(
##                spreadsheetId=SSID,
##                range=f"{sheet}!A1",
##                valueInputOption='RAW',
##                insertDataOption='INSERT_ROWS',
##                body={'values': rows}
##            ).execute(num_retries=3)  # reintentos internos de googleapiclient
##            return True
##        except (HttpError, OSError) as e:
##            # OSError captura ssl.SSLEOFError y errores de socket
##            if intento == retries - 1:
##                raise
##            # Backoff exponencial simple
##            time.sleep(pause * (2 ** intento))
##
##def gs_append(sheet, rows, batch_size=200):
##    """
##    Divide en lotes pequeños para evitar paquetes grandes y reintenta si la conexión se cae.
##    """
##    if not rows:
##        return
##    total = len(rows)
##    bloques = math.ceil(total / batch_size)
##    enviados = 0
##    for i in range(bloques):
##        chunk = rows[i*batch_size:(i+1)*batch_size]
##        _append_chunk(sheet, chunk)
##        enviados += len(chunk)
##    LOG("SHEETS", f"{sheet}: +{enviados} filas (en {bloques} bloque(s))")
##
##def gs_meta():
##    return GSVC.spreadsheets().get(spreadsheetId=SSID).execute().get("sheets", [])
##
##def gs_sheet_id(title):
##    for s in gs_meta():
##        if s["properties"]["title"] == title:
##            return s["properties"]["sheetId"]
##    return None
##
##def ensure_header(sheet, header):
##    if not gs_get(f"{sheet}!A1:1"):
##        gs_update(f"{sheet}!A1", [header])
##        LOG("SHEETS", f"{sheet}: header ok")

# ===================================================================
# Creacion de tabla para Base de Datos y funcion de insertar filas 
# ===================================================================

def db_init():
    """Crea la base de datos y la tabla principal si no existen."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS actos_publicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha_actualizacion TEXT,
                publicacion TEXT,
                enlace TEXT UNIQUE,
                titulo TEXT,
                precio_referencia REAL,
                fecha TEXT,
                entidad TEXT,
                unidad_solic TEXT,
                termino_entrega TEXT,
                ficha_detectada TEXT,
                estado TEXT,
                descripcion TEXT,
                razon_social TEXT,
                nombre_comercial TEXT,
                fecha_adjudicacion TEXT,
                total_items_ofertados TEXT,
                num_participantes TEXT
            );
        """)
        conn.commit()
        conn.close()
        LOG("DB", "Base de datos y tabla creadas o ya existentes (con UNIQUE en enlace).")
    except Error as e:
        LOG("DB", f"Error creando tabla: {e}")

def asegurar_columnas_dinamicas(df):
    """
    Agrega automáticamente columnas tipo 'Proponente i' y 'Precio Proponente i'
    a la tabla SQLite si aún no existen.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(actos_publicos);")
        cols_existentes = {r[1] for r in cur.fetchall()}

        nuevas_cols = [c for c in df.columns if (c.startswith("Proponente ") or c.startswith("Precio Proponente ")) and c not in cols_existentes]
        for col in nuevas_cols:
            cur.execute(f"ALTER TABLE actos_publicos ADD COLUMN '{col}' TEXT;")
            LOG("DB", f"Columna añadida: {col}")

        conn.commit()
        conn.close()
    except Exception as e:
        LOG("DB", f"Error al asegurar columnas dinámicas: {e}")


def db_insert_rows(df):
    """Inserta o actualiza filas en la base de datos (UPSERT por enlace), incluyendo columnas dinámicas."""
    if df.empty:
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # === 1. Leer columnas reales de la tabla ===
        cur.execute("PRAGMA table_info(actos_publicos);")
        cols_db = {r[1] for r in cur.fetchall()}

        # === 2. Mapear columnas del DataFrame a las de la DB ===
        map_names = {
            "Fecha de Actualización": "fecha_actualizacion",
            "publicacion": "publicacion",
            "enlace": "enlace",
            "titulo": "titulo",
            "precio_referencia": "precio_referencia",
            "fecha": "fecha",
            "entidad": "entidad",
            "unidad_solic": "unidad_solic", 
            "termino_entrega": "termino_entrega",
            "ficha_detectada": "ficha_detectada",
            "Estado": "estado",
            "descripcion": "descripcion",
            "Razon Social": "razon_social",
            "Nombre comercial": "nombre_comercial",
            "fecha_adjudicacion": "fecha_adjudicacion",
            "Total Items Ofertados": "total_items_ofertados",
            "# de participantes": "num_participantes",
            "proponentes_data": "proponentes_data"
        }

        # === 3. Detectar columnas dinámicas (Proponente/Precio) ===
        dyn_cols = [c for c in df.columns if c.startswith("Proponente ") or c.startswith("Precio Proponente ")]
        asegurar_columnas_dinamicas(df)

        # === 4. Construir lista final de columnas a insertar (solo las que existen) ===
        mapped_cols = []
        for c in df.columns:
            db_col = map_names.get(c, c)
            if db_col in cols_db or db_col.startswith("Proponente ") or db_col.startswith("Precio Proponente "):
                mapped_cols.append((c, db_col))

        if not mapped_cols:
            LOG("DB", "⚠️ Ninguna columna coincide con la estructura de la base. Revisa nombres.")
            return

        # === 5. Construir SQL dinámico ===
        col_names = ", ".join([f'"{db_col}"' for _, db_col in mapped_cols])
        placeholders = ", ".join(["?"] * len(mapped_cols))
        updates = ", ".join([f'"{db_col}" = excluded."{db_col}"' for _, db_col in mapped_cols if db_col != "enlace"])

        insert_sql = f"""
            INSERT INTO actos_publicos ({col_names})
            VALUES ({placeholders})
            ON CONFLICT(enlace) DO UPDATE SET {updates}
        """

        # === 6. Insertar filas (en lote) ===
        rows = []
        for _, row in df.iterrows():
            vals = [str(row.get(col_df, "")) for col_df, _ in mapped_cols]
            rows.append(vals)

        cur.executemany(insert_sql, rows)
        conn.commit()
        conn.close()

        LOG("DB", f"Insertadas/actualizadas {len(df)} filas correctamente (columnas válidas={len(mapped_cols)}).")

    except Exception as e:
        LOG("DB", f"Error insertando o actualizando filas: {e}")





# =========================
# SELENIUM (navegación) y filtrado
# =========================
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
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
            if disabled: return False
            try: self.d.execute_script("arguments[0].scrollIntoView();", nxt); time.sleep(0.15); self.d.execute_script("window.scrollBy(0,-160);")
            except: pass
            try: nxt.click()
            except ElementClickInterceptedException: self.d.execute_script("arguments[0].click();", nxt)
            return True
        except NoSuchElementException:
            return False

def filtrar_nuevos_enlaces(nuevos_links):
    """
    Compara los enlaces capturados con los ya almacenados en la base de datos
    y devuelve solo los que no existen aún.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT enlace FROM actos_publicos;")
        existentes = {r[0] for r in cur.fetchall()}
        conn.close()
    except Exception as e:
        LOG("DB", f"Error leyendo enlaces existentes: {e}")
        existentes = set()

    nuevos_unicos = [u for u in nuevos_links if u not in existentes]
    LOG("DB", f"Filtrados: nuevos={len(nuevos_unicos)} | existentes={len(nuevos_links) - len(nuevos_unicos)} | total={len(nuevos_links)}")
    return nuevos_unicos


# =========================
# HELPERS DE XPATH
# =========================
def _first_text_by_xpaths(driver, xps, default=""):
    if isinstance(xps, str): xps = [xps]
    for xp in xps:
        els = driver.find_elements(By.XPATH, xp)
        if els:
            txt = els[0].text.strip()
            if txt: return txt
    return default

# =========================
# SCRAPE DETALLE
# =========================
def scrape_detail(driver, link):
    xp = CFG["xpath_map"]
    driver.get(link)
    driver.refresh()

    # ========= Utilidades SOLO para debug/log =========
    def _safe(s):
        try:
            return (s or "").replace("\n", " ").replace("\r", " ").strip()
        except Exception:
            return "<unprintable>"

    def _trunc(s, n=800):
        s = _safe(s)
        return s if len(s) <= n else s[:n] + f"... [trunc {len(s)-n} chars]"

    def _get_outer_html(selector_css=None, selector_xpath=None, max_len=1200):
        try:
            if selector_css:
                el = driver.find_element(By.CSS_SELECTOR, selector_css)
                html = driver.execute_script("return arguments[0].outerHTML;", el)
                return _trunc(html, max_len)
            if selector_xpath:
                el = driver.find_element(By.XPATH, selector_xpath)
                html = driver.execute_script("return arguments[0].outerHTML;", el)
                return _trunc(html, max_len)
        except Exception as e:
            return f"<no HTML: {e}>"
        return "<no HTML>"

    def _list_rows_text(limit=8):
        """
        Devuelve un resumen de las primeras filas (texto de la primera celda)
        en field-actas-pliego table (para ver qué rotulos ve realmente).
        """
        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "field-actas-pliego table tbody tr")
            out = []
            for i, tr in enumerate(rows[:limit]):
                try:
                    td1 = tr.find_element(By.XPATH, ".//td[1]")
                    out.append(f"{i+1}. {_safe(td1.text)}")
                except Exception:
                    out.append(f"{i+1}. <sin td[1] visible>")
            return "; ".join(out) if out else "<sin filas>"
        except Exception as e:
            return f"<no rows: {e}>"

    # ========= INICIO =========
    LOG("NAV", f"detalle -> {link}")

    # Espera algún campo clave (título o precio) para ambas plantillas
    try:
        LOG("WAIT", "Esperando cabecera (Título/Monto/Precio ref)")
        WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((By.XPATH, "(//th[normalize-space()='Título']|//th[contains(.,'Monto de la contratación')]|//th[contains(.,'Precio de referencia')])[1]"))
        )
        LOG("WAIT", "Cabecera detectada OK")
    except TimeoutException:
        LOG("SCRAPE","timeout detalle en cabecera")
        # Dump pequeño del body para diagnosticar
        try:
            body_html = driver.execute_script("return document.body.outerHTML || document.documentElement.outerHTML;")
            LOG("HTML", f"body (trunc): {_trunc(body_html, 1600)}")
        except Exception as e:
            LOG("HTML", f"no se pudo capturar body: {e}")
        return None

    # Captura de campos principales (con info de si están vacíos)
    def _ft(name, val):
        LOG("FIELD", f"{name} = {_safe(val)[:200] or '<vacío>'}")

    titulo  = _first_text_by_xpaths(driver, xp["titulo"]);              _ft("titulo", titulo)
    precio_referencia  = _first_text_by_xpaths(driver, xp["precio"]);   _ft("precio_referencia", precio_referencia)
    #fecha   = _first_text_by_xpaths(driver, xp["fecha"]);               _ft("fecha", fecha)
    entidad = _first_text_by_xpaths(driver, xp["entidad"]);             _ft("entidad", entidad)
    termino = _first_text_by_xpaths(driver, xp["termino"]);             _ft("termino_entrega", termino)
    desc    = _first_text_by_xpaths(driver, xp["desc"]);                _ft("descripcion", desc)
    pub     = _first_text_by_xpaths(driver, xp["pub"]);                 _ft("publicacion_raw", pub)

    # Items (descripciones)
    items = []
    for xpath in (xp["items"] if isinstance(xp["items"], list) else [xp["items"]]):
        els = driver.find_elements(By.XPATH, xpath)
        if els:
            items = [e.text.strip() for e in els]
            break

    # ===============================
    # 🔍 DETECCIÓN DE FICHA TÉCNICA
    # ===============================
    try:
        textos = [titulo, desc] + items
        mix = " ".join([t for t in textos if t])
        ficha_detectada = detectar_fichas_y_nombres(mix)
        if ficha_detectada:
            LOG("FICHA", f"Ficha técnica detectada: {ficha_detectada}")
        else:
            LOG("FICHA", "Sin ficha detectada")
    except Exception as e:
        LOG("FICHA", f"Error durante detección: {e}")
        ficha_detectada = ""
   

    # Nuevos campos
   
    fecha_adj = ""
    razon_social  = _first_text_by_xpaths(driver, xp["razon_social"]);        _ft("razon_social", razon_social)
    fecha         = _first_text_by_xpaths(driver, xp["fecha"]);               _ft("fecha", fecha)
    nombre_com    = _first_text_by_xpaths(driver, xp["nombre_comercial"]);    _ft("nombre_comercial", nombre_com)
    total_ofert   = _first_text_by_xpaths(driver, xp["total_items_ofertados"]);_ft("total_items_ofertados", total_ofert)

    unidad_compra = _first_text_by_xpaths(driver, xp["unidad_compra"]);       _ft("unidad_compra", unidad_compra)

    fecha_adj = _first_text_by_xpaths(driver, xp["fecha_adjudicacion"]).strip()
    if fecha_adj:
        # Si por alguna razón viniera con hora, cortamos en " - "
        fecha_adj = fecha_adj.split(' - ')[0].strip()
    _ft("fecha_adjudicacion", fecha_adj)
    

    proponentes_data = []
    num_participantes = ""

    # ===== Si existe Razón Social o Nombre Comercial o Total Ofertado, entrar al "Documento Original CL" =====
    if razon_social or nombre_com or total_ofert:
        try:
            LOG("DOC_ORIGINAL", "Intentando acceder al Documento Original CL (Proceso original)...")
            doc_links = driver.find_elements(
                By.XPATH,
                "//tr[.//th[contains(translate(normalize-space(.), 'ÁÉÍÓÚÜáéíóúüABCDEFGHIJKLMNOPQRSTUVWXYZ', "
                "'AEIOUUaeiouuabcdefghijklmnopqrstuvwxyz'),'proceso original')]]//a"
            )
            if doc_links:
                doc_href = doc_links[0].get_attribute("href") or doc_links[0].get_attribute("data-uw-original-href")
                if doc_href:
                    LOG("DOC_ORIGINAL", f"Enlace encontrado: {doc_href}")
                    driver.get(doc_href)
                    try:
                        WebDriverWait(driver, 15).until(
                            EC.presence_of_element_located(
                                (By.XPATH, "//a[@aria-label='Ver documento - abrir en una nueva pestaña']")
                            )
                        )
                    except TimeoutException:
                        LOG("DOC_ORIGINAL", "⚠️ No se detectó el enlace 'Ver documento - abrir en una nueva pestaña' después de 15s (posible estructura diferente).")
                    # --- Extraer el precio estimado del Proceso Original antes de entrar al enlace interno ---
                    try:
                        LOG("DOC_ORIGINAL", "Intentando leer 'Precio estimado' del Proceso Original...")
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, "//th[normalize-space(text())='Precio estimado']/following-sibling::td"))
                        )
                        precio_est = driver.find_element(By.XPATH, "//th[normalize-space(text())='Precio estimado']/following-sibling::td").text.strip()
                        fecha   = _first_text_by_xpaths(driver, xp["fecha"]);               _ft("fecha", fecha)
                        if precio_est:
                            LOG("DOC_ORIGINAL", f"Precio estimado detectado: {precio_est} — reemplazando precio anterior.")
                            precio_referencia = precio_est
                        else:
                            LOG("DOC_ORIGINAL", "⚠️ No se encontró texto válido en campo 'Precio estimado'.")
                    except TimeoutException:
                        LOG("DOC_ORIGINAL", "⚠️ No se encontró el campo 'Precio estimado' tras 10s de espera.")
                    except Exception as e:
                        LOG("DOC_ORIGINAL", f"Error al extraer 'Precio estimado': {e}")

                    
                    # --- Paso 2: Buscar enlace interno "Ver documento - abrir en una nueva pestaña"
                    LOG("DOC_ORIGINAL", "Buscando enlace interno 'Ver documento - abrir en una nueva pestaña'...")
                    inner_links = driver.find_elements(By.XPATH, "//a[@aria-label='Ver documento - abrir en una nueva pestaña']") 
                    if inner_links:
                        inner_href = inner_links[0].get_attribute("href") or inner_links[0].get_attribute("data-uw-original-href")
                        if inner_href:
                            LOG("DOC_ORIGINAL", f"Entrando a enlace interno: {inner_href}")
                            driver.get(inner_href)

                            LOG("DOC_ORIGINAL", "Esperando tabla de propuestas o mensaje 'No se encontró registro'...")
                            WebDriverWait(driver, 15).until(EC.presence_of_element_located((
                                By.XPATH,
                                "//th[normalize-space(text())='Número']/following-sibling::td"
                                " | //h1[normalize-space(text())='Cuadro de Cotizaciones V3']"
                            )))
                            #---------------------------------------------
                            
                            try:
                                LOG("CUADRO", "Extrayendo proponentes y precios del enlace interno...")

                                # Esperar a que aparezcan proponentes o mensajes de "no registro"
                                try:
                                    WebDriverWait(driver, 10).until(
                                        EC.presence_of_element_located((
                                            By.XPATH,
                                            "//caption//a[contains(@class, 'cursor-pointer') and not(contains(@title, 'Ver Cotización'))]"
                                            " | //div[contains(text(),'No se encontró registro')]"
                                            " | //td[contains(text(),'No hay propuesta')]"
                                        ))
                                    )
                                except TimeoutException:
                                    LOG("CUADRO", "⚠️ No se detectaron proponentes ni mensajes tras la espera.")

                                # Obtener listas de proponentes y precios
                                nombres = driver.find_elements(By.XPATH,
                                    "//caption//a[contains(@class, 'cursor-pointer') and not(contains(@title, 'Ver Cotización'))]"
                                )

                                precios = driver.find_elements(By.XPATH,
                                    "//tr[th[normalize-space()='Total']]/td"
                                )


                                LOG("CUADRO", f"Detectados: {len(nombres)} proponentes, {len(precios)} precios.")

                                tmp = []
                                for i, enlace in enumerate(nombres):
                                    nombre = enlace.text.strip()
                                    precio = precios[i].text.strip() if i < len(precios) else ""
                                    if nombre:
                                        tmp.append((nombre, precio))
                                        LOG("CUADRO", f"Proponente: {nombre} — Precio: {precio}")

                                # Guardar resultados en variables principales
                                if tmp:
                                    proponentes_data = tmp
                                    num_participantes = str(len(tmp))
                                    LOG("CUADRO", f"✅ Total de proponentes detectados: {num_participantes}")
                                else:
                                    # Detectar mensajes de ausencia de propuestas
                                    if driver.find_elements(By.XPATH, "//div[contains(text(),'No se encontró registro')]") \
                                       or driver.find_elements(By.XPATH, "//td[contains(text(),'No hay propuesta')]"):
                                        LOG("CUADRO", "⚠️ No se encontraron proponentes (mensaje oficial mostrado).")
                                        proponentes_data = []
                                        num_participantes = "0"
                                    else:
                                        LOG("CUADRO", "⚠️ No se detectaron proponentes ni precios — posible formato desconocido.")
                                        proponentes_data = []
                                        num_participantes = "0"

                            except Exception as e:
                                LOG("CUADRO", f"Error al extraer proponentes y precios: {e}")
                                proponentes_data = []
                                num_participantes = "0"

                            #---------------------------------------------

                           
                        else:
                            LOG("DOC_ORIGINAL", "El enlace interno no tiene href válido.")
                    else:
                        LOG("DOC_ORIGINAL", "No se encontró enlace interno 'Ver documento - abrir en una nueva pestaña' dentro del Proceso original.")
                else:
                    LOG("DOC_ORIGINAL", "No se encontró href válido en enlace de Proceso Original.")
            else:
                LOG("DOC_ORIGINAL", "No existe fila con 'Proceso original' en esta ficha.")
                if (not proponentes_data) and (not (fecha_adj or "").strip()):
                    if (fecha or "").strip():
                        fecha_adj = fecha
                        LOG("AUTO", f"fecha_adjudicacion asignada desde fecha: {fecha_adj}")
                
        except Exception as e:
            LOG("DOC_ORIGINAL", f"Error al acceder al Documento Original CL: {e}")



    publ = (pub.split(' -')[0]) if pub else ""
    LOG("FIELD", f"publicacion (normalizada) = {_safe(publ)}")

    # ===== Buscar enlace al cuadro y extraer participantes =====
    # Si ya se extrajeron proponentes del Documento Original CL, no se reinicia nada.
    if not proponentes_data:

        try:
            def _abs_url(u: str) -> str:
                u = (u or "").strip()
                return ("https://www.panamacompra.gob.pa/Inicio/" + u[2:]) if u.startswith("#/") else u

            # 1) Intento original: "Cuadro de propuesta presentada" (enlace /cuadro-de-propuestas/)
            cuadro_href = ""
            link1 = driver.find_elements(
                By.XPATH,
                "//field-actas-pliego//table//tr[.//td[normalize-space()='Cuadro de propuesta presentada']]"
                "//a[contains(@href,'/cuadro-de-propuestas/') or contains(@data-uw-original-href,'/cuadro-de-propuestas/')]"
            )
            if link1:
                cuadro_href = (link1[0].get_attribute("href") or link1[0].get_attribute("data-uw-original-href") or "").strip()

            # 2) Fallback: "Acta de apertura" dentro de 'Documentos del acto público' (enlace /ver-documento/)
            if not cuadro_href:
                link2 = driver.find_elements(
                    By.XPATH,
                    "//field-actas-pliego//tbody//tr[.//td[normalize-space()='Acta de apertura']]"
                    "//a[contains(@href,'/ver-documento/') or contains(@data-uw-original-href,'/ver-documento/')]"
                )
                if not link2:
                    # Variante tolerante: cualquier <a> cuyo texto contenga 'acta de apertura'
                    link2 = driver.find_elements(
                        By.XPATH,
                        "//field-actas-pliego//a[contains(@href,'/ver-documento/') or contains(@data-uw-original-href,'/ver-documento/')]"
                        "[contains(translate(normalize-space(.), 'ÁÉÍÓÚÜáéíóúüABCDEFGHIJKLMNOPQRSTUVWXYZ', 'AEIOUUaeiouuabcdefghijklmnopqrstuvwxyz'), 'acta de apertura')]"
                    )
                if link2:
                    cuadro_href = (link2[0].get_attribute("href") or link2[0].get_attribute("data-uw-original-href") or "").strip()

            if cuadro_href:
                cuadro_href = _abs_url(cuadro_href)
                LOG("NAV", f"Abriendo enlace cuadro/acta: {cuadro_href}")
                driver.get(cuadro_href)

                LOG("CUADRO", "Esperando tabla de propuestas (Angular o clásica)...")
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((
                    By.XPATH,
                    "//field-cuadro-propuesta-or//table"
                    " | //table[caption//a[contains(.,'Ver Propuesta')]]"
                    " | //*[contains(@class,'card-body') and contains(normalize-space(.),'No se encontró registro')]"
                    " | //td[contains(@class,'text-muted') and contains(normalize-space(.),'No hay propuesta')]"
                )))

                # --- Detectar mensaje de "sin proponentes" usando XPaths conocidos ---
                if driver.find_elements(By.XPATH, "//*[contains(@class,'card-body') and contains(normalize-space(.),'No se encontró registro')]") \
                   or driver.find_elements(By.XPATH, "//td[contains(@class,'text-muted') and contains(normalize-space(.),'No hay propuesta')]"):
                    LOG("CUADRO", "Detectado 'sin proponentes' — num_participantes=0")
                    num_participantes = "0"
                    proponentes_data = []

                # ========== A) NUEVA ESTRUCTURA ANGULAR ==========
                if not proponentes_data:
                    filas_acta = driver.find_elements(By.XPATH, "//field-cuadro-propuesta-or//tbody/tr")
                    LOG("ACTA", f"Angular detectado? filas={len(filas_acta)}")
                    if filas_acta:
                        tmp = []
                        for tr in filas_acta:
                            tds = tr.find_elements(By.XPATH, ".//td")
                            if len(tds) >= 4:
                                nombre = tds[1].text.strip() or ""
                                total_txt = tds[3].text.strip()
                                if nombre and re.search(r"\d", total_txt):
                                    tmp.append((nombre, total_txt))
                        if tmp:
                            proponentes_data = tmp
                            num_participantes = str(len(tmp))
                            LOG("ACTA", f"OK Angular — {num_participantes} participantes")
                        else:
                            LOG("ACTA", "Sin datos válidos en Angular")

                # ========== B) FALLBACK: FORMATO CLÁSICO ==========
                if not proponentes_data:
                    LOG("CLASICO", "Buscando tablas clásicas de propuestas...")
                    tablas = driver.find_elements(By.XPATH, "//table[caption//a[contains(@title,'Ver Propuesta')]]")
                    LOG("CLASICO", f"Tablas detectadas: {len(tablas)}")
                    tmp = []
                    for tbl in tablas:
                        try:
                            nombre = tbl.find_element(By.XPATH, ".//caption//a[1]").text.strip()
                        except:
                            continue
                        try:
                            total_txt = tbl.find_element(By.XPATH, ".//tfoot//tr[contains(.,'Total')]/td[last()]").text.strip()
                        except:
                            total_txt = ""
                        if nombre and re.search(r"\d", total_txt):
                            tmp.append((nombre, total_txt))
                    if tmp:
                        proponentes_data = tmp
                        num_participantes = str(len(tmp))
                        LOG("CLASICO", f"OK Clásico — {num_participantes} participantes")
                    else:
                        LOG("CLASICO", "Sin datos válidos en formato clásico")
                        

            else:
                LOG("CUADRO", "No se encontró enlace a cuadro-de-propuestas ni Acta de apertura en esta ficha.")
                fecha = fecha_adj     
        except Exception as e:
            LOG("CUADRO", f"no se pudo extraer participantes: {e}")

        #fecha   = _first_text_by_xpaths(driver, xp["fecha"]);               _ft("fecha", fecha)
        
        # NUEVO: si no hay proponentes y no hay fecha_adjudicacion, usa 'fecha'
        if (not proponentes_data) and (not (fecha_adj or "").strip()):
            if (fecha or "").strip():
                fecha_adj = fecha
                LOG("AUTO", f"fecha_adjudicacion asignada desde fecha: {fecha_adj}")


    # === Serializar datos para Google Sheets ===
    if proponentes_data:
        try:
            # Convierte la lista de tuplas [(nombre, monto), ...] a texto multilínea
            proponentes_texto = "\n".join([f"{n} — {m}" for n, m in proponentes_data])
            
        except Exception:
            proponentes_texto = str(proponentes_data)
        #fecha   = _first_text_by_xpaths(driver, xp["fecha"]);               _ft("fecha", fecha)
    else:
        proponentes_texto = ""

        # === AUTOCOMPLETAR DATOS FALTANTES SEGÚN PROPONENTES ===
    try:
        if proponentes_data:
            # Normalizar precios numéricos
            def parse_price(p):
                val = precio_num(p)
                return val if val is not None else float('inf')

            # Obtener proponente con menor precio válido
            precios_validos = [
                (n, p, parse_price(p))
                for n, p in proponentes_data
                if parse_price(p) != float('inf')
            ]

            if precios_validos:
                proveedor_min, precio_txt, precio_val = min(precios_validos, key=lambda x: x[2])

                # Si no hay total_items_ofertados, usar el precio mínimo
                if not total_ofert.strip():
                    total_ofert = f"*{precio_txt}"

                # Si razón social y nombre comercial están vacíos, usar el proveedor más barato
                if not razon_social.strip() and not nombre_com.strip():
                    razon_social = f"*{proveedor_min}"
                    nombre_com = f"*{proveedor_min}"

    except Exception as e:
        LOG("AUTO", f"Error completando campos por proponentes: {e}")


    return {
        "publicacion": publ,
        "enlace": link,
        "titulo": titulo,
        "precio_referencia": precio_referencia,
        "fecha": fecha,
        "entidad": entidad,
        "termino_entrega": termino,
        "descripcion": desc,
        # nuevos:
        "Razon Social": razon_social,
        "Nombre comercial": nombre_com,
        "Total Items Ofertados": total_ofert,
        "# de participantes": num_participantes,   # << se mantiene
        "proponentes_data": proponentes_data,      # << NUEVO (lista de tuplas)
        # para compatibilidad con DB:
        "ficha_detectada": ficha_detectada or "No Detectada",
        "unidad_solic": unidad_compra,
        "fecha_adjudicacion": fecha_adj,
    }


# =========================
# LISTADO POR ESTADO
# =========================
def collect_links_by_state(driver, url_with_state_param):
    driver.get(url_with_state_param)

    # Espera la tabla de resultados inicial
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, CFG["css_links"]))
        )
    except TimeoutException:
        LOG("TIMEOUT", "No se detectaron enlaces en la tabla tras 30s (posible lista vacía).")
        return []

    # === Forzar 50 por página ===
    try:
        sel = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//tabla-busqueda-avanzada-v3//select[contains(@class,'form-select-sm')]"
            ))
        )
        Select(sel).select_by_visible_text("50")
        time.sleep(0.3)
        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, CFG["css_links"]))
        )
        LOG("PAGE", "cambiado a 50 por página")
    except Exception:
        LOG("PAGE", "no se pudo cambiar a 50 por página")

    # =======================================
    # Paginación completa (multi-página)
    # =======================================
    PT = PageTools(driver)
    links, seen = [], set()
    pages_done = 0

    while True:
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, CFG["css_links"]))
        )

        urls = [PT.a_url(a) for a in PT.find_css(CFG["css_links"])]
        added = 0
        for u in urls:
            if u and u not in seen:
                seen.add(u)
                links.append(u)
                added += 1
        LOG("CAP", f"+{added} | total={len(links)}")

        pages_done += 1

        # ---------------------------------------
        # 🔧 OPCIONAL: limitar páginas manualmente
        # ---------------------------------------
        # Si quieres limitar la cantidad de páginas para pruebas,
        # cambia el valor del límite aquí (por ejemplo, 2 o 3 páginas).
        #
        # EJEMPLO: para solo 2 páginas, descomenta esta línea ↓↓↓
        #
##        if pages_done >= 3:
##           LOG("PAGE", "Fin de paginación manual (límite de prueba alcanzado)")
##           break
        #
        # ---------------------------------------

        # Guardar referencia a la tabla actual antes de hacer click en "Siguiente"
        tbody_old = PT.tbody_ref()
        if not PT.click_next():
            LOG("PAGE", "No hay más páginas disponibles (Next deshabilitado)")
            break

        try:
            if tbody_old is not None:
                WebDriverWait(driver, 15).until(EC.staleness_of(tbody_old))
            else:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, CFG["css_links"]))
                )
        except TimeoutException:
            time.sleep(1.0)

    LOG("DONE", f"enlaces capturados={len(links)}")
    return links



def main():
    db_init()
    rows_all_adj, rows_all_des = [], []  # ✅ listas acumuladoras
    # Header (sin columnas dinámicas)
    base_cols = [
        'Fecha de Actualización',
        'publicacion', 'enlace', 'titulo', 'precio_referencia', 'fecha',
        'entidad', 'unidad_solic', 'termino_entrega', 'ficha_detectada', 'Estado',
        'descripcion',
        'Razon Social',
        'Nombre comercial','fecha_adjudicacion',
        'Total Items Ofertados',
        '# de participantes',
    ]

    driver = start_browser()

    # =========================
    # ESTADO: ADJUDICADO
    # =========================
    LOG("ESTADO", "Adjudicado")
    links_adj = filtrar_nuevos_enlaces(collect_links_by_state(driver, CFG["query_adjudicado"]))
    rows_adj = []

    for i, link in enumerate(links_adj, 1):
        info = scrape_detail(driver, link)
        if not info:
            continue
        info["Estado"] = "Adjudicado"
        rows_adj.append(info)

        # --- GUARDAR PROGRESIVAMENTE CADA LOTE ---
        if i % SAVE_BATCH_SIZE == 0:
            LOG("DB", f"Guardando lote intermedio #{i // SAVE_BATCH_SIZE} ({len(rows_adj)} filas)")
            df_partial = pd.DataFrame(rows_adj)
            df_partial.insert(0, "Fecha de Actualización", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            # === NUEVO: expandir proponentes antes de guardar ===
            if not df_partial.empty and "proponentes_data" in df_partial.columns:
                max_props = 0
                for x in df_partial["proponentes_data"]:
                    if isinstance(x, list) and len(x) > max_props:
                        max_props = len(x)
                for n in range(1, max_props + 1):
                    df_partial[f"Proponente {n}"] = df_partial["proponentes_data"].apply(
                        lambda lst: lst[n-1][0] if isinstance(lst, list) and len(lst) >= n else ""
                    )
                    df_partial[f"Precio Proponente {n}"] = df_partial["proponentes_data"].apply(
                        lambda lst: lst[n-1][1] if isinstance(lst, list) and len(lst) >= n else ""
                    )
                df_partial.drop(columns=["proponentes_data"], inplace=True, errors="ignore")

            asegurar_columnas_dinamicas(df_partial)
            db_insert_rows(df_partial)
            rows_all_adj.extend(rows_adj)  # ✅ acumula
            rows_adj = []

    # === NUEVO: acumular los que quedaron (< SAVE_BATCH_SIZE) ===
    if rows_adj:
        rows_all_adj.extend(rows_adj)

    # =========================
    # ESTADO: DESIERTO
    # =========================
    LOG("ESTADO", "Desierto")
    links_des = filtrar_nuevos_enlaces(collect_links_by_state(driver, CFG["query_desierto"]))
    rows_des = []

    for i, link in enumerate(links_des, 1):
        info = scrape_detail(driver, link)
        if not info:
            continue
        info["Estado"] = "Desierto"
        rows_des.append(info)

        # --- GUARDAR PROGRESIVAMENTE CADA LOTE ---
        if i % SAVE_BATCH_SIZE == 0:
            LOG("DB", f"Guardando lote intermedio #{i // SAVE_BATCH_SIZE} ({len(rows_des)} filas)")
            df_partial = pd.DataFrame(rows_des)
            df_partial.insert(0, "Fecha de Actualización", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

            # === NUEVO: expandir proponentes antes de guardar ===
            if not df_partial.empty and "proponentes_data" in df_partial.columns:
                max_props = 0
                for x in df_partial["proponentes_data"]:
                    if isinstance(x, list) and len(x) > max_props:
                        max_props = len(x)
                for n in range(1, max_props + 1):
                    df_partial[f"Proponente {n}"] = df_partial["proponentes_data"].apply(
                        lambda lst: lst[n-1][0] if isinstance(lst, list) and len(lst) >= n else ""
                    )
                    df_partial[f"Precio Proponente {n}"] = df_partial["proponentes_data"].apply(
                        lambda lst: lst[n-1][1] if isinstance(lst, list) and len(lst) >= n else ""
                    )
                df_partial.drop(columns=["proponentes_data"], inplace=True, errors="ignore")

            if "proponentes_data" in df_partial.columns:
                df_partial.drop(columns=["proponentes_data"], inplace=True, errors="ignore")
            asegurar_columnas_dinamicas(df_partial)
            db_insert_rows(df_partial)
            rows_all_des.extend(rows_des)  # ✅ acumula
            rows_des = []
    # === NUEVO: acumular los que quedaron (< SAVE_BATCH_SIZE) ===
    if rows_des:
        rows_all_des.extend(rows_des)

    
    try:
        driver.quit()
    except:
        pass

    # =========================
    # MERGE FINAL Y GUARDADO
    # =========================
    def df_prepare(lst):
        df = pd.DataFrame(lst)
        if df.empty:
            return df
        df.insert(0, "Fecha de Actualización", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # Precio → numérico
        if "precio_referencia" in df.columns:
            serie = df["precio_referencia"].apply(precio_num)
            df["precio_referencia"] = pd.to_numeric(serie, errors="coerce")

        # Eliminar items si existen
        if "items" in df.columns:
            df.drop(columns=["items"], inplace=True, errors="ignore")

        df.fillna("", inplace=True)
        return df

    df_all = pd.concat([df_prepare(rows_all_adj), df_prepare(rows_all_des)], ignore_index=True) \
                  if (rows_all_adj or rows_all_des) else pd.DataFrame()


    # === Expandir columnas dinámicas al final ===
    if not df_all.empty and "proponentes_data" in df_all.columns:
        max_props = 0
        for x in df_all["proponentes_data"]:
            if isinstance(x, list) and len(x) > max_props:
                max_props = len(x)
        for i in range(1, max_props + 1):
            df_all[f"Proponente {i}"] = df_all["proponentes_data"].apply(
                lambda lst: (lst[i-1][0] if isinstance(lst, list) and len(lst) >= i else "")
            )
            df_all[f"Precio Proponente {i}"] = df_all["proponentes_data"].apply(
                lambda lst: (lst[i-1][1] if isinstance(lst, list) and len(lst) >= i else "")
            )
        df_all.drop(columns=["proponentes_data"], inplace=True, errors="ignore")

    # ---- Guardado final ----
    if not df_all.empty:
        LOG("DB", f"Insertando {len(df_all)} filas nuevas en la base de datos local")
        asegurar_columnas_dinamicas(df_all)
        db_insert_rows(df_all)
    else:
        LOG("DB", "0 filas nuevas")


if __name__ == "__main__":
    main()
