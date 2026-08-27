# -*- coding: utf-8 -*-
"""Actualizador robusto de la base histórica de Panamá Compra.

La fuente principal es la API pública V3. El proceso trabaja por ventanas de
fechas, guarda checkpoints y puede reanudarse después de una interrupción.
Publica una copia validada para GEAPP/OneDrive y, si existe DATABASE_URL o
SUPABASE_DB_URL en el entorno, sincroniza las filas procesadas con PostgreSQL.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import json
import os
import re
import shutil
import sqlite3
import sys
import threading
import time
import unicodedata
from contextlib import contextmanager
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from requests.adapters import HTTPAdapter


REPO_ROOT = Path(__file__).resolve().parents[1]
COMMON_DIR = REPO_ROOT / "common"
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from ficha_utils import (  # noqa: E402 - depende de REPO_ROOT
    DETECTOR_VERSION,
    FichaMatch,
    detectar_fichas_detalladas,
    detection_json,
    get_catalog,
    legacy_tokens,
)

DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "db" / "panamacompra.db"
CONFIG_PATH = Path(__file__).with_name("update_config.json")
CREDENTIALS_FILE = REPO_ROOT / "credentials" / "service-account.json"

PANAMA_TZ = ZoneInfo("America/Panama")
API_ROOT = "https://apisv3.panamacompra.gob.pa"
LIST_ENDPOINT = f"{API_ROOT}/busqueda/proceso-lista-publico"
DETAIL_ENDPOINT = (
    API_ROOT
    + "/procesos-configuracion/pagina-componentes-publico/{tipo}/procesoVistaPliego/{flujo}"
)
API_PAGE_SIZE = 5000
DEFAULT_WINDOW_DAYS = 7
DEFAULT_OVERLAP_DAYS = 14
DEFAULT_TERMINAL_LOOKBACK_DAYS = 180
DEFAULT_WORKERS = 12
DEFAULT_BACKUP_KEEP = 4
API_TIMEOUT = (10, 60)
STATES = {1011: "Adjudicado", 16: "Desierto"}
RESULT_ENRICHMENT_VERSION = "2026-08-26-actas-v1"

BASE_COLUMNS = {
    "fecha_actualizacion": "TEXT",
    "publicacion": "TEXT",
    "enlace": "TEXT",
    "titulo": "TEXT",
    "precio_referencia": "REAL",
    "fecha": "TEXT",
    "entidad": "TEXT",
    "unidad_solic": "TEXT",
    "termino_entrega": "TEXT",
    "ficha_detectada": "TEXT",
    "fichas_detectadas_json": "TEXT",
    "ficha_detector_version": "TEXT",
    "ficha_catalogo_version": "TEXT",
    "ficha_detectada_at": "TEXT",
    "items_json": "TEXT",
    "source_flow_id": "TEXT",
    "source_tipo_proceso": "TEXT",
    "source_record_json": "TEXT",
    "estado": "TEXT",
    "descripcion": "TEXT",
    "razon_social": "TEXT",
    "nombre_comercial": "TEXT",
    "fecha_adjudicacion": "TEXT",
    "total_items_ofertados": "TEXT",
    "num_participantes": "TEXT",
    "proponentes_json": "TEXT",
    "ganadores_json": "TEXT",
    "resultado_fuente_version": "TEXT",
    "resultado_fuente_estado": "TEXT",
}

_thread_local = threading.local()
_process_lock_handle = None


def log(tag: str, message: str) -> None:
    now = datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} | {tag:<10} | {message}", flush=True)


def acquire_process_lock() -> None:
    """Impide dos actualizaciones concurrentes sobre la misma SQLite."""
    global _process_lock_handle
    lock_path = DB_PATH.with_suffix(".update.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "a+", encoding="utf-8")  # noqa: SIM115 - debe vivir todo el proceso
    try:
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            if lock_path.stat().st_size == 0:
                handle.write(" ")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        handle.close()
        raise RuntimeError(
            "Ya existe otra actualización de panamacompra.db en ejecución; no se iniciará una segunda."
        ) from exc
    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    _process_lock_handle = handle


def _session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "Accept": "application/json;charset=utf-8",
                "Referer": "https://www.panamacompra.gob.pa/",
                "Origin": "https://www.panamacompra.gob.pa",
                "User-Agent": "GEAPP-PanamaCompra-Updater/2.0",
            }
        )
        _thread_local.session = session
    return session


def request_json(method: str, url: str, *, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(6):
        try:
            response = _session().request(method, url, json=payload, timeout=API_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or data.get("status") not in (1, "1", True):
                raise RuntimeError(f"Respuesta API no exitosa: {str(data)[:500]}")
            return data
        except Exception as exc:  # red remota: reintento controlado
            last_error = exc
            if attempt == 5:
                break
            delay = min(30.0, 1.5 * (2**attempt))
            log("RETRY", f"{url.rsplit('/', 1)[-1]} intento {attempt + 1}/6: {exc}; espera {delay:.1f}s")
            time.sleep(delay)
    raise RuntimeError(f"API agotó reintentos para {url}: {last_error}")


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        log("WARN", f"No se pudo leer {CONFIG_PATH}: {exc}")
        return {}


def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


@contextmanager
def connect_db(path: Path | None = None):
    path = path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with connect_db() as conn:
        conn.execute(
            """
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
                fichas_detectadas_json TEXT,
                ficha_detector_version TEXT,
                ficha_catalogo_version TEXT,
                ficha_detectada_at TEXT,
                items_json TEXT,
                source_flow_id TEXT,
                source_tipo_proceso TEXT,
                source_record_json TEXT,
                estado TEXT,
                descripcion TEXT,
                razon_social TEXT,
                nombre_comercial TEXT,
                fecha_adjudicacion TEXT,
                total_items_ofertados TEXT,
                num_participantes TEXT
            )
            """
        )
        existing = {row[1] for row in conn.execute("PRAGMA table_info(actos_publicos)")}
        for column, sql_type in BASE_COLUMNS.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE actos_publicos ADD COLUMN {qident(column)} {sql_type}")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS db_metadata (
                key TEXT PRIMARY KEY, value TEXT, updated_at TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS db_update_windows (
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                status TEXT NOT NULL,
                records_found INTEGER DEFAULT 0,
                rows_written INTEGER DEFAULT 0,
                detail_failures INTEGER DEFAULT 0,
                error TEXT DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(window_start, window_end)
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS db_failed_processes (
                flow_id INTEGER PRIMARY KEY,
                process_json TEXT NOT NULL,
                last_error TEXT NOT NULL,
                attempts INTEGER DEFAULT 1,
                updated_at TEXT NOT NULL
            )"""
        )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_actos_publicos_enlace ON actos_publicos(enlace)")
        conn.execute(
            """CREATE INDEX IF NOT EXISTS ix_actos_ficha_detector_versions
               ON actos_publicos(ficha_detector_version, ficha_catalogo_version)"""
        )


def create_database_backup(keep: int = DEFAULT_BACKUP_KEEP) -> Path | None:
    """Crea una copia SQLite consistente antes de modificar la base."""
    if not DB_PATH.exists() or DB_PATH.stat().st_size == 0:
        return None
    backup_dir = DB_PATH.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(PANAMA_TZ).strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"panamacompra_{stamp}.db"
    source = sqlite3.connect(DB_PATH, timeout=60)
    target = sqlite3.connect(backup_path, timeout=60)
    try:
        source.backup(target)
        target.commit()
    finally:
        target.close()
        source.close()
    verify_sqlite(backup_path)
    log("BACKUP", f"Copia previa creada: {backup_path}")

    backups = sorted(backup_dir.glob("panamacompra_*.db"), key=lambda item: item.stat().st_mtime)
    for old_backup in backups[: max(0, len(backups) - max(1, keep))]:
        old_backup.unlink(missing_ok=True)
        log("BACKUP", f"Copia antigua eliminada: {old_backup.name}")
    return backup_path


def metadata_get(key: str) -> str:
    try:
        with connect_db() as conn:
            row = conn.execute("SELECT value FROM db_metadata WHERE key=?", (key,)).fetchone()
            return str(row[0]).strip() if row and row[0] is not None else ""
    except Exception:
        return ""


def metadata_set(key: str, value: Any) -> None:
    stamp = datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    with connect_db() as conn:
        conn.execute(
            """INSERT INTO db_metadata(key,value,updated_at) VALUES(?,?,?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
            (key, str(value), stamp),
        )


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = "".join(
        ch for ch in unicodedata.normalize("NFD", text.lower()) if unicodedata.category(ch) != "Mn"
    )
    return re.sub(r"\s+", " ", text).strip()


def _numeric_ficha(value: Any) -> str:
    text = str(value or "").strip()
    match = re.fullmatch(r"(\d{1,6})(?:\.0+)?", text)
    return match.group(1) if match else ""


def _detect_fichas_with_line_evidence(
    fields: dict[str, Any],
) -> tuple[list[FichaMatch], list[FichaMatch]]:
    """Detecta fichas y conserva cada renglÃ³n donde aparecieron.

    ``detectar_fichas_detalladas`` devuelve la mejor coincidencia por ficha.
    Para la clasificaciÃ³n general eso es correcto, pero la atribuciÃ³n monetaria
    necesita saber si la misma ficha aparece en item_1, item_2, etc. Esta capa
    mantiene ambas vistas sin cambiar la columna legada ``ficha_detectada``.
    """

    best: dict[str, FichaMatch] = {}
    detailed: list[FichaMatch] = []
    seen: set[tuple[str, str, str, str, int]] = set()
    for field, value in fields.items():
        if value is None or not str(value).strip():
            continue
        for match in detectar_fichas_detalladas({str(field): value}):
            identity = (
                match.code,
                match.method,
                match.field,
                match.evidence,
                int(match.score),
            )
            if identity not in seen:
                seen.add(identity)
                detailed.append(match)
            current = best.get(match.code)
            if current is None or match.score > current.score:
                best[match.code] = match
    ordered_best = sorted(best.values(), key=lambda item: (-item.score, int(item.code)))
    ordered_detail = sorted(
        detailed,
        key=lambda item: (int(item.code), item.field, -item.score, item.method),
    )
    return ordered_best, ordered_detail


class FichaMatcher:
    def __init__(self) -> None:
        catalog = get_catalog()
        self.valid = set(catalog.valid_codes)
        self.names_by_token: dict[str, list[tuple[str, str]]] = {}
        self.catalog_version = catalog.version
        aliases_count = sum(len(entries) for entries in catalog.aliases_by_anchor.values())
        log(
            "FICHAS",
            f"detector={DETECTOR_VERSION} catalogo={self.catalog_version} | "
            f"{len(self.valid):,} numeros y {aliases_count:,} aliases indexados",
        )

    def _load(self) -> None:
        numeric_candidates = [
            DATA_DIR / "fichas" / "todas_las_fichas.xlsx",
            Path.home() / "fichas" / "fichas-y-nombre.xlsx",
        ]
        name_candidates = [
            Path.home() / "fichas" / "fichas-y-nombre.xlsx",
            Path.home() / "GEAPP" / "fichas_ctni_con_enlace.xlsx",
        ]
        for path in numeric_candidates:
            if not path.exists():
                continue
            try:
                frame = pd.read_excel(path, header=None, dtype=str)
                self.valid.update(filter(None, (_numeric_ficha(v) for v in frame.iloc[:, 0].tolist())))
            except Exception as exc:
                log("WARN", f"No se pudieron cargar fichas de {path}: {exc}")

        names: dict[str, str] = {}
        for path in name_candidates:
            if not path.exists():
                continue
            try:
                frame = pd.read_excel(path, header=None, dtype=str)
                if frame.shape[1] < 2:
                    continue
                for _, row in frame.iterrows():
                    ficha = _numeric_ficha(row.iloc[0])
                    generic = normalize_text(row.iloc[1])
                    if ficha and generic and generic not in {"nombre generico", "nombre generico."}:
                        self.valid.add(ficha)
                        names.setdefault(generic, ficha)
            except Exception as exc:
                log("WARN", f"No se pudieron cargar nombres de {path}: {exc}")

        for generic, ficha in names.items():
            tokens = re.findall(r"[a-z0-9]+", generic)
            if not tokens:
                continue
            token = max(tokens, key=len)
            self.names_by_token.setdefault(token, []).append((generic, ficha))
        log("FICHAS", f"{len(self.valid):,} números y {len(names):,} nombres genéricos disponibles")

    def detect(self, text: str) -> str:
        return self.classify({"texto": text})["ficha_detectada"]

    def classify(self, fields: dict[str, Any]) -> dict[str, str]:
        matches, detailed_matches = _detect_fichas_with_line_evidence(fields)
        tokens = legacy_tokens(matches)
        return {
            "ficha_detectada": ", ".join(tokens) if tokens else "No Detectada",
            "fichas_detectadas_json": detection_json(detailed_matches),
            "ficha_detector_version": DETECTOR_VERSION,
            "ficha_catalogo_version": self.catalog_version,
            "ficha_detectada_at": datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        }

    def detect_legacy(self, text: str) -> str:
        """Implementacion anterior conservada solo para comparar diagnosticos."""
        if not text:
            return "No Detectada"
        numeric: set[str] = set()
        for match in re.finditer(r"(?<![\w\d])(\d{1,6})(?![\w\d])", text):
            ficha = match.group(1)
            if ficha in self.valid:
                numeric.add(ficha)

        normalized = normalize_text(text)
        padded = f" {normalized} "
        named: set[str] = set()
        for token in set(re.findall(r"[a-z0-9]+", normalized)):
            for generic, ficha in self.names_by_token.get(token, ()):  # candidatos pequeños por token
                if ficha not in numeric and f" {generic} " in padded:
                    named.add(ficha)
        ordered = sorted(numeric, key=lambda x: int(x)) + [
            f"* {value}" for value in sorted(named, key=lambda x: int(x))
        ]
        return ", ".join(ordered) if ordered else "No Detectada"


def parse_source_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for pattern, order in (
        (r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b", "ymd"),
        (r"\b(\d{1,2})[-/](\d{1,2})[-/](20\d{2})\b", "dmy"),
    ):
        match = re.search(pattern, text)
        if not match:
            continue
        try:
            parts = [int(part) for part in match.groups()]
            return date(*parts) if order == "ymd" else date(parts[2], parts[1], parts[0])
        except ValueError:
            continue
    return None


def latest_source_date() -> date | None:
    latest: date | None = None
    with connect_db() as conn:
        cursor = conn.execute("SELECT publicacion, fecha, fecha_adjudicacion FROM actos_publicos")
        for row in cursor:
            for value in row:
                parsed = parse_source_date(value)
                if parsed and parsed <= datetime.now(PANAMA_TZ).date() and (latest is None or parsed > latest):
                    latest = parsed
    return latest


def date_to_api_start(value: date) -> datetime:
    return datetime.combine(value, dt_time.min, PANAMA_TZ)


def api_iso(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def listing_payload(state: int, start: datetime, end: datetime, size: int = API_PAGE_SIZE) -> dict[str, Any]:
    return {
        "registrosPorPagina": size,
        "valorSiguiente": "",
        "filtro": {
            "idEstado": state,
            "idTipoProceso": -1,
            "titulo": "",
            "fechaDesde": api_iso(start),
            "fechaHasta": api_iso(end),
            "idProvincia": 0,
        },
    }


def fetch_listing(state: int, start: datetime, end: datetime, depth: int = 0) -> list[dict[str, Any]]:
    result = request_json("POST", LIST_ENDPOINT, payload=listing_payload(state, start, end)).get("result") or {}
    records = result.get("registros") or []
    if len(records) < API_PAGE_SIZE:
        return records

    span = end - start
    if span <= timedelta(seconds=1) or depth >= 30:
        raise RuntimeError(
            f"Ventana saturada con {len(records)} registros entre {start.isoformat()} y {end.isoformat()}"
        )
    midpoint = start + span / 2
    right_start = midpoint + timedelta(milliseconds=1)
    log("SPLIT", f"Estado {state}: API alcanzó {API_PAGE_SIZE}; dividiendo {start} -> {end}")
    return fetch_listing(state, start, midpoint, depth + 1) + fetch_listing(
        state, right_start, end, depth + 1
    )


def encode_route_payload(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    # El frontend elimina el padding Base64 antes de invertir el token.
    return base64.b64encode(raw).decode("ascii").rstrip("=")[::-1]


def process_link(record: dict[str, Any]) -> str:
    prefix = str(record.get("prefijo") or "").upper()
    route = "solicitud-de-cotizacion" if prefix == "CL" else "pliego-de-cargos"
    route_payload: dict[str, Any] = {
        "i": int(record["idProcesosContratacionFlujos"]),
        "tp": int(record["idTipoProceso"]),
    }
    if record.get("rutaNueva") is not None:
        route_payload["rn"] = record["rutaNueva"]
    token = encode_route_payload(route_payload)
    return (
        "https://www.panamacompra.gob.pa/Inicio/#/"
        f"{route}/{record.get('numProceso', '')}/{token}"
    )


def _component_labels(components: list[dict[str, Any]]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for component in components:
        value = component.get("value")
        if not isinstance(value, list):
            continue
        for item in value:
            if isinstance(item, dict) and "nombre" in item and "value" in item:
                labels.setdefault(normalize_text(item.get("nombre")), str(item.get("value") or "").strip())
    return labels


def _label(labels: dict[str, str], *names: str) -> str:
    for name in names:
        value = labels.get(normalize_text(name), "")
        if value:
            return value
    return ""


def _first_label_containing(labels: dict[str, str], phrases: Iterable[str]) -> str:
    normalized = [normalize_text(value) for value in phrases]
    for key, value in labels.items():
        if value and any(phrase in key for phrase in normalized):
            return value
    return ""


def money_number(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = re.search(r"-?\d[\d.,]*", text)
    if not match:
        return None
    number = match.group(0)
    if "," in number and "." in number:
        number = number.replace(",", "") if number.rfind(".") > number.rfind(",") else number.replace(".", "").replace(",", ".")
    elif number.count(",") == 1 and len(number.rsplit(",", 1)[1]) <= 2:
        number = number.replace(",", ".")
    else:
        number = number.replace(",", "")
    try:
        return float(number)
    except ValueError:
        return None


def format_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(PANAMA_TZ).strftime("%d-%m-%Y")
    except ValueError:
        parsed_date = parse_source_date(text)
        return parsed_date.strftime("%d-%m-%Y") if parsed_date else text.split(" - ", 1)[0].strip()


def fallback_row(record: dict[str, Any], state_name: str, run_stamp: str, matcher: FichaMatcher) -> dict[str, Any]:
    title = str(record.get("titulo") or "").strip()
    description = str(record.get("observaciones") or "").strip()
    publication = format_date(record.get("fechaPublicacion"))
    state_date = format_date(record.get("fechaEstado"))
    classification = matcher.classify({"titulo": title, "descripcion": description})
    row = {
        "fecha_actualizacion": run_stamp,
        "publicacion": publication,
        "enlace": process_link(record),
        "titulo": title,
        "precio_referencia": None,
        "fecha": state_date,
        "entidad": str(record.get("nombreEntidad") or "").strip(),
        "unidad_solic": str(record.get("nombreUnidadCompra") or "").strip(),
        "termino_entrega": "",
        "estado": state_name,
        "descripcion": description,
        "items_json": "[]",
        "source_flow_id": str(record.get("idProcesosContratacionFlujos") or ""),
        "source_tipo_proceso": str(record.get("idTipoProceso") or ""),
        "source_record_json": json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str),
        "razon_social": "",
        "nombre_comercial": "",
        "fecha_adjudicacion": state_date if state_name == "Adjudicado" else "",
        "total_items_ofertados": "",
        "num_participantes": "",
        "proponentes_json": "[]",
        "ganadores_json": "[]",
        "resultado_fuente_version": "",
        "resultado_fuente_estado": "detalle_principal_fallido",
    }
    row.update(classification)
    return row


def _component_rows(component: dict[str, Any]) -> list[dict[str, Any]]:
    """Obtiene filas aunque la API cambie el contenedor list/dict."""
    value = component.get("value")
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        for key in ("registros", "items", "data", "result"):
            nested = value.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
    return []


def _nested_dicts(value: Any) -> Iterable[dict[str, Any]]:
    """Recorre contenedores variables de las actas sin asumir una sola forma."""

    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _nested_dicts(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _nested_dicts(nested)


def _offer_name(value: dict[str, Any]) -> str:
    company = value.get("empresa") if isinstance(value.get("empresa"), dict) else {}
    return str(
        company.get("nombreComercial")
        or company.get("razonSocial")
        or value.get("nombreComercial")
        or value.get("razonSocial")
        or value.get("nombreProponente")
        or value.get("proveedor")
        or ""
    ).strip()


def _offer_ruc(value: dict[str, Any]) -> str:
    company = value.get("empresa") if isinstance(value.get("empresa"), dict) else {}
    return str(company.get("ruc") or value.get("ruc") or "").strip()


def _offer_amount(value: dict[str, Any]) -> float:
    direct = _first_money(
        value,
        ("nuevoPrecioTotal", "precioTotal", "montoTotal", "totalOferta", "total"),
    )
    if direct is not None:
        return round(float(direct), 2)

    offered_items = value.get("procesosOfertasItems")
    if isinstance(offered_items, list):
        total = 0.0
        found = False
        for item in offered_items:
            if not isinstance(item, dict):
                continue
            amount = _first_money(
                item,
                ("nuevoPrecioTotal", "precioTotal", "montoTotal", "total"),
            )
            if amount is not None:
                total += float(amount)
                found = True
        if found:
            return round(total, 2)
    return 0.0


def _offers_from_components(
    components: Iterable[dict[str, Any]],
    component_types: set[str],
    *,
    source: str,
) -> list[dict[str, Any]]:
    offers: list[dict[str, Any]] = []
    normalized_types = {normalize_text(value).replace(" ", "") for value in component_types}
    for component in components:
        component_type = normalize_text(component.get("tipo")).replace(" ", "")
        if component_type not in normalized_types:
            continue
        for candidate in _nested_dicts(component.get("value")):
            name = _offer_name(candidate)
            if not name:
                continue
            # Evita materializar el subobjeto ``empresa`` ademas de su oferta.
            if set(candidate).issubset({"nombreComercial", "razonSocial", "ruc"}):
                continue
            offers.append(
                {
                    "nombre": name,
                    "ruc": _offer_ruc(candidate),
                    "monto": _offer_amount(candidate),
                    "fuente": source,
                }
            )
    return offers


def _dedupe_offers(values: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Consolida la misma empresa repetida en apertura y adjudicacion."""

    result: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for value in values:
        name = str(value.get("nombre") or "").strip()
        key = normalize_text(name)
        if not key:
            continue
        if key not in result:
            result[key] = dict(value)
            order.append(key)
            continue
        current = result[key]
        if float(value.get("monto") or 0.0) > float(current.get("monto") or 0.0):
            current["monto"] = float(value.get("monto") or 0.0)
        if not current.get("ruc") and value.get("ruc"):
            current["ruc"] = value.get("ruc")
        sources = {
            item.strip()
            for item in f"{current.get('fuente', '')},{value.get('fuente', '')}".split(",")
            if item.strip()
        }
        current["fuente"] = ",".join(sorted(sources))
    return [result[key] for key in order]


def _official_result_pages(
    components: list[dict[str, Any]],
    *,
    tipo: int,
    flow: int,
) -> tuple[list[dict[str, Any]], int, int]:
    """Carga apertura/adjudicacion, donde Panama Compra guarda el resultado real.

    Algunos LP/CM no publican proponentes ni adjudicatarios en la pantalla
    principal. Las rutas oficiales vienen declaradas en
    ``componentProcesosActasPliego`` y se consumen con el mismo contrato usado
    por el frontend. Un fallo temporal no elimina el detalle principal: la
    version de enriquecimiento queda pendiente para reintentarlo.
    """

    actas = [
        item
        for component in components
        if normalize_text(component.get("tipo")).replace(" ", "")
        == "componentprocesosactaspliego"
        for item in _component_rows(component)
    ]
    requests_to_make: list[tuple[str, dict[str, Any]]] = []
    seen: set[str] = set()
    for acta in actas:
        route = str(acta.get("rutaNueva") or "").strip()
        route_norm = normalize_text(f"{route} {acta.get('nombre', '')}")
        if not route or not any(
            token in route_norm
            for token in ("acta apertura", "ver acta apertura", "adjudicacion")
        ):
            continue
        payload = dict(acta.get("paramsBody") or {})
        payload["idTipoProceso"] = tipo
        payload["idProcesosContratacionFlujos"] = flow
        key = f"{route}|{json.dumps(payload, sort_keys=True, default=str)}"
        if key in seen:
            continue
        seen.add(key)
        requests_to_make.append((route, payload))

    result_components: list[dict[str, Any]] = []
    failures = 0
    for route, payload in requests_to_make:
        endpoint = f"{API_ROOT}{route.rstrip('/')}"
        if not endpoint.endswith("/get-page"):
            endpoint += "/get-page"
        try:
            page = request_json("POST", endpoint, payload=payload).get("result") or {}
            result_components.extend(
                item
                for item in (page.get("pageComponentes") or [])
                if isinstance(item, dict)
            )
        except Exception as exc:
            failures += 1
            log("ACTA_WARN", f"flujo={flow} ruta={route}: {exc}")
    return result_components, len(requests_to_make), failures


def _item_description(item: dict[str, Any]) -> str:
    """Lee las variantes conocidas del texto de un renglon solicitado."""
    for key in ("descripcion", "descripcionItem", "description", "detalle", "nombre"):
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    nested = item.get("item")
    return _item_description(nested) if isinstance(nested, dict) else ""


def _first_money(item: dict[str, Any], keys: Iterable[str]) -> float | None:
    """Devuelve el primer importe numérico presente, incluido cero explícito."""

    for key in keys:
        if key not in item or item.get(key) in (None, ""):
            continue
        value = money_number(item.get(key))
        if value is not None:
            return float(value)
    return None


def _item_detail_record(item: dict[str, Any]) -> dict[str, Any]:
    """Conserva el detalle mínimo necesario para atribuir montos por renglón.

    Las corridas antiguas almacenaban únicamente una lista de descripciones.
    El formato enriquecido sigue siendo compatible con ``_item_fields_from_json``
    y añade cantidad, número de renglón e importes referenciales auditables.
    """

    nested = item.get("item") if isinstance(item.get("item"), dict) else {}
    source = {**nested, **item}
    quantity = _first_money(
        source,
        ("cantidad", "cantidadSolicitada", "cantidadRequerida", "quantity"),
    )
    reference_total = _first_money(
        source,
        (
            "precioReferenciaTotal",
            "montoReferencia",
            "montoTotalReferencia",
            "precioTotalReferencia",
            "subtotalReferencia",
        ),
    )
    reference_unit = _first_money(
        source,
        (
            "precioReferenciaUnitario",
            "precioUnitarioReferencia",
            "referenceUnitPrice",
        ),
    )
    legacy_reference = _first_money(source, ("precioReferencia",))

    # La API histórica expone ``precioReferencia`` como importe del renglón.
    # Se conserva esa semántica, ya utilizada para formar el total del acto.
    if reference_total is None:
        reference_total = legacy_reference
    if reference_total is None and reference_unit is not None and quantity and quantity > 0:
        reference_total = reference_unit * quantity
    if reference_unit is None and reference_total is not None:
        reference_unit = (
            reference_total / quantity
            if quantity and quantity > 0
            else reference_total
        )

    return {
        "descripcion": _item_description(source),
        "numero_renglon": str(
            source.get("numRenglon")
            or source.get("numeroRenglon")
            or source.get("renglon")
            or source.get("itemNumber")
            or ""
        ).strip(),
        "cantidad": float(quantity or 0.0),
        "unidad": str(
            source.get("unidad")
            or source.get("unidadMedida")
            or source.get("nombreUnidad")
            or ""
        ).strip(),
        "precio_referencia_unitario": float(reference_unit or 0.0),
        "precio_referencia_total": float(reference_total or 0.0),
    }


def parse_detail(
    record: dict[str, Any], state_name: str, run_stamp: str, matcher: FichaMatcher
) -> dict[str, Any]:
    flow = int(record["idProcesosContratacionFlujos"])
    tipo = int(record["idTipoProceso"])
    url = DETAIL_ENDPOINT.format(tipo=tipo, flujo=flow)
    detail = request_json("GET", url).get("result") or {}
    components = detail.get("pageComponentes") or []
    labels = _component_labels(components)

    items: list[dict[str, Any]] = []
    actas: list[dict[str, Any]] = []
    for component in components:
        component_type = str(component.get("tipo") or "")
        component_type_normalized = normalize_text(component_type).replace(" ", "")
        rows = _component_rows(component)
        if component_type_normalized in {"componentitems", "componentitemspliego"}:
            items.extend(rows)
        elif component_type_normalized == "componentprocesosactaspliego":
            actas.extend(rows)

    result_components, result_routes, result_failures = _official_result_pages(
        components,
        tipo=tipo,
        flow=flow,
    )
    all_result_components = [*components, *result_components]

    title = _label(labels, "Título") or str(record.get("titulo") or "").strip()
    description = _label(labels, "Descripción") or str(record.get("observaciones") or "").strip()
    item_details = [
        detail
        for item in items
        if (detail := _item_detail_record(item)).get("descripcion")
    ]
    item_descriptions = [str(detail["descripcion"]) for detail in item_details]
    item_fields = {
        f"item_{index}": value for index, value in enumerate(item_descriptions, 1) if value
    }
    classification = matcher.classify(
        {"titulo": title, "descripcion": description, **item_fields}
    )

    winners = _dedupe_offers(
        _offers_from_components(
            all_result_components,
            {"componentOfertasAdjudicadasProponentes"},
            source="adjudicacion_oficial",
        )
    )
    participants = _dedupe_offers(
        [
            *_offers_from_components(
                all_result_components,
                {
                    "componentProcesosActasCuadroCotizacionesOR",
                    "componentProcesosActasCuadroCotizacionesOD",
                },
                source="acta_apertura",
            ),
            *winners,
        ]
    )

    award_date = ""
    for acta in actas:
        if "adjudic" in normalize_text(acta.get("nombre")):
            award_date = format_date(acta.get("fecha") or acta.get("fechaRealizada"))
            if award_date:
                break
    if not award_date and state_name == "Adjudicado":
        award_date = format_date(record.get("fechaEstado"))

    price_text = _label(labels, "Precio de referencia", "Monto de la contratación", "Precio estimado")
    reference_price = money_number(price_text)
    if reference_price is None and items:
        values = [money_number(item.get("precioReferencia")) for item in items]
        reference_price = round(sum(value for value in values if value is not None), 2)

    presentation_date = _first_label_containing(
        labels,
        ("fecha y hora presentacion", "fecha y hora de presentacion", "fecha y hora de apertura"),
    )
    publication = _label(labels, "Fecha de Publicación") or record.get("fechaPublicacion")
    reason = _label(labels, "Razón Social")
    commercial = _label(labels, "Nombre Comercial")
    if state_name == "Desierto":
        # Una propuesta residual no convierte un acto desierto en adjudicado.
        reason = ""
        commercial = ""
        winners = []
    elif winners:
        commercial = str(winners[0].get("nombre") or commercial).strip()
        reason = reason or commercial

    winner_total = round(sum(float(value.get("monto") or 0.0) for value in winners), 2)
    result_complete = result_failures == 0 and (
        state_name == "Desierto"
        or bool(winners)
        or bool(reason or commercial)
    )
    if result_failures:
        result_status = "actas_con_error_temporal"
    elif result_complete:
        result_status = (
            "completo_actas_oficiales"
            if result_routes
            else "completo_detalle_principal"
        )
    else:
        result_status = "adjudicatario_pendiente"
    row: dict[str, Any] = {
        "fecha_actualizacion": run_stamp,
        "publicacion": format_date(publication),
        "enlace": process_link(record),
        "titulo": title,
        "precio_referencia": reference_price,
        "fecha": format_date(presentation_date or record.get("fechaEstado")),
        "entidad": _label(labels, "Entidad") or str(record.get("nombreEntidad") or "").strip(),
        "unidad_solic": _label(labels, "Unidad de Compra") or str(record.get("nombreUnidadCompra") or "").strip(),
        "termino_entrega": _label(labels, "Término de entrega"),
        "estado": state_name,
        "descripcion": description,
        "items_json": json.dumps(item_details, ensure_ascii=False, separators=(",", ":")),
        "source_flow_id": str(flow),
        "source_tipo_proceso": str(tipo),
        "source_record_json": json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str),
        "razon_social": reason,
        "nombre_comercial": commercial,
        "fecha_adjudicacion": award_date,
        "total_items_ofertados": f"{winner_total:.2f}" if winners else "",
        "num_participantes": str(len(participants)),
        "proponentes_json": json.dumps(
            participants,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "ganadores_json": json.dumps(
            winners,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "resultado_fuente_version": RESULT_ENRICHMENT_VERSION if result_complete else "",
        "resultado_fuente_estado": result_status,
    }
    row.update(classification)
    for index, participant in enumerate(participants, 1):
        row[f"Proponente {index}"] = str(participant.get("nombre") or "")
        row[f"Precio Proponente {index}"] = (
            f"{float(participant.get('monto') or 0.0):.2f}"
        )
    return row


def ensure_columns(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> list[str]:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(actos_publicos)")}
    # Incluye las columnas dinamicas ya existentes para limpiar propuestas
    # obsoletas cuando una reconciliacion devuelve menos participantes.
    wanted = set(BASE_COLUMNS) | {
        column
        for column in existing
        if re.fullmatch(r"(?:Proponente|Precio Proponente) \d+", column)
    }
    for row in rows:
        wanted.update(
            key
            for key in row
            if re.fullmatch(r"(?:Proponente|Precio Proponente) \d+", key)
        )
    for column in sorted(wanted - existing):
        sql_type = BASE_COLUMNS.get(column, "TEXT")
        conn.execute(f"ALTER TABLE actos_publicos ADD COLUMN {qident(column)} {sql_type}")
    return [column for column in BASE_COLUMNS if column in wanted] + sorted(
        (column for column in wanted if column not in BASE_COLUMNS),
        key=lambda value: ("Precio" in value, int(re.search(r"\d+", value).group())),
    )


def upsert_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with connect_db() as conn:
        columns = ensure_columns(conn, rows)
        placeholders = ",".join("?" for _ in columns)
        updates = ",".join(
            f"{qident(column)}=excluded.{qident(column)}" for column in columns if column != "enlace"
        )
        statement = (
            f"INSERT INTO actos_publicos ({','.join(qident(column) for column in columns)}) "
            f"VALUES ({placeholders}) ON CONFLICT(enlace) DO UPDATE SET {updates}"
        )
        values = []
        for row in rows:
            values.append(
                tuple(
                    None
                    if row.get(column) is None
                    else row.get(column)
                    if isinstance(row.get(column), (int, float))
                    else str(row.get(column, ""))
                    for column in columns
                )
            )
        conn.executemany(statement, values)
    return len(rows)


def _item_fields_from_json(raw_value: Any) -> dict[str, str]:
    try:
        payload = json.loads(str(raw_value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, list):
        return {}
    fields: dict[str, str] = {}
    for index, item in enumerate(payload, 1):
        if isinstance(item, dict):
            value = item.get("descripcion") or item.get("description") or ""
        else:
            value = item
        text = str(value or "").strip()
        if text:
            fields[f"item_{index}"] = text
    return fields


def _merge_legacy_matches(
    matches: list[FichaMatch], old_value: Any, valid_codes: set[str]
) -> list[FichaMatch]:
    """No borra fichas antiguas cuya evidencia de item aun no fue preservada."""
    found = {match.code for match in matches}
    merged = list(matches)
    old_text = str(old_value or "")
    for raw_token in old_text.split(","):
        code_match = re.search(r"(?<!\d)(\d{1,6})(?!\d)", raw_token)
        if not code_match:
            continue
        code = code_match.group(1).lstrip("0") or "0"
        if code in found:
            continue
        in_catalog = code in valid_codes
        merged.append(
            FichaMatch(
                code=code,
                method=(
                    "legacy_nombre_preservado"
                    if raw_token.strip().startswith("*")
                    else "codigo_legacy_preservado"
                ),
                field="ficha_detectada_anterior",
                evidence=(
                    raw_token.strip()
                    if in_catalog
                    else f"{raw_token.strip()} (fuera del catalogo actual)"
                ),
                score=70 if in_catalog else 60,
            )
        )
        found.add(code)
    return sorted(merged, key=lambda item: (-item.score, int(item.code)))


def reclassify_existing(
    matcher: FichaMatcher,
    *,
    force: bool = False,
    only_unresolved: bool = False,
    batch_size: int = 1000,
) -> tuple[int, int]:
    """Reclasifica localmente sin descargar la API y conserva evidencia previa.

    Por defecto solo procesa filas creadas con otra version del detector o del
    catalogo. ``force`` permite una reconstruccion completa y repetible.
    """
    processed = 0
    changed = 0
    last_id = 0
    run_stamp = datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    version_clause = "1=1" if force else (
        "(COALESCE(ficha_detector_version,'')<>? OR "
        "COALESCE(ficha_catalogo_version,'')<>?)"
    )
    unresolved_clause = (
        " AND (TRIM(COALESCE(ficha_detectada,''))='' OR lower(ficha_detectada)='no detectada')"
        if only_unresolved else ""
    )

    with connect_db() as conn:
        while True:
            params: list[Any] = [last_id]
            if not force:
                params.extend([DETECTOR_VERSION, matcher.catalog_version])
            params.append(max(1, int(batch_size)))
            rows = conn.execute(
                f"""SELECT id,titulo,descripcion,items_json,ficha_detectada
                    FROM actos_publicos
                    WHERE id>? AND {version_clause}{unresolved_clause}
                    ORDER BY id LIMIT ?""",
                params,
            ).fetchall()
            if not rows:
                break

            updates: list[tuple[Any, ...]] = []
            for row in rows:
                row_id, title, description, items_json, old_detection = row
                fields = {
                    "titulo": str(title or ""),
                    "descripcion": str(description or ""),
                    **_item_fields_from_json(items_json),
                }
                matches, detailed_matches = _detect_fichas_with_line_evidence(fields)
                merged_matches = _merge_legacy_matches(matches, old_detection, matcher.valid)
                detailed_identities = {
                    (match.code, match.method, match.field, match.evidence, int(match.score))
                    for match in detailed_matches
                }
                for match in merged_matches:
                    identity = (
                        match.code,
                        match.method,
                        match.field,
                        match.evidence,
                        int(match.score),
                    )
                    if identity not in detailed_identities:
                        detailed_matches.append(match)
                        detailed_identities.add(identity)
                tokens = legacy_tokens(merged_matches)
                new_detection = ", ".join(tokens) if tokens else "No Detectada"
                changed += int(new_detection != str(old_detection or ""))
                updates.append(
                    (
                        new_detection,
                        detection_json(detailed_matches),
                        DETECTOR_VERSION,
                        matcher.catalog_version,
                        run_stamp,
                        row_id,
                    )
                )
                last_id = int(row_id)
            conn.executemany(
                """UPDATE actos_publicos SET
                       ficha_detectada=?, fichas_detectadas_json=?,
                       ficha_detector_version=?, ficha_catalogo_version=?, ficha_detectada_at=?
                   WHERE id=?""",
                updates,
            )
            processed += len(updates)
            if processed % 10000 == 0 or len(updates) < batch_size:
                log("RECLASS", f"{processed:,} filas procesadas | {changed:,} clasificaciones cambiadas")

    metadata_set("last_reclassification_at", run_stamp)
    metadata_set("last_reclassification_rows", processed)
    metadata_set("last_reclassification_changed", changed)
    metadata_set("ficha_detector_version", DETECTOR_VERSION)
    metadata_set("ficha_catalogo_version", matcher.catalog_version)
    return processed, changed


def record_window(
    start: date,
    end_exclusive: date,
    status: str,
    found: int,
    written: int,
    failures: int,
    error: str = "",
) -> None:
    stamp = datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    with connect_db() as conn:
        conn.execute(
            """INSERT INTO db_update_windows
               (window_start,window_end,status,records_found,rows_written,detail_failures,error,updated_at)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(window_start,window_end) DO UPDATE SET
                 status=excluded.status, records_found=excluded.records_found,
                 rows_written=excluded.rows_written, detail_failures=excluded.detail_failures,
                 error=excluded.error, updated_at=excluded.updated_at""",
            (str(start), str(end_exclusive), status, found, written, failures, error[:2000], stamp),
        )


def save_failed(record: dict[str, Any], error: str) -> None:
    flow = int(record["idProcesosContratacionFlujos"])
    stamp = datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    with connect_db() as conn:
        conn.execute(
            """INSERT INTO db_failed_processes(flow_id,process_json,last_error,attempts,updated_at)
               VALUES(?,?,?,?,?) ON CONFLICT(flow_id) DO UPDATE SET
                 process_json=excluded.process_json, last_error=excluded.last_error,
                 attempts=db_failed_processes.attempts+1, updated_at=excluded.updated_at""",
            (flow, json.dumps(record, ensure_ascii=False), error[:2000], 1, stamp),
        )


def clear_failed(flow: int) -> None:
    with connect_db() as conn:
        conn.execute("DELETE FROM db_failed_processes WHERE flow_id=?", (flow,))


def process_records(
    records: list[dict[str, Any]],
    run_stamp: str,
    matcher: FichaMatcher,
    workers: int,
) -> tuple[int, int]:
    rows: list[dict[str, Any]] = []
    failures = 0

    def task(record: dict[str, Any]) -> tuple[dict[str, Any], str]:
        state_name = str(record.pop("_target_state", record.get("nombreRealizado") or ""))
        try:
            return parse_detail(record, state_name, run_stamp, matcher), ""
        except Exception as exc:
            fallback = fallback_row(record, state_name, run_stamp, matcher)
            return fallback, str(exc)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        future_map = {pool.submit(task, dict(record)): record for record in records}
        for index, future in enumerate(concurrent.futures.as_completed(future_map), 1):
            record = future_map[future]
            row, error = future.result()
            rows.append(row)
            flow = int(record["idProcesosContratacionFlujos"])
            if error:
                failures += 1
                save_failed(record, error)
            else:
                clear_failed(flow)
            if len(rows) >= 250:
                upsert_rows(rows)
                rows.clear()
            if index % 250 == 0 or index == len(records):
                log("DETAIL", f"{index:,}/{len(records):,} procesados; fallos con datos básicos={failures}")
    written = upsert_rows(rows)
    # Los lotes anteriores ya fueron escritos; el total lógico es len(records).
    return len(records), failures


def dedupe_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[int, dict[str, Any]] = {}
    for record in records:
        try:
            unique[int(record["idProcesosContratacionFlujos"])] = record
        except (KeyError, TypeError, ValueError):
            continue
    return list(unique.values())


def filter_new_records(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Selecciona actos nuevos, fallidos o cuyo estado terminal cambió.

    La API filtra por fecha de publicación, no por fecha de adjudicación. Por
    eso cada corrida vuelve a listar una ventana histórica amplia, pero solo
    descarga el detalle costoso cuando el enlace no existe, quedó fallido,
    cambió entre ``Desierto`` y ``Adjudicado`` o aún carece del resultado
    oficial enriquecido (participantes/ganadores de las actas).
    """
    if not records:
        return [], 0
    links = [process_link(record) for record in records]
    existing_details: dict[str, dict[str, str]] = {}
    with connect_db() as conn:
        # Un enlace con detalle fallido ya existe por el fallback básico. Debe
        # volver a procesarse hasta completar el detalle, no quedar omitido para
        # siempre por el filtro de enlaces conocidos.
        failed_flows = {
            int(row[0]) for row in conn.execute("SELECT flow_id FROM db_failed_processes")
        }
        table_columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(actos_publicos)")
        }
        first_proponent_expr = (
            'COALESCE("Proponente 1",\'\')'
            if "Proponente 1" in table_columns
            else "''"
        )
        for offset in range(0, len(links), 800):
            chunk = links[offset : offset + 800]
            placeholders = ",".join("?" for _ in chunk)
            query = (
                "SELECT enlace,estado,resultado_fuente_version,razon_social,"
                "nombre_comercial,proponentes_json,ganadores_json,"
                "resultado_fuente_estado,"
                f"{first_proponent_expr} AS first_proponent "
                f"FROM actos_publicos WHERE enlace IN ({placeholders})"
            )
            for row in conn.execute(query, chunk):
                existing_details[str(row[0])] = {
                    "estado": normalize_text(row[1]),
                    "version": str(row[2] or "").strip(),
                    "razon_social": str(row[3] or "").strip(),
                    "nombre_comercial": str(row[4] or "").strip(),
                    "proponentes_json": str(row[5] or "").strip(),
                    "ganadores_json": str(row[6] or "").strip(),
                    "resultado_fuente_estado": str(row[7] or "").strip(),
                    "first_proponent": str(row[8] or "").strip(),
                }
    selected: list[dict[str, Any]] = []
    for record, link in zip(records, links):
        flow = int(record.get("idProcesosContratacionFlujos") or 0)
        target_state = normalize_text(
            record.get("_target_state") or record.get("nombreRealizado") or ""
        )
        existing = existing_details.get(link)
        existing_state = existing.get("estado") if existing else None
        has_participant = bool(
            existing
            and (
                existing.get("first_proponent")
                or existing.get("proponentes_json") not in {"", "[]", "null"}
            )
        )
        has_winner = bool(
            existing
            and (
                existing.get("razon_social")
                or existing.get("nombre_comercial")
                or existing.get("ganadores_json") not in {"", "[]", "null"}
            )
        )
        needs_result_enrichment = bool(
            existing
            and existing.get("version") != RESULT_ENRICHMENT_VERSION
            and (
                not has_participant
                or (target_state == "adjudicado" and not has_winner)
                or existing.get("resultado_fuente_estado")
                == "actas_con_error_temporal"
            )
        )
        if (
            existing is None
            or flow in failed_flows
            or (target_state and target_state != existing_state)
            or needs_result_enrichment
        ):
            selected.append(record)
    return selected, len(records) - len(selected)


def verify_sqlite(path: Path) -> tuple[int, str]:
    conn = sqlite3.connect(path, timeout=60)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"Integridad SQLite inválida en {path}: {integrity}")
        count = int(conn.execute("SELECT COUNT(*) FROM actos_publicos").fetchone()[0])
        # `fecha` puede ser la fecha futura de celebración/presentación. Para
        # informar hasta cuándo están actualizados los datos usamos únicamente
        # publicación/adjudicación y descartamos fechas futuras.
        values = conn.execute("SELECT publicacion,fecha_adjudicacion FROM actos_publicos")
        latest: date | None = None
        today = datetime.now(PANAMA_TZ).date()
        for row in values:
            for value in row:
                parsed = parse_source_date(value)
                if parsed and parsed <= today and (latest is None or parsed > latest):
                    latest = parsed
        return count, str(latest or "")
    finally:
        conn.close()


def checkpoint_sqlite(max_attempts: int = 3) -> bool:
    """Intenta consolidar el WAL; devuelve False ante un lector persistente."""
    last_result: tuple[Any, ...] | None = None
    for attempt in range(1, max(1, max_attempts) + 1):
        conn = sqlite3.connect(DB_PATH, timeout=5, isolation_level=None)
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        finally:
            conn.close()
        last_result = tuple(result) if result else None
        if not result or int(result[0]) == 0:
            if attempt > 1:
                log("WAL", f"Checkpoint completado en el intento {attempt}")
            return True
        if attempt < max_attempts:
            delay = min(2.0 * attempt, 10.0)
            log(
                "RETRY",
                f"WAL ocupado {result}; intento {attempt}/{max_attempts}, espera {delay:.1f}s",
            )
            time.sleep(delay)
    log(
        "WARN",
        f"WAL ocupado tras {max_attempts} intentos ({last_result}); "
        "se publicará una instantánea SQLite consistente.",
    )
    return False


def create_publish_snapshot() -> Path:
    """Crea una copia autocontenida incluyendo cualquier contenido del WAL."""
    snapshot_dir = DB_PATH.parent / "publish_snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / f"panamacompra_publish_{os.getpid()}.db"
    source = sqlite3.connect(DB_PATH, timeout=60)
    target = sqlite3.connect(snapshot_path, timeout=60)
    try:
        source.backup(target, pages=4096, sleep=0.05)
        target.commit()
    finally:
        target.close()
        source.close()
    verify_sqlite(snapshot_path)
    log("SNAPSHOT", f"Instantánea consistente creada: {snapshot_path}")
    return snapshot_path


def atomic_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    shutil.copy2(source, temp)
    verify_sqlite(temp)
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            os.replace(temp, target)
            log("PUBLISH", f"Copia local publicada: {target}")
            return
        except OSError as exc:
            last_error = exc
            time.sleep(1.5 * (attempt + 1))
    temp.unlink(missing_ok=True)
    raise RuntimeError(f"No se pudo reemplazar {target}: {last_error}")


def publish_local(config: dict[str, Any], source: Path = DB_PATH) -> None:
    configured = config.get("publish_targets") or []
    targets = [Path(os.path.expandvars(os.path.expanduser(str(value)))) for value in configured]
    if not targets:
        targets = [
            Path.home() / "GEAPP" / "panamacompra.db",
            Path.home() / "OneDrive" / "cl" / "panamacompra.db",
        ]
    live_db_resolved = DB_PATH.resolve()
    for target in targets:
        if target.resolve() == live_db_resolved:
            continue
        atomic_copy(source, target)


def publish_drive(config: dict[str, Any], source: Path = DB_PATH) -> bool:
    file_id = os.environ.get("DRIVE_PANAMACOMPRA_FILE_ID") or config.get("drive_file_id")
    if not file_id:
        log("DRIVE", "Sin DRIVE_PANAMACOMPRA_FILE_ID; publicación omitida")
        return False
    if not CREDENTIALS_FILE.exists():
        log("DRIVE", f"Cuenta de servicio no encontrada: {CREDENTIALS_FILE}")
        return False
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        credentials = Credentials.from_service_account_file(
            str(CREDENTIALS_FILE), scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build("drive", "v3", credentials=credentials, cache_discovery=False)
        media = MediaFileUpload(str(source), mimetype="application/x-sqlite3", resumable=True)
        # El archivo vive en una unidad compartida. Sin supportsAllDrives=True la
        # API responde 404 aunque la cuenta de servicio tenga permiso de edición.
        request = service.files().update(
            fileId=str(file_id),
            media_body=media,
            fields="id,name,modifiedTime,size",
            supportsAllDrives=True,
        )
        response = None
        while response is None:
            _, response = request.next_chunk(num_retries=5)
        log("DRIVE", f"Archivo actualizado: {response.get('name')} ({response.get('size')} bytes)")
        return True
    except Exception as exc:
        log("WARN", f"No se pudo publicar la DB en Drive: {exc}")
        return False


def postgres_reconciliation_required(
    local_count: int,
    remote_count: int,
    *,
    requested_full: bool = False,
) -> bool:
    """Indica si Supabase necesita recibir y reconciliar la tabla completa."""
    return bool(requested_full or int(local_count) != int(remote_count))


def sync_postgres(run_stamp: str, *, full: bool = False) -> bool:
    dsn = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        log("POSTGRES", "Sin SUPABASE_DB_URL/DATABASE_URL local; sincronización omitida")
        return False
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError:
        log("WARN", "Falta psycopg2-binary; no se pudo sincronizar PostgreSQL")
        return False

    with connect_db() as sqlite_conn:
        sqlite_conn.row_factory = sqlite3.Row
        local_count = int(
            sqlite_conn.execute("SELECT COUNT(*) FROM actos_publicos").fetchone()[0]
        )
        if full:
            rows = sqlite_conn.execute("SELECT * FROM actos_publicos").fetchall()
        else:
            rows = sqlite_conn.execute(
                "SELECT * FROM actos_publicos WHERE fecha_actualizacion=?", (run_stamp,)
            ).fetchall()
        columns = [item[1] for item in sqlite_conn.execute("PRAGMA table_info(actos_publicos)") if item[1] != "id"]

    connection = psycopg2.connect(dsn, connect_timeout=20)
    try:
        connection.autocommit = False
        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS actos_publicos (
                    id BIGSERIAL PRIMARY KEY, enlace TEXT UNIQUE
                )"""
            )
            cursor.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='actos_publicos'"
            )
            existing = {row[0] for row in cursor.fetchall()}
            for column in columns:
                if column not in existing:
                    pg_type = "DOUBLE PRECISION" if column == "precio_referencia" else "TEXT"
                    cursor.execute(
                        f"ALTER TABLE actos_publicos ADD COLUMN IF NOT EXISTS {qident(column)} {pg_type}"
                    )
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_actos_publicos_enlace ON actos_publicos(enlace)"
            )
            cursor.execute("SELECT COUNT(*) FROM actos_publicos")
            remote_count_before = int(cursor.fetchone()[0])
            reconcile_full = postgres_reconciliation_required(
                local_count,
                remote_count_before,
                requested_full=full,
            )
            if reconcile_full and not full:
                log(
                    "RECONCILE",
                    "Diferencia SQLite/Supabase detectada "
                    f"({local_count:,} vs {remote_count_before:,}); "
                    "se sincronizará y depurará la tabla completa.",
                )
                with connect_db() as sqlite_conn:
                    sqlite_conn.row_factory = sqlite3.Row
                    rows = sqlite_conn.execute(
                        "SELECT * FROM actos_publicos"
                    ).fetchall()

            if not rows and not reconcile_full:
                connection.commit()
                metadata_set("last_postgres_local_rows", local_count)
                metadata_set("last_postgres_remote_rows", remote_count_before)
                metadata_set("last_postgres_sync_scope", "sin cambios; conteos alineados")
                log(
                    "POSTGRES",
                    "No hay filas nuevas y Supabase ya coincide con SQLite "
                    f"({local_count:,} filas)",
                )
                return True

            updates = ",".join(
                f"{qident(column)}=EXCLUDED.{qident(column)}" for column in columns if column != "enlace"
            )
            statement = (
                f"INSERT INTO actos_publicos ({','.join(qident(column) for column in columns)}) VALUES %s "
                f"ON CONFLICT(enlace) DO UPDATE SET {updates}"
            )
            values = [tuple(row[column] for column in columns) for row in rows]
            for offset in range(0, len(values), 1000):
                execute_values(cursor, statement, values[offset : offset + 1000], page_size=1000)

            if reconcile_full:
                enlace_index = columns.index("enlace")
                cursor.execute(
                    "CREATE TEMP TABLE sync_actos_enlaces "
                    "(enlace TEXT PRIMARY KEY) ON COMMIT DROP"
                )
                link_values = [
                    (str(value[enlace_index]),)
                    for value in values
                    if value[enlace_index] is not None
                    and str(value[enlace_index]).strip()
                ]
                link_statement = (
                    "INSERT INTO sync_actos_enlaces (enlace) VALUES %s "
                    "ON CONFLICT (enlace) DO NOTHING"
                )
                for offset in range(0, len(link_values), 2000):
                    execute_values(
                        cursor,
                        link_statement,
                        link_values[offset : offset + 2000],
                        page_size=2000,
                    )
                cursor.execute(
                    "DELETE FROM actos_publicos AS remote "
                    "WHERE NOT EXISTS ("
                    "SELECT 1 FROM sync_actos_enlaces AS source "
                    "WHERE source.enlace = remote.enlace"
                    ")"
                )
                removed = max(0, int(cursor.rowcount or 0))
                if removed:
                    log(
                        "RECONCILE",
                        f"Se eliminaron {removed:,} filas remotas ausentes en SQLite.",
                    )

            cursor.execute("SELECT COUNT(*) FROM actos_publicos")
            remote_count_after = int(cursor.fetchone()[0])
            if remote_count_after != local_count:
                raise RuntimeError(
                    "Supabase no quedó alineado con SQLite: "
                    f"local={local_count:,}, remoto={remote_count_after:,}"
                )
        connection.commit()
        scope = "base completa reconciliada" if reconcile_full else "corrida actual"
        metadata_set("last_postgres_local_rows", local_count)
        metadata_set("last_postgres_remote_rows", remote_count_after)
        metadata_set("last_postgres_sync_scope", scope)
        log("POSTGRES", f"{len(rows):,} filas sincronizadas con Supabase/PostgreSQL ({scope})")
        return True
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def determine_range(args: argparse.Namespace) -> tuple[date, date]:
    today = datetime.now(PANAMA_TZ).date()
    end = date.fromisoformat(args.to_date) if args.to_date else today
    if args.from_date:
        start = date.fromisoformat(args.from_date)
    else:
        checkpoint_text = metadata_get("last_successful_window_end")
        checkpoint = None
        try:
            checkpoint = date.fromisoformat(checkpoint_text) if checkpoint_text else None
        except ValueError:
            checkpoint = None
        if checkpoint:
            start = checkpoint - timedelta(days=args.overlap_days)
        else:
            latest = latest_source_date()
            start = (latest - timedelta(days=args.overlap_days)) if latest else date(2024, 1, 1)
        # Un acto puede publicarse hoy y adjudicarse varias semanas después.
        # Como la API solo permite recuperarlo por fecha de publicación, el
        # solapamiento corto no basta. El filtro posterior evita volver a pedir
        # detalles costosos cuando el acto y su estado no cambiaron.
        terminal_lookback_days = max(0, int(args.terminal_lookback_days))
        if terminal_lookback_days:
            start = min(start, end - timedelta(days=terminal_lookback_days))
    if start > end:
        start = max(end - timedelta(days=args.overlap_days), date(2024, 1, 1))
    return start, end


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Actualiza panamacompra.db desde la API pública V3")
    parser.add_argument("--from-date", default=os.environ.get("DB_UPDATE_FROM", ""), help="YYYY-MM-DD")
    parser.add_argument("--to-date", default=os.environ.get("DB_UPDATE_TO", ""), help="YYYY-MM-DD inclusivo")
    parser.add_argument("--window-days", type=int, default=int(os.environ.get("DB_UPDATE_WINDOW_DAYS", DEFAULT_WINDOW_DAYS)))
    parser.add_argument("--overlap-days", type=int, default=int(os.environ.get("DB_UPDATE_OVERLAP_DAYS", DEFAULT_OVERLAP_DAYS)))
    parser.add_argument(
        "--terminal-lookback-days",
        type=int,
        default=int(
            os.environ.get(
                "DB_UPDATE_TERMINAL_LOOKBACK_DAYS",
                DEFAULT_TERMINAL_LOOKBACK_DAYS,
            )
        ),
        help=(
            "Días de publicaciones anteriores que se vuelven a listar para "
            "capturar adjudicaciones/desiertos tardíos; 0 desactiva la barrera"
        ),
    )
    parser.add_argument("--workers", type=int, default=int(os.environ.get("DB_UPDATE_WORKERS", DEFAULT_WORKERS)))
    parser.add_argument("--skip-publish", action="store_true")
    parser.add_argument("--skip-drive", action="store_true")
    parser.add_argument("--skip-postgres", action="store_true")
    parser.add_argument("--skip-backup", action="store_true")
    parser.add_argument(
        "--backup-keep",
        type=int,
        default=int(os.environ.get("DB_UPDATE_BACKUP_KEEP", DEFAULT_BACKUP_KEEP)),
    )
    parser.add_argument(
        "--postgres-full",
        action="store_true",
        help="Sincroniza toda la tabla con PostgreSQL (útil después de una recuperación histórica)",
    )
    parser.add_argument(
        "--require-postgres",
        action="store_true",
        help=(
            "Termina con error si PostgreSQL/Supabase no puede sincronizarse. "
            "Evita informar una publicacion completa cuando solo se actualizo SQLite/Drive."
        ),
    )
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        default=os.environ.get("DB_UPDATE_REFRESH_EXISTING", "").strip().lower() in {"1", "true", "yes", "si"},
        help="Vuelve a descargar también actos ya almacenados",
    )
    parser.add_argument(
        "--reclassify-only",
        action="store_true",
        help="No consulta la API; reclasifica, valida y publica la base existente",
    )
    parser.add_argument(
        "--force-reclassify",
        action="store_true",
        help="Reclasifica todas las filas aunque ya tengan la version vigente",
    )
    parser.add_argument(
        "--skip-reclassify",
        action="store_true",
        help="Omite la reclasificacion automatica por version de detector/catalogo",
    )
    parser.add_argument(
        "--reclassify-only-unresolved",
        action="store_true",
        help="Limita la reclasificacion a filas vacias o No Detectada",
    )
    parser.add_argument("--max-windows", type=int, default=0, help="Solo diagnóstico/pruebas")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    acquire_process_lock()
    init_db()
    if not args.skip_backup:
        backup_path = create_database_backup(args.backup_keep)
        if backup_path:
            metadata_set("last_backup_path", backup_path)
    config = load_config()
    matcher = FichaMatcher()
    run_started = datetime.now(PANAMA_TZ)
    run_stamp = run_started.strftime("%Y-%m-%d %H:%M:%S")
    metadata_set("last_run_started_at", run_stamp)
    metadata_set("last_run_status", "running")
    metadata_set("last_run_script", str(Path(__file__).resolve()))
    metadata_set("last_local_update_status", "running")
    metadata_set("last_local_update_started_at", run_stamp)
    metadata_set("last_local_update_error", "")
    if args.require_postgres and not args.skip_postgres:
        metadata_set("last_postgres_sync_status", "pending")
        metadata_set("last_postgres_sync_error", "")

    start, end_inclusive = determine_range(args)
    end_exclusive = end_inclusive + timedelta(days=1)
    if not args.reclassify_only:
        metadata_set("last_requested_from", start)
        metadata_set("last_requested_to", end_inclusive)
        log("RANGE", f"Recuperación {start} -> {end_inclusive} (ventanas de {args.window_days} días)")
    else:
        log("RECLASS", "Modo solo reclasificacion: no se consultara la API")

    total_found = 0
    total_new = 0
    total_written = 0
    total_failures = 0
    reclassified = 0
    reclassification_changes = 0
    windows_done = 0
    cursor = start
    try:
        while not args.reclassify_only and cursor < end_exclusive:
            if args.max_windows and windows_done >= args.max_windows:
                log("LIMIT", f"Prueba detenida tras {windows_done} ventana(s)")
                break
            window_end = min(cursor + timedelta(days=max(1, args.window_days)), end_exclusive)
            api_start = date_to_api_start(cursor)
            api_end = date_to_api_start(window_end) - timedelta(milliseconds=1)
            log("WINDOW", f"{cursor} -> {window_end - timedelta(days=1)}")
            records: list[dict[str, Any]] = []
            for state, state_name in STATES.items():
                state_records = fetch_listing(state, api_start, api_end)
                for record in state_records:
                    record["_target_state"] = state_name
                records.extend(state_records)
                log("LIST", f"{state_name}: {len(state_records):,} registros")
            records = dedupe_records(records)
            found = len(records)
            if not found and (window_end - cursor).days >= 3:
                raise RuntimeError(
                    f"La API devolvió 0 actos para ambos estados entre {cursor} y {window_end}; "
                    "se rechaza el falso éxito para proteger la actualización."
                )
            selected = records
            known = 0
            if not args.refresh_existing:
                selected, known = filter_new_records(records)
                log("FILTER", f"nuevos={len(selected):,} | ya almacenados={known:,}")
            written, failures = process_records(selected, run_stamp, matcher, args.workers)
            failure_ratio = (failures / len(selected)) if selected else 0.0
            if len(selected) >= 20 and failure_ratio > 0.20:
                raise RuntimeError(
                    f"Falló el detalle de {failures}/{len(selected)} actos ({failure_ratio:.1%}); ventana no confirmada"
                )
            record_window(cursor, window_end, "success", found, written, failures)
            metadata_set("last_successful_window_end", window_end)
            total_found += found
            total_new += len(selected)
            total_written += written
            total_failures += failures
            windows_done += 1
            cursor = window_end

        if not args.skip_reclassify:
            reclassified, reclassification_changes = reclassify_existing(
                matcher,
                force=args.force_reclassify,
                only_unresolved=args.reclassify_only_unresolved,
            )

        count, source_max = verify_sqlite(DB_PATH)
        if source_max:
            metadata_set("last_data_update_at", source_max)
            metadata_set("last_data_source_max_date", source_max)
        metadata_set("last_run_completed_at", datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S"))
        metadata_set("last_run_status", "success")
        metadata_set("last_run_error", "")
        metadata_set("last_rows_written", total_written)
        metadata_set("last_new_links_count", total_new)
        metadata_set("last_api_records_count", total_found)
        metadata_set("last_detail_failures", total_failures)
        metadata_set("last_reclassified_rows", reclassified)
        metadata_set("last_reclassification_changes", reclassification_changes)
        metadata_set("last_total_rows", count)
        metadata_set("last_local_update_status", "success")
        metadata_set(
            "last_local_update_completed_at",
            datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        )
        with connect_db() as conn:
            pending_failures = int(
                conn.execute("SELECT COUNT(*) FROM db_failed_processes").fetchone()[0]
            )
        metadata_set("pending_detail_failures", pending_failures)
        log("VERIFY", f"SQLite íntegra: {count:,} filas; fecha fuente máxima={source_max}")

        if not args.skip_publish:
            publish_source = DB_PATH
            snapshot_path: Path | None = None
            if not checkpoint_sqlite():
                snapshot_path = create_publish_snapshot()
                publish_source = snapshot_path
            try:
                publish_local(config, publish_source)
                if not args.skip_drive:
                    drive_ok = publish_drive(config, publish_source)
                    metadata_set(
                        "last_drive_publish_status",
                        "success" if drive_ok else "warning",
                    )
                if not args.skip_postgres:
                    try:
                        metadata_set("last_postgres_sync_status", "running")
                        metadata_set(
                            "last_postgres_sync_started_at",
                            datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        postgres_ok = sync_postgres(
                            run_stamp,
                            full=args.postgres_full or reclassified > 0,
                        )
                        metadata_set(
                            "last_postgres_sync_status",
                            "success" if postgres_ok else "skipped",
                        )
                        metadata_set(
                            "last_postgres_sync_completed_at",
                            datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        if args.require_postgres and not postgres_ok:
                            raise RuntimeError(
                                "Supabase/PostgreSQL era obligatorio, pero la sincronizacion fue omitida"
                            )
                    except Exception as exc:  # espejo opcional; SQLite/Drive siguen válidos
                        metadata_set("last_postgres_sync_status", "error")
                        metadata_set("last_postgres_sync_error", str(exc)[:2000])
                        log("WARN", f"No se pudo sincronizar PostgreSQL: {exc}")
                        if args.require_postgres:
                            raise RuntimeError(
                                f"SQLite/Drive quedaron actualizados, pero Supabase fallo: {exc}"
                            ) from exc
            finally:
                if snapshot_path is not None:
                    snapshot_path.unlink(missing_ok=True)
        duration = datetime.now(PANAMA_TZ) - run_started
        log(
            "DONE",
            f"ventanas={windows_done} | API={total_found:,} | nuevos={total_new:,} | escritos={total_written:,} "
            f"| reclasificadas={reclassified:,} (cambios={reclassification_changes:,}) "
            f"| detalles fallback={total_failures} | duración={duration}",
        )
        return 0
    except Exception as exc:
        if metadata_get("last_local_update_status") != "success":
            metadata_set("last_local_update_status", "error")
            metadata_set("last_local_update_error", str(exc)[:2000])
            metadata_set(
                "last_local_update_completed_at",
                datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            )
        metadata_set("last_run_completed_at", datetime.now(PANAMA_TZ).strftime("%Y-%m-%d %H:%M:%S"))
        metadata_set("last_run_status", "error")
        metadata_set("last_run_error", str(exc)[:4000])
        if not args.reclassify_only:
            record_window(cursor, min(cursor + timedelta(days=max(1, args.window_days)), end_exclusive), "error", 0, 0, 0, str(exc))
        log("ERROR", str(exc))
        raise


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        log("STOP", "Interrumpido por el usuario; el checkpoint previo se conserva")
        raise SystemExit(130)
