from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import urljoin, urlparse

import requests


BASE_URL = "https://ctni.minsa.gob.pa"
REQUESTS_LIST_PATH = "/Formularios/CargarFormulariosEstado"
REQUEST_DETAIL_PATH = "/Formularios/FormularioInfo"
WORKED_FICHAS_PATH = "/Home/LoadFichasTrabajadas"
PUBLISHED_FICHAS_PATH = "/Home/LoadFichas"
HOME_PATH = "/"

REQUESTS_SHEET = "ctni_solicitudes"
HOMOLOGATIONS_SHEET = "ctni_homologaciones"
FICHAS_SHEET = "ctni_fichas"
EVENTS_SHEET = "ctni_eventos"
HEALTH_SHEET = "ctni_health"

DEFAULT_SPREADSHEET_ID = "17hOfP-vMdJ4D7xym1cUp7vAcd8XJPErpY3V-9Ui2tCo"
DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "ctni" / "ctni_monitor.db"

REQUEST_HEADERS = [
    "record_key",
    "id_oficial",
    "numero_formulario",
    "tipo",
    "numero_ficha",
    "subcomite",
    "institucion",
    "unidad_ejecutora",
    "producto",
    "fecha",
    "estado",
    "fecha_recibida",
    "fecha_respuesta",
    "fecha_proceso",
    "fecha_completada",
    "fecha_final",
    "observacion_estado",
    "enlace_oficial",
    "primera_deteccion",
    "ultima_deteccion",
    "condicion",
]

HOMOLOGATION_HEADERS = [
    "record_key",
    "tipo_evento",
    "fecha",
    "hora",
    "subcomite",
    "producto",
    "numero_formulario",
    "numero_ficha",
    "estado",
    "enlace_oficial",
    "enlace_adjunto",
    "primera_deteccion",
    "ultima_deteccion",
    "condicion",
]

FICHA_HEADERS = [
    "record_key",
    "id_oficial",
    "numero_ficha",
    "accion",
    "accion_original",
    "acta",
    "descripcion_acta",
    "subcomite",
    "producto",
    "fecha",
    "estado",
    "publicada",
    "confirmacion_publicada",
    "enlace_oficial",
    "primera_deteccion",
    "ultima_deteccion",
    "condicion",
]

EVENT_HEADERS = [
    "event_id",
    "categoria",
    "record_key",
    "tipo_evento",
    "fecha_deteccion",
    "notificar",
    "estado_anterior",
    "estado_nuevo",
    "producto",
    "numero",
    "enlace",
]

HEALTH_HEADERS = [
    "fuente",
    "estado",
    "ultimo_exito",
    "ultimo_error",
    "detalle_error",
    "registros",
    "duracion_segundos",
    "actualizado",
]

TERMINAL_REQUEST_STATES = {"Completado", "Finalizado"}
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


class CtniError(RuntimeError):
    """Error controlado del monitor CTNI."""


def _now_text() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalized_text(value: Any) -> str:
    raw = clean_text(value).lower()
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", raw)
        if not unicodedata.combining(char)
    )


def normalize_number(value: Any) -> str:
    digits = re.sub(r"\D+", "", clean_text(value))
    return digits.lstrip("0") or ("0" if digits else "")


def stable_hash(*values: Any) -> str:
    canonical = "|".join(clean_text(value).lower() for value in values)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def payload_hash(payload: Mapping[str, Any]) -> str:
    excluded = {"primera_deteccion", "ultima_deteccion", "condicion"}
    canonical = {key: payload.get(key, "") for key in sorted(payload) if key not in excluded}
    raw = json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_source_date(value: Any) -> date | None:
    raw = clean_text(value)
    if not raw:
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    match = re.fullmatch(r"(\d{1,2})[-\s]+([A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+)[-\s]+(\d{4})", raw)
    if match:
        month = SPANISH_MONTHS.get(normalized_text(match.group(2)))
        if month:
            try:
                return date(int(match.group(3)), month, int(match.group(1)))
            except ValueError:
                return None
    return None


def normalize_source_date(value: Any) -> str:
    parsed = parse_source_date(value)
    return parsed.isoformat() if parsed else clean_text(value)


def canonical_ficha_action(value: Any) -> str:
    action = normalized_text(value)
    if action in {"elaborada", "elaboracion", "nueva ficha"}:
        return "Elaborada"
    if action in {"actualizada", "actualizacion"}:
        return "Actualizada"
    if action in {"corregida", "correccion"}:
        return "Corregida"
    if action in {"habilitada", "habilitacion"}:
        return "Habilitada"
    if action in {"inhabilitada", "inhabilitacion"}:
        return "Inhabilitada"
    return clean_text(value) or "Sin acción"


def derive_request_state(detail: Mapping[str, Any]) -> tuple[str, str]:
    milestones = [
        ("fechaFinal", "observacionFinal", "Finalizado"),
        ("fechaCompletada", "observacionCompletada", "Completado"),
        ("fechaDevueltaProceso", "observacionDevueltaProceso", "Devuelto en proceso"),
        ("fechaDevueltaRecibida", "observacionDevueltaRecibida", "Devuelto recibido"),
        ("fechaProceso", "observacionProceso", "En proceso"),
        ("fechaRespuesta", "observacionRespuesta", "Respondido"),
        ("fechaRecibida", "observacionRecibida", "Recibido"),
    ]
    explicit = clean_text(detail.get("estado"))
    if explicit:
        return explicit, explicit
    for date_key, observation_key, state in milestones:
        if clean_text(detail.get(date_key)) or clean_text(detail.get(observation_key)):
            return state, clean_text(detail.get(observation_key))
    return "Solicitud registrada", ""


def request_record_key(row: Mapping[str, Any]) -> str:
    official_id = clean_text(row.get("id") or row.get("id_oficial"))
    form_number = normalize_number(row.get("numFormulario") or row.get("numero_formulario"))
    return f"solicitud:id:{official_id}" if official_id else f"solicitud:form:{form_number}"


def ficha_record_key(row: Mapping[str, Any]) -> str:
    number = normalize_number(row.get("numFicha") or row.get("numero_ficha"))
    action = normalized_text(canonical_ficha_action(row.get("accion") or row.get("accion_original")))
    act = normalized_text(row.get("numacta") or row.get("acta"))
    source_date = normalize_source_date(row.get("fecha"))
    return f"ficha:{number}|{action}|{act}|{source_date}"


def _usable_attachment_link(value: Any) -> str:
    link = clean_text(value)
    if not link:
        return ""
    parsed = urlparse(link)
    if parsed.path.rstrip("/").lower() in {"", "/documentos/avisos"}:
        return ""
    return link


def homologation_record_key(row: Mapping[str, Any]) -> str:
    attachment = _usable_attachment_link(row.get("enlace_adjunto"))
    if attachment:
        return f"homologacion:url:{stable_hash(attachment)}"
    return "homologacion:huella:" + stable_hash(
        row.get("fecha"), row.get("hora"), row.get("subcomite"), row.get("producto")
    )


def _extract_form_and_ficha(text: str) -> tuple[str, str]:
    form_match = re.search(r"formulario\s*(?:n[°º.]?\s*)?(\d+)", text, re.IGNORECASE)
    ficha_match = re.search(
        r"ficha(?:\s+t[eé]cnica)?\s*(?:n[°º.]?\s*)?(\d+)", text, re.IGNORECASE
    )
    return (
        normalize_number(form_match.group(1)) if form_match else "",
        normalize_number(ficha_match.group(1)) if ficha_match else "",
    )


class CtniHttpClient:
    """Cliente HTTP con timeout y tres reintentos progresivos."""

    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        timeout: tuple[float, float] = (10.0, 45.0),
        retries: int = 3,
        sleeper: Callable[[float], None] = time.sleep,
        session_factory: Callable[[], requests.Session] = requests.Session,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = max(0, retries)
        self.sleeper = sleeper
        self.session_factory = session_factory
        self._local = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = self.session_factory()
            session.headers.update(
                {
                    "User-Agent": "Mozilla/5.0 (compatible; RIR-CTNI-Monitor/1.0)",
                    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
                }
            )
            self._local.session = session
        return session

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        url = path if path.startswith("http") else urljoin(f"{self.base_url}/", path.lstrip("/"))
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                response = self._session().request(method, url, timeout=self.timeout, **kwargs)
                if response.status_code in RETRYABLE_STATUS_CODES:
                    raise CtniError(f"HTTP {response.status_code} en {url}")
                response.raise_for_status()
                return response
            except (requests.RequestException, CtniError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    break
                wait_seconds = float(2**attempt)
                logging.warning(
                    "CTNI %s %s falló (%s). Reintento %s/%s en %.1fs",
                    method,
                    url,
                    exc,
                    attempt + 1,
                    self.retries,
                    wait_seconds,
                )
                self.sleeper(wait_seconds)
        raise CtniError(f"No fue posible consultar {url} después de tres reintentos: {last_error}")

    def _datatable(
        self,
        path: str,
        base_payload: Mapping[str, Any],
        *,
        page_size: int,
        max_pages: int = 1000,
        allow_partial: bool = False,
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        start = 0
        total: int | None = None
        previous_signature = ""
        for page_number in range(max_pages):
            payload = {
                **base_payload,
                "draw": str(page_number + 1),
                "start": str(start),
                "length": str(page_size),
                "search[value]": "",
                "search[regex]": "false",
            }
            response = self.request("POST", path, data=payload)
            try:
                body = response.json()
            except ValueError as exc:
                raise CtniError(f"Respuesta JSON inválida en {path}: {exc}") from exc
            rows = body.get("data")
            if not isinstance(rows, list):
                raise CtniError(f"El endpoint {path} no devolvió una lista en data")
            normalized_rows = [row for row in rows if isinstance(row, dict)]
            if not normalized_rows:
                break
            signature = stable_hash(
                normalized_rows[0].get("id"),
                normalized_rows[-1].get("id"),
                len(normalized_rows),
            )
            if signature == previous_signature:
                raise CtniError(f"Paginación repetida detectada en {path} (start={start})")
            previous_signature = signature
            output.extend(normalized_rows)
            if total is None:
                raw_total = body.get("recordsFiltered", body.get("recordsTotal", 0))
                try:
                    total = int(raw_total)
                except (TypeError, ValueError):
                    total = 0
            start += len(normalized_rows)
            if len(normalized_rows) < page_size or (total and start >= total):
                break
        else:
            if not allow_partial:
                raise CtniError(f"Se alcanzó el máximo de páginas al consultar {path}")
        return output

    def fetch_requests(self) -> list[dict[str, Any]]:
        return self._datatable(
            REQUESTS_LIST_PATH,
            {"filtro": ""},
            page_size=int(os.environ.get("CTNI_REQUEST_PAGE_SIZE", "500")),
        )

    def fetch_request_detail(self, official_id: str) -> dict[str, Any]:
        body = self.request("GET", REQUEST_DETAIL_PATH, params={"Id": official_id}).json()
        detail = body.get("data") if isinstance(body, dict) else None
        if not isinstance(detail, dict):
            raise CtniError(f"Detalle vacío para solicitud CTNI {official_id}")
        return detail

    def fetch_worked_fichas(self) -> list[dict[str, Any]]:
        return self._datatable(
            WORKED_FICHAS_PATH,
            {"idSubComite": "0", "idFiltro": "0", "filtro": ""},
            page_size=int(os.environ.get("CTNI_FICHA_PAGE_SIZE", "1000")),
        )

    def confirm_published_ficha(self, ficha_number: str) -> bool:
        expected = normalize_number(ficha_number)
        if not expected:
            return False
        payload = {
            "All": "0",
            "IdSubComite": "0",
            "IdSubGrupo": "0",
            "IdTipoProducto": "0",
            "Especialidad": "0",
            "IdCriterio": "1",
            "Filtro": expected,
            "draw": "1",
            "start": "0",
            "length": "25",
        }
        body = self.request("POST", PUBLISHED_FICHAS_PATH, data=payload).json()
        rows = body.get("data", []) if isinstance(body, dict) else []
        return any(normalize_number(row.get("numFicha")) == expected for row in rows if isinstance(row, dict))

    def fetch_homepage(self) -> str:
        return self.request("GET", HOME_PATH).text


class _HomepageTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.last_heading = ""
        self.heading_parts: list[str] | None = None
        self.tables: list[dict[str, Any]] = []
        self.table: dict[str, Any] | None = None
        self.row: list[dict[str, Any]] | None = None
        self.cell: dict[str, Any] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        if tag in {"h4", "h5", "h6"}:
            self.heading_parts = []
        elif tag == "table":
            self.table = {
                "id": clean_text(attr_map.get("id")),
                "heading": self.last_heading,
                "rows": [],
            }
            self.tables.append(self.table)
        elif tag == "tr" and self.table is not None:
            self.row = []
            self.table["rows"].append(self.row)
        elif tag in {"td", "th"} and self.row is not None:
            self.cell = {"parts": [], "links": []}
            self.row.append(self.cell)
        elif tag == "a" and self.cell is not None:
            href = clean_text(attr_map.get("href"))
            if href:
                self.cell["links"].append(urljoin(f"{BASE_URL}/", href))

    def handle_endtag(self, tag: str) -> None:
        if tag in {"h4", "h5", "h6"} and self.heading_parts is not None:
            self.last_heading = clean_text(" ".join(self.heading_parts))
            self.heading_parts = None
        elif tag in {"td", "th"}:
            self.cell = None
        elif tag == "tr":
            self.row = None
        elif tag == "table":
            self.table = None

    def handle_data(self, data: str) -> None:
        if self.heading_parts is not None:
            self.heading_parts.append(data)
        if self.cell is not None:
            self.cell["parts"].append(data)


def _cell_text(cell: Mapping[str, Any]) -> str:
    return clean_text(" ".join(cell.get("parts", [])))


def _notice_event_type(value: str) -> str:
    text = normalized_text(value)
    if "reprogram" in text:
        return "Reprogramada"
    if "suspend" in text:
        return "Suspendida"
    if "cancel" in text or "dejar sin efecto" in text:
        return "Cancelada"
    return "Modificada"


def parse_homepage_homologations(html: str) -> list[dict[str, Any]]:
    parser = _HomepageTableParser()
    parser.feed(html)
    records: list[dict[str, Any]] = []
    for table in parser.tables:
        heading = normalized_text(table.get("heading"))
        table_id = normalized_text(table.get("id"))
        if "alerta homologaciones virtuales" in heading:
            for row in table.get("rows", []):
                if not row:
                    continue
                text = _cell_text(row[0])
                match = re.search(
                    r"Fecha:\s*(.*?)\s+Hora:\s*(.*?)\s+[ÁA]rea:\s*(.*?)\s+Titulo:\s*(.*)",
                    text,
                    re.IGNORECASE,
                )
                if not match:
                    continue
                title = clean_text(match.group(4))
                form_number, ficha_number = _extract_form_and_ficha(title)
                links = row[0].get("links", [])
                record = {
                    "tipo_evento": "Programada",
                    "fecha": clean_text(match.group(1)),
                    "hora": clean_text(match.group(2)),
                    "subcomite": clean_text(match.group(3)),
                    "producto": title,
                    "numero_formulario": form_number,
                    "numero_ficha": ficha_number,
                    "estado": "Programada",
                    "enlace_oficial": BASE_URL,
                    "enlace_adjunto": clean_text(links[0]) if links else "",
                }
                record["record_key"] = homologation_record_key(record)
                records.append(record)
        elif table_id == "avisos":
            for row in table.get("rows", [])[1:]:
                if len(row) < 3:
                    continue
                name = _cell_text(row[0])
                committee = _cell_text(row[1])
                observation = _cell_text(row[2])
                combined = clean_text(f"{name} {observation}")
                normalized = normalized_text(combined)
                if "homolog" not in normalized:
                    continue
                if not any(token in normalized for token in ("cancel", "suspend", "reprogram", "sin efecto")):
                    continue
                form_number, ficha_number = _extract_form_and_ficha(observation)
                date_match = re.search(
                    r"(\d{1,2}\s+de\s+[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+\s+de\s+\d{4})",
                    observation,
                    re.IGNORECASE,
                )
                hour_match = re.search(r"(\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?))", observation, re.IGNORECASE)
                links = row[3].get("links", []) if len(row) > 3 else []
                event_type = _notice_event_type(combined)
                record = {
                    "tipo_evento": event_type,
                    "fecha": clean_text(date_match.group(1)) if date_match else "",
                    "hora": clean_text(hour_match.group(1)) if hour_match else "",
                    "subcomite": committee,
                    "producto": combined,
                    "numero_formulario": form_number,
                    "numero_ficha": ficha_number,
                    "estado": event_type,
                    "enlace_oficial": BASE_URL,
                    "enlace_adjunto": clean_text(links[0]) if links else "",
                }
                record["record_key"] = homologation_record_key(record)
                records.append(record)
    deduplicated: dict[str, dict[str, Any]] = {}
    for record in records:
        deduplicated[record["record_key"]] = record
    return list(deduplicated.values())


def _request_payload(row: Mapping[str, Any], detail: Mapping[str, Any] | None) -> dict[str, Any]:
    merged = dict(row)
    if detail:
        merged.update({key: value for key, value in detail.items() if value not in (None, "")})
    state, observation = derive_request_state(merged)
    official_id = clean_text(row.get("id"))
    payload = {
        "record_key": request_record_key(row),
        "id_oficial": official_id,
        "numero_formulario": normalize_number(merged.get("numFormulario")),
        "tipo": clean_text(merged.get("tipoFormulario")),
        "numero_ficha": normalize_number(merged.get("numFicha")),
        "subcomite": clean_text(merged.get("subComite")),
        "institucion": clean_text(merged.get("institucion") or merged.get("entidad")),
        "unidad_ejecutora": clean_text(merged.get("unidadEjecutora")),
        "producto": clean_text(merged.get("nombreGenerico")),
        "fecha": normalize_source_date(merged.get("fecha")),
        "estado": state,
        "fecha_recibida": normalize_source_date(merged.get("fechaRecibida")),
        "fecha_respuesta": normalize_source_date(merged.get("fechaRespuesta")),
        "fecha_proceso": normalize_source_date(merged.get("fechaProceso")),
        "fecha_completada": normalize_source_date(merged.get("fechaCompletada")),
        "fecha_final": normalize_source_date(merged.get("fechaFinal")),
        "observacion_estado": observation,
        "enlace_oficial": urljoin(f"{BASE_URL}/", f"Formularios/FormularioInfo?Id={official_id}"),
        "detalle_disponible": bool(detail),
    }
    return payload


def _ficha_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    original_action = clean_text(row.get("accion"))
    action = canonical_ficha_action(original_action)
    number = normalize_number(row.get("numFicha"))
    return {
        "record_key": ficha_record_key(row),
        "id_oficial": clean_text(row.get("id") or row.get("idtabla")),
        "numero_ficha": number,
        "accion": action,
        "accion_original": original_action,
        "acta": clean_text(row.get("numacta")),
        "descripcion_acta": clean_text(row.get("descacta") or row.get("notainhabilitada")),
        "subcomite": clean_text(row.get("subcomite")),
        "producto": clean_text(row.get("titulo")),
        "fecha": normalize_source_date(row.get("fecha")),
        "estado": "Pendiente de revisión manual" if action == "Elaborada" else action,
        "publicada": clean_text(row.get("publicada")),
        "confirmacion_publicada": "Pendiente" if action == "Elaborada" else "No aplica",
        "enlace_oficial": urljoin(f"{BASE_URL}/", "Home/ConsultarFichas"),
    }


@dataclass
class ApplyResult:
    baseline: bool
    changed_records: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)


class CtniRepository:
    def __init__(self, db_path: str | Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self._ensure_schema()

    def close(self) -> None:
        self.connection.close()

    def _ensure_schema(self) -> None:
        self.connection.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS records (
                categoria TEXT NOT NULL,
                record_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                condicion TEXT NOT NULL,
                PRIMARY KEY (categoria, record_key)
            );
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                categoria TEXT NOT NULL,
                record_key TEXT NOT NULL,
                event_type TEXT NOT NULL,
                detected_at TEXT NOT NULL,
                notify INTEGER NOT NULL DEFAULT 0,
                before_json TEXT,
                after_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS source_health (
                source TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                last_success_at TEXT,
                last_error_at TEXT,
                error_detail TEXT,
                record_count INTEGER NOT NULL DEFAULT 0,
                duration_seconds REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_records_category ON records(categoria);
            CREATE INDEX IF NOT EXISTS idx_events_detected ON events(detected_at);
            """
        )
        self.connection.commit()

    def is_initialized(self, source: str) -> bool:
        row = self.connection.execute(
            "SELECT value FROM metadata WHERE key=?", (f"baseline:{source}",)
        ).fetchone()
        return bool(row and row["value"] == "1")

    def _mark_initialized(self, source: str) -> None:
        self.connection.execute(
            "INSERT INTO metadata(key,value) VALUES(?, '1') "
            "ON CONFLICT(key) DO UPDATE SET value='1'",
            (f"baseline:{source}",),
        )

    def get_metadata(self, key: str, default: str = "") -> str:
        row = self.connection.execute(
            "SELECT value FROM metadata WHERE key=?", (key,)
        ).fetchone()
        return clean_text(row["value"]) if row else default

    def set_metadata(self, key: str, value: Any) -> None:
        self.connection.execute(
            "INSERT INTO metadata(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, clean_text(value)),
        )
        self.connection.commit()

    def get_records(self, category: str) -> dict[str, dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT record_key,payload_json,content_hash,first_seen_at,last_seen_at,condicion "
            "FROM records WHERE categoria=?",
            (category,),
        ).fetchall()
        output: dict[str, dict[str, Any]] = {}
        for row in rows:
            payload = json.loads(row["payload_json"])
            payload.update(
                {
                    "record_key": row["record_key"],
                    "primera_deteccion": row["first_seen_at"],
                    "ultima_deteccion": row["last_seen_at"],
                    "condicion": row["condicion"],
                    "_content_hash": row["content_hash"],
                }
            )
            output[row["record_key"]] = payload
        return output

    def apply_records(
        self,
        *,
        source: str,
        category: str,
        records: Sequence[Mapping[str, Any]],
        event_decider: Callable[[Mapping[str, Any] | None, Mapping[str, Any], bool], tuple[str, bool] | None],
        detected_at: str | None = None,
    ) -> ApplyResult:
        now = detected_at or _now_text()
        baseline = not self.is_initialized(source)
        existing = self.get_records(category)
        result = ApplyResult(baseline=baseline)
        for raw in records:
            payload = {
                key: value
                for key, value in dict(raw).items()
                if not key.startswith("_")
                and key not in {"primera_deteccion", "ultima_deteccion", "condicion"}
            }
            key = clean_text(payload.get("record_key"))
            if not key:
                raise CtniError(f"Registro {category} sin record_key")
            previous = existing.get(key)
            new_hash = payload_hash(payload)
            if previous and previous.get("_content_hash") == new_hash:
                self.connection.execute(
                    "UPDATE records SET last_seen_at=? WHERE categoria=? AND record_key=?",
                    (now, category, key),
                )
                continue

            is_new = previous is None
            first_seen = previous.get("primera_deteccion", now) if previous else now
            condition = "Línea base" if baseline and is_new else ("Nuevo" if is_new else "Actualizado")
            payload_for_storage = dict(payload)
            self.connection.execute(
                """
                INSERT INTO records(
                    categoria,record_key,payload_json,content_hash,first_seen_at,last_seen_at,condicion
                ) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(categoria,record_key) DO UPDATE SET
                    payload_json=excluded.payload_json,
                    content_hash=excluded.content_hash,
                    last_seen_at=excluded.last_seen_at,
                    condicion=excluded.condicion
                """,
                (
                    category,
                    key,
                    json.dumps(payload_for_storage, ensure_ascii=False, sort_keys=True),
                    new_hash,
                    first_seen,
                    now,
                    condition,
                ),
            )
            rendered = {
                **payload_for_storage,
                "record_key": key,
                "primera_deteccion": first_seen,
                "ultima_deteccion": now,
                "condicion": condition,
            }
            result.changed_records.append(rendered)

            if not baseline:
                decision = event_decider(previous, rendered, is_new)
                event_type, notify = decision if decision else ("Registro actualizado", False)
                event_id = stable_hash(category, key, event_type, new_hash)
                cursor = self.connection.execute(
                    """
                    INSERT OR IGNORE INTO events(
                        event_id,categoria,record_key,event_type,detected_at,notify,before_json,after_json
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        event_id,
                        category,
                        key,
                        event_type,
                        now,
                        1 if notify else 0,
                        json.dumps(previous or {}, ensure_ascii=False, sort_keys=True),
                        json.dumps(rendered, ensure_ascii=False, sort_keys=True),
                    ),
                )
                if cursor.rowcount:
                    event = {
                        "event_id": event_id,
                        "categoria": category,
                        "record_key": key,
                        "tipo_evento": event_type,
                        "fecha_deteccion": now,
                        "notificar": bool(notify),
                        "before": previous or {},
                        "after": rendered,
                    }
                    result.events.append(event)

        self._mark_initialized(source)
        self.connection.commit()
        return result

    def record_health(
        self,
        source: str,
        *,
        status: str,
        count: int,
        duration_seconds: float,
        error: str = "",
    ) -> None:
        now = _now_text()
        previous = self.connection.execute(
            "SELECT last_success_at,last_error_at FROM source_health WHERE source=?", (source,)
        ).fetchone()
        last_success = now if status == "success" else (previous["last_success_at"] if previous else "")
        last_error = now if status == "error" else (previous["last_error_at"] if previous else "")
        self.connection.execute(
            """
            INSERT INTO source_health(
                source,status,last_success_at,last_error_at,error_detail,record_count,duration_seconds,updated_at
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(source) DO UPDATE SET
                status=excluded.status,
                last_success_at=excluded.last_success_at,
                last_error_at=excluded.last_error_at,
                error_detail=excluded.error_detail,
                record_count=excluded.record_count,
                duration_seconds=excluded.duration_seconds,
                updated_at=excluded.updated_at
            """,
            (source, status, last_success or "", last_error or "", clean_text(error), count, duration_seconds, now),
        )
        self.connection.commit()

    def health_rows(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM source_health ORDER BY source"
        ).fetchall()
        return [
            {
                "fuente": row["source"],
                "estado": row["status"],
                "ultimo_exito": row["last_success_at"] or "",
                "ultimo_error": row["last_error_at"] or "",
                "detalle_error": row["error_detail"] or "",
                "registros": row["record_count"],
                "duracion_segundos": round(float(row["duration_seconds"] or 0), 2),
                "actualizado": row["updated_at"],
            }
            for row in rows
        ]

    def event_rows(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM events ORDER BY detected_at,event_id"
        ).fetchall()
        output: list[dict[str, Any]] = []
        for row in rows:
            output.append(
                _event_sheet_row(
                    {
                        "event_id": row["event_id"],
                        "categoria": row["categoria"],
                        "record_key": row["record_key"],
                        "tipo_evento": row["event_type"],
                        "fecha_deteccion": row["detected_at"],
                        "notificar": bool(row["notify"]),
                        "before": json.loads(row["before_json"] or "{}"),
                        "after": json.loads(row["after_json"] or "{}"),
                    }
                )
            )
        return output


def _request_event_decider(
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    is_new: bool,
) -> tuple[str, bool] | None:
    if is_new:
        return "Solicitud nueva", True
    old_state = clean_text(previous.get("estado"))
    new_state = clean_text(current.get("estado"))
    if old_state != new_state:
        return f"Estado: {old_state or 'Sin estado'} → {new_state or 'Sin estado'}", True
    if not bool(previous.get("detalle_disponible")) and bool(current.get("detalle_disponible")):
        return "Detalle inicial recuperado", False
    return None


def _ficha_event_decider(
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    is_new: bool,
) -> tuple[str, bool] | None:
    if is_new and current.get("accion") == "Elaborada":
        return "Ficha nueva publicada", True
    return None


def _homologation_event_decider(
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    is_new: bool,
) -> tuple[str, bool] | None:
    event_type = clean_text(current.get("tipo_evento")) or "Homologación"
    return ((f"Homologación {event_type.lower()}" if is_new else f"Homologación modificada: {event_type}"), True)


def _fetch_request_details(
    client: CtniHttpClient,
    rows: Sequence[Mapping[str, Any]],
    existing: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], int]:
    lookback_days = max(30, int(os.environ.get("CTNI_REQUEST_DETAIL_LOOKBACK_DAYS", "1460")))
    terminal_refresh_days = max(
        1, int(os.environ.get("CTNI_TERMINAL_REFRESH_DAYS", "180"))
    )
    max_details = max(1, int(os.environ.get("CTNI_REQUEST_DETAIL_LIMIT", "15000")))
    workers = max(1, min(12, int(os.environ.get("CTNI_REQUEST_DETAIL_WORKERS", "6"))))
    cutoff = date.today() - timedelta(days=lookback_days)
    terminal_cutoff = date.today() - timedelta(days=terminal_refresh_days)
    candidates: list[Mapping[str, Any]] = []
    for row in rows:
        key = request_record_key(row)
        previous = existing.get(key)
        source_date = parse_source_date(row.get("fecha"))
        recent = source_date is None or source_date >= cutoff
        recently_registered = source_date is None or source_date >= terminal_cutoff
        active = previous is None or clean_text(previous.get("estado")) not in TERMINAL_REQUEST_STATES
        missing_detail = previous is None or not bool(previous.get("detalle_disponible"))
        if recent and (active or missing_detail or recently_registered):
            candidates.append(row)
    candidates.sort(key=lambda item: parse_source_date(item.get("fecha")) or date.min, reverse=True)
    candidates = candidates[:max_details]
    details: dict[str, dict[str, Any]] = {}
    failures = 0
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="ctni-detail") as executor:
        futures = {
            executor.submit(client.fetch_request_detail, clean_text(row.get("id"))): request_record_key(row)
            for row in candidates
            if clean_text(row.get("id"))
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                details[key] = future.result()
            except Exception as exc:  # individual failures preserve prior data
                failures += 1
                logging.warning("No se pudo leer detalle CTNI %s: %s", key, exc)
    return details, failures


class SheetStore:
    def __init__(self, *, spreadsheet_id: str | None = None, credential_file: str | Path | None = None) -> None:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build

        self.spreadsheet_id = spreadsheet_id or os.environ.get(
            "CTNI_SPREADSHEET_ID",
            os.environ.get("ORQUESTADOR_PANAMACOMPRA_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
        )
        candidates = [
            clean_text(credential_file),
            clean_text(os.environ.get("ORQUESTADOR_GOOGLE_SERVICE_ACCOUNT")),
            clean_text(os.environ.get("ORQUESTADOR_SERVICE_ACCOUNT_FILE")),
            clean_text(os.environ.get("FINAPP_SERVICE_ACCOUNT_FILE")),
            str(
                Path(__file__).resolve().parents[1]
                / "orquestador"
                / "pure-beach-474203-p1-fdc9557f33d0.json"
            ),
            str(Path(__file__).resolve().parents[1] / "credentials" / "service-account.json"),
        ]
        selected = next((Path(path).expanduser() for path in candidates if path and Path(path).expanduser().exists()), None)
        if selected is None:
            raise CtniError("No se encontró la cuenta de servicio existente para sincronizar CTNI")
        credentials = Credentials.from_service_account_file(
            str(selected), scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        self.service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
        self._titles: set[str] | None = None

    @staticmethod
    def _column_letter(index: int) -> str:
        letters = ""
        while index:
            index, remainder = divmod(index - 1, 26)
            letters = chr(65 + remainder) + letters
        return letters

    def _call(self, action: Callable[[], Any], label: str) -> Any:
        last_error: Exception | None = None
        for attempt in range(4):
            try:
                return action()
            except Exception as exc:  # Google client exposes several transport exception types
                last_error = exc
                if attempt == 3:
                    break
                wait = float(2**attempt)
                logging.warning("Sheets %s falló: %s. Reintento en %.1fs", label, exc, wait)
                time.sleep(wait)
        raise CtniError(f"No fue posible sincronizar {label}: {last_error}")

    def _load_titles(self) -> set[str]:
        if self._titles is None:
            response = self._call(
                lambda: self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute(),
                "listar pestañas",
            )
            self._titles = {
                clean_text(item.get("properties", {}).get("title"))
                for item in response.get("sheets", [])
            }
        return self._titles

    def ensure_sheet(self, title: str, columns: int) -> None:
        if title in self._load_titles():
            return
        body = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": title,
                            "gridProperties": {"rowCount": 2000, "columnCount": max(10, columns)},
                        }
                    }
                }
            ]
        }
        self._call(
            lambda: self.service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id, body=body
            ).execute(),
            f"crear {title}",
        )
        self._titles.add(title)

    def _read_values(self, title: str) -> list[list[str]]:
        response = self._call(
            lambda: self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range=f"'{title}'!A1:ZZ"
            ).execute(),
            f"leer {title}",
        )
        return response.get("values", [])

    def upsert_rows(
        self,
        title: str,
        headers: Sequence[str],
        rows: Sequence[Mapping[str, Any]],
        *,
        key_field: str,
    ) -> int:
        self.ensure_sheet(title, len(headers))
        last_column = self._column_letter(len(headers))
        values = self._read_values(title)
        existing_headers = values[0] if values else []
        if list(existing_headers[: len(headers)]) != list(headers):
            self._call(
                lambda: self.service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"'{title}'!A1:{last_column}1",
                    valueInputOption="RAW",
                    body={"values": [list(headers)]},
                ).execute(),
                f"encabezados {title}",
            )
        key_index = list(headers).index(key_field)
        existing: dict[str, tuple[int, list[str]]] = {}
        for sheet_row, row in enumerate(values[1:], start=2):
            if key_index < len(row) and clean_text(row[key_index]):
                comparable = [clean_text(value) for value in (row[: len(headers)] + [""] * len(headers))[: len(headers)]]
                existing[clean_text(row[key_index])] = (sheet_row, comparable)
        updates: list[dict[str, Any]] = []
        appends: list[list[Any]] = []
        for row in rows:
            key = clean_text(row.get(key_field))
            if not key:
                continue
            rendered = [row.get(header, "") for header in headers]
            rendered = [json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value for value in rendered]
            if key in existing:
                sheet_row, old_rendered = existing[key]
                new_rendered = [
                    "TRUE" if value is True else "FALSE" if value is False else clean_text(value)
                    for value in rendered
                ]
                if old_rendered == new_rendered:
                    continue
                updates.append(
                    {"range": f"'{title}'!A{sheet_row}:{last_column}{sheet_row}", "values": [rendered]}
                )
            else:
                appends.append(rendered)
        for start in range(0, len(updates), 300):
            chunk = updates[start : start + 300]
            self._call(
                lambda chunk=chunk: self.service.spreadsheets().values().batchUpdate(
                    spreadsheetId=self.spreadsheet_id,
                    body={"valueInputOption": "RAW", "data": chunk},
                ).execute(),
                f"actualizar {title}",
            )
        for start in range(0, len(appends), 1000):
            chunk = appends[start : start + 1000]
            self._call(
                lambda chunk=chunk: self.service.spreadsheets().values().append(
                    spreadsheetId=self.spreadsheet_id,
                    range=f"'{title}'!A:{last_column}",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": chunk},
                ).execute(),
                f"agregar {title}",
            )
        return len(updates) + len(appends)


def _event_sheet_row(event: Mapping[str, Any]) -> dict[str, Any]:
    before = event.get("before") if isinstance(event.get("before"), Mapping) else {}
    after = event.get("after") if isinstance(event.get("after"), Mapping) else {}
    return {
        "event_id": event.get("event_id", ""),
        "categoria": event.get("categoria", ""),
        "record_key": event.get("record_key", ""),
        "tipo_evento": event.get("tipo_evento", ""),
        "fecha_deteccion": event.get("fecha_deteccion", ""),
        "notificar": bool(event.get("notificar")),
        "estado_anterior": before.get("estado", ""),
        "estado_nuevo": after.get("estado", ""),
        "producto": after.get("producto", ""),
        "numero": after.get("numero_formulario") or after.get("numero_ficha") or "",
        "enlace": after.get("enlace_adjunto") or after.get("enlace_oficial") or "",
    }


@dataclass
class CtniRunResult:
    baseline_sources: list[str] = field(default_factory=list)
    source_status: dict[str, str] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)
    sheets_synced: bool = False

    def summary(self) -> dict[str, Any]:
        notification_events = []
        for event in self.events:
            if not event.get("notificar"):
                continue
            after = event.get("after") if isinstance(event.get("after"), Mapping) else {}
            notification_events.append(
                {
                    "event_id": event.get("event_id", ""),
                    "categoria": event.get("categoria", ""),
                    "tipo_evento": event.get("tipo_evento", ""),
                    "producto": after.get("producto", ""),
                    "numero_formulario": after.get("numero_formulario", ""),
                    "numero_ficha": after.get("numero_ficha", ""),
                    "subcomite": after.get("subcomite", ""),
                    "fecha": after.get("fecha", ""),
                    "hora": after.get("hora", ""),
                    "estado": after.get("estado", ""),
                    "enlace": after.get("enlace_adjunto") or after.get("enlace_oficial") or "",
                }
            )
        return {
            "baseline_sources": self.baseline_sources,
            "source_status": self.source_status,
            "counts": self.counts,
            "events": notification_events,
            "errors": self.errors,
            "sheets_synced": self.sheets_synced,
        }


def run_monitor(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    client: CtniHttpClient | None = None,
    sheet_store: SheetStore | None = None,
    sync_sheets: bool = True,
) -> CtniRunResult:
    repository = CtniRepository(db_path)
    client = client or CtniHttpClient()
    result = CtniRunResult()
    changed_by_sheet: dict[str, list[dict[str, Any]]] = {
        REQUESTS_SHEET: [],
        HOMOLOGATIONS_SHEET: [],
        FICHAS_SHEET: [],
    }
    try:
        # Solicitudes de elaboración/actualización and their state details.
        started = time.monotonic()
        try:
            rows = client.fetch_requests()
            existing = repository.get_records("solicitudes")
            details, detail_failures = _fetch_request_details(client, rows, existing)
            payloads: list[dict[str, Any]] = []
            for row in rows:
                key = request_record_key(row)
                detail = details.get(key)
                if detail is None and key in existing:
                    previous = existing[key]
                    detail = {
                        "estado": previous.get("estado", ""),
                        "fechaRecibida": previous.get("fecha_recibida", ""),
                        "fechaRespuesta": previous.get("fecha_respuesta", ""),
                        "fechaProceso": previous.get("fecha_proceso", ""),
                        "fechaCompletada": previous.get("fecha_completada", ""),
                        "fechaFinal": previous.get("fecha_final", ""),
                        "observacionProceso": previous.get("observacion_estado", ""),
                    }
                    payload = _request_payload(row, detail)
                    payload["detalle_disponible"] = bool(previous.get("detalle_disponible"))
                else:
                    payload = _request_payload(row, detail)
                payloads.append(payload)
            applied = repository.apply_records(
                source="solicitudes",
                category="solicitudes",
                records=payloads,
                event_decider=_request_event_decider,
            )
            if applied.baseline:
                result.baseline_sources.append("solicitudes")
            changed_by_sheet[REQUESTS_SHEET] = applied.changed_records
            result.events.extend(applied.events)
            result.counts["solicitudes"] = len(rows)
            result.counts["solicitudes_detalle_fallido"] = detail_failures
            result.source_status["solicitudes"] = "success"
            repository.record_health(
                "solicitudes",
                status="success",
                count=len(rows),
                duration_seconds=time.monotonic() - started,
                error=f"{detail_failures} detalles individuales no disponibles" if detail_failures else "",
            )
        except Exception as exc:
            result.source_status["solicitudes"] = "error"
            result.errors["solicitudes"] = str(exc)
            repository.record_health(
                "solicitudes", status="error", count=0, duration_seconds=time.monotonic() - started, error=str(exc)
            )
            logging.exception("Falló la fuente CTNI de solicitudes")

        # Fichas trabajadas: new, updated, corrected, enabled and disabled.
        started = time.monotonic()
        try:
            rows = client.fetch_worked_fichas()
            payloads = [_ficha_payload(row) for row in rows]
            existing_fichas = repository.get_records("fichas")
            for payload in payloads:
                previous = existing_fichas.get(clean_text(payload.get("record_key")))
                if previous and clean_text(previous.get("confirmacion_publicada")):
                    payload["confirmacion_publicada"] = previous.get("confirmacion_publicada")
            applied = repository.apply_records(
                source="fichas",
                category="fichas",
                records=payloads,
                event_decider=_ficha_event_decider,
            )
            if applied.baseline:
                result.baseline_sources.append("fichas")
            # ConsultarFichas is deliberately secondary and only confirms newly detected official fichas.
            new_fichas = [
                row
                for row in applied.changed_records
                if row.get("condicion") == "Nuevo" and row.get("accion") == "Elaborada"
            ]
            for payload in new_fichas:
                try:
                    confirmed = client.confirm_published_ficha(clean_text(payload.get("numero_ficha")))
                except Exception as exc:
                    logging.warning("No se pudo confirmar ficha %s: %s", payload.get("numero_ficha"), exc)
                    payload["confirmacion_publicada"] = "No disponible"
                else:
                    payload["confirmacion_publicada"] = "Sí" if confirmed else "No"
            if new_fichas:
                confirmation_apply = repository.apply_records(
                    source="fichas",
                    category="fichas",
                    records=new_fichas,
                    event_decider=lambda *_args: None,
                )
                confirmed_by_key = {row["record_key"]: row for row in confirmation_apply.changed_records}
                applied.changed_records = [confirmed_by_key.get(row["record_key"], row) for row in applied.changed_records]
                result.events.extend(confirmation_apply.events)
            history_days = max(365, int(os.environ.get("CTNI_SHEET_HISTORY_DAYS", "1825")))
            cutoff = date.today() - timedelta(days=history_days)
            changed_by_sheet[FICHAS_SHEET] = [
                row
                for row in applied.changed_records
                if row.get("condicion") != "Línea base"
                or (parse_source_date(row.get("fecha")) or date.max) >= cutoff
            ]
            result.events.extend(applied.events)
            result.counts["fichas"] = len(rows)
            result.source_status["fichas"] = "success"
            repository.record_health(
                "fichas", status="success", count=len(rows), duration_seconds=time.monotonic() - started
            )
        except Exception as exc:
            result.source_status["fichas"] = "error"
            result.errors["fichas"] = str(exc)
            repository.record_health(
                "fichas", status="error", count=0, duration_seconds=time.monotonic() - started, error=str(exc)
            )
            logging.exception("Falló la fuente CTNI de fichas trabajadas")

        # Homepage homologation schedule and cancellation/suspension/reprogram notices.
        started = time.monotonic()
        try:
            rows = parse_homepage_homologations(client.fetch_homepage())
            applied = repository.apply_records(
                source="homologaciones",
                category="homologaciones",
                records=rows,
                event_decider=_homologation_event_decider,
            )
            if applied.baseline:
                result.baseline_sources.append("homologaciones")
            changed_by_sheet[HOMOLOGATIONS_SHEET] = applied.changed_records
            result.events.extend(applied.events)
            result.counts["homologaciones"] = len(rows)
            result.source_status["homologaciones"] = "success"
            repository.record_health(
                "homologaciones", status="success", count=len(rows), duration_seconds=time.monotonic() - started
            )
        except Exception as exc:
            result.source_status["homologaciones"] = "error"
            result.errors["homologaciones"] = str(exc)
            repository.record_health(
                "homologaciones", status="error", count=0, duration_seconds=time.monotonic() - started, error=str(exc)
            )
            logging.exception("Falló la fuente CTNI de homologaciones")

        sheet_content_changed = any(changed_by_sheet.values()) or bool(result.events)
        if sheet_content_changed:
            repository.set_metadata("google_sheets_dirty", "1")

        if sync_sheets:
            try:
                sheets = sheet_store or SheetStore()
                # Si una ejecución anterior no pudo llegar a Sheets, se reconstruye
                # el espejo desde SQLite. Así un 502 o una caída de Google no deja
                # cambios perdidos aunque la siguiente corrida ya no los vea como nuevos.
                full_resync = repository.get_metadata("google_sheets_dirty") == "1"
                if full_resync:
                    changed_by_sheet[REQUESTS_SHEET] = list(
                        repository.get_records("solicitudes").values()
                    )
                    changed_by_sheet[HOMOLOGATIONS_SHEET] = list(
                        repository.get_records("homologaciones").values()
                    )
                    history_days = max(365, int(os.environ.get("CTNI_SHEET_HISTORY_DAYS", "1825")))
                    cutoff = date.today() - timedelta(days=history_days)
                    changed_by_sheet[FICHAS_SHEET] = [
                        row
                        for row in repository.get_records("fichas").values()
                        if (parse_source_date(row.get("fecha")) or date.max) >= cutoff
                    ]
                sheets.upsert_rows(
                    REQUESTS_SHEET, REQUEST_HEADERS, changed_by_sheet[REQUESTS_SHEET], key_field="record_key"
                )
                sheets.upsert_rows(
                    HOMOLOGATIONS_SHEET,
                    HOMOLOGATION_HEADERS,
                    changed_by_sheet[HOMOLOGATIONS_SHEET],
                    key_field="record_key",
                )
                sheets.upsert_rows(
                    FICHAS_SHEET, FICHA_HEADERS, changed_by_sheet[FICHAS_SHEET], key_field="record_key"
                )
                sheets.upsert_rows(
                    EVENTS_SHEET,
                    EVENT_HEADERS,
                    repository.event_rows() if full_resync else [_event_sheet_row(event) for event in result.events],
                    key_field="event_id",
                )
                repository.set_metadata("google_sheets_dirty", "0")
                repository.record_health(
                    "google_sheets",
                    status="success",
                    count=sum(len(rows) for rows in changed_by_sheet.values()),
                    duration_seconds=0,
                )
                sheets.upsert_rows(
                    HEALTH_SHEET, HEALTH_HEADERS, repository.health_rows(), key_field="fuente"
                )
                result.sheets_synced = True
            except Exception as exc:
                result.errors["google_sheets"] = str(exc)
                repository.set_metadata("google_sheets_dirty", "1")
                repository.record_health(
                    "google_sheets", status="error", count=0, duration_seconds=0, error=str(exc)
                )
                logging.exception("No se pudieron sincronizar las hojas CTNI")
        return result
    finally:
        repository.close()
