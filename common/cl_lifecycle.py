# -*- coding: utf-8 -*-
"""Seguimiento durable del ciclo de vida de Cotizaciones en Línea (CL).

Este módulo mantiene separadas dos realidades:

* ``actos_publicos`` contiene estados oficiales (Adjudicado/Desierto).
* ``cl_cotizaciones`` contiene estados operativos derivados de las CL.

Una ausencia de XPath, un timeout o una pantalla todavía no publicada nunca se
interpreta como cero propuestas. ``cerrada_sin_propuestas`` solo se emite
cuando el Cuadro de Cotizaciones cargó para el número de CL esperado y el
portal informa explícitamente cero/no hay registros.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
import unicodedata
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urljoin, urlparse, urlunparse
from zoneinfo import ZoneInfo


PANAMA_TZ = ZoneInfo("America/Panama")
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = REPO_ROOT / "data" / "db" / "panamacompra.db"

FINAL_STATES = {
    "cerrada_sin_propuestas",
    "cerrada_con_propuestas",
    "continuada_a_otro_proceso",
    "desierta_oficial",
}
FINAL_EVIDENCE_FIELDS = {
    "estado_derivado",
    "proposal_count",
    "proponents_json",
    "evidence_type",
    "evidence_url",
    "confidence",
    "closed_at",
    "last_check_at",
    "next_check_at",
    "check_attempts",
    "successor_process_number",
    "successor_url",
    "last_error",
}
RETRY_STATES = {
    "cerrada_pendiente_publicacion",
    "cerrada_pendiente_verificacion",
    "error_verificacion",
}

CL_FIELDS = [
    "cl_key",
    "numero_cl",
    "enlace",
    "titulo",
    "entidad",
    "unidad_solicitante",
    "precio_referencia",
    "fecha_publicacion",
    "fecha_presentacion_texto",
    "fecha_cierre",
    "fichas_detectadas",
    "source_sheets",
    "estado_derivado",
    "proposal_count",
    "proponents_json",
    "evidence_type",
    "evidence_url",
    "confidence",
    "first_seen_at",
    "last_seen_at",
    "closed_at",
    "last_check_at",
    "next_check_at",
    "check_attempts",
    "successor_process_number",
    "successor_url",
    "last_error",
    "source_payload_json",
    "updated_at",
]

OBS_FIELDS = [
    "observation_id",
    "cl_key",
    "numero_cl",
    "checked_at",
    "estado_derivado",
    "proposal_count",
    "proponents_json",
    "evidence_type",
    "evidence_url",
    "confidence",
    "error",
    "page_text_excerpt",
]


def _now() -> datetime:
    return datetime.now(PANAMA_TZ)


def now_iso() -> str:
    return _now().isoformat(timespec="seconds")


def normalize_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = "".join(
        ch
        for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    return re.sub(r"\s+", " ", text).strip().lower()


def clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", "" if value is None else str(value)).strip()


def normalize_url(value: object) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    if raw.startswith("/"):
        raw = urljoin("https://www.panamacompra.gob.pa/Inicio/", raw)
    parsed = urlparse(raw)
    host = parsed.netloc.lower()
    if host.endswith("panamacompra.gob.pa") and not host.startswith("www."):
        host = f"www.{host}"
    path = re.sub(r"/{2,}", "/", parsed.path).rstrip("/") or "/"
    fragment = re.sub(r"/{2,}", "/", parsed.fragment).rstrip("/")
    return urlunparse(("https", host, path, "", "", fragment))


def extract_cl_number(value: object) -> str:
    match = re.search(
        r"\b\d{4}-\d-\d{2}-\d{2}-\d{2}-CL-\d+\b",
        clean_text(value),
        flags=re.I,
    )
    return match.group(0).upper() if match else ""


def cl_key_for(enlace: object, numero_cl: object = "") -> str:
    number = extract_cl_number(numero_cl) or extract_cl_number(enlace)
    if number:
        return number
    normalized = normalize_url(enlace)
    return "url:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def parse_money(value: object) -> float:
    text = clean_text(value)
    if not text:
        return 0.0
    match = re.search(
        r"(?<!\d)-?(?:\d{1,3}(?:[.,]\d{3})+(?:[.,]\d+)?|\d+(?:[.,]\d+)?)(?!\d)",
        text,
    )
    if not match:
        return 0.0
    token = match.group(0)
    if "," in token and "." in token:
        if token.rfind(",") > token.rfind("."):
            token = token.replace(".", "").replace(",", ".")
        else:
            token = token.replace(",", "")
    elif "," in token:
        tail = token.rsplit(",", 1)[-1]
        token = token.replace(",", ".") if len(tail) <= 2 else token.replace(",", "")
    try:
        return float(token)
    except ValueError:
        return 0.0


def parse_cl_deadline(value: object) -> datetime | None:
    """Devuelve la fecha/hora final de presentación en hora de Panamá."""

    text = clean_text(value).replace("–", "-").replace("—", "-")
    if not text:
        return None

    date_tokens = re.findall(r"\b(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b", text)
    if not date_tokens:
        return None
    token = date_tokens[-1]
    parsed_date = None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d-%m-%y", "%d/%m/%y"):
        try:
            parsed_date = datetime.strptime(token, fmt)
            break
        except ValueError:
            continue
    if parsed_date is None:
        return None

    time_tokens = list(
        re.finditer(
            r"\b(\d{1,2}):(\d{2})(?:\s*([AaPp])\.?\s*[Mm]\.?)?\b",
            text,
        )
    )
    if time_tokens:
        last = time_tokens[-1]
        hour = int(last.group(1))
        minute = int(last.group(2))
        meridiem = (last.group(3) or "").upper()
        if meridiem == "A" and hour == 12:
            hour = 0
        elif meridiem == "P" and hour != 12:
            hour += 12
    else:
        # Las hojas históricas a veces solo contienen el día final.
        hour, minute = 23, 59

    return parsed_date.replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0,
        tzinfo=PANAMA_TZ,
    )


def is_closed(fecha_texto: object, *, reference: datetime | None = None) -> bool:
    deadline = parse_cl_deadline(fecha_texto)
    if deadline is None:
        return False
    current = reference or _now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=PANAMA_TZ)
    return deadline <= current.astimezone(PANAMA_TZ)


def should_inspect_cl(
    fecha_texto: object,
    enlace: object,
    *,
    active_listing_links: Iterable[str] | None = None,
    reference: datetime | None = None,
) -> bool:
    """Decide el cierre sin inventar una hora que el portal no publica.

    Una fecha/hora explícita vencida siempre se revisa. Cuando la hoja solo
    guarda días, una CL cuya fecha final es hoy se revisa en cuanto desaparece
    de una captura *completa* del listado oficial de abiertas.
    """

    deadline = parse_cl_deadline(fecha_texto)
    if deadline is None:
        return False
    current = reference or _now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=PANAMA_TZ)
    current = current.astimezone(PANAMA_TZ)
    if deadline <= current:
        return True
    if active_listing_links is None or deadline.date() != current.date():
        return False
    active = {normalize_url(item) for item in active_listing_links if clean_text(item)}
    return normalize_url(enlace) not in active


@dataclass
class Proposal:
    name: str
    total: float = 0.0


@dataclass
class ProposalObservation:
    numero_cl: str
    cl_url: str
    status: str
    proposal_count: int | None
    proponents: list[Proposal] = field(default_factory=list)
    evidence_type: str = ""
    evidence_url: str = ""
    confidence: float = 0.0
    error: str = ""
    page_text_excerpt: str = ""

    def proponents_json(self) -> str:
        return json.dumps([asdict(item) for item in self.proponents], ensure_ascii=False)


def _driver_body_text(driver: object) -> str:
    from selenium.webdriver.common.by import By  # type: ignore

    try:
        return str(driver.find_element(By.TAG_NAME, "body").text or "")
    except Exception:
        return ""


def _wait_expected_page(
    driver: object,
    expected_number: str,
    *,
    timeout: int,
    ready_tokens: Sequence[str],
) -> str:
    deadline = time.time() + max(3, timeout)
    last_text = ""
    expected_norm = normalize_text(expected_number)
    token_norms = [normalize_text(token) for token in ready_tokens]
    while time.time() < deadline:
        current = _driver_body_text(driver)
        current_norm = normalize_text(current)
        number_ok = not expected_norm or expected_norm in current_norm
        ready = any(token in current_norm for token in token_norms)
        if number_ok and ready:
            return current
        last_text = current or last_text
        time.sleep(0.6)
    return last_text


def _all_candidate_links(driver: object) -> list[str]:
    from selenium.webdriver.common.by import By  # type: ignore

    candidates: list[str] = []
    selectors = (
        "a[href*='/cuadro-de-cotizaciones/']",
        "a[data-uw-original-href*='/cuadro-de-cotizaciones/']",
        "a[href*='/cuadro-de-propuestas/']",
        "a[data-uw-original-href*='/cuadro-de-propuestas/']",
        "a[aria-label*='Ver documento']",
    )
    for selector in selectors:
        try:
            anchors = driver.find_elements(By.CSS_SELECTOR, selector)
        except Exception:
            anchors = []
        for anchor in anchors:
            href = clean_text(
                anchor.get_attribute("href")
                or anchor.get_attribute("data-uw-original-href")
            )
            if not href:
                continue
            href = normalize_url(href)
            if href and href not in candidates:
                candidates.append(href)
    # Prioriza el cuadro; los documentos adjuntos no son evidencia de propuestas.
    return sorted(
        candidates,
        key=lambda url: (
            0 if "/cuadro-de-cotizaciones/" in url else
            1 if "/cuadro-de-propuestas/" in url else 2
        ),
    )


def _provider_tables(driver: object) -> tuple[list[Proposal], bool]:
    """Extrae todos los proponentes y devuelve si hubo estructura de cuadro."""

    from selenium.webdriver.common.by import By  # type: ignore

    proposals: list[Proposal] = []
    proposal_structure = False
    seen: set[str] = set()

    try:
        tables = driver.find_elements(By.CSS_SELECTOR, "table")
    except Exception:
        tables = []

    for table in tables:
        try:
            headers = [
                normalize_text(cell.text)
                for cell in table.find_elements(By.CSS_SELECTOR, "thead th")
            ]
            captions = table.find_elements(By.CSS_SELECTOR, "caption a")
        except Exception:
            continue

        provider = ""
        for anchor in captions:
            label = clean_text(anchor.text)
            title = normalize_text(anchor.get_attribute("title"))
            if not label or "ver cotizacion" in normalize_text(label) or "ver cotizacion" in title:
                continue
            provider = label
            break

        if provider:
            proposal_structure = True
            total = 0.0
            try:
                footer_rows = table.find_elements(By.CSS_SELECTOR, "tfoot tr")
            except Exception:
                footer_rows = []
            for footer in footer_rows:
                if "total" not in normalize_text(footer.text):
                    continue
                cells = footer.find_elements(By.CSS_SELECTOR, "th,td")
                if cells:
                    total = max(total, parse_money(cells[-1].text))
            key = normalize_text(provider)
            if key and key not in seen:
                seen.add(key)
                proposals.append(Proposal(provider, total))
            continue

        # Estructura Angular alternativa: una fila por proponente.
        provider_index = next(
            (
                index
                for index, header in enumerate(headers)
                if any(
                    token in header
                    for token in ("proponente", "proveedor", "razon social", "nombre comercial")
                )
            ),
            -1,
        )
        total_index = next(
            (
                index
                for index, header in enumerate(headers)
                if "monto total" in header or header == "total"
            ),
            -1,
        )
        if provider_index < 0:
            continue
        proposal_structure = True
        try:
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        except Exception:
            rows = []
        for row in rows:
            cells = row.find_elements(By.CSS_SELECTOR, "th,td")
            if provider_index >= len(cells):
                continue
            provider = clean_text(cells[provider_index].text)
            if not provider or normalize_text(provider) in {"no hay propuesta", "no se encontro registro"}:
                continue
            total = (
                parse_money(cells[total_index].text)
                if total_index >= 0 and total_index < len(cells)
                else 0.0
            )
            key = normalize_text(provider)
            if key and key not in seen:
                seen.add(key)
                proposals.append(Proposal(provider, total))

    return proposals, proposal_structure


def _declared_proponent_count(text: str) -> int | None:
    normalized = normalize_text(text)
    match = re.search(
        r"proponentes?\s+participantes?\s*:?\s*(\d+)",
        normalized,
    )
    return int(match.group(1)) if match else None


def _inspect_current_proposal_page(
    driver: object,
    *,
    expected_number: str,
    evidence_url: str,
) -> ProposalObservation:
    text = _driver_body_text(driver)
    normalized = normalize_text(text)
    expected_norm = normalize_text(expected_number)
    if expected_norm and expected_norm not in normalized:
        return ProposalObservation(
            numero_cl=expected_number,
            cl_url=evidence_url,
            status="error_verificacion",
            proposal_count=None,
            evidence_type="pagina_desalineada",
            evidence_url=evidence_url,
            confidence=0.0,
            error=f"La página cargada no contiene el número esperado {expected_number}.",
            page_text_excerpt=clean_text(text)[-1000:],
        )

    proposals, has_structure = _provider_tables(driver)
    declared = _declared_proponent_count(text)
    explicit_empty = any(
        marker in normalized
        for marker in (
            "no se encontro registro",
            "no hay propuesta",
            "no se encontraron propuestas",
            "sin propuestas presentadas",
        )
    )

    if proposals:
        count = max(len(proposals), declared or 0)
        return ProposalObservation(
            numero_cl=expected_number,
            cl_url=evidence_url,
            status="cerrada_con_propuestas",
            proposal_count=count,
            proponents=proposals,
            evidence_type="cuadro_cotizaciones_con_proponentes",
            evidence_url=evidence_url,
            confidence=1.0 if count == len(proposals) else 0.95,
            page_text_excerpt=clean_text(text)[-1000:],
        )

    if declared is not None and declared > 0:
        return ProposalObservation(
            numero_cl=expected_number,
            cl_url=evidence_url,
            status="cerrada_con_propuestas",
            proposal_count=declared,
            proponents=[],
            evidence_type="contador_oficial_sin_detalle",
            evidence_url=evidence_url,
            confidence=0.9,
            error="El contador informó propuestas, pero no se pudieron leer sus nombres.",
            page_text_excerpt=clean_text(text)[-1000:],
        )

    if declared == 0 and (
        explicit_empty
        or "cuadro de cotizaciones presentadas por proponente" in normalized
    ):
        return ProposalObservation(
            numero_cl=expected_number,
            cl_url=evidence_url,
            status="cerrada_sin_propuestas",
            proposal_count=0,
            proponents=[],
            evidence_type="contador_oficial_cero",
            evidence_url=evidence_url,
            confidence=1.0,
            page_text_excerpt=clean_text(text)[-1000:],
        )

    if explicit_empty and has_structure:
        return ProposalObservation(
            numero_cl=expected_number,
            cl_url=evidence_url,
            status="cerrada_sin_propuestas",
            proposal_count=0,
            proponents=[],
            evidence_type="mensaje_oficial_sin_propuestas",
            evidence_url=evidence_url,
            confidence=1.0,
            page_text_excerpt=clean_text(text)[-1000:],
        )

    return ProposalObservation(
        numero_cl=expected_number,
        cl_url=evidence_url,
        status="cerrada_pendiente_publicacion",
        proposal_count=None,
        proponents=[],
        evidence_type="cuadro_no_concluyente",
        evidence_url=evidence_url,
        confidence=0.25,
        error="El portal no mostró todavía un cuadro de propuestas concluyente.",
        page_text_excerpt=clean_text(text)[-1000:],
    )


def inspect_closed_cl(
    driver: object,
    cl_url: str,
    *,
    timeout: int = 35,
) -> ProposalObservation:
    """Inspecciona una CL vencida sin convertir errores técnicos en cero."""

    from selenium.webdriver.common.by import By  # type: ignore
    from selenium.webdriver.support import expected_conditions as EC  # type: ignore
    from selenium.webdriver.support.ui import WebDriverWait  # type: ignore

    url = normalize_url(cl_url)
    expected_number = extract_cl_number(url)
    if not url or not expected_number:
        return ProposalObservation(
            numero_cl=expected_number,
            cl_url=url,
            status="error_verificacion",
            proposal_count=None,
            confidence=0.0,
            error="Enlace o número de CL inválido.",
        )

    try:
        driver.get("about:blank")
        driver.get(url)
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        main_text = _wait_expected_page(
            driver,
            expected_number,
            timeout=timeout,
            ready_tokens=(
                "Solicitud de Cotización V3",
                "Procesos relacionados",
                "Cuadro de cotizaciones",
            ),
        )
        if expected_number.lower() not in main_text.lower():
            return ProposalObservation(
                numero_cl=expected_number,
                cl_url=url,
                status="error_verificacion",
                proposal_count=None,
                evidence_type="detalle_cl_no_cargo",
                evidence_url=url,
                confidence=0.0,
                error=f"No cargó el detalle correspondiente a {expected_number}.",
                page_text_excerpt=clean_text(main_text)[-1000:],
            )

        candidate_links = _all_candidate_links(driver)
        cuadro_links = [
            link
            for link in candidate_links
            if "/cuadro-de-cotizaciones/" in link or "/cuadro-de-propuestas/" in link
        ]
        if not cuadro_links:
            return ProposalObservation(
                numero_cl=expected_number,
                cl_url=url,
                status="cerrada_pendiente_publicacion",
                proposal_count=None,
                evidence_type="sin_enlace_cuadro",
                evidence_url=url,
                confidence=0.2,
                error="La CL venció, pero el portal aún no publicó el Cuadro de Cotizaciones.",
                page_text_excerpt=clean_text(main_text)[-1000:],
            )

        last_result = None
        for evidence_url in cuadro_links[:3]:
            driver.get(evidence_url)
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            _wait_expected_page(
                driver,
                expected_number,
                timeout=timeout,
                ready_tokens=(
                    "Proponentes participante",
                    "Cuadro de Cotizaciones Presentadas",
                    "No se encontró registro",
                    "No hay propuesta",
                ),
            )
            result = _inspect_current_proposal_page(
                driver,
                expected_number=expected_number,
                evidence_url=evidence_url,
            )
            result.cl_url = url
            last_result = result
            if result.status in {"cerrada_con_propuestas", "cerrada_sin_propuestas"}:
                return result
        return last_result or ProposalObservation(
            numero_cl=expected_number,
            cl_url=url,
            status="cerrada_pendiente_verificacion",
            proposal_count=None,
            confidence=0.1,
            error="No se pudo obtener evidencia concluyente.",
        )
    except Exception as exc:
        return ProposalObservation(
            numero_cl=expected_number,
            cl_url=url,
            status="error_verificacion",
            proposal_count=None,
            evidence_type="error_selenium",
            evidence_url=clean_text(getattr(driver, "current_url", "")) or url,
            confidence=0.0,
            error=f"{type(exc).__name__}: {exc}",
            page_text_excerpt=clean_text(_driver_body_text(driver))[-1000:],
        )


def _normalized_mapping(data: Mapping[str, Any]) -> dict[str, Any]:
    return {normalize_text(key): value for key, value in data.items()}


def record_from_mapping(
    data: Mapping[str, Any],
    *,
    source_sheets: Iterable[str] = (),
    estado: str = "abierta",
) -> dict[str, Any]:
    mapped = _normalized_mapping(data)

    def pick(*names: str) -> Any:
        for name in names:
            key = normalize_text(name)
            if key in mapped and clean_text(mapped[key]):
                return mapped[key]
        return ""

    enlace = normalize_url(pick("enlace", "url", "acto_url"))
    numero_cl = extract_cl_number(enlace) or extract_cl_number(pick("numero_cl", "numero"))
    fecha_text = clean_text(pick("fecha", "fecha_presentacion_texto"))
    deadline = parse_cl_deadline(fecha_text)
    timestamp = now_iso()
    sheets = sorted({clean_text(sheet) for sheet in source_sheets if clean_text(sheet)})
    raw_fichas = pick("ficha_detectada", "fichas_detectadas")
    return {
        "cl_key": cl_key_for(enlace, numero_cl),
        "numero_cl": numero_cl,
        "enlace": enlace,
        "titulo": clean_text(pick("titulo", "descripcion")),
        "entidad": clean_text(pick("entidad")),
        "unidad_solicitante": clean_text(pick("unidad solicitante", "unidad_solic")),
        "precio_referencia": parse_money(pick("precio_referencia", "precio estimado")),
        "fecha_publicacion": clean_text(pick("publicacion", "fecha_publicacion")),
        "fecha_presentacion_texto": fecha_text,
        "fecha_cierre": deadline.isoformat(timespec="seconds") if deadline else "",
        "fichas_detectadas": clean_text(raw_fichas),
        "source_sheets": ",".join(sheets),
        "estado_derivado": estado,
        "proposal_count": None,
        "proponents_json": "[]",
        "evidence_type": "",
        "evidence_url": "",
        "confidence": 0.7 if estado == "abierta" else 0.3,
        "first_seen_at": timestamp,
        "last_seen_at": timestamp,
        "closed_at": "",
        "last_check_at": "",
        "next_check_at": "",
        "check_attempts": 0,
        "successor_process_number": "",
        "successor_url": "",
        "last_error": "",
        "source_payload_json": json.dumps(dict(data), ensure_ascii=False, default=str),
        "updated_at": timestamp,
    }


def apply_observation(
    record: Mapping[str, Any],
    observation: ProposalObservation,
) -> tuple[dict[str, Any], dict[str, Any]]:
    timestamp = now_iso()
    attempts = int(record.get("check_attempts") or 0) + 1
    retry_at = ""
    if observation.status in RETRY_STATES:
        delay_minutes = min(180, 15 * (2 ** min(attempts - 1, 3)))
        retry_at = (_now() + timedelta(minutes=delay_minutes)).isoformat(timespec="seconds")
    updated = dict(record)
    updated.update(
        {
            "numero_cl": observation.numero_cl or record.get("numero_cl", ""),
            "enlace": observation.cl_url or record.get("enlace", ""),
            "estado_derivado": observation.status,
            "proposal_count": observation.proposal_count,
            "proponents_json": observation.proponents_json(),
            "evidence_type": observation.evidence_type,
            "evidence_url": observation.evidence_url,
            "confidence": observation.confidence,
            "closed_at": clean_text(record.get("closed_at")) or timestamp,
            "last_check_at": timestamp,
            "next_check_at": retry_at,
            "check_attempts": attempts,
            "last_error": observation.error,
            "updated_at": timestamp,
        }
    )
    obs_row = {
        "observation_id": str(uuid.uuid4()),
        "cl_key": updated["cl_key"],
        "numero_cl": updated["numero_cl"],
        "checked_at": timestamp,
        "estado_derivado": observation.status,
        "proposal_count": observation.proposal_count,
        "proponents_json": observation.proponents_json(),
        "evidence_type": observation.evidence_type,
        "evidence_url": observation.evidence_url,
        "confidence": observation.confidence,
        "error": observation.error,
        "page_text_excerpt": observation.page_text_excerpt,
    }
    return updated, obs_row


def resolve_local_db_path(explicit: str | Path | None = None) -> Path:
    if explicit:
        target = Path(explicit)
        target.parent.mkdir(parents=True, exist_ok=True)
        return target
    configured = clean_text(os.environ.get("PANAMACOMPRA_DB_PATH"))
    if configured:
        target = Path(configured)
        target.parent.mkdir(parents=True, exist_ok=True)
        return target
    candidates = [
        DEFAULT_DB_PATH,
        Path.home() / "GEAPP" / "panamacompra.db",
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    target = DEFAULT_DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def _ensure_sqlite_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS cl_cotizaciones (
            cl_key TEXT PRIMARY KEY,
            numero_cl TEXT,
            enlace TEXT,
            titulo TEXT,
            entidad TEXT,
            unidad_solicitante TEXT,
            precio_referencia REAL,
            fecha_publicacion TEXT,
            fecha_presentacion_texto TEXT,
            fecha_cierre TEXT,
            fichas_detectadas TEXT,
            source_sheets TEXT,
            estado_derivado TEXT,
            proposal_count INTEGER,
            proponents_json TEXT,
            evidence_type TEXT,
            evidence_url TEXT,
            confidence REAL,
            first_seen_at TEXT,
            last_seen_at TEXT,
            closed_at TEXT,
            last_check_at TEXT,
            next_check_at TEXT,
            check_attempts INTEGER DEFAULT 0,
            successor_process_number TEXT,
            successor_url TEXT,
            last_error TEXT,
            source_payload_json TEXT,
            updated_at TEXT,
            postgres_synced INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_cl_estado
            ON cl_cotizaciones(estado_derivado);
        CREATE INDEX IF NOT EXISTS idx_cl_cierre
            ON cl_cotizaciones(fecha_cierre);
        CREATE INDEX IF NOT EXISTS idx_cl_numero
            ON cl_cotizaciones(numero_cl);

        CREATE TABLE IF NOT EXISTS cl_cotizaciones_observaciones (
            observation_id TEXT PRIMARY KEY,
            cl_key TEXT NOT NULL,
            numero_cl TEXT,
            checked_at TEXT,
            estado_derivado TEXT,
            proposal_count INTEGER,
            proponents_json TEXT,
            evidence_type TEXT,
            evidence_url TEXT,
            confidence REAL,
            error TEXT,
            page_text_excerpt TEXT,
            postgres_synced INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_cl_obs_key
            ON cl_cotizaciones_observaciones(cl_key, checked_at);
        """
    )


def persist_local(
    records: Sequence[Mapping[str, Any]],
    observations: Sequence[Mapping[str, Any]] = (),
    *,
    db_path: str | Path | None = None,
) -> Path:
    path = resolve_local_db_path(db_path)
    connection = sqlite3.connect(path, timeout=60)
    try:
        _ensure_sqlite_schema(connection)
        for incoming in records:
            row = {field: incoming.get(field) for field in CL_FIELDS}
            existing = connection.execute(
                f"SELECT {','.join(CL_FIELDS)} "
                "FROM cl_cotizaciones WHERE cl_key = ?",
                (row["cl_key"],),
            ).fetchone()
            if existing:
                previous = dict(zip(CL_FIELDS, existing))
                old_state = previous.get("estado_derivado")
                if old_state in FINAL_STATES and row["estado_derivado"] == "abierta":
                    # Una reaparición en una hoja activa puede refrescar datos
                    # descriptivos, pero jamás borrar una clasificación final,
                    # sus proponentes ni la evidencia oficial que la sustenta.
                    for field in FINAL_EVIDENCE_FIELDS:
                        row[field] = previous.get(field)
                row["first_seen_at"] = (
                    previous.get("first_seen_at") or row["first_seen_at"]
                )
                merged_sheets = sorted(
                    {
                        item.strip()
                        for value in (
                            previous.get("source_sheets", ""),
                            row.get("source_sheets", ""),
                        )
                        for item in clean_text(value).split(",")
                        if item.strip()
                    }
                )
                row["source_sheets"] = ",".join(merged_sheets)
            placeholders = ",".join("?" for _ in CL_FIELDS)
            updates = ",".join(
                f"{field}=excluded.{field}"
                for field in CL_FIELDS
                if field not in {"cl_key", "first_seen_at"}
            )
            connection.execute(
                f"INSERT INTO cl_cotizaciones ({','.join(CL_FIELDS)}, postgres_synced) "
                f"VALUES ({placeholders}, 0) "
                f"ON CONFLICT(cl_key) DO UPDATE SET {updates}, postgres_synced=0",
                [row[field] for field in CL_FIELDS],
            )
        for incoming in observations:
            row = {field: incoming.get(field) for field in OBS_FIELDS}
            connection.execute(
                f"INSERT OR IGNORE INTO cl_cotizaciones_observaciones "
                f"({','.join(OBS_FIELDS)}, postgres_synced) "
                f"VALUES ({','.join('?' for _ in OBS_FIELDS)}, 0)",
                [row[field] for field in OBS_FIELDS],
            )
        connection.commit()
    finally:
        connection.close()
    return path


def load_retry_records(
    *,
    db_path: str | Path | None = None,
    limit: int = 30,
) -> list[dict[str, Any]]:
    path = resolve_local_db_path(db_path)
    if not path.exists():
        return []
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        _ensure_sqlite_schema(connection)
        now_value = now_iso()
        rows = connection.execute(
            "SELECT * FROM cl_cotizaciones "
            "WHERE estado_derivado IN (?,?,?) "
            "AND (next_check_at IS NULL OR next_check_at = '' OR next_check_at <= ?) "
            "ORDER BY COALESCE(next_check_at, ''), updated_at "
            "LIMIT ?",
            (*sorted(RETRY_STATES), now_value, max(1, int(limit))),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def load_known_links(
    *,
    db_path: str | Path | None = None,
) -> set[str]:
    """Enlaces ya inventariados, incluidos los que salieron de hojas activas."""

    path = resolve_local_db_path(db_path)
    if not path.exists():
        return set()
    connection = sqlite3.connect(path, timeout=30)
    try:
        _ensure_sqlite_schema(connection)
        return {
            clean_text(row[0])
            for row in connection.execute(
                "SELECT enlace FROM cl_cotizaciones "
                "WHERE enlace IS NOT NULL AND enlace <> ''"
            ).fetchall()
            if clean_text(row[0])
        }
    finally:
        connection.close()


def _ensure_postgres_schema(cursor: Any) -> None:
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cl_cotizaciones (
            cl_key TEXT PRIMARY KEY,
            numero_cl TEXT,
            enlace TEXT,
            titulo TEXT,
            entidad TEXT,
            unidad_solicitante TEXT,
            precio_referencia DOUBLE PRECISION,
            fecha_publicacion TEXT,
            fecha_presentacion_texto TEXT,
            fecha_cierre TEXT,
            fichas_detectadas TEXT,
            source_sheets TEXT,
            estado_derivado TEXT,
            proposal_count INTEGER,
            proponents_json TEXT,
            evidence_type TEXT,
            evidence_url TEXT,
            confidence DOUBLE PRECISION,
            first_seen_at TEXT,
            last_seen_at TEXT,
            closed_at TEXT,
            last_check_at TEXT,
            next_check_at TEXT,
            check_attempts INTEGER DEFAULT 0,
            successor_process_number TEXT,
            successor_url TEXT,
            last_error TEXT,
            source_payload_json TEXT,
            updated_at TEXT
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_cl_estado ON cl_cotizaciones(estado_derivado)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_cl_cierre ON cl_cotizaciones(fecha_cierre)"
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cl_cotizaciones_observaciones (
            observation_id TEXT PRIMARY KEY,
            cl_key TEXT NOT NULL,
            numero_cl TEXT,
            checked_at TEXT,
            estado_derivado TEXT,
            proposal_count INTEGER,
            proponents_json TEXT,
            evidence_type TEXT,
            evidence_url TEXT,
            confidence DOUBLE PRECISION,
            error TEXT,
            page_text_excerpt TEXT
        )
        """
    )


def sync_pending_to_postgres(
    *,
    db_path: str | Path | None = None,
    dsn: str = "",
    limit: int = 2500,
) -> dict[str, Any]:
    url = clean_text(dsn or os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL"))
    if not url:
        return {"configured": False, "records": 0, "observations": 0}

    import psycopg2  # type: ignore

    path = resolve_local_db_path(db_path)
    local = sqlite3.connect(path, timeout=60)
    local.row_factory = sqlite3.Row
    try:
        _ensure_sqlite_schema(local)
        records = [
            dict(row)
            for row in local.execute(
                "SELECT * FROM cl_cotizaciones WHERE postgres_synced = 0 "
                "ORDER BY updated_at LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        ]
        observations = [
            dict(row)
            for row in local.execute(
                "SELECT * FROM cl_cotizaciones_observaciones WHERE postgres_synced = 0 "
                "ORDER BY checked_at LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        ]

        remote = psycopg2.connect(url, connect_timeout=20)
        try:
            with remote.cursor() as cursor:
                _ensure_postgres_schema(cursor)
                if records:
                    placeholders = ",".join(["%s"] * len(CL_FIELDS))
                    updates = ",".join(
                        f"{field}=EXCLUDED.{field}"
                        for field in CL_FIELDS
                        if field not in {"cl_key", "first_seen_at"}
                    )
                    cursor.executemany(
                        f"INSERT INTO cl_cotizaciones ({','.join(CL_FIELDS)}) "
                        f"VALUES ({placeholders}) "
                        f"ON CONFLICT(cl_key) DO UPDATE SET {updates}",
                        [[row.get(field) for field in CL_FIELDS] for row in records],
                    )
                if observations:
                    cursor.executemany(
                        f"INSERT INTO cl_cotizaciones_observaciones "
                        f"({','.join(OBS_FIELDS)}) "
                        f"VALUES ({','.join(['%s'] * len(OBS_FIELDS))}) "
                        f"ON CONFLICT(observation_id) DO NOTHING",
                        [[row.get(field) for field in OBS_FIELDS] for row in observations],
                    )
            remote.commit()
        finally:
            remote.close()

        if records:
            local.executemany(
                "UPDATE cl_cotizaciones SET postgres_synced = 1 WHERE cl_key = ?",
                [(row["cl_key"],) for row in records],
            )
        if observations:
            local.executemany(
                "UPDATE cl_cotizaciones_observaciones SET postgres_synced = 1 "
                "WHERE observation_id = ?",
                [(row["observation_id"],) for row in observations],
            )
        local.commit()
        return {
            "configured": True,
            "records": len(records),
            "observations": len(observations),
        }
    finally:
        local.close()
