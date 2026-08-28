from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalized_text(value: object) -> str:
    text = clean_text(value).lower()
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    return re.sub(r"[^0-9a-z]+", " ", text).strip()


def canonical_url(value: object) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except ValueError:
        return raw
    if not parts.scheme or not parts.netloc:
        return raw
    path = re.sub(r"/{2,}", "/", parts.path or "/")
    path = path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, parts.query, ""))


def stable_hash(*values: object, length: int = 40) -> str:
    payload = "|".join(clean_text(value) for value in values)
    return hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()[:length]


@dataclass(slots=True)
class SourceDocument:
    title: str
    url: str
    document_type: str = "Documento"

    def normalized(self) -> "SourceDocument":
        return SourceDocument(
            title=clean_text(self.title) or "Documento",
            url=canonical_url(self.url),
            document_type=clean_text(self.document_type) or "Documento",
        )


@dataclass(slots=True)
class Opportunity:
    source: str
    external_id: str
    title: str
    source_url: str
    source_group: str = "Otras fuentes"
    source_type: str = "Convocatoria"
    description: str = ""
    buyer: str = ""
    country: str = "Panamá"
    province: str = ""
    publication_date: str = ""
    deadline: str = ""
    status: str = "Activa"
    procurement_method: str = ""
    estimated_value: float | None = None
    currency: str = "USD"
    sector: str = ""
    registration_required: str = ""
    submission_channel: str = ""
    eligibility: str = ""
    documents: list[SourceDocument] = field(default_factory=list)
    raw_payload: dict[str, Any] = field(default_factory=dict)
    matched_company: str = ""
    matched_keywords: list[str] = field(default_factory=list)
    matched_fields: list[str] = field(default_factory=list)
    fit_score: float = 0.0
    priority: str = "Baja"
    parser_version: str = "1.0.0"

    def normalize(self) -> "Opportunity":
        self.source = clean_text(self.source).lower()
        self.title = clean_text(self.title)
        self.description = clean_text(self.description)
        self.buyer = clean_text(self.buyer)
        self.source_url = canonical_url(self.source_url)
        self.external_id = clean_text(self.external_id) or stable_hash(
            self.source, self.source_url, self.title
        )
        self.documents = [
            document.normalized()
            for document in self.documents
            if canonical_url(document.url)
        ]
        self.matched_keywords = list(dict.fromkeys(clean_text(v) for v in self.matched_keywords if clean_text(v)))
        self.matched_fields = list(dict.fromkeys(clean_text(v) for v in self.matched_fields if clean_text(v)))
        if self.estimated_value is not None:
            try:
                self.estimated_value = float(self.estimated_value)
            except (TypeError, ValueError):
                self.estimated_value = None
        return self

    @property
    def id(self) -> str:
        return stable_hash(self.source, self.external_id, length=32)

    @property
    def cross_source_key(self) -> str:
        return stable_hash(
            normalized_text(self.title),
            normalized_text(self.buyer),
            clean_text(self.deadline)[:10],
            length=32,
        )

    @property
    def content_hash(self) -> str:
        # Solo cambios sustantivos de la oportunidad deben generar eventos.
        # `raw_payload` puede incluir contadores dinámicos (p. ej. UNGM) y la
        # clasificación puede variar al ajustar reglas internas sin que la
        # convocatoria oficial haya cambiado.
        payload = {
            "source": self.source,
            "external_id": self.external_id,
            "title": self.title,
            "source_url": canonical_url(self.source_url),
            "source_type": self.source_type,
            "description": self.description,
            "buyer": self.buyer,
            "country": self.country,
            "province": self.province,
            "publication_date": self.publication_date,
            "deadline": self.deadline,
            "status": self.status,
            "procurement_method": self.procurement_method,
            "estimated_value": self.estimated_value,
            "currency": self.currency,
            "sector": self.sector,
            "registration_required": self.registration_required,
            "submission_channel": self.submission_channel,
            "eligibility": self.eligibility,
            "documents": sorted(
                (asdict(document.normalized()) for document in self.documents),
                key=lambda item: (item["url"], item["document_type"], item["title"]),
            ),
        }
        return stable_hash(json.dumps(payload, ensure_ascii=False, sort_keys=True), length=64)

    def as_storage_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["id"] = self.id
        payload["canonical_url"] = canonical_url(self.source_url)
        payload["cross_source_key"] = self.cross_source_key
        payload["documents_json"] = json.dumps(
            payload.pop("documents", []), ensure_ascii=False, sort_keys=True
        )
        payload["raw_payload_json"] = json.dumps(
            payload.pop("raw_payload", {}), ensure_ascii=False, sort_keys=True, default=str
        )
        payload["matched_keywords_json"] = json.dumps(
            payload.pop("matched_keywords", []), ensure_ascii=False
        )
        payload["matched_fields_json"] = json.dumps(
            payload.pop("matched_fields", []), ensure_ascii=False
        )
        payload["content_hash"] = self.content_hash
        payload["scraped_at"] = utc_now_iso()
        payload["is_active"] = 0 if normalized_text(self.status) in {
            "cerrada", "cancelada", "desierta", "adjudicada", "vencida"
        } else 1
        return payload


@dataclass(slots=True)
class SourceFetchResult:
    source: str
    opportunities: list[Opportunity]
    status: str = "success"
    error: str = ""
    coverage: str = "Completa"
    pages_fetched: int = 1
    response_ms: int = 0


@dataclass(slots=True)
class MonitorResult:
    run_id: str
    started_at: str
    finished_at: str
    status: str
    source_results: list[dict[str, Any]]
    counts: dict[str, int]
    events: list[dict[str, Any]]
    postgres_synced: bool
    errors: dict[str, str]

    def summary(self) -> dict[str, Any]:
        return asdict(self)
