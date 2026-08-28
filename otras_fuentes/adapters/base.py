from __future__ import annotations

import re
import time
from abc import ABC, abstractmethod
from datetime import datetime
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from ..http import ResilientHttpClient
from ..models import Opportunity, SourceFetchResult, clean_text


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


def absolute_url(base_url: str, value: object) -> str:
    return urljoin(base_url, clean_text(value))


def slug_id(url: str) -> str:
    path = urlsplit(url).path.rstrip("/")
    return path.rsplit("/", 1)[-1] or url


def date_from_url(url: str) -> str:
    match = re.search(r"/(20\d{2})/(0?[1-9]|1[0-2])/", url)
    if not match:
        return ""
    return f"{match.group(1)}-{int(match.group(2)):02d}-01"


def parse_date(value: object) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    raw = re.sub(r"\s+", " ", raw)
    for fmt in (
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d-%b-%Y",
        "%a, %d %b %Y %H:%M:%S %z",
        "%d %B %Y",
    ):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass
    lowered = raw.lower()
    month_match = re.search(
        r"(\d{1,2})\s+(" + "|".join(SPANISH_MONTHS) + r")\s+(20\d{2})",
        lowered,
    )
    if month_match:
        return datetime(
            int(month_match.group(3)),
            SPANISH_MONTHS[month_match.group(2)],
            int(month_match.group(1)),
        ).date().isoformat()
    iso_match = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", raw)
    if iso_match:
        return f"{iso_match.group(1)}-{int(iso_match.group(2)):02d}-{int(iso_match.group(3)):02d}"
    return raw


def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html or "", "html.parser")


class SourceAdapter(ABC):
    source = "base"
    source_name = "Fuente"
    parser_version = "1.0.0"

    def __init__(self, client: ResilientHttpClient | None = None) -> None:
        self.client = client or ResilientHttpClient()

    def fetch(self) -> SourceFetchResult:
        started = time.monotonic()
        try:
            opportunities = [item.normalize() for item in self.fetch_opportunities()]
            unique: dict[str, Opportunity] = {}
            for item in opportunities:
                unique[item.external_id] = item
            return SourceFetchResult(
                source=self.source,
                opportunities=list(unique.values()),
                status="success",
                coverage="Completa",
                response_ms=int((time.monotonic() - started) * 1000),
            )
        except Exception as exc:  # adapters are isolated by design
            return SourceFetchResult(
                source=self.source,
                opportunities=[],
                status="error",
                error=f"{type(exc).__name__}: {exc}",
                coverage="No disponible",
                response_ms=int((time.monotonic() - started) * 1000),
            )

    @abstractmethod
    def fetch_opportunities(self) -> list[Opportunity]:
        raise NotImplementedError
