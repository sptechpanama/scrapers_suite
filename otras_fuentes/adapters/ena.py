from __future__ import annotations

import re

from ..models import Opportunity, SourceDocument, clean_text
from .base import SourceAdapter, date_from_url, slug_id, soup_from_html


class EnaAdapter(SourceAdapter):
    source = "ena"
    source_name = "ENA Corredores"
    parser_version = "1.0.0"
    url = "https://ena.com.pa/activas/"
    _PRIMARY = re.compile(
        r"solicitud(?:es)? de (?:cotizaci[oó]n|informaci[oó]n)|aviso de convocatoria|bases? de precalificaci[oó]n",
        re.IGNORECASE,
    )

    def fetch_opportunities(self) -> list[Opportunity]:
        response = self.client.get(self.url).response
        soup = soup_from_html(response.text)
        rows: list[Opportunity] = []
        for anchor in soup.select('a[href*="/wp-content/uploads/"]'):
            href = clean_text(anchor.get("href"))
            title = clean_text(anchor.get_text(" ", strip=True))
            context = clean_text(anchor.parent.get_text(" ", strip=True) if anchor.parent else title)
            if "/2026/" not in href or not href.lower().endswith((".pdf", ".doc", ".docx", ".zip")):
                continue
            if not self._PRIMARY.search(f"{title} {context}"):
                continue
            external = next(
                iter(re.findall(r"(?:LCT|SC|SDI)?[-_\sN°.#]*(\d{2,4}[-_]\d{2})", f"{title} {href}", re.IGNORECASE)),
                "",
            ) or slug_id(href)
            descriptive = context if len(context) > len(title) else title
            rows.append(
                Opportunity(
                    source=self.source,
                    external_id=external,
                    title=descriptive,
                    source_url=href,
                    source_type="Solicitud de cotización / licitación",
                    buyer=self.source_name,
                    publication_date=date_from_url(href),
                    status="Publicada",
                    submission_channel="Documento oficial ENA",
                    documents=[SourceDocument(title=title, url=href, document_type="Documento oficial")],
                    raw_payload={"listing_url": self.url},
                    parser_version=self.parser_version,
                )
            )
        return rows
