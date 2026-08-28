from __future__ import annotations

from ..models import Opportunity, SourceDocument, clean_text
from .base import SourceAdapter, absolute_url, date_from_url, slug_id, soup_from_html


class AcpAdapter(SourceAdapter):
    source = "acp"
    source_name = "Autoridad del Canal de Panamá"
    parser_version = "1.0.0"
    url = "https://pancanal.com/solicitudes-de-informaciones/"

    def fetch_opportunities(self) -> list[Opportunity]:
        response = self.client.get(self.url).response
        soup = soup_from_html(response.text)
        rows: list[Opportunity] = []
        for anchor in soup.select('a[href*="/wp-content/uploads/"]'):
            href = absolute_url(self.url, anchor.get("href"))
            title = clean_text(anchor.get_text(" ", strip=True))
            if not href.lower().endswith((".pdf", ".doc", ".docx", ".zip")):
                continue
            if not title:
                continue
            rows.append(
                Opportunity(
                    source=self.source,
                    external_id=slug_id(href),
                    title=title.replace("(opens in new tab)", "").strip(),
                    source_url=href,
                    source_type="Solicitud de información / estudio de mercado",
                    buyer=self.source_name,
                    publication_date=date_from_url(href),
                    status="Activa",
                    procurement_method="RFI / estudio de mercado",
                    submission_channel="Portal ACP",
                    documents=[SourceDocument(title=title, url=href, document_type="Documento oficial")],
                    raw_payload={"listing_url": self.url},
                    parser_version=self.parser_version,
                )
            )
        return rows
