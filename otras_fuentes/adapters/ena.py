from __future__ import annotations

import re

from ..models import Opportunity, SourceDocument, clean_text
from .base import SourceAdapter, date_from_url, slug_id, soup_from_html
from .public_pages import document_links, official_date


class EnaAdapter(SourceAdapter):
    source = "ena"
    source_name = "ENA Corredores"
    parser_version = "2.0.0"
    url = "https://ena.com.pa/activas/"
    _PRIMARY = re.compile(
        r"solicitud(?:es)? de (?:cotizaci[oó]n|informaci[oó]n)|aviso de convocatoria|bases? de precalificaci[oó]n",
        re.IGNORECASE,
    )

    def fetch_opportunities(self) -> list[Opportunity]:
        response = self.client.get(self.url).response
        soup = soup_from_html(response.text)
        rows: list[Opportunity] = []
        handled = set()
        # Current ENA notices group their title, code and ALL annexes in one card.
        for badge in soup.find_all("span"):
            code = clean_text(badge.get_text(" ", strip=True))
            if not re.fullmatch(r"(?:COT|LCT|SC|SDI|LIC|LPN)\s*[\d-]+", code, re.I):
                continue
            card = badge.parent.parent
            docs = document_links(card, self.url)
            if not docs or id(card) in handled:
                continue
            handled.add(id(card))
            for anchor in card.select("a[href]"):
                handled.add(anchor.get("href"))
            spans = badge.parent.find_all("span", recursive=False)
            title = clean_text(spans[-1].get_text(" ", strip=True)) if len(spans) > 1 else code
            primary = next((d for d in docs if re.search(r"solicitud.*cotiz|solicitud.*informa|aviso.*convoc", d.title, re.I)), docs[0])
            dates = [official_date(m) for m in re.findall(r"\d{2}/\d{2}/\d{2,4}", card.get_text(" ", strip=True))]
            published = min((v for v in dates if v), default="")
            rows.append(Opportunity(
                source=self.source, external_id=code, title=title, source_url=primary.url,
                source_type="Solicitud de información" if code.upper().startswith("SDI") else "Solicitud de cotización / licitación",
                buyer=self.source_name, publication_date=published, status="Publicada",
                submission_channel="Documento oficial ENA", documents=docs,
                raw_payload={"listing_url": self.url, "official_number": code, "listing_evidence": clean_text(card.get_text(" ", strip=True))},
                parser_version=self.parser_version,
            ))
        for anchor in soup.select('a[href*="/wp-content/uploads/"]'):
            href = clean_text(anchor.get("href"))
            if href in handled:
                continue
            title = clean_text(anchor.get_text(" ", strip=True))
            context = clean_text(anchor.parent.get_text(" ", strip=True) if anchor.parent else title)
            if not href.lower().endswith((".pdf", ".doc", ".docx", ".zip")):
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
                    publication_date="",
                    status="Publicada",
                    submission_channel="Documento oficial ENA",
                    documents=[SourceDocument(title=title, url=href, document_type="Documento oficial")],
                    raw_payload={"listing_url": self.url},
                    parser_version=self.parser_version,
                )
            )
        return rows
