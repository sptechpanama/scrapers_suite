from __future__ import annotations

import re

from ..models import Opportunity, SourceDocument, clean_text
from .base import SourceAdapter, absolute_url, parse_date, soup_from_html


class CruzRojaAdapter(SourceAdapter):
    source = "cruz_roja"
    source_name = "Cruz Roja Panameña"
    parser_version = "1.0.0"
    url = "https://cruzroja.org.pa/licitaciones-publicas/"

    def fetch_opportunities(self) -> list[Opportunity]:
        response = self.client.get(self.url).response
        soup = soup_from_html(response.text)
        table = soup.find("table")
        if table is None:
            return []
        headers = [clean_text(cell.get_text(" ", strip=True)).lower() for cell in table.find_all("th")]
        rows: list[Opportunity] = []
        for tr in table.find_all("tr"):
            cells = [clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all("td")]
            if len(cells) < 3:
                continue
            mapping = {
                headers[index] if index < len(headers) else f"col_{index}": value
                for index, value in enumerate(cells)
            }
            code = mapping.get("código") or mapping.get("codigo") or cells[0]
            title = mapping.get("título") or mapping.get("titulo") or cells[1]
            deadline = mapping.get("fecha de cierre") or ""
            status = mapping.get("estado") or "Publicada"
            category = mapping.get("categoria") or ""
            modality = mapping.get("modalidad") or ""
            anchor = tr.find("a", href=True)
            link = absolute_url(self.url, anchor.get("href")) if anchor else self.url
            documents = []
            if anchor:
                documents.append(SourceDocument(title="Aplicación", url=link, document_type="Aplicación"))
            rows.append(
                Opportunity(
                    source=self.source,
                    external_id=clean_text(code),
                    title=clean_text(title),
                    source_url=link,
                    source_type="Licitación humanitaria",
                    buyer=self.source_name,
                    deadline=parse_date(deadline),
                    status=clean_text(status),
                    procurement_method=clean_text(modality),
                    sector=clean_text(category),
                    submission_channel="Cruz Roja Panameña",
                    documents=documents,
                    raw_payload=mapping,
                    parser_version=self.parser_version,
                )
            )
        return rows
