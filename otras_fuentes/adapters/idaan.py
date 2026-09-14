from __future__ import annotations

import re

from ..models import Opportunity, clean_text, normalized_text
from .public_pages import document_links, official_date
from .base import SourceAdapter, absolute_url, parse_date, soup_from_html


class IdaanAdapter(SourceAdapter):
    source = "idaan"
    source_name = "IDAAN"
    parser_version = "2.0.0"
    url = "https://compras.idaan.gob.pa/home"

    @staticmethod
    def _amount(text: str) -> float | None:
        match = re.search(r"Monto:\s*B/\.\s*([0-9.,]+)", text, re.IGNORECASE)
        if not match:
            return None
        raw = match.group(1).replace(",", "")
        try:
            return float(raw)
        except ValueError:
            return None

    def fetch_opportunities(self) -> list[Opportunity]:
        response = self.client.get(self.url).response
        soup = soup_from_html(response.text)
        if not soup.find('table'):
            raise RuntimeError('IDAAN no entregó las tablas de compras')
        rows: list[Opportunity] = []
        for table in soup.find_all("table"):
            headers = [clean_text(cell.get_text(" ", strip=True)).lower() for cell in table.find_all("th")]
            for tr in table.find_all("tr"):
                cells = [clean_text(cell.get_text(" ", strip=True)) for cell in tr.find_all("td")]
                if not cells:
                    continue
                number = next((cell for cell in cells if re.fullmatch(r"20\d{2}-\d-[A-Z0-9-]+", cell)), "")
                if not number:
                    continue
                joined = " | ".join(cells)
                status = next(
                    (value for value in ("Adjudicada", "Desierta", "Cancelada", "Activa") if value.lower() in joined.lower()),
                    "Publicada",
                )
                columns = dict(zip((normalized_text(h) for h in headers), cells))
                description = columns.get('descripcion', '')
                for cell in cells:
                    if columns.get('descripcion'):
                        break
                    if cell == number or cell.isdigit() or status.lower() == cell.lower():
                        continue
                    if len(cell) > len(description) and not cell.lower().startswith(("ver ", "ganador:")):
                        description = cell
                link_node = tr.find("a", href=True)
                link = absolute_url(self.url, link_node.get("href")) if link_node else self.url
                deadline = official_date(columns.get('fecha cierre', ''), local_time=True)
                rows.append(
                    Opportunity(
                        source=self.source,
                        external_id=number,
                        title=description or number,
                        description=joined,
                        source_url=link,
                        source_type="Compra corporativa",
                        buyer=self.source_name,
                        deadline=deadline,
                        status=status,
                        procurement_method="Portal de Compras IDAAN",
                        estimated_value=None,
                        documents=document_links(tr, self.url),
                        submission_channel="Portal IDAAN",
                        raw_payload={"headers": headers, "cells": cells, "deadline_raw": deadline,
                                     "awarded_value": self._amount(columns.get('adjudicacion', joined))},
                        parser_version=self.parser_version,
                    )
                )
        return rows
