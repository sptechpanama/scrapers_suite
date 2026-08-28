from __future__ import annotations

import os

from ..models import Opportunity, clean_text
from .base import SourceAdapter, absolute_url, parse_date, soup_from_html


class UngmAdapter(SourceAdapter):
    source = "ungm"
    source_name = "UN Global Marketplace"
    parser_version = "1.0.0"
    page_url = "https://www.ungm.org/Public/Notice"
    search_url = "https://www.ungm.org/Public/Notice/Search"
    panama_country_id = "2449"

    def _payload(self, page_index: int) -> dict[str, object]:
        return {
            "PageIndex": page_index,
            "PageSize": 15,
            "Title": "",
            "Description": "",
            "Reference": "",
            "PublishedFrom": "",
            "PublishedTo": "",
            "DeadlineFrom": "",
            "DeadlineTo": "",
            "Countries": [self.panama_country_id],
            "Agencies": [],
            "UNSPSCs": [],
            "NoticeTypes": [],
            "SortField": "DatePublished",
            "SortAscending": False,
            "isPicker": False,
            "IsSustainable": False,
            "IsActive": True,
            "NoticeDisplayType": "",
            "NoticeSearchTotalLabelId": "noticeSearchTotal",
            "TypeOfCompetitions": [],
        }

    def fetch_opportunities(self) -> list[Opportunity]:
        self.client.get(self.page_url)
        headers = {"X-Requested-With": "XMLHttpRequest", "Referer": self.page_url}
        # Recorre hasta encontrar la última página; el tope evita un ciclo
        # accidental si UNGM cambia su paginación.
        max_pages = max(1, min(int(os.environ.get("OTRAS_FUENTES_UNGM_PAGES", "20")), 20))
        rows: list[Opportunity] = []
        for page_index in range(max_pages):
            response = self.client.post(
                self.search_url,
                json=self._payload(page_index),
                headers=headers,
            ).response
            soup = soup_from_html(response.text)
            data_rows = soup.select(".dataRow[data-noticeid]")
            if not data_rows:
                break
            for row in data_rows:
                notice_id = clean_text(row.get("data-noticeid"))
                cells = row.select(".tableCell")
                title_node = row.select_one(".resultTitle .ungm-title")
                title = clean_text(title_node.get_text(" ", strip=True) if title_node else "")
                link_node = row.select_one('.resultTitle a[href*="/Public/Notice/"]')
                link = absolute_url(self.page_url, link_node.get("href")) if link_node else f"{self.page_url}/{notice_id}"
                values = [clean_text(cell.get_text(" ", strip=True)) for cell in cells]
                deadline_node = row.select_one('[data-description="Deadline"] span')
                deadline = parse_date(clean_text(deadline_node.get_text(" ", strip=True)).split("(")[0]) if deadline_node else ""
                published = parse_date(values[3]) if len(values) > 3 else ""
                agency = values[4] if len(values) > 4 else ""
                method = values[5] if len(values) > 5 else ""
                reference = values[6] if len(values) > 6 else ""
                destination = values[7] if len(values) > 7 else ""
                rows.append(
                    Opportunity(
                        source=self.source,
                        external_id=notice_id or reference,
                        title=title or reference,
                        description=f"Referencia: {reference}. Destino: {destination}",
                        source_url=link,
                        source_type="Oportunidad internacional",
                        buyer=agency,
                        country="Panamá",
                        publication_date=published,
                        deadline=deadline,
                        status="Activa",
                        procurement_method=method,
                        submission_channel="UNGM",
                        raw_payload={"cells": values, "notice_id": notice_id},
                        parser_version=self.parser_version,
                    )
                )
            if len(data_rows) < 15:
                break
        return rows
