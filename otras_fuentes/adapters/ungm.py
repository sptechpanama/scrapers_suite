from __future__ import annotations

import os
import re

from ..models import Opportunity, clean_text, normalized_text
from .base import SourceAdapter, absolute_url, parse_date, soup_from_html


LAC_ISO_CODES = {
    "AG", "AR", "BS", "BB", "BZ", "BO", "BR", "CL", "CO", "CR",
    "CU", "DM", "DO", "EC", "SV", "GD", "GT", "GY", "HT", "HN",
    "JM", "MX", "NI", "PA", "PY", "PE", "KN", "LC", "VC", "SR",
    "TT", "UY", "VE",
}

# Respaldo mínimo para que una variación temporal del HTML de UNGM no anule
# la cobertura regional. La lista completa se descubre desde el selector
# oficial de países en cada corrida.
LAC_COUNTRY_ID_FALLBACK = {
    "2313",  # Belice
    "2340",  # Costa Rica
    "2350",  # República Dominicana
    "2354",  # El Salvador
    "2377",  # Guatemala
    "2382",  # Honduras
    "2441",  # Nicaragua
}


class UngmAdapter(SourceAdapter):
    """Avisos de UNGM destinados a Panamá.

    Conserva el identificador histórico ``ungm`` para no alterar su línea
    base ni volver a notificar avisos ya conocidos.
    """

    source = "ungm"
    source_name = "UN Global Marketplace - Panamá"
    parser_version = "2.0.0"
    page_url = "https://www.ungm.org/Public/Notice"
    search_url = "https://www.ungm.org/Public/Notice/Search"
    panama_country_id = "2449"
    page_size = 15

    def _payload(
        self,
        page_index: int,
        *,
        countries: list[str] | None = None,
        agencies: list[str] | None = None,
    ) -> dict[str, object]:
        return {
            "PageIndex": page_index,
            "PageSize": self.page_size,
            "Title": "",
            "Description": "",
            "Reference": "",
            "PublishedFrom": "",
            "PublishedTo": "",
            "DeadlineFrom": "",
            "DeadlineTo": "",
            "Countries": countries or [],
            "Agencies": agencies or [],
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

    @staticmethod
    def _is_unicef(agency: str) -> bool:
        value = normalized_text(agency)
        return "unicef" in value or "united nations childrens fund" in value

    def _opportunity_from_row(
        self,
        row,
        *,
        scope: str,
        country_fallback: str,
        exclude_unicef: bool,
    ) -> Opportunity | None:
        notice_id = clean_text(row.get("data-noticeid"))
        values = [
            clean_text(cell.get_text(" ", strip=True))
            for cell in row.select(".tableCell")
        ]
        title_node = row.select_one(".resultTitle .ungm-title")
        title = clean_text(title_node.get_text(" ", strip=True) if title_node else "")
        link_node = row.select_one('.resultTitle a[href*="/Public/Notice/"]')
        link = (
            absolute_url(self.page_url, link_node.get("href"))
            if link_node
            else f"{self.page_url}/{notice_id}"
        )
        published = parse_date(values[3]) if len(values) > 3 else ""
        agency = values[4] if len(values) > 4 else ""
        method = values[5] if len(values) > 5 else ""
        reference = values[6] if len(values) > 6 else ""
        destination = values[7] if len(values) > 7 else ""
        if exclude_unicef and self._is_unicef(agency):
            return None
        deadline_node = row.select_one('[data-description="Deadline"] span')
        deadline_raw = (
            clean_text(deadline_node.get_text(" ", strip=True)).split("(")[0]
            if deadline_node
            else ""
        )
        if not notice_id and not reference:
            return None
        return Opportunity(
            source=self.source,
            external_id=notice_id or reference,
            title=title or reference,
            description=f"Referencia: {reference}. Destino: {destination}",
            source_url=link,
            source_type="Oportunidad internacional UNGM",
            buyer=agency,
            country=destination or country_fallback,
            publication_date=published,
            deadline=parse_date(deadline_raw),
            status="Activa",
            procurement_method=method,
            submission_channel="UNGM",
            eligibility="Ver requisitos de elegibilidad del aviso oficial",
            raw_payload={"cells": values, "notice_id": notice_id, "scope": scope},
            parser_version=self.parser_version,
        )

    def _fetch_profile(
        self,
        *,
        scope: str,
        countries: list[str] | None = None,
        agencies: list[str] | None = None,
        country_fallback: str = "Internacional",
        exclude_unicef: bool = True,
    ) -> list[Opportunity]:
        headers = {"X-Requested-With": "XMLHttpRequest", "Referer": self.page_url}
        max_pages = max(
            1,
            min(int(os.environ.get("OTRAS_FUENTES_UNGM_PAGES", "20")), 40),
        )
        opportunities: list[Opportunity] = []
        for page_index in range(max_pages):
            response = self.client.post(
                self.search_url,
                json=self._payload(
                    page_index,
                    countries=countries,
                    agencies=agencies,
                ),
                headers=headers,
            ).response
            rows = soup_from_html(response.text).select(".dataRow[data-noticeid]")
            if not rows:
                break
            for row in rows:
                opportunity = self._opportunity_from_row(
                    row,
                    scope=scope,
                    country_fallback=country_fallback,
                    exclude_unicef=exclude_unicef,
                )
                if opportunity is not None:
                    opportunities.append(opportunity)
            if len(rows) < self.page_size:
                break
        return opportunities

    def fetch_opportunities(self) -> list[Opportunity]:
        self.client.get(self.page_url)
        return self._fetch_profile(
            scope="panama",
            countries=[self.panama_country_id],
            country_fallback="Panamá",
        )


class UngmInternationalAdapter(UngmAdapter):
    """Cobertura UNGM regional y global, con una línea base independiente."""

    source = "ungm_international"
    source_name = "UNGM regional y global"

    @staticmethod
    def _regional_country_ids(page_html: str) -> list[str]:
        ids: set[str] = set(LAC_COUNTRY_ID_FALLBACK)
        for option in soup_from_html(page_html).select("option[value]"):
            value = clean_text(option.get("value"))
            aliases = clean_text(option.get("data-alternative-spellings"))
            iso_match = re.search(
                r"(?:^|[\s,;])([A-Z]{2})(?:$|[\s,;])", aliases.upper()
            )
            if value and iso_match and iso_match.group(1) in LAC_ISO_CODES:
                ids.add(value)
        ids.discard(UngmAdapter.panama_country_id)
        return sorted(ids)

    def fetch_opportunities(self) -> list[Opportunity]:
        page_response = self.client.get(self.page_url).response
        opportunities = self._fetch_profile(
            scope="latinoamerica_caribe",
            countries=self._regional_country_ids(page_response.text),
            country_fallback="América Latina y el Caribe",
        )
        opportunities.extend(
            self._fetch_profile(scope="global", country_fallback="Internacional")
        )
        # Panamá mantiene su fuente histórica propia. UNICEF se separa para
        # impedir alertas duplicadas entre perfiles.
        return [
            item
            for item in opportunities
            if normalized_text(item.country) not in {"panama", "republica de panama"}
        ]


class UnicefAdapter(UngmAdapter):
    """Convocatorias activas de UNICEF publicadas oficialmente en UNGM."""

    source = "unicef"
    source_name = "UNICEF"
    unicef_agency_id = "26"

    def fetch_opportunities(self) -> list[Opportunity]:
        self.client.get(self.page_url)
        opportunities = self._fetch_profile(
            scope="unicef_global",
            agencies=[self.unicef_agency_id],
            country_fallback="Internacional",
            exclude_unicef=False,
        )
        for item in opportunities:
            item.source_type = "Oportunidad UNICEF"
            item.submission_channel = "UNGM / UNICEF"
        return opportunities
