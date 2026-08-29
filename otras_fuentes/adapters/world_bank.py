from __future__ import annotations

import os
from datetime import date

from ..models import Opportunity, clean_text
from .base import SourceAdapter, parse_date


class WorldBankAdapter(SourceAdapter):
    """Oportunidades vigentes de proyectos financiados por el Banco Mundial."""

    source = "world_bank"
    source_name = "Banco Mundial"
    parser_version = "1.0.0"
    api_url = "https://search.worldbank.org/api/v2/procnotices"
    detail_base = "https://projects.worldbank.org/en/projects-operations/procurement-detail/"

    fields = (
        "id,notice_type,noticedate,notice_status,submission_deadline_date,"
        "project_ctry_name,project_id,project_name,bid_reference_no,bid_description,"
        "procurement_group,procurement_method_name,contact_organization,contact_email,"
        "contact_name,contact_phone_no"
    )

    def fetch_opportunities(self) -> list[Opportunity]:
        rows_limit = max(
            100, min(int(os.environ.get("OTRAS_FUENTES_WORLD_BANK_ROWS", "1000")), 3000)
        )
        today = date.today().isoformat()
        payload = self.client.get(
            self.api_url,
            params={
                "format": "json",
                "rows": rows_limit,
                "os": 0,
                "apilang": "en",
                "srce": "both",
                "srt": "submission_deadline_date",
                "order": "asc",
                "deadline_strdate": today,
                "notice_type_exact": (
                    "Invitation for Bids^Invitation for Prequalification^"
                    "Request for Expression of Interest"
                ),
                "fl": self.fields,
            },
        ).response.json()

        rows: list[Opportunity] = []
        for record in payload.get("procnotices", []):
            notice_id = clean_text(record.get("id"))
            title = clean_text(record.get("bid_description"))
            if not notice_id or not title:
                continue
            deadline = parse_date(record.get("submission_deadline_date"))
            if deadline and deadline[:10] < today:
                continue
            project_name = clean_text(record.get("project_name"))
            reference = clean_text(record.get("bid_reference_no"))
            description = ". ".join(
                value
                for value in (
                    f"Proyecto: {project_name}" if project_name else "",
                    f"Referencia: {reference}" if reference else "",
                    f"Grupo: {clean_text(record.get('procurement_group'))}"
                    if clean_text(record.get("procurement_group"))
                    else "",
                )
                if value
            )
            contact = clean_text(record.get("contact_organization"))
            rows.append(
                Opportunity(
                    source=self.source,
                    external_id=notice_id,
                    title=title,
                    description=description,
                    source_url=self.detail_base + notice_id,
                    source_type=clean_text(record.get("notice_type")) or "Convocatoria internacional",
                    buyer=contact or self.source_name,
                    country=clean_text(record.get("project_ctry_name")) or "Internacional",
                    publication_date=parse_date(record.get("noticedate")),
                    deadline=deadline,
                    status=clean_text(record.get("notice_status")) or "Publicada",
                    procurement_method=clean_text(record.get("procurement_method_name")),
                    submission_channel="Portal de adquisiciones del Banco Mundial",
                    eligibility="Ver documento oficial de la convocatoria",
                    raw_payload=record,
                    parser_version=self.parser_version,
                )
            )
        return rows
