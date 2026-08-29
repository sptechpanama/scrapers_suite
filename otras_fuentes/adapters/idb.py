from __future__ import annotations

import os
from datetime import date

from ..models import Opportunity, SourceDocument, clean_text
from .base import SourceAdapter, parse_date


class IdbAdapter(SourceAdapter):
    """Avisos de adquisiciones de proyectos financiados por el BID."""

    source = "idb"
    source_name = "Banco Interamericano de Desarrollo"
    parser_version = "1.0.0"
    api_url = "https://data.iadb.org/api/action/datastore_search"
    resource_id = "856aabfd-2c6a-48fb-a8b8-19f3ff443618"

    def fetch_opportunities(self) -> list[Opportunity]:
        limit = max(100, min(int(os.environ.get("OTRAS_FUENTES_IDB_LIMIT", "1000")), 3000))
        payload = self.client.get(
            self.api_url,
            params={
                "resource_id": self.resource_id,
                "limit": limit,
                "sort": "publicationdate desc",
            },
        ).response.json()
        if not payload.get("success"):
            raise RuntimeError(f"La API del BID no devolvio exito: {payload!r}")

        today = date.today().isoformat()
        include_awards = os.environ.get("OTRAS_FUENTES_IDB_INCLUDE_AWARDS", "").strip().lower() in {
            "1", "true", "yes", "si", "sí",
        }
        rows: list[Opportunity] = []
        for record in (payload.get("result") or {}).get("records", []):
            notice_type = clean_text(record.get("type")).upper()
            if "AWARD" in notice_type and not include_awards:
                continue
            deadline = parse_date(record.get("deadline"))
            if deadline and deadline[:10] < today:
                continue
            title = clean_text(record.get("noticetitle"))
            if not title:
                continue
            notice_id = clean_text(
                record.get("noticeid") or record.get("ezshareid") or record.get("_id")
            )
            document_url = clean_text(record.get("documenturl"))
            project_url = clean_text(record.get("proyecturl"))
            source_url = document_url or project_url or "https://www.iadb.org/en/project-procurement"
            project_name = clean_text(record.get("projectname"))
            project_number = clean_text(record.get("projectnumber"))
            description = ". ".join(
                value
                for value in (
                    f"Proyecto: {project_name}" if project_name else "",
                    f"Numero de proyecto: {project_number}" if project_number else "",
                    clean_text(record.get("process_desc")),
                )
                if value
            )
            documents = (
                [SourceDocument(title="Documento oficial BID", url=document_url)]
                if document_url
                else []
            )
            rows.append(
                Opportunity(
                    source=self.source,
                    external_id=notice_id,
                    title=title,
                    description=description,
                    source_url=source_url,
                    source_type=f"Convocatoria BID - {notice_type.title() or 'Aviso'}",
                    buyer=self.source_name,
                    country=clean_text(record.get("countryname")) or "America Latina y el Caribe",
                    publication_date=parse_date(record.get("publicationdate")),
                    deadline=deadline,
                    status="Activa" if deadline else "Publicada",
                    procurement_method=clean_text(
                        record.get("prcrmnt_mthd_engl_nm") or notice_type
                    ),
                    sector=clean_text(record.get("sectorenglnm") or record.get("sector")),
                    submission_channel="Portal de adquisiciones BID",
                    eligibility="Ver documentos oficiales y elegibilidad de paises miembros del BID",
                    documents=documents,
                    raw_payload=record,
                    parser_version=self.parser_version,
                )
            )
        return rows
