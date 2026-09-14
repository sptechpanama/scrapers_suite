from __future__ import annotations

import os

from bs4 import BeautifulSoup

from ..models import Opportunity, clean_text
from .base import SourceAdapter, absolute_url, parse_date


class CiudadSaberAdapter(SourceAdapter):
    source = "ciudad_saber"
    source_name = "Ciudad del Saber"
    parser_version = "1.0.0"
    api_url = "https://backend.ciudaddelsaber.org/api/convene-opportunities"
    public_base = "https://ciudaddelsaber.org/es/oportunidades/convocatorias/"

    def fetch_opportunities(self) -> list[Opportunity]:
        page_size = max(9, min(int(os.environ.get("OTRAS_FUENTES_CDS_PAGE_SIZE", "100")), 100))
        params = {
            "pagination[page]": 1,
            "pagination[pageSize]": page_size,
            "locale": "es",
            "populate": "*",
            "sort[publishedAt]": "desc",
        }
        rows: list[Opportunity] = []
        items = []
        page_signatures = set()
        while params["pagination[page]"] <= 100:
            try:
                payload = self.client.get(self.api_url, params=params).response.json()
                if not isinstance(payload.get("data"), list):
                    raise RuntimeError("Respuesta de Ciudad del Saber sin listado data")
                batch = payload["data"]
                signature = tuple(str(row.get('id')) for row in batch)
                if batch and signature in page_signatures:
                    raise RuntimeError('El portal repitió una página; faltan resultados por verificar')
                page_signatures.add(signature)
                items.extend(batch)
                self.pages_fetched += 1
                page_count = (payload.get("meta", {}).get("pagination") or {}).get("pageCount")
                if not batch or (page_count and params["pagination[page]"] >= int(page_count)) or (not page_count and len(batch) < page_size):
                    break
                params["pagination[page]"] += 1
            except Exception as exc:
                if not items:
                    raise
                self.incomplete(f"Ciudad del Saber, página {params['pagination[page]']}: {type(exc).__name__}")
                break
        else:
            self.incomplete("Ciudad del Saber: límite de páginas alcanzado")
        for item in items:
            attrs = item.get("attributes") or item
            general = attrs.get("generalInfo") or {}
            organization = attrs.get("organization") or {}
            org_attrs = (organization.get("data") or {}).get("attributes", {}) if isinstance(organization, dict) else {}
            slug = clean_text(attrs.get("slug"))
            title = clean_text(attrs.get("title"))
            if not title:
                continue
            link = absolute_url(self.public_base, slug)
            deadline = parse_date(general.get("deadline") or attrs.get("deadline"))
            published = parse_date(attrs.get("publishedAt") or attrs.get("createdAt"))
            description_html = clean_text(
                attrs.get("description")
                or attrs.get("requirements")
                or general.get("opportunityDescription")
                or general.get("description")
            )
            description = clean_text(BeautifulSoup(description_html, "html.parser").get_text(" "))
            buyer = clean_text(
                org_attrs.get("directoryName")
                or attrs.get("organizationName")
                or self.source_name
            )
            rows.append(
                Opportunity(
                    source=self.source,
                    external_id=str(item.get("id") or slug),
                    title=title,
                    description=description,
                    source_url=link,
                    source_type="Convocatoria comunitaria",
                    buyer=buyer,
                    publication_date=published,
                    deadline=deadline,
                    status="Activa" if deadline else "Publicada",
                    submission_channel="Ciudad del Saber",
                    raw_payload=item,
                    parser_version=self.parser_version,
                )
            )
        return rows
