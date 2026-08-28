from __future__ import annotations

from email.utils import parsedate_to_datetime
from xml.etree import ElementTree

from ..models import Opportunity, clean_text
from .base import SourceAdapter, slug_id


class EnsaAdapter(SourceAdapter):
    source = "ensa"
    source_name = "ENSA"
    parser_version = "1.0.0"
    url = "https://ensa.com.pa/licitaciones/rss"

    def fetch_opportunities(self) -> list[Opportunity]:
        response = self.client.get(self.url).response
        rows: list[Opportunity] = []
        root = ElementTree.fromstring(response.text)
        for item in root.findall(".//item"):
            title = clean_text(item.findtext("title"))
            link = clean_text(item.findtext("link"))
            if not title or not link:
                continue
            published = ""
            pub_date = clean_text(item.findtext("pubDate"))
            if pub_date:
                try:
                    published = parsedate_to_datetime(pub_date).date().isoformat()
                except (TypeError, ValueError, OverflowError):
                    published = pub_date
            description = clean_text(item.findtext("description"))
            rows.append(
                Opportunity(
                    source=self.source,
                    external_id=slug_id(link),
                    title=title,
                    description=description,
                    source_url=link,
                    source_type="Licitación privada",
                    buyer=self.source_name,
                    publication_date=published,
                    status="Publicada",
                    submission_channel="Portal ENSA",
                    raw_payload={"feed": self.url},
                    parser_version=self.parser_version,
                )
            )
        return rows
