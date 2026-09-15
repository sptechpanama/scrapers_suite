from __future__ import annotations

from email.utils import parsedate_to_datetime
from xml.etree import ElementTree
import re
from urllib.parse import urljoin, urlsplit

from ..models import Opportunity, clean_text
from .base import SourceAdapter, slug_id
from .public_pages import official_date, html_page, next_pages


class EnsaAdapter(SourceAdapter):
    source = "ensa"
    source_name = "ENSA"
    parser_version = "2.0.0"
    url = "https://ensa.com.pa/licitaciones/rss"
    listing_url = "https://ensa.com.pa/licitaciones/"

    def _feed(self) -> list[Opportunity]:
        response = self.client.get(self.url, timeout=10).response
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

    def fetch_opportunities(self) -> list[Opportunity]:
        rows = {}
        try:
            feed = {item.external_id: item for item in self._feed()}
        except Exception:
            feed = {}
        pending = [self.listing_url]
        visited = set()
        page_ids = set()
        while pending and len(visited) < 100:
            url = pending.pop(0)
            if url in visited:
                continue
            visited.add(url)
            try:
                soup = html_page(self.client, url)
                self.pages_fetched += 1
                count = 0
                current_ids = set()
                for a in soup.select('a[href*="/licitaciones/"]'):
                    link = urljoin(url, a["href"])
                    path = urlsplit(link).path.rstrip("/")
                    title = clean_text(a.get_text(" ", strip=True))
                    if path in {"/licitaciones", "/licitaciones/rss"} or "/page/" in path or len(title) < 15 or title.lower() == "conoce más":
                        continue
                    container = a
                    for parent in a.parents:
                        text = clean_text(parent.get_text(" ", strip=True))
                        if "Fecha final para licitar:" in text:
                            container = parent
                            break
                    evidence = clean_text(container.get_text(" ", strip=True))
                    closing_match = re.search(r"Fecha final para licitar:\s*(\d{2}/\d{2}/\d{4}(?:\s+\d{1,2}:\d{2}\s*[ap]m)?)", evidence, re.I)
                    closing = official_date(closing_match[1], local_time=True) if closing_match else ""
                    code = re.search(r"\b[A-Z]{2,6}(?:-[A-Z]{1,4})*-\d{2,4}-20\d{2}\b", evidence)
                    external_id = slug_id(link)
                    previous = feed.get(external_id)
                    item = Opportunity(
                        source=self.source, external_id=external_id, title=title, source_url=link,
                        source_type="Licitación privada", buyer=self.source_name,
                        publication_date=previous.publication_date if previous else "", deadline=closing[:10],
                        status="Publicada", submission_channel="Portal ENSA",
                        raw_payload={"listing_url": url, "deadline_raw": closing,
                                     "official_number": code[0] if code else "", "listing_evidence": evidence},
                        parser_version=self.parser_version,
                    )
                    rows[external_id] = item
                    current_ids.add(external_id)
                    count += 1
                if not count:
                    raise RuntimeError("El listado ENSA no contiene convocatorias interpretables")
                if not current_ids - page_ids:
                    raise RuntimeError('ENSA repitió una página; queda cobertura por confirmar')
                page_ids.update(current_ids)
                pending.extend(u for u in next_pages(soup, url) if u not in visited and u not in pending)
            except Exception as exc:
                self.incomplete(f"ENSA: {type(exc).__name__}: {str(exc)[:180]}")
        if pending:
            self.incomplete("ENSA: quedan páginas pendientes de consultar")
        # RSS complements the complete listing; it cannot erase an official date.
        for key, item in feed.items():
            rows.setdefault(key, item)
        if not rows and self.coverage_notes:
            raise RuntimeError("; ".join(self.coverage_notes))
        return list(rows.values())
