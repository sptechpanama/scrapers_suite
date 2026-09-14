from __future__ import annotations

import re
from ..models import Opportunity, clean_text, stable_hash
from .base import SourceAdapter
from .public_pages import html_page, document_links, official_date


class IfrcAdapter(SourceAdapter):
    source = "ifrc"
    source_name = "IFRC · Compras humanitarias"
    parser_version = "2.0.0"
    url = "https://www.ifrc.org/es/nuestra-promesa/servicios-humanitarios-globales/oportunidades-negocio"

    def fetch_opportunities(self):
        soup = html_page(self.client, self.url)
        body = soup.select_one("main, article") or soup
        rows = []
        for heading in body.find_all(["h3", "h4"]):
            title = clean_text(heading.get_text(" ", strip=True))
            if not re.search(r"tender|licitaci[oó]n|RFQ|RFP", title, re.I):
                continue
            block = heading.find_parent(class_=re.compile("accordion-item|paragraph--type--accordion-item"))
            if block is None:
                block = heading.parent
            text = clean_text(block.get_text(" ", strip=True))
            reference = re.search(r"\b(?:RFQ|RFP|ITB)[-\s][A-Z0-9-]+", text)
            code = reference[0] if reference else stable_hash(re.sub(r"\s*[-–]\s*Deadline.*", "", title, flags=re.I))
            closing = re.search(r"Deadline\s+(\d{1,2}\s+[A-Za-z]+\s+20\d{2})", title, re.I)
            rows.append(Opportunity(
                source=self.source, external_id=code, title=title, description=text, source_url=self.url,
                buyer="IFRC", source_type="Licitación humanitaria", country="Internacional",
                deadline=official_date(closing[1]) if closing else "", status="Publicada",
                registration_required="Registro IFRC; revisar formulario de proveedor médico cuando corresponda",
                submission_channel="Según convocatoria oficial IFRC", documents=document_links(block, self.url),
                raw_payload={"listing_url": self.url, "document_analysis": {"status": "ok", "text": text}},
                parser_version=self.parser_version,
            ))
        if not rows and not re.search(r"no (?:current|open) tenders|no hay licitaciones", body.get_text(" ", strip=True), re.I):
            raise RuntimeError("IFRC no entregó convocatorias interpretables ni confirmó un listado vacío")
        return rows
