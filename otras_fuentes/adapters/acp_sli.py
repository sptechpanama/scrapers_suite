"""ACP SLI public search, with its real form and public pagination."""
from __future__ import annotations

import os
import re
import ssl
from urllib.parse import urljoin, parse_qs, urlsplit

from requests.adapters import HTTPAdapter

from ..models import Opportunity, clean_text
from .base import SourceAdapter, soup_from_html
from .public_pages import official_date


class NativeTLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        import truststore
        kwargs["ssl_context"] = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        return super().init_poolmanager(*args, **kwargs)


class AcpSliAdapter(SourceAdapter):
    source = "acp_sli"
    source_name = "ACP · Licitaciones SLI"
    parser_version = "2.0.0"
    url = "https://apps.pancanal.com/sli/"

    def __init__(self, client=None):
        super().__init__(client)
        if client is None:
            # Native trust validates ACP's certificate chain on the Windows server.
            # Certificate and hostname verification remain enabled.
            self.client.session.mount("https://apps.pancanal.com/", NativeTLSAdapter())

    @staticmethod
    def parse_listing(soup, status: str) -> list[Opportunity]:
        rows = []
        for link in soup.select('a[href*="RedirectLicitaciones"]'):
            url = urljoin(AcpSliAdapter.url, link["href"])
            params = parse_qs(urlsplit(url).query)
            code = clean_text((params.get("NumeroLicitacion") or [link.get_text()])[0])
            container = link
            for parent in link.parents:
                text = clean_text(parent.get_text(" ", strip=True))
                if "Fecha de publicación" in text and "Fecha y hora de cierre" in text:
                    container = parent
                    break
            text = clean_text(container.get_text(" ", strip=True))
            if "Fecha y hora de cierre" not in text:
                continue
            def between(start, end):
                match = re.search(re.escape(start) + r"\s*(.*?)\s*" + re.escape(end), text, re.I)
                return match[1].strip() if match else ""
            title = between(code, "Agente de compras")
            publication = official_date(between("Fecha de publicación", "Última revisión"))
            revision = between("Última revisión", "Fecha y hora de cierre")
            closing = official_date(between("Fecha y hora de cierre", "Unidad de compras"), local_time=True)
            buyer_unit = between("Unidad de compras", "# Enmienda")
            rows.append(Opportunity(
                source="acp_sli", external_id=code, title=title or code, source_url=url,
                description=title, buyer="Autoridad del Canal de Panamá", source_type="Licitación ACP",
                publication_date=publication, deadline=closing[:10], status="Enmendada" if status == "EN" else "Abierta",
                submission_channel="Sistema de Licitaciones por Internet (SLI)", registration_required="Registro de proveedor en SLI para presentar oferta",
                raw_payload={"listing_url": AcpSliAdapter.url, "deadline_raw": closing,
                             "official_updated_at": revision, "purchase_unit": buyer_unit,
                             "purchase_contact": between("Agente de compras", "Fecha de publicación"),
                             "listing_evidence": text, "header_id": (params.get("HeaderId") or [""])[0]},
                parser_version="2.0.0",
            ))
        return rows

    def fetch_opportunities(self):
        all_rows = []
        def page_key(url):
            parsed = urlsplit(url)
            return parsed.path.rstrip('/').lower(), (parse_qs(parsed.query).get('pagina') or ['1'])[0]
        max_pages = max(1, min(int(os.getenv("OTRAS_FUENTES_ACP_MAX_PAGES", "100")), 500))
        for status in ("AN", "EN"):
            try:
                soup = soup_from_html(self.client.get(self.url).response.text)
                form = soup.select_one("#frmBusqueda3")
                if form is None:
                    raise RuntimeError("No se encontró el formulario público SLI")
                data = {t["name"]: t.get("value", "") for t in form.select("input[name]")}
                data.update(EstatusSeleccionadoID=status, CategoriaSeleccionadaID="TODOS", status=status, categorias="TODOS")
                for token in soup.select("input[name=__RequestVerificationToken]"):
                    data[token["name"]] = token.get("value", "")
                response = self.client.post(urljoin(self.url, form["action"]), data=data, headers={"Referer": self.url}).response
                pending = [(getattr(response, "url", self.url), response.text)]
                visited = set()
                state_ids = set()
                while pending and len(visited) < max_pages:
                    page_url, content = pending.pop(0)
                    if page_key(page_url) in visited:
                        continue
                    visited.add(page_key(page_url))
                    if content is None:
                        content = self.client.get(page_url).response.text
                    page = soup_from_html(content)
                    rows = self.parse_listing(page, status)
                    page_text = clean_text(page.get_text(" ", strip=True))
                    if not rows and not re.search(r"no (?:se )?(?:encontraron|existe(?:n)?|hay)|0\s+(?:resultados|licitaciones)", page_text, re.I):
                        raise RuntimeError("SLI no devolvió filas ni una confirmación de listado vacío")
                    self.pages_fetched += 1
                    if rows and not {row.external_id for row in rows} - state_ids:
                        raise RuntimeError('SLI repitió una página; se conservan los actos leídos y queda cobertura pendiente')
                    for row in rows:
                        if row.external_id not in state_ids:
                            all_rows.append(row)
                            state_ids.add(row.external_id)
                    for a in page.select('a[href*="BusquedaLicitacionesResultados?pagina="]'):
                        target = urljoin(self.url, a["href"])
                        if page_key(target) not in visited and all(page_key(u) != page_key(target) for u, _ in pending):
                            pending.append((target, None))
                if pending:
                    self.incomplete(f"SLI {status}: límite de {max_pages} páginas; quedan páginas pendientes")
            except Exception as exc:
                self.incomplete(f"SLI {status}: {type(exc).__name__}: {str(exc)[:220]}")
        if not all_rows and self.coverage_notes:
            raise RuntimeError("; ".join(self.coverage_notes))
        return all_rows
