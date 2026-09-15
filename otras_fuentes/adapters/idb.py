from __future__ import annotations

import os
import csv
import io
import logging
from datetime import date, datetime, timedelta
import re
from urllib.parse import urlsplit

from ..models import Opportunity, SourceDocument, clean_text
from .base import SourceAdapter, parse_date


class IdbAdapter(SourceAdapter):
    """Avisos de adquisiciones de proyectos financiados por el BID."""

    source = "idb"
    source_name = "Banco Interamericano de Desarrollo"
    parser_version = "1.0.0"
    api_url = "https://data.iadb.org/api/action/datastore_search"
    resource_id = "856aabfd-2c6a-48fb-a8b8-19f3ff443618"

    @staticmethod
    def _official_date(value, *, csv_format=False):
        raw = clean_text(value)
        if not raw or raw.upper() in {'NULL', 'N/A', 'NONE'}:
            return ''
        try:
            if re.match(r'^\d{4}-\d{2}-\d{2}', raw):
                return date.fromisoformat(raw[:10]).isoformat()
            if csv_format and re.fullmatch(r'\d{1,2}/\d{1,2}/\d{4}', raw):
                # The official BID CSV uses US month/day/year, unlike PC.
                return datetime.strptime(raw, '%m/%d/%Y').date().isoformat()
            parsed = parse_date(raw)
            return date.fromisoformat(parsed).isoformat()
        except ValueError:
            return ''

    def _records(self, limit):
        """The CKAN datastore can disappear while the official CSV still exists."""
        rows = []
        try:
            for offset in range(0, 200000, limit):
                payload = self.client.get(self.api_url, params={'resource_id': self.resource_id,
                    'limit': limit, 'offset': offset, 'sort': 'publicationdate desc'}).response.json()
                if not payload.get('success'):
                    raise RuntimeError('API BID sin respuesta válida')
                result = payload.get('result') or {}
                batch = result.get('records', [])
                fields = {f.get('id') for f in result.get('fields', [])}
                if not batch and fields and 'noticetitle' not in fields:
                    raise RuntimeError('El recurso BID perdió sus columnas de avisos; no confirma cero oportunidades')
                if batch and not any('noticetitle' in r for r in batch):
                    raise RuntimeError('El recurso BID cambió su estructura de avisos')
                rows.extend(batch)
                self.pages_fetched += 1
                if len(batch) < limit or (result.get('total') is not None and len(rows) >= result['total']):
                    return rows
            self.incomplete('BID: se alcanzó el límite de lectura del historial')
            return rows
        except Exception as exc:
            logging.getLogger('otras_fuentes').warning('BID: API no disponible; comprobando CSV oficial: %s', type(exc).__name__)
            api_error = str(exc)[:180]
        try:
            metadata = self.client.get('https://data.iadb.org/api/action/package_show', params={
                'id': 'project-procurement-bidding-notices-and-notification-of-contract-awards'}).response.json()
            resources = (metadata.get('result') or {}).get('resources', [])
            resource = next(r for r in resources if r.get('id') == self.resource_id and r.get('format', '').upper() == 'CSV')
            url = resource.get('url', '')
            if urlsplit(url).scheme != 'https' or urlsplit(url).hostname != 'data.iadb.org':
                raise RuntimeError('Descarga BID no corresponde al dominio oficial esperado')
            response = self.client.get(url, stream=True).response
            try:
                if response.status_code != 200:
                    raise RuntimeError(f'Descarga oficial BID respondió HTTP {response.status_code}; sin archivo disponible')
                data = bytearray()
                for part in response.iter_content(65536):
                    data.extend(part)
                    if len(data) > 50 * 1024 * 1024:
                        raise RuntimeError('CSV BID excede 50 MB; requiere revisión')
            finally:
                response.close()
            reader = csv.DictReader(io.StringIO(data.decode('utf-8-sig')))
            if not {'noticetitle', 'noticeid', 'publicationdate'}.issubset(reader.fieldnames or []):
                raise RuntimeError('La descarga BID no entregó el CSV de avisos esperado')
            self.pages_fetched += 1
            records = list(reader)
            latest = max((self._official_date(r.get('publicationdate')) for r in records), default='')
            if not latest or latest < (date.today() - timedelta(days=90)).isoformat():
                raise RuntimeError(f'CSV oficial desactualizado (última publicación: {latest or "sin fecha"}); no confirma oportunidades actuales')
            for record in records:
                record['deadline'] = self._official_date(record.get('deadline'), csv_format=True)
            return records
        except Exception as exc:
            if rows:
                self.incomplete(f'BID: se conservan {len(rows)} registros; API y CSV pendientes: {type(exc).__name__}')
                return rows
            raise RuntimeError(f'BID: API de avisos no disponible ({api_error}); respaldo CSV: {str(exc)[:200]}') from exc

    def fetch_opportunities(self) -> list[Opportunity]:
        limit = max(100, min(int(os.environ.get("OTRAS_FUENTES_IDB_LIMIT", "1000")), 3000))
        records = self._records(limit)

        today = date.today().isoformat()
        include_awards = os.environ.get("OTRAS_FUENTES_IDB_INCLUDE_AWARDS", "").strip().lower() in {
            "1", "true", "yes", "si", "sí",
        }
        rows: list[Opportunity] = []
        for record in records:
            notice_type = clean_text(record.get("type")).upper()
            if "AWARD" in notice_type and not include_awards:
                continue
            deadline = self._official_date(record.get("deadline"))
            publication = self._official_date(record.get('publicationdate'))
            if deadline and deadline[:10] < today:
                continue
            if not deadline and publication and publication < (date.today() - timedelta(days=90)).isoformat():
                # A general notice from years ago without a deadline is not a
                # new actionable tender. Existing history is never deleted.
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
                    publication_date=publication,
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
