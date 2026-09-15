"""Public BID for the Americas catalog, linked by IDB's procurement page.

Uses the same anonymous read endpoint as its public website; no auth, private
supplier profiles or account endpoints. Official tender PDFs remain evidence.
"""
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

from ..models import Opportunity, SourceDocument, clean_text
from .base import SourceAdapter


def translation(rows):
    rows = [r for r in (rows or []) if isinstance(r, dict)]
    return next((r for r in rows if r.get('languages_code') == 'es'), rows[0] if rows else {})


class IdbCurrentAdapter(SourceAdapter):
    source = 'idb'
    source_name = 'BID · BID for the Americas'
    parser_version = '2.0.0'
    url = 'https://bidfa-admin.connectamericas.com/items/tenders'
    fields = ('id,status,date_created,date_updated,date_close,amount,undb_id,project_url,'
              'tender_url,project_number,files_urls,translations.name,translations.contract_object,'
              'translations.executing_unit,translations.languages_code,country.code,country.translations.*')

    def fetch_opportunities(self):
        rows, seen, expected = [], set(), None
        for offset in range(0, 10000, 100):
            try:
                payload = self.client.get(self.url, params={
                    'fields': self.fields, 'filter[status][_eq]': 'approved',
                    'sort': '-date_created,id', 'limit': 100, 'offset': offset, 'meta': 'filter_count',
                }).response.json()
                batch = payload.get('data')
                total = (payload.get('meta') or {}).get('filter_count')
                if not isinstance(batch, list) or not isinstance(total, int) or isinstance(total, bool):
                    raise ValueError('BID for the Americas no confirma registros ni total oficial')
                self.pages_fetched += 1
                if expected is None:
                    expected = total
                elif total != expected:
                    self.incomplete('El total oficial cambió durante la lectura; reconfirmar en la siguiente corrida')
                ids = {str(r.get('id')) for r in batch if r.get('id') is not None}
                if batch and not ids - seen:
                    raise ValueError('BID for the Americas repitió una página')
                for record in batch:
                    identifier = str(record.get('id') or '')
                    if not identifier or identifier in seen:
                        continue
                    item = self.map_record(record)
                    seen.add(identifier)
                    rows.append(item)
                if len(seen) >= total:
                    return rows
                if not batch:
                    raise ValueError(f'Lectura parcial: {len(seen)} de {total} avisos oficiales')
            except Exception as exc:
                if not rows:
                    raise
                self.incomplete(f'BID for the Americas: {len(rows)} registros conservados; {type(exc).__name__}: {str(exc)[:180]}')
                return rows
        self.incomplete('BID for the Americas: límite de 10,000 registros; captura parcial')
        return rows

    def map_record(self, record):
        name = translation(record.get('translations'))
        title = clean_text(name.get('name'))
        url = clean_text(record.get('tender_url') or record.get('project_url'))
        if not title or urlsplit(url).scheme not in {'https', 'http'}:
            raise ValueError('Aviso sin título o enlace oficial interpretable')
        country = translation((record.get('country') or {}).get('translations')).get('name', '')
        closing = clean_text(record.get('date_close'))
        if closing:
            try:
                close = datetime.fromisoformat(closing.replace('Z', '+00:00'))
                if close.tzinfo is None:
                    raise ValueError('zona horaria ausente')
                closing = close.astimezone(timezone(timedelta(hours=-5))).isoformat()
            except ValueError:
                raise ValueError('Fecha de cierre BID no interpretable')
        docs = {}
        urls = [record.get('tender_url'), *[x.get('URL') for x in record.get('files_urls') or [] if isinstance(x, dict)]]
        for link in urls:
            if link and urlsplit(link).scheme in {'https', 'http'}:
                docs[link] = SourceDocument('Documento oficial BID', link)
        raw = {k: record.get(k) for k in ('id', 'status', 'date_created', 'date_updated', 'date_close',
                                         'amount', 'undb_id', 'project_number', 'project_url')}
        raw.update({'listing_url': self.url, 'catalog': 'BID for the Americas',
                    'official_number': record.get('undb_id') or f"BIDFA-{record['id']}",
                    'official_updated_at': record.get('date_updated') or '', 'deadline_raw': closing,
                    'publication_date_basis': 'Fecha de creación del aviso en el catálogo oficial BID for the Americas'})
        # The public amount has no currency field; keep its original value in
        # evidence rather than assuming every figure is a USD tender budget.
        return Opportunity(source='idb', external_id=f"bidfa:{record['id']}", title=title,
            description=clean_text(name.get('contract_object')), source_url=url,
            source_type='Convocatoria BID · BID for the Americas', buyer=clean_text(name.get('executing_unit')) or 'BID',
            country=clean_text(country), publication_date=clean_text(record.get('date_created'))[:10],
            deadline=closing[:10], status='Publicada' if record.get('status') == 'approved' else clean_text(record.get('status')),
            registration_required='Según las bases del organismo ejecutor',
            submission_channel='Según el documento oficial; verificar fecha y requisitos en las bases',
            documents=list(docs.values()), raw_payload=raw, parser_version=self.parser_version)
