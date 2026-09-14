"""Bounded public-document reads in the server, never in Streamlit reruns."""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit, urljoin

from bs4 import BeautifulSoup
from .http import ResilientHttpClient
from .models import clean_text, normalized_text
from .qualification import deadline_info

MAX_BYTES = 10 * 1024 * 1024


def extract_deadline(text: str) -> str:
    """Read dates only near an explicit submission label, never arbitrary dates."""
    norm = normalized_text(text)
    date_pattern = (r'(?:20\d{2} \d{1,2} \d{1,2}|\d{1,2} \d{1,2} 20\d{2}|'
                    r'\d{1,2} (?:de )?[a-z]+ (?:de )?20\d{2}|[a-z]+ \d{1,2} 20\d{2})')
    found = []
    for match in re.finditer(r'(?:fecha final para licitar|fecha de cierre|fecha limite|'
                             r'fecha de entrega de sus respuestas|deadline on|submission deadline|'
                             r'fecha (?:y hora )?de (?:recepcion|presentacion) de (?:propuestas|ofertas))', norm):
        nearby = norm[match.end():match.end() + 180]
        candidate = re.search(date_pattern, nearby)
        if candidate:
            value = candidate[0]
            if re.fullmatch(r'\d{1,2} \d{1,2} 20\d{2}', value): value = value.replace(' ', '/')
            if re.fullmatch(r'20\d{2} \d{1,2} \d{1,2}', value): value = value.replace(' ', '-')
            day, _ = deadline_info(value)
            if day: found.append(day)
    # Conflicting dates demand review; never guess that the latest is an extension.
    unique = set(found)
    return unique.pop() if len(unique) == 1 else ''


def read_document(url: str, *, context_url: str = '') -> dict:
    if urlsplit(url).scheme not in {'http', 'https'}:
        return {'status': 'error', 'text': '', 'error': 'Enlace no HTTP'}
    client = ResilientHttpClient(timeout=12, retries=1)
    if urlsplit(url).netloc == 'apps.pancanal.com':
        from .adapters.acp_sli import NativeTLSAdapter
        client.session.mount('https://apps.pancanal.com/', NativeTLSAdapter())
    headers = {}
    if urlsplit(url).netloc == 'apps.pancanal.com':
        # The public SLI document controls send this referer. Without it ACP
        # returns an error image with HTTP 200 instead of the PDF.
        headers['Referer'] = 'https://apps.pancanal.com/sli/Licitaciones/LicitacionHeader'
        if context_url and urlsplit(context_url).netloc == 'apps.pancanal.com':
            # Public document controllers also use the tender selected in the
            # anonymous SLI session (no login or provider credentials involved).
            client.get(context_url, headers=headers)
    response = client.get(url, stream=True, headers=headers).response
    try:
        data = bytearray()
        for part in response.iter_content(64 * 1024):
            data.extend(part)
            if len(data) > MAX_BYTES: raise ValueError('Documento excede 10 MB; requiere revisión')
        is_pdf = bytes(data[:5]) == b'%PDF-'
        links = []
        if is_pdf:
            import fitz
            with fitz.open(stream=bytes(data), filetype='pdf') as pdf:
                text = '\n'.join(pdf[i].get_text() for i in range(min(len(pdf), 150)))
                partial = len(pdf) > 150
        else:
            if 'html' not in response.headers.get('Content-Type', '').lower():
                return {'status': 'unreadable', 'text': '', 'error': 'Formato pendiente de interpretación'}
            soup = BeautifulSoup(bytes(data), 'html.parser')
            if 'RedirectLicitaciones' in url:
                from .acp_documents import acp_detail
                text, links = acp_detail(soup, url)
                return {'status': 'ok', 'text': clean_text(text)[:80000], 'links': links, 'deadline': ''}
            for node in soup.select('script, style, nav, footer, header, .related-posts, .unspscCodes'):
                node.decompose()
            # UNGM's code taxonomy is not part of the commercial description.
            if 'ungm.org' in urlsplit(url).netloc:
                text = soup.get_text(' ', strip=True)
                start = text.find('Description')
                end = text.find('Contacts', start)
                text = text[start:end] if start >= 0 and end > start else text[:12000]
                if 'UNSPSC codes' in text: text = text.split('UNSPSC codes')[0]
            else:
                body = soup.select_one('article .entry-content, article, main, .entry-content') or soup
                text = body.get_text(' ', strip=True)
                from .adapters.public_pages import document_links
                links = [{'title': d.title, 'url': d.url} for d in document_links(body, url)]
            partial = False
        text = clean_text(text)
        if 'ensa.com.pa' in urlsplit(url).netloc and 'Agende su cita' in text:
            text = text.split('Agende su cita')[0].strip()
        status = 'partial' if partial or len(text) > 80000 else ('ok' if len(text) >= 60 else 'unreadable')
        heading = re.search(r'Relacionado a:\s*[“"\']([^”"\']{12,350})', text, re.I)
        return {'status': status, 'text': text[:80000], 'deadline': extract_deadline(text), 'links': links,
                'title': clean_text(heading[1]) if heading else '',
                'error': ('PDF sin texto suficiente; requiere revisión visual u OCR' if is_pdf else 'Página sin texto interpretable') if status == 'unreadable' else ''}
    finally:
        response.close()
        client.session.close()


class DetailEnricher:
    def __init__(self, path: Path | str | None = None, *, fetcher=read_document, budget: int = 90):
        path = Path(path or os.getenv('OTRAS_FUENTES_DETAILS_PATH') or Path(__file__).resolve().parents[1] / 'data/otras_fuentes/details.db')
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=30)
        self.connection.execute('CREATE TABLE IF NOT EXISTS detail_cache (url TEXT PRIMARY KEY, payload TEXT NOT NULL, checked REAL NOT NULL, attempted REAL NOT NULL)')
        self.fetcher, self.budget = fetcher, budget
        self.contexts = {}

    def close(self):
        self.connection.close()

    def _read_cached(self, urls, limit=60):
        urls = list(dict.fromkeys(urls))
        cached = {}
        due = []
        now = time.time()
        due_candidates = []
        for url in urls:
            row = self.connection.execute('SELECT payload,checked,attempted FROM detail_cache WHERE url=?', (url,)).fetchone()
            if row:
                cached[url] = json.loads(row[0])
                ttl = 12 * 3600 if cached[url].get('status') == 'ok' else 3600
                needs_context_retry = (urlsplit(url).netloc == 'apps.pancanal.com' and url in self.contexts
                                       and cached[url].get('status') == 'unreadable' and cached[url].get('reader_version', 0) < 2)
                if now - row[2] < ttl and not needs_context_retry: continue
            due_candidates.append((row[2] if row else 0, url))
        # Never-read documents precede expired cache entries, avoiding starvation.
        due = [url for _, url in sorted(due_candidates)[:min(self.budget, limit)]]
        self.budget -= len(due)
        def read(url):
            try:
                result = (self.fetcher(url, context_url=self.contexts.get(url, '')) if self.fetcher is read_document else self.fetcher(url))
                return url, result
            except Exception as exc: return url, {'status': 'error', 'error': type(exc).__name__, 'text': ''}
        with ThreadPoolExecutor(max_workers=3) as pool:
            for url, result in pool.map(read, due):
                previous = cached.get(url, {})
                if result.get('status') in {'error', 'unreadable'} and previous.get('text'):
                    result = {**previous, 'status': 'partial', 'error': result.get('error', 'Lectura incompleta'), 'using_previous': True}
                result['checked_at'] = now
                result['reader_version'] = 2
                cached[url] = result
                self.connection.execute('INSERT INTO detail_cache VALUES (?,?,?,?) ON CONFLICT(url) DO UPDATE SET payload=excluded.payload,checked=excluded.checked,attempted=excluded.attempted',
                                        (url, json.dumps(result, ensure_ascii=False), now, now))
        self.connection.commit()
        return cached

    def enrich(self, opportunities):
        selected = [item for item in opportunities if item.source in {'ena', 'ensa', 'acp', 'acp_sli', 'cruz_roja'}
                    or (item.raw_payload.get('qualification') or {}).get('bucket') in {'relevant', 'review'}]
        cached = self._read_cached(item.source_url for item in selected)
        now = time.time()
        for item in selected:
            previous = item.raw_payload.get('document_analysis') or {}
            result = dict(cached.get(item.source_url) or previous or {'status': 'pending', 'text': ''})
            if item.source == 'ensa' and result.get('text'):
                result['text'] = result['text'].split('Agende su cita')[0].strip()
            if result.get('text'):
                result['deadline'] = extract_deadline(result['text'])
            if result.get('checked_at') and now - result['checked_at'] > 24 * 3600:
                result['status'] = 'partial'
            item.raw_payload['document_analysis'] = result
            from .models import SourceDocument
            existing = {d.url for d in item.documents}
            for link in result.get('links', []):
                if link['url'] not in existing:
                    item.documents.append(SourceDocument(**link)); existing.add(link['url'])
        # All public attachments remain linked. Reads are budgeted and resume
        # from persistent cache, with never-read URLs first on subsequent runs.
        candidates = [item for item in selected if (item.raw_payload.get('qualification') or {}).get('bucket') != 'historical']
        self.contexts.update({d.url: item.source_url for item in candidates for d in item.documents if d.url != item.source_url})
        attachment_cache = self._read_cached(d.url for item in candidates for d in item.documents if d.url != item.source_url)
        for item in candidates:
            result = item.raw_payload['document_analysis']
            parts = [result.get('text', '')]
            docs = [d for d in item.documents if d.url != item.source_url]
            evidence = []
            for doc in docs:
                detail = attachment_cache.get(doc.url, {})
                status = detail.get('status', 'pending')
                evidence.append({'url': doc.url, 'status': status})
                if detail.get('text'):
                    parts.append(doc.title + ': ' + detail['text'])
            result['attachments_total'] = len(docs)
            result['attachments_read'] = sum(e['status'] == 'ok' for e in evidence)
            result['attachment_evidence'] = evidence
            combined = clean_text(' '.join(parts))
            result['text'] = combined[:160000]
            if len(combined) > 160000:
                result['status'] = 'partial'
            # Dates extracted from annexes can conflict; never overwrite the
            # structured closing date from the official tender listing.
            result['deadline'] = extract_deadline(combined)
        return opportunities
