"""Bounded public-document reads in the server, never in Streamlit reruns."""
from __future__ import annotations

import json
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


def read_document(url: str) -> dict:
    if urlsplit(url).scheme not in {'http', 'https'}:
        return {'status': 'error', 'text': '', 'error': 'Enlace no HTTP'}
    client = ResilientHttpClient(timeout=12, retries=1)
    response = client.get(url, stream=True).response
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
                text = '\n'.join(pdf[i].get_text() for i in range(min(len(pdf), 25)))
                partial = len(pdf) > 25
        else:
            if 'html' not in response.headers.get('Content-Type', '').lower():
                return {'status': 'unreadable', 'text': '', 'error': 'Formato pendiente de interpretación'}
            soup = BeautifulSoup(bytes(data), 'html.parser')
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
                links = [{'title': a.get_text(' ', strip=True) or 'Documento oficial', 'url': urljoin(url, a['href'])}
                         for a in body.select('a[href]') if urlsplit(a['href']).path.lower().endswith('.pdf')][:2]
            partial = False
        text = clean_text(text)
        if 'ensa.com.pa' in urlsplit(url).netloc and 'Agende su cita' in text:
            text = text.split('Agende su cita')[0].strip()
        status = 'partial' if partial or len(text) > 80000 else ('ok' if len(text) >= 60 else 'unreadable')
        heading = re.search(r'Relacionado a:\s*[“"\']([^”"\']{12,350})', text, re.I)
        return {'status': status, 'text': text[:80000], 'deadline': extract_deadline(text), 'links': links,
                'title': clean_text(heading[1]) if heading else ''}
    finally:
        response.close()
        client.session.close()


class DetailEnricher:
    def __init__(self, path: Path | str | None = None, *, fetcher=read_document, budget: int = 90):
        path = Path(path or Path(__file__).resolve().parents[1] / 'data/otras_fuentes/details.db')
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=30)
        self.connection.execute('CREATE TABLE IF NOT EXISTS detail_cache (url TEXT PRIMARY KEY, payload TEXT NOT NULL, checked REAL NOT NULL, attempted REAL NOT NULL)')
        self.fetcher, self.budget = fetcher, budget

    def close(self):
        self.connection.close()

    def enrich(self, opportunities):
        selected = []
        for item in opportunities:
            # Local document-based portals need detail even when the title is generic.
            quality = item.raw_payload.get('qualification') or {}
            if item.source in {'ena', 'ensa', 'acp'} or quality.get('bucket') in {'relevant', 'review'}:
                selected.append(item)
        urls = list(dict.fromkeys(item.source_url for item in selected))
        cached = {}
        due = []
        now = time.time()
        for url in urls:
            row = self.connection.execute('SELECT payload,checked,attempted FROM detail_cache WHERE url=?', (url,)).fetchone()
            if row:
                cached[url] = json.loads(row[0])
                ttl = 12 * 3600 if cached[url].get('status') == 'ok' else 3600
                if now - row[2] < ttl: continue
            if self.budget > 0:
                due.append(url); self.budget -= 1
        def read(url):
            try: return url, self.fetcher(url)
            except Exception as exc: return url, {'status': 'error', 'error': type(exc).__name__, 'text': ''}
        with ThreadPoolExecutor(max_workers=3) as pool:
            for url, result in pool.map(read, due):
                previous = cached.get(url, {})
                if result.get('status') in {'error', 'unreadable'} and previous.get('text'):
                    result = {**previous, 'status': 'partial', 'error': result.get('error', 'Lectura incompleta'), 'using_previous': True}
                result['checked_at'] = now
                cached[url] = result
                self.connection.execute('INSERT INTO detail_cache VALUES (?,?,?,?) ON CONFLICT(url) DO UPDATE SET payload=excluded.payload,checked=excluded.checked,attempted=excluded.attempted',
                                        (url, json.dumps(result, ensure_ascii=False), now, now))
        self.connection.commit()
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
        return opportunities
