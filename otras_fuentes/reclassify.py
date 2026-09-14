"""Idempotent maintenance of screening metadata without alerts or fake fresh dates."""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import fields
from .models import Opportunity, SourceDocument
from .classifier import classify_opportunity
from .qualification import effective_bucket


def from_record(row: dict) -> Opportunity:
    names = {f.name for f in fields(Opportunity)}
    args = {k: v for k, v in row.items() if k in names and v is not None}
    args['documents'] = [SourceDocument(**x) for x in json.loads(row.get('documents_json') or '[]')]
    args['raw_payload'] = json.loads(row.get('raw_payload_json') or '{}')
    args['matched_keywords'] = json.loads(row.get('matched_keywords_json') or '[]')
    args['matched_fields'] = json.loads(row.get('matched_fields_json') or '[]')
    item = Opportunity(**args)
    # Preserve the timezone exposed in the original UNGM listing.
    cells = item.raw_payload.get('cells') or []
    if not item.raw_payload.get('deadline_raw') and len(cells) > 2 and 'GMT' in str(cells[2]):
        import re
        match = re.search(r'\d{1,2}-[A-Za-z]{3}-20\d{2}\s+\d{1,2}:\d{2}\s*\(GMT\s*[+-]\d{1,2}[.:]\d{2}\)', str(cells[2]))
        if match: item.raw_payload['deadline_raw'] = match[0]
    return item.normalize()


def reclassify_store(store, *, enricher=None, apply=False) -> dict:
    cursor = store.connection.cursor()
    cursor.execute('SELECT * FROM external_opportunities ORDER BY source,id')
    names = [x[0] for x in cursor.description]
    records = [dict(zip(names, row)) for row in cursor.fetchall()]
    cursor.close()
    items = [classify_opportunity(from_record(row)) for row in records]
    if enricher:
        # Prioritize local portals and matched current notices when warming the cache.
        ordered = sorted(items, key=lambda x: (x.source not in {'ena','ensa','acp'},
                         effective_bucket(x.raw_payload['qualification']) == 'historical'))
        enricher.enrich(ordered)
    counts = Counter()
    changes = 0
    updates = []
    for row, item in zip(records, items):
        classify_opportunity(item)
        payload = item.as_storage_dict()
        quality = item.raw_payload['qualification']
        counts[effective_bucket(quality, last_seen=row.get('last_seen_at') or '')] += 1
        keys = ('raw_payload_json','documents_json','matched_company','matched_keywords_json','matched_fields_json','fit_score','priority','is_active','content_hash')
        if any(row.get(k) != payload[k] for k in keys):
            updates.append(tuple(payload[k] for k in keys) + (row['id'],)); changes += 1
    if apply and updates:
        p = store.placeholder
        with store.transaction() as cursor:
            cursor.executemany('UPDATE external_opportunities SET ' + ','.join(k+'='+p for k in keys) + ' WHERE id='+p, updates)
            from .models import stable_hash, utc_now_iso
            for item in items:
                for document in item.documents:
                    now = utc_now_iso()
                    cursor.execute(f'INSERT INTO external_opportunity_documents (id,opportunity_id,title,url,document_type,first_seen_at,last_seen_at) VALUES ({",".join([p]*7)}) ON CONFLICT(id) DO NOTHING',
                                   (stable_hash(item.id,document.url,length=32),item.id,document.title,document.url,document.document_type,now,now))
    return {'records': len(records), 'changes': changes, 'views': dict(counts), 'applied': apply,
            'alerts_created': 0, 'historical_rows_deleted': 0}
