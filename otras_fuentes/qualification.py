"""Conservative, evidence-based screening; never deletes an official notice."""
from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from .models import normalized_text

VERSION = 1
PANAMA = timezone(timedelta(hours=-5))
MONTHS = {m: i for i, m in enumerate(
    ('enero febrero marzo abril mayo junio julio agosto septiembre octubre noviembre diciembre').split(), 1)}
MONTHS.update({m: i for i, m in enumerate('jan feb mar apr may jun jul aug sep oct nov dec'.split(), 1)})
MONTHS['setiembre'] = 9
CLOSED = {'cerrada', 'cerrado', 'closed', 'cancelada', 'cancelado', 'cancelled', 'canceled',
          'adjudicada', 'adjudicado', 'awarded', 'desierta', 'desierto', 'vencida', 'expired'}
LAC = {'argentina', 'belize', 'belice', 'bolivia', 'brazil', 'brasil', 'chile', 'colombia',
       'costa rica', 'cuba', 'dominican republic', 'republica dominicana', 'ecuador',
       'el salvador', 'guatemala', 'guyana', 'haiti', 'honduras', 'jamaica', 'mexico',
       'nicaragua', 'paraguay', 'peru', 'suriname', 'surinam', 'uruguay', 'venezuela',
       'caribbean', 'caribe', 'barbados', 'bahamas', 'trinidad and tobago',
       'eastern caribbean', 'latin america and caribbean', 'america latina y el caribe'}


def deadline_info(raw: object) -> tuple[str, str]:
    """A date without an explicit timezone stays date-only (no invented hour)."""
    value = str(raw or '').strip()
    date = None
    iso = re.search(r'\b(20\d{2})-(\d{2})-(\d{2})\b', value)
    if iso:
        parts = tuple(map(int, iso.groups()))
    else:
        norm = normalized_text(value)
        numeric = re.search(r'\b(\d{1,2})[/-](\d{1,2})[/-](20\d{2})\b', value)
        month = re.search(r'\b(\d{1,2})\s+(?:de\s+)?(' + '|'.join(MONTHS) + r')\s+(?:de\s+)?(20\d{2})\b', norm)
        reverse = re.search(r'\b(' + '|'.join(MONTHS) + r')\s+(\d{1,2})\s+(20\d{2})\b', norm)
        if numeric:
            day, mon, year = map(int, numeric.groups()); parts = (year, mon, day)
        elif month:
            parts = (int(month[3]), MONTHS[month[2]], int(month[1]))
        elif reverse:
            parts = (int(reverse[3]), MONTHS[reverse[1]], int(reverse[2]))
        else:
            return '', ''
    try:
        date = datetime(*parts)
    except ValueError:
        return '', ''
    hour = re.search(r'(?:T|\s)(\d{1,2}):(\d{2})(?::\d{2})?\s*(am|pm)?', value, re.I)
    zone = re.search(r'(?:GMT\s*)?([+-])(\d{1,2})[:.](\d{2})\b', value, re.I)
    at = ''
    if hour and (zone or value.endswith('Z')):
        h, minute = int(hour[1]), int(hour[2])
        if hour[3]: h = h % 12 + (12 if hour[3].lower() == 'pm' else 0)
        offset = (int(zone[2]) * 60 + int(zone[3])) * (1 if zone[1] == '+' else -1) if zone else 0
        try:
            at = date.replace(hour=h, minute=minute, tzinfo=timezone(timedelta(minutes=offset))).astimezone(timezone.utc).isoformat(timespec='seconds')
        except ValueError:
            pass
    return date.date().isoformat(), at


def screen(opportunity, *, strong: bool, ambiguous: bool = False) -> dict:
    detail = opportunity.raw_payload.get('document_analysis') or {}
    body = ' '.join([opportunity.title, opportunity.description, opportunity.eligibility,
                     str(detail.get('text') or '')])
    norm = normalized_text(body)
    raw_deadline = opportunity.raw_payload.get('deadline_raw') or opportunity.deadline
    # A verified detail can fill missing dates; it never overwrites a newer listing deadline.
    if not deadline_info(raw_deadline)[0]: raw_deadline = detail.get('deadline') or raw_deadline
    day, at = deadline_info(raw_deadline)
    country = normalized_text(opportunity.country)
    scope = 'Panamá' if country in {'panama', 'republica de panama'} else ('Región' if country in LAC else 'Global')
    local_only = bool(re.search(
        r'\b(?:solo proveedores (?:nacionales|locales)|(?:local|national) suppliers only|'
        r'for local suppliers|seeking a local firm|domestic bidders only|'
        r'exclusivamente (?:a )?empresas (?:nacionales|locales))\b', norm))
    reasons = []
    bucket = 'relevant' if strong else ('review' if ambiguous else 'no_match')
    if not strong:
        reasons.append('Coincidencia ambigua; revisar el objeto y los documentos' if ambiguous else 'Sin coincidencia sectorial suficiente para RS/SP o RIR')
        if re.search(r'\b(?:laboratorio de suelos|soil testing|muestras de suelos|diagnostico archivistico|cultura organizacional|diagnostico territorial|diagnostico socioeconomico|new york|vehicle maintenance)\b', normalized_text(opportunity.title)):
            bucket = 'no_match'; reasons = ['El objeto corresponde a otra actividad; no se confirma suministro médico ni servicio de RS/SP']
    generic_document = bool(opportunity.documents) and len(normalized_text(opportunity.title).split()) <= 9
    if not strong and generic_document and detail.get('status') != 'ok':
        bucket = 'review'
        reasons = ['Documento sin interpretar; puede contener una oportunidad aplicable']
    if strong:
        reasons.append('Coincidencia sectorial en el objeto o documento; validar costos y requisitos')
        if not day:
            bucket = 'review'; reasons.append('Fecha límite no confirmada')
        if local_only and scope != 'Panamá':
            bucket = 'review'; reasons.append('Restringida a proveedores locales del país de destino; verificar participación o socio')
        if detail.get('status') in {'error', 'unreadable', 'partial', 'pending'}:
            bucket = 'review'; reasons.append('Detalle o documento pendiente de interpretación completa')
        if re.search(r'\b(?:estudio de mercado|solicitud de informacion|request for information|market survey)\b', normalized_text(opportunity.source_type + ' ' + opportunity.title)):
            bucket = 'review'; reasons.append('Consulta de mercado; no equivale a una contratación abierta')
        title = normalized_text(opportunity.title)
        if re.search(r'\b(?:flota|vehiculos|vehicle|automotriz)\b', title) and re.search(r'\b(?:aire acondicionado|air conditioning|mantenimiento|maintenance)\b', title):
            bucket = 'no_match'; reasons = ['Mantenimiento automotor; no corresponde a climatización de edificios']
        elif re.search(r'\b(?:consultoria|consultancy|supervision|construccion|construction)\b', title):
            bucket = 'review'; reasons.append('Alcance integral de obra o consultoría; comprobar si existe un lote o servicio aplicable')
    return {'version': VERSION, 'bucket': bucket, 'reason': '; '.join(reasons),
            'display_title': detail.get('title', '') if generic_document else '',
            'deadline_date': day, 'deadline_at': at, 'closed': normalized_text(opportunity.status) in CLOSED,
            'scope': scope, 'local_only': local_only, 'detail_status': detail.get('status', 'listing')}


def effective_bucket(quality: dict, *, now: datetime | None = None, last_seen: str = '') -> str:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None: now = now.replace(tzinfo=PANAMA)
    day, at = quality.get('deadline_date', ''), quality.get('deadline_at', '')
    if quality.get('closed') or (at and datetime.fromisoformat(at) < now) or (not at and day and day < now.astimezone(PANAMA).date().isoformat()):
        return 'historical'
    bucket = quality.get('bucket', 'review')
    if bucket == 'relevant' and last_seen and last_seen[:10] < (now.astimezone(PANAMA) - timedelta(days=7)).date().isoformat():
        return 'review'
    return bucket
