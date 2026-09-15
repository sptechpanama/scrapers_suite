"""Persistent delivery receipts for the existing external-opportunity alerts.

SMTP acceptance is recorded per recipient; it is not proof of inbox delivery.
Uncertain transport results are held for review rather than blindly duplicated.
"""
from __future__ import annotations

import json
import logging
import smtplib
import ssl
from collections import Counter
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage

from .models import stable_hash
from .qualification import effective_bucket

LOG = logging.getLogger('otras_fuentes')


def _key(recipient):
    return stable_hash(recipient.strip().lower(), length=32)


def _record_check(store, result, stamp):
    with store.transaction() as cur:
        cur.execute("INSERT INTO external_email_state(key,value) VALUES('smtp_health',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (json.dumps({**result, 'checked_at': stamp}),))
    return result


def initialize_outbox(store, sent_keys=None, *, now=None):
    """A one-time migration never resends old events with missing receipts."""
    now = now or datetime.now(timezone.utc).isoformat()
    with store.transaction() as cur:
        cur.execute("INSERT INTO external_email_state(key,value) VALUES('enabled_at',?) "
                    "ON CONFLICT(key) DO NOTHING", (now,))
        for event_id, sent_at in (sent_keys or {}).items():
            cur.execute('UPDATE external_alert_events SET notified_at=? WHERE id=? AND notified_at IS NULL',
                        (str(sent_at), event_id))


def _events(store):
    return [dict(row) for row in store.connection.execute(
        'SELECT e.*,o.raw_payload_json,o.matched_company AS current_company,o.is_active '
        'FROM external_alert_events e JOIN external_opportunities o ON o.id=e.opportunity_id '
        'ORDER BY e.created_at,e.id')]


def _eligible(event):
    try:
        raw = json.loads(event.get('raw_payload_json') or '{}')
    except (ValueError, TypeError):
        return False
    return bool(event.get('is_active') and event.get('current_company') and
                effective_bucket(raw.get('qualification') or {}) == 'relevant')


def _body(events):
    lines = ['Oportunidades nuevas o modificadas en fuentes externas.', '']
    for index, event in enumerate(events, 1):
        lines.extend([f"{index}. {event.get('title') or 'Oportunidad'}",
            f"Fuente: {event.get('source', '')} | Empresa: {event.get('matched_company', '')}",
            f"Evento: {event.get('event_type', '')} | Prioridad: {event.get('priority', '')}",
            f"Fecha límite: {event.get('deadline', '')}", event.get('source_url') or '', ''])
    return '\n'.join(lines)


def dispatch_pending(store, config, *, smtp_factory=None, now=None, probe_id=''):
    """Uses the same Gmail SMTP/configuration as CTNI; no recipient literals."""
    now = now or datetime.now(timezone.utc)
    stamp = now.isoformat()
    initialize_outbox(store, now=stamp)
    sender, password, recipients = config
    recipients = list(dict.fromkeys(r.strip().lower() for r in recipients if r.strip()))
    if not sender or not password or not recipients:
        return _record_check(store, {'status': 'configuration_missing', 'accepted': 0}, stamp)
    smtp_factory = smtp_factory or smtplib.SMTP_SSL
    enabled_at = store.connection.execute("SELECT value FROM external_email_state WHERE key='enabled_at'").fetchone()[0]
    events = _events(store)
    if probe_id:
        events = [{'id': 'verification:' + probe_id, 'created_at': stamp, 'notified_at': None,
                   'title': 'Verificación del correo de Oportunidades externas', 'event_type': 'verification'}]
    by_id = {event['id']: event for event in events}
    with store.transaction() as cur:
        for event in events:
            status = ('sent' if event.get('notified_at') else
                      'legacy_unverified' if event['created_at'] < enabled_at else 'pending')
            known = cur.execute('SELECT 1 FROM external_email_deliveries WHERE event_id=? LIMIT 1', (event['id'],)).fetchone()
            if not known:
                # Freeze destinations for an event. Adding a new recipient next
                # month must neither resend history nor invent their receipt.
                for recipient in recipients:
                    cur.execute('INSERT INTO external_email_deliveries(event_id,recipient_key,status,updated_at) '
                                'VALUES(?,?,?,?) ON CONFLICT(event_id,recipient_key) DO NOTHING',
                                (event['id'], _key(recipient), status, stamp))
            if not probe_id and not _eligible(event):
                cur.execute("UPDATE external_email_deliveries SET status='skipped',updated_at=?,"
                            "last_error='Ya no es una oportunidad vigente y relevante' "
                            "WHERE event_id=? AND status='pending'", (stamp, event['id']))
        # A crashed sender may already have submitted its message to SMTP.
        cur.execute("UPDATE external_email_deliveries SET status='uncertain',updated_at=?,"
                    "last_error='Envío interrumpido; comprobar correo antes de reintentar' "
                    "WHERE status='sending' AND updated_at<?", (stamp, (now-timedelta(minutes=30)).isoformat()))

    accepted = 0
    for recipient in recipients:
        key = _key(recipient)
        # SQLite is the single sender's durable queue. Claim under a write lock;
        # a second process cannot select the same pending deliveries.
        store.connection.execute('BEGIN IMMEDIATE')
        try:
            pending = [dict(r) for r in store.connection.execute(
                "SELECT * FROM external_email_deliveries WHERE recipient_key=? AND status='pending' "
                "AND (attempts=0 OR updated_at<=?) ORDER BY updated_at,event_id LIMIT 100",
                (key, (now-timedelta(minutes=15)).isoformat())) if r['event_id'] in by_id]
            if not pending:
                store.connection.commit()
                continue
            ids = [r['event_id'] for r in pending]
            message_id = '<external-' + stable_hash(key, *sorted(ids), length=40) + '@rs-sp-rir.streamlit.app>'
            for event_id in ids:
                store.connection.execute("UPDATE external_email_deliveries SET status='sending',attempts=attempts+1,"
                    "updated_at=?,message_id=?,last_error='' WHERE event_id=? AND recipient_key=?",
                    (stamp, message_id, event_id, key))
            store.connection.commit()
        except Exception:
            store.connection.rollback()
            raise
        msg = EmailMessage()
        msg['From'], msg['To'], msg['Message-ID'] = sender, recipient, message_id
        msg['Subject'] = ('Verificación de Oportunidades externas' if probe_id else
                          f'Otras fuentes: {len(ids)} oportunidad(es)')
        msg.set_content(('Comprobación solicitada del sistema de alertas. Este mensaje no es una licitación.\n'
                         'La captura, los reintentos y los comprobantes de envío se están verificando.\n'
                         f'Identificador de prueba: {probe_id}\n') if probe_id else _body([by_id[i] for i in ids]))
        status, error, submitting = 'pending', '', False
        try:
            with smtp_factory('smtp.gmail.com', 465, context=ssl.create_default_context(), timeout=30) as server:
                server.login(sender, password)
                submitting = True
                refused = server.send_message(msg)
                status = 'pending' if refused else 'sent'
                error = 'Destinatario rechazado por SMTP' if refused else ''
                # Receipt is durable before QUIT; a disconnected QUIT cannot
                # turn a confirmed SMTP acceptance into a duplicate resend.
                _finish(store, ids, key, status, stamp, error)
                if status == 'sent':
                    accepted += len(ids)
        except (smtplib.SMTPRecipientsRefused, smtplib.SMTPDataError) as exc:
            error = type(exc).__name__
            _finish(store, ids, key, 'pending', stamp, error)
        except Exception as exc:
            if status == 'sent':
                continue
            status = 'uncertain' if submitting else 'pending'
            error = type(exc).__name__
            _finish(store, ids, key, status, stamp, error)
            LOG.warning('Correo externo %s: %s; eventos conservados', status, error)
    counts = dict(Counter(row[0] for row in store.connection.execute('SELECT status FROM external_email_deliveries')))
    return _record_check(store, {
        'status': 'review' if counts.get('uncertain') else 'pending' if counts.get('pending') else 'ok',
        'accepted': accepted, **counts}, stamp)


def _finish(store, ids, key, status, stamp, error):
    with store.transaction() as cur:
        for event_id in ids:
            cur.execute('UPDATE external_email_deliveries SET status=?,updated_at=?,last_error=? '
                        'WHERE event_id=? AND recipient_key=?', (status, stamp, error, event_id, key))
            if status == 'sent':
                cur.execute("UPDATE external_alert_events SET notified_at=? WHERE id=? AND NOT EXISTS "
                            "(SELECT 1 FROM external_email_deliveries WHERE event_id=? AND status<>'sent')",
                            (stamp, event_id, event_id))


def sync_receipts(local, remote):
    """Mirror receipts, never run another sender against Supabase."""
    p = remote.placeholder
    with remote.transaction() as cur:
        for row in local.connection.execute("SELECT key,value FROM external_email_state WHERE key='smtp_health'"):
            cur.execute(f'INSERT INTO external_email_state(key,value) VALUES({p},{p}) '
                        'ON CONFLICT(key) DO UPDATE SET value=excluded.value', tuple(row))
        for row in local.connection.execute('SELECT * FROM external_email_deliveries'):
            cur.execute('INSERT INTO external_email_deliveries(event_id,recipient_key,status,attempts,updated_at,last_error,message_id) '
                        f"VALUES({','.join([p]*7)}) ON CONFLICT(event_id,recipient_key) DO UPDATE SET "
                        'status=excluded.status,attempts=excluded.attempts,updated_at=excluded.updated_at,'
                        'last_error=excluded.last_error,message_id=excluded.message_id', tuple(row))
        for event_id, notified_at in local.connection.execute('SELECT id,notified_at FROM external_alert_events WHERE notified_at IS NOT NULL'):
            cur.execute(f'UPDATE external_alert_events SET notified_at={p} WHERE id={p}', (notified_at, event_id))


def deliver_for_orchestrator(*, probe_id=''):
    """Loaded by each job subprocess so the running scheduler needs no restart."""
    from orquestador.main import _ctni_email_config, load_state
    from .storage import OpportunityStore, default_sqlite_path, postgres_dsn
    local = OpportunityStore.sqlite(default_sqlite_path())
    remote = None
    try:
        initialize_outbox(local, load_state().get('otras_fuentes_email_sent_keys', {}))
        result = dispatch_pending(local, _ctni_email_config(), probe_id=probe_id)
        if postgres_dsn():
            remote = OpportunityStore.postgres(postgres_dsn())
            sync_receipts(local, remote)
        return result
    finally:
        local.close()
        if remote is not None:
            remote.close()
