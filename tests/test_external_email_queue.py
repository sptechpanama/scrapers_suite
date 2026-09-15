import json
import smtplib
from datetime import datetime, timedelta, timezone

import pytest

from otras_fuentes.models import Opportunity, SourceFetchResult
from otras_fuentes.storage import OpportunityStore
from otras_fuentes.notifications import dispatch_pending, initialize_outbox, sync_receipts

NOW = datetime(2026, 9, 15, 18, tzinfo=timezone.utc)
CONFIG = ('sender@example.test', 'unused', ['one@example.test', 'two@example.test'])


class SMTP:
    messages = []
    fail_login = False
    fail_send = False
    refuse = ''
    fail_quit = False

    def __init__(self, *a, **kw): pass
    def __enter__(self): return self
    def __exit__(self, *a):
        if self.fail_quit: raise smtplib.SMTPServerDisconnected('QUIT')
    def login(self, *a):
        if self.fail_login: raise smtplib.SMTPAuthenticationError(535, b'bad')
    def send_message(self, msg):
        if self.fail_send: raise smtplib.SMTPServerDisconnected('DATA')
        if self.refuse == msg['To']: return {self.refuse: (450, b'temporary')}
        self.messages.append(msg)
        return {}


@pytest.fixture
def store(tmp_path):
    SMTP.messages = []
    SMTP.fail_login = SMTP.fail_send = SMTP.fail_quit = False
    SMTP.refuse = ''
    s = OpportunityStore.sqlite(tmp_path/'alerts.db')
    initialize_outbox(s, now=(NOW-timedelta(hours=1)).isoformat())
    yield s
    s.close()


def event(store, code='test-1', when=NOW):
    item = Opportunity('ensa', code, 'Suministro de chiller', 'https://ensa.com.pa/'+code,
                       deadline='2099-01-01', matched_company='RS/SP', priority='Alta', fit_score=90,
                       raw_payload={'qualification': {'bucket':'relevant','deadline_date':'2099-01-01'}})
    store.ingest_source('base', NOW.isoformat(), NOW.isoformat(), SourceFetchResult('ensa', [item]))
    with store.transaction() as cur:
        cur.execute('INSERT INTO external_alert_events(id,run_id,opportunity_id,source,event_type,title,created_at,deadline) '
                    'VALUES(?,?,?,?,?,?,?,?)', (code,'run',item.as_storage_dict()['id'],'ensa','new',item.title,when.isoformat(),'2099-01-01'))


def send(store, **kw):
    return dispatch_pending(store, CONFIG, smtp_factory=SMTP, now=kw.pop('now', NOW), **kw)


def test_real_events_delivered_once_per_recipient_and_receipts_persist(store):
    event(store)
    assert send(store)['accepted']==2
    assert send(store, now=NOW+timedelta(hours=6))['accepted']==0
    assert len(SMTP.messages)==2
    assert store.connection.execute('SELECT notified_at FROM external_alert_events').fetchone()[0]


def test_failed_login_is_retried_on_later_run_without_new_events(store):
    event(store); SMTP.fail_login=True
    assert send(store)['pending']==2
    SMTP.fail_login=False
    assert send(store,now=NOW+timedelta(minutes=1))['accepted']==0
    assert send(store,now=NOW+timedelta(minutes=16))['accepted']==2


def test_partial_recipient_failure_never_resends_to_accepted_recipient(store):
    event(store); SMTP.refuse=CONFIG[2][1]
    first=send(store)
    assert first['accepted']==1 and first['pending']==1
    SMTP.refuse=''
    assert send(store,now=NOW+timedelta(minutes=16))['accepted']==1
    assert [m['To'] for m in SMTP.messages]==CONFIG[2]


def test_disconnect_during_submission_is_held_without_blind_resend(store):
    event(store); SMTP.fail_send=True
    assert send(store)['uncertain']==2
    SMTP.fail_send=False
    assert send(store,now=NOW+timedelta(hours=6))['accepted']==0


def test_successful_send_then_quit_failure_is_still_sent(store):
    event(store); SMTP.fail_quit=True
    assert send(store)['accepted']==2
    assert send(store)['accepted']==0


def test_active_sender_cannot_be_claimed_again_and_stale_claim_is_uncertain(store):
    event(store); SMTP.fail_login=True;send(store)
    store.connection.execute("UPDATE external_email_deliveries SET status='sending'");store.connection.commit()
    SMTP.fail_login=False
    assert send(store,now=NOW+timedelta(minutes=20))['accepted']==0
    assert send(store,now=NOW+timedelta(minutes=31))['uncertain']==2


def test_legacy_events_without_proof_are_preserved_but_not_resent(store):
    event(store,when=NOW-timedelta(days=1))
    assert send(store)['legacy_unverified']==2
    assert not SMTP.messages


def test_known_legacy_receipt_is_migrated_without_resend(store):
    event(store,when=NOW-timedelta(days=1))
    initialize_outbox(store,{'test-1':NOW.isoformat()})
    assert send(store)['sent']==2
    assert not SMTP.messages


def test_closed_opportunities_are_not_retried_after_smtp_failure(store):
    event(store);SMTP.fail_login=True;send(store)
    store.connection.execute('UPDATE external_opportunities SET is_active=0');store.connection.commit()
    SMTP.fail_login=False
    assert send(store,now=NOW+timedelta(hours=1))['skipped']==2
    assert not SMTP.messages


def test_missing_credentials_never_acknowledges_an_event(store):
    event(store)
    assert dispatch_pending(store,('', '', []),now=NOW)['status']=='configuration_missing'
    assert not store.connection.execute('SELECT notified_at FROM external_alert_events').fetchone()[0]
    assert json.loads(store.connection.execute("SELECT value FROM external_email_state WHERE key='smtp_health'").fetchone()[0])['status']=='configuration_missing'


def test_new_recipient_does_not_receive_history_or_get_a_fake_receipt(store):
    event(store);send(store)
    changed=(*CONFIG[:2],[*CONFIG[2],'new@example.test'])
    assert dispatch_pending(store,changed,smtp_factory=SMTP,now=NOW)['accepted']==0
    assert store.connection.execute('SELECT count(*) FROM external_email_deliveries').fetchone()[0]==2


def test_probe_does_not_create_fake_opportunities_and_is_idempotent(store):
    assert send(store,probe_id='acceptance-test')['accepted']==2
    assert send(store,probe_id='acceptance-test')['accepted']==0
    assert not store.connection.execute('SELECT count(*) FROM external_alert_events').fetchone()[0]
    assert not store.connection.execute('SELECT count(*) FROM external_opportunities').fetchone()[0]
    assert all('no es una licitación' in m.get_content() for m in SMTP.messages)


def test_receipts_mirror_is_idempotent_and_not_another_sender(store,tmp_path):
    event(store);send(store)
    other=OpportunityStore.sqlite(tmp_path/'remote.db')
    try:
        sync_receipts(store,other);sync_receipts(store,other)
        assert other.connection.execute('SELECT count(*) FROM external_email_deliveries').fetchone()[0]==2
    finally:other.close()


@pytest.mark.parametrize('silent', [False, True])
def test_job_wrapper_owns_delivery_without_legacy_duplicate_or_silent_email(monkeypatch,capsys,silent):
    from types import SimpleNamespace
    from orquestador import run_otras_fuentes as job
    calls=[]
    monkeypatch.delenv('OTRAS_FUENTES_SILENT_RUN', raising=False)
    monkeypatch.setattr('sys.argv',['run_otras_fuentes.py','--sources','ensa',*(['--silent'] if silent else [])])
    monkeypatch.setattr(job,'_deliver_notifications',lambda:calls.append('delivery') or {'status':'ok'})
    result=SimpleNamespace(status='success',postgres_synced=True,summary=lambda:{'events':[{'id':'one'}]})
    monkeypatch.setattr(job,'run_monitor',lambda *a,**kw:result)
    assert job.main()==0
    summary=json.loads(capsys.readouterr().out.split('OTRAS_FUENTES_SUMMARY_JSON=')[1])
    assert summary['events']==[] and summary['notifications_handled']
    assert len(calls)==(0 if silent else 2)
