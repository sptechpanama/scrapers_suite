import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "orquestador"))
import ficha_deadline_reminders as rem

URL = 'https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-1-10-01-03-CL-050508/token'
NOW = datetime(2026, 9, 16, 7, 0, tzinfo=rem.PANAMA)


def entry(**kw):
    return dict({'enlace': URL, 'fecha': '11-09-2026 a 16-09-2026', 'ficha_detectada': '43358',
                 'titulo': 'Kit de circuito de paciente', 'entidad': 'CSS', 'precio_referencia': '37500'}, **kw)


@pytest.mark.parametrize('text,expected_hour,exact', [
    ('11-09-2026 a 16-09-2026', 23, False),
    ('11-09-2026 08:00 AM a 16-09-2026', 23, False),
    ('16-09-2026 - 08:00 AM a 03:00 PM', 15, True),
    ('16/09/2026 hasta 12:00 AM', 0, True),
    ('16/09/2026 hasta 12:00 P.M.', 12, True),
    ('16-09-2026 hasta 14:30', 14, True),
])
def test_deadline_precision(text, expected_hour, exact):
    parsed, precision = rem.deadline(text)
    assert parsed.day == 16 and parsed.hour == expected_hour
    assert precision is exact


@pytest.mark.parametrize('text', ['', 'sin fecha', '31-02-2026', '16-09-2026 hasta 25:70'])
def test_invalid_dates_cannot_trigger_reminders(text):
    assert rem.deadline(text)[0] is None


def test_050508_last_day_is_independent_of_programmed_and_open_alerts():
    sent = {'cl-v1|2026-1-10-01-03-CL-050508|abierta': '2026-09-11'}
    rows, errors = rem.collect_reminders([entry(), entry()], sent, NOW, verify=lambda x: x)
    assert not errors and len(rows) == 1
    assert rows[0]['reminder_key'] == '43358|2026-1-10-01-03-CL-050508|2026-09-16'
    assert 'no asumas' in rem.message_body(rows)
    assert rem.collect_reminders([entry()], {rows[0]['reminder_key']: 'sent'}, NOW, verify=lambda x: x)[0] == []


@pytest.mark.parametrize('kw', [
    {'ficha_detectada': '143358'}, {'ficha_detectada': '107260'},
    {'fecha': '16-09-2026 a 17-09-2026'}, {'fecha': '15-09-2026'},
])
def test_other_fichas_or_other_days_do_not_call_portal(kw):
    def forbidden(*a): pytest.fail('unnecessary API call')
    assert rem.collect_reminders([entry(**kw)], {}, NOW, verify=forbidden) == ([], [])


def test_before_seven_waits_and_late_start_catches_up():
    def forbidden(*a): pytest.fail('too early')
    assert rem.collect_reminders([entry()], {}, NOW.replace(hour=6), verify=forbidden) == ([], [])
    assert len(rem.collect_reminders([entry()], {}, NOW.replace(hour=13), verify=lambda x:x)[0]) == 1


def test_official_extension_closed_state_and_passed_hour_cancel_reminder():
    for verified in [None, entry(fecha='11-09-2026 a 17-09-2026'), entry(fecha='16-09-2026 hasta 06:59 AM')]:
        assert rem.collect_reminders([entry()], {}, NOW, verify=lambda x:verified)[0] == []


def test_api_failure_is_reported_and_can_retry_without_sent_marker():
    def fail(x): raise TimeoutError('temporary')
    sent = {}
    rows, errors = rem.collect_reminders([entry()], sent, NOW, verify=fail)
    assert not rows and errors and not sent
    assert len(rem.collect_reminders([entry()], sent, NOW, verify=lambda x:x)[0]) == 1


def test_rescheduled_cl_can_have_one_reminder_on_new_day():
    sent = {'43358|2026-1-10-01-03-CL-050508|2026-09-16': 'sent'}
    updated = entry(fecha='11-09-2026 a 18-09-2026')
    rows, errors = rem.collect_reminders([updated], sent, NOW.replace(day=18), verify=lambda x:x)
    assert len(rows) == 1 and rows[0]['reminder_key'].endswith('2026-09-18')


def test_public_verification_checks_exact_number_and_state(monkeypatch):
    calls = []
    def fake(method, url, **kw):
        calls.append((method, url, kw))
        if method == 'POST':
            return {'registros': [{'numProceso': 'another-act', 'idEstado': 8}]}
        pytest.fail('cannot fetch detail for another act')
    monkeypatch.setattr(rem, '_json_request', fake)
    assert rem.verify_public_entry(entry()) is None
    assert len(calls) == 2


def test_watched_ap_with_presentation_deadline_is_supported():
    ap = entry(enlace=URL.replace('solicitud-de-cotizacion', 'pliego-de-cargos').replace('-CL-', '-LP-'),
               fecha='16-09-2026 hasta 10:00 AM')
    rows, errors = rem.collect_reminders([ap], {}, NOW, verify=lambda x:x)
    assert len(rows) == 1 and not errors


def test_monitor_retries_smtp_and_records_only_success(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location('reminder_main_test', ROOT/'orquestador/main.py')
    o = importlib.util.module_from_spec(spec);sys.modules[spec.name] = o;spec.loader.exec_module(o)
    path = tmp_path/'state.json';path.write_text(json.dumps({'last_run': {}, 'ct_rir_email_sent_keys': {'ordinary': 'old'}}))
    monkeypatch.setattr(o, 'STATE_PATH', path)
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None): return NOW if tz else NOW.replace(tzinfo=None)
    monkeypatch.setattr(o, 'datetime', FixedDatetime)
    monkeypatch.setattr(o, '_scan_ct_rir_candidates', lambda **kw:[entry()])
    monkeypatch.setattr(o, 'collect_43358_reminders', lambda entries,sent,now:rem.collect_reminders(entries,sent,now,verify=lambda x:x))
    monkeypatch.setattr(o, '_ct_rir_email_config', lambda:('sender@example.test','secret',['to@example.test']))
    class SMTP:
        fail = True
        calls = 0
        def __init__(self,*a,**kw): pass
        def __enter__(self): return self
        def __exit__(self,*a): return False
        def login(self,*a): pass
        def send_message(self,msg):
            SMTP.calls += 1
            assert '43358' in msg['Subject'] and '050508' in msg.get_content()
            if SMTP.fail: raise TimeoutError('temporary')
            return {}
    monkeypatch.setattr(o.smtplib,'SMTP_SSL',SMTP)
    assert o._run_43358_deadline_watchdog()['status'] == 'error'
    assert not json.loads(path.read_text()).get('ct_rir_43358_reminder_sent_keys')
    SMTP.fail=False
    assert o._run_43358_deadline_watchdog()['sent'] == 1
    assert o._run_43358_deadline_watchdog()['sent'] == 0
    assert SMTP.calls == 2
    assert json.loads(path.read_text())['ct_rir_email_sent_keys'] == {'ordinary':'old'}
