import ast
import copy
import http.client
import json
import logging
from pathlib import Path
import socket
import sys
import textwrap
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import httplib2
import pytest
from google.auth.exceptions import TransportError
from googleapiclient.errors import HttpError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'orquestador'))
import google_transport as transport
import sheets_bridge as bridge
import main as orchestrator


@pytest.fixture(autouse=True)
def isolate_network(monkeypatch):
    monkeypatch.setattr(transport.time, 'sleep', lambda _: None)
    monkeypatch.setattr(bridge, '_service_local', threading.local())
    monkeypatch.setattr(bridge, '_verified_headers', {})
    monkeypatch.setattr(bridge, '_get_credentials', lambda: object())
    monkeypatch.setattr(bridge, '_ensure_sheet_exists', lambda s: None)


class FakeService:
    def __init__(self, data, actions, failures=None):
        self.data, self.actions = data, actions
        self.failures = failures if failures is not None else []
        self.closed = False

    def close(self): self.closed = True
    def spreadsheets(self): return self
    def values(self): return self

    def get(self, **kwargs):
        def execute():
            assert not self.closed, 'Stale transport reused'
            self.actions.append(('get', kwargs))
            if self.failures:
                raise self.failures.pop(0)
            return {'values': copy.deepcopy(self.data)}
        return SimpleNamespace(execute=execute)

    def update(self, **kwargs):
        def execute():
            assert not self.closed
            self.actions.append(('update', kwargs))
            self.data[:] = copy.deepcopy(kwargs['body']['values'])
            if self.failures:
                raise self.failures.pop(0)
            return {}
        return SimpleNamespace(execute=execute)


@pytest.mark.parametrize('error', [socket.gaierror(11001, 'getaddrinfo failed'),
    http.client.RemoteDisconnected('closed'), httplib2.ServerNotFoundError('DNS'),
    TransportError('oauth2 unavailable'), ConnectionResetError('reset')])
def test_google_read_recovers_using_new_client(error, monkeypatch):
    built=[];data=[['name'], ['keep']];actions=[]
    def build(_):
        service=FakeService(data, actions, [error] if not built else [])
        built.append(service)
        return service
    monkeypatch.setattr(bridge, 'build_sheets_service', build)
    assert bridge._get_values('pc_manual!A1:A2') == data
    assert len(built)==2 and built[0].closed and not built[1].closed
    assert len(actions)==2


@pytest.mark.parametrize('status', [429, 500, 502, 503, 504])
def test_transient_http_errors_retry(status):
    error=HttpError(httplib2.Response({'status':status}), b'{"error":{"message":"temporary"}}')
    calls=[];resets=[]
    def action():
        calls.append(1)
        if len(calls)==1: raise error
        return 'ok'
    assert transport.retry_google_call(action,reset=lambda:resets.append(1),label='test')=='ok'
    assert len(calls)==2 and len(resets)==1


def test_outage_is_bounded_and_never_becomes_empty_result():
    calls=[];resets=[]
    def action():
        calls.append(1)
        raise socket.gaierror(11001,'DNS')
    with pytest.raises(socket.gaierror):
        transport.retry_google_call(action,reset=lambda:resets.append(1),label='test')
    assert len(calls)==len(resets)==3


@pytest.mark.parametrize('error', [ValueError('invalid'), PermissionError('forbidden'),
    HttpError(httplib2.Response({'status':403}), b'{"error":{"message":"denied"}}')])
def test_non_transient_errors_are_not_hidden(error):
    def action(): raise error
    with pytest.raises(type(error)):
        transport.retry_google_call(action, reset=lambda:pytest.fail('unnecessary retry'), label='test')


def test_transport_sets_timeout_and_authenticated_http(monkeypatch):
    calls={}
    monkeypatch.setattr(transport.httplib2,'Http',lambda **k:calls.update(k) or 'raw-http')
    monkeypatch.setattr(transport,'AuthorizedHttp',lambda creds,http: (creds,http))
    monkeypatch.setattr(transport,'build',lambda *args,**kw:kw)
    result=transport.build_sheets_service('credentials')
    assert calls['timeout']==15
    assert result['http']==('credentials','raw-http') and result['cache_discovery'] is False


@pytest.mark.parametrize('pool', ['bridge','panama'])
def test_parallel_monitors_have_distinct_clients(pool, monkeypatch, tmp_path):
    barrier=threading.Barrier(2)
    if pool=='bridge':
        monkeypatch.setattr(bridge,'build_sheets_service',lambda _:object())
        get=bridge._get_service
    else:
        key=tmp_path/'test.json';key.write_text('{}')
        monkeypatch.setenv('ORQUESTADOR_PANAMACOMPRA_SERVICE_ACCOUNT_FILE',str(key))
        monkeypatch.setattr(orchestrator,'_PANAMACOMPRA_SERVICE_LOCAL',threading.local())
        monkeypatch.setattr(orchestrator.Credentials,'from_service_account_file',lambda *a,**k:object())
        monkeypatch.setattr(orchestrator,'build_sheets_service',lambda _:object())
        get=orchestrator._get_panamacompra_service
    def worker():
        first=get();barrier.wait(timeout=5)
        assert get() is first
        return first
    with ThreadPoolExecutor(2) as executor:
        results=list(executor.map(lambda _:worker(),range(2)))
    assert results[0] is not results[1]


def test_header_validation_is_cached_only_after_success(monkeypatch):
    requests=[];data=[['id','status']]
    monkeypatch.setattr(bridge,'build_sheets_service',lambda _:FakeService(data,requests))
    bridge._ensure_headers('manual',['id','status'])
    bridge._ensure_headers('manual',['id','status'])
    assert len(requests)==1
    bridge._verified_headers.clear()
    monkeypatch.setattr(bridge,'_call_with_backoff',lambda *a,**k:(_ for _ in ()).throw(socket.gaierror(11001,'DNS')))
    with pytest.raises(socket.gaierror): bridge._ensure_headers('manual',['id','status'])
    assert not bridge._verified_headers


def test_state_publishing_is_single_write_and_retry_safe_after_lost_ack(monkeypatch):
    remote=[bridge.STATE_HEADERS, ['old']*7,['stale']*7];actions=[];built=[]
    class LostAck(FakeService):
        def update(self,**kwargs):
            self.failures=[http.client.RemoteDisconnected('lost acknowledgement')]
            return super().update(**kwargs)
    def build(_):
        cls=LostAck if not built else FakeService
        result=cls(remote,actions);built.append(result)
        return result
    monkeypatch.setattr(bridge,'build_sheets_service',build)
    bridge.push_state_to_sheet({'last_run':{'clv':{'status':'success'}}})
    assert remote[0]==bridge.STATE_HEADERS
    assert remote[1][:2]==['clv','success']
    assert remote[2]==['']*7
    assert [kind for kind,_ in actions]==['get','update','update']
    assert actions[1][1]==actions[2][1]


def test_outage_before_state_write_preserves_remote_table(monkeypatch):
    remote=[bridge.STATE_HEADERS, ['clv','success']];before=copy.deepcopy(remote);actions=[]
    monkeypatch.setattr(bridge,'build_sheets_service',lambda _:FakeService(remote,actions,[socket.gaierror(11001,'DNS')]))
    with pytest.raises(socket.gaierror): bridge.push_state_to_sheet({'last_run':{}})
    assert remote==before and all(kind=='get' for kind,_ in actions)


@pytest.fixture
def config_setup(monkeypatch,tmp_path):
    local={'name':'clv','python':'python','script':'clv.py','days_of_week':['mon'],'times':['07:45']}
    remote=dict(local,times=['09:00'])
    path=tmp_path/'config.json';path.write_text(json.dumps({'jobs':[local]}))
    monkeypatch.setattr(orchestrator,'CONFIG_PATH',path)
    monkeypatch.setattr(orchestrator,'CONFIG_CACHE_PATH',tmp_path/'config.last_good.json')
    monkeypatch.setattr(orchestrator,'REQUIRED_FALLBACK_JOB_NAMES',set())
    monkeypatch.setattr(orchestrator,'push_jobs_to_sheet',lambda _:pytest.fail('Unexpected configuration write'))
    monkeypatch.setattr(orchestrator,'fetch_jobs_from_sheet',lambda:([remote],True))
    return remote


def test_remote_config_survives_outage_and_restart(config_setup,monkeypatch):
    current=orchestrator.load_config(require_remote=True)
    assert current.jobs[0].times==['09:00']
    saved=orchestrator.CONFIG_CACHE_PATH.read_bytes()
    def fail(): raise socket.gaierror(11001,'DNS')
    monkeypatch.setattr(orchestrator,'fetch_jobs_from_sheet',fail)
    with pytest.raises(socket.gaierror): orchestrator.load_config(require_remote=True)
    assert orchestrator.CONFIG_CACHE_PATH.read_bytes()==saved
    assert orchestrator.load_config().jobs[0].times==['09:00']


def test_config_recovery_accepts_actual_remote_change(config_setup,monkeypatch):
    orchestrator.load_config(require_remote=True)
    monkeypatch.setattr(orchestrator,'fetch_jobs_from_sheet',lambda:([dict(config_setup,times=['10:00'])],True))
    assert orchestrator.load_config(require_remote=True).jobs[0].times==['10:00']
    assert orchestrator._load_last_good_config().jobs[0].times==['10:00']


def test_empty_response_does_not_replace_running_schedule(config_setup,monkeypatch):
    orchestrator.load_config(require_remote=True)
    monkeypatch.setattr(orchestrator,'fetch_jobs_from_sheet',lambda:([],False))
    with pytest.raises(ValueError): orchestrator.load_config(require_remote=True)
    assert orchestrator._load_last_good_config().jobs[0].times==['09:00']


def test_cache_from_other_sheet_is_not_used(config_setup,monkeypatch):
    orchestrator.load_config(require_remote=True)
    monkeypatch.setattr(orchestrator,'SPREADSHEET_ID','different')
    assert orchestrator._load_last_good_config() is None


def nested_function(name):
    tree=ast.parse(Path(orchestrator.__file__).read_text(encoding='utf-8-sig'))
    main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
    return next(n for n in main.body if isinstance(n,ast.FunctionDef) and n.name==name)


def test_failed_manual_poll_is_not_reported_as_success(caplog):
    error=socket.gaierror(11001,'DNS')
    def fail(): raise error
    namespace={**vars(orchestrator),'fetch_manual_requests':fail,'describe_job':lambda _: 'monitor manual'}
    for name in ['poll_manual_requests','job_event_listener']:
        exec(compile(ast.Module(body=[nested_function(name)],type_ignores=[]),'test','exec'),namespace)
    with caplog.at_level(logging.INFO):
        result=namespace['poll_manual_requests']()
        namespace['job_event_listener'](SimpleNamespace(code=orchestrator.EVENT_JOB_EXECUTED,job_id='manual',retval=result))
    assert result['status']=='error'
    assert 'ejecucion exitosa' not in caplog.text
    assert 'pendiente/con error' in caplog.text


def test_config_monitor_does_not_reschedule_during_outage():
    node=nested_function('refresh_config_from_sheet')
    code='def harness():\n    current_config = "keep"\n    current_signature = "keep"\n    job_lookup = {}\n'
    code+=textwrap.indent(ast.unparse(node),'    ')+'\n    return refresh_config_from_sheet\n'
    def fail(**kwargs):
        assert kwargs=={'require_remote':True}
        raise socket.gaierror(11001,'DNS')
    namespace={**vars(orchestrator),'load_config':fail,'apply_config_to_scheduler':lambda *a:pytest.fail('rescheduled')}
    exec(code,namespace)
    assert namespace['harness']()()['status']=='error'
