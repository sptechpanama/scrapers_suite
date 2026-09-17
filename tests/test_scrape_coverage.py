import ast
import json
from pathlib import Path
import pytest
from common import scrape_coverage as c


def url(n):
    return f'https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-1-10-01-08-CL-{n:06d}/token'


def snapshot(page, numbers, *, total=120, more=False, disabled=False):
    return {'text':f'Pagina: {page} / {page if disabled else page+1}{" +" if more else ""}\nTotal: {total}{" +" if more else ""}',
            'links':[url(n) for n in numbers], 'next_disabled':disabled,'first_disabled':page==1,'rows':[]}


class FakePage:
    def __init__(self, pages, *, delay=0, stuck=False):
        self.pages, self.index, self.clicks = pages, 0, 0
        self.delay, self.stuck, self.pending = delay, stuck, None
        self.d=self
    def close_popup(self): pass
    def collect_links(self): return self.pages[self.index]['links']
    def click_next(self):
        self.clicks+=1
        if not self.stuck: self.pending=self.delay
        return True
    def execute_script(self, script, *args):
        if 'aria-label=First' in script:
            self.index=0
            return
        if self.pending is not None:
            if self.pending<=0:
                self.index+=1
                self.pending=None
            else: self.pending-=1
        return dict(self.pages[self.index])


@pytest.fixture(autouse=True)
def fake_time(monkeypatch):
    clock=[0.0]
    monkeypatch.setattr(c.time,'monotonic',lambda:clock[0])
    monkeypatch.setattr(c.time,'sleep',lambda seconds:clock.__setitem__(0,clock[0]+seconds))


def capture(p): return c.collect_listing(p,'a',lambda *a:None,timeout=3,settle=0.2)


def test_lazy_plus_is_not_final_page_and_all_rows_are_collected():
    p=FakePage([snapshot(1,range(1,51),total=100,more=True),snapshot(2,range(51,101),total=100,more=True),snapshot(3,range(101,121),disabled=True)])
    r=capture(p)
    assert r['complete'] and len(r['links'])==120 and p.clicks==2
    assert r['pages'][0]['first'].endswith('000001')


def test_slow_angular_transition_waits_without_clicking_twice():
    p=FakePage([snapshot(1,range(1,51),total=70),snapshot(2,range(51,71),total=70,disabled=True)],delay=9)
    r=capture(p)
    assert r['complete'] and len(r['links'])==70 and p.clicks==1


def test_first_page_is_explicitly_reset():
    p=FakePage([snapshot(1,range(1,51),total=70),snapshot(2,range(51,71),total=70,disabled=True)])
    p.index=1
    assert capture(p)['links'][0]==url(1)


def test_stalled_next_is_partial_not_infinite_loop_or_double_click():
    p=FakePage([snapshot(1,range(1,51),more=True)],stuck=True)
    r=capture(p)
    assert not r['complete'] and len(r['links'])==50 and p.clicks==1


def test_disabled_next_with_plus_is_unverified_not_complete():
    r=capture(FakePage([snapshot(1,range(1,51),total=50,more=True,disabled=True)]))
    assert not r['complete'] and len(r['links'])==50


def test_unknown_next_is_not_the_end():
    r=capture(FakePage([snapshot(1,range(1,51),disabled=None)]))
    assert not r['complete'] and len(r['links'])==50


def test_missing_rows_or_duplicate_processes_are_detected():
    p=FakePage([snapshot(1,range(1,51),total=70),snapshot(2,range(50,70),total=70,disabled=True)])
    r=capture(p)
    assert not r['complete'] and len(r['links'])==69 and '70' in r['error']


def test_repaint_during_collect_is_rejected():
    p=FakePage([snapshot(1,range(1,21),total=20,disabled=True)])
    p.collect_links=lambda:[url(99)]
    assert not capture(p)['complete']


def test_genuinely_empty_list_is_confirmed():
    s=snapshot(1,[],total=0,disabled=True);s['text']='No se encontraron registros'
    r=capture(FakePage([s]));assert r['complete'] and not r['links']


def test_no_table_is_not_an_empty_success():
    p=FakePage([]);p.execute_script=lambda *a:None
    assert not capture(p)['complete']


def test_previous_tab_rows_are_not_accepted_for_scheduled_quotes():
    p=FakePage([snapshot(1,range(1,21),total=20,disabled=True)])
    p.pages[0]['states']=['Abierta']*20
    r=c.collect_listing(p,'a',lambda *a:None,expected_state='Programada',timeout=1,settle=.2)
    assert not r['complete'] and not r['links']


def test_expected_tab_and_non_medical_buyers_are_not_filtered_out():
    p=FakePage([snapshot(1,range(1,21),total=20,disabled=True)])
    p.pages[0]['states']=['Programada']*20
    r=c.collect_listing(p,'a',lambda *a:None,expected_state='Programada',timeout=1,settle=.2)
    assert r['complete'] and len(r['links'])==20


@pytest.mark.parametrize('text,valid', [('2026-1-10-01-08-CL-000001',True),('2026-1-10-01-08-CL-000002',False),('Error 502',False)])
def test_detail_must_belong_to_requested_act(text,valid):
    p=FakePage([]);p.execute_script=lambda *a:text
    if valid: c.verify_detail(p,url(1),{'titulo':'Compra','entidad':'MINSA'})
    else:
        with pytest.raises(c.ListingIncomplete): c.verify_detail(p,url(1),{'titulo':'Compra','entidad':'MINSA'})


def test_missing_detail_cannot_become_success():
    p=FakePage([]);p.execute_script=lambda *a:c.process_key(url(1))
    with pytest.raises(c.ListingIncomplete):c.verify_detail(p,url(1),{'titulo':'No Disponible','entidad':'MINSA'})


def test_failed_detail_survives_restart_and_resolves_after_publication(tmp_path,capsys):
    listing={'links':[url(1),url(2)],'pages':[],'complete':True,'error':''}
    a=c.DetailCoverage(tmp_path,'clv',listing)
    a.failure(url(1),'timeout')
    b=c.DetailCoverage(tmp_path,'clv',listing)
    assert c.process_key(url(1)) in b.pending
    b.success(url(1))
    assert c.process_key(url(1)) in json.loads(b.path.read_text())
    b.finish()
    assert not json.loads(b.path.read_text())
    assert not c.coverage_problem(capsys.readouterr().out)


def test_partial_run_is_reported_to_orchestrator(tmp_path,capsys):
    a=c.DetailCoverage(tmp_path,'rir1',{'links':[url(1)],'pages':[],'complete':True,'error':''})
    a.failure(url(1),'timeout');a.finish()
    assert '1 detalles pendientes' in c.coverage_problem(capsys.readouterr().out)


def test_technical_discard_is_distinct_from_a_real_discard():
    assert c.technical_discard([url(1),'skip_timeout_xpath'])
    assert not c.technical_discard([url(1),'2026-09-17'])


@pytest.mark.parametrize('job',['clv','clrir','rir1'])
def test_every_production_job_uses_verified_listing_and_details(job):
    root=Path(__file__).resolve().parents[1]
    tree=ast.parse((root/job/(job+'.py')).read_text(encoding='utf-8'))
    main=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='main')
    calls={n.func.id for n in ast.walk(main) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)}
    assert {'collect_listing','DetailCoverage','verify_detail','technical_discard'}<=calls
    assert 'skip_timeout_xpath' not in ast.unparse(main)


def api_response(records, size=5000, status=1):
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'status':status,'result':{'registros':records,'registrosPorPagina':size}}
    return Response()


def api_record(n):
    return {'numProceso':c.process_key(url(n)), 'idProcesosContratacionFlujos':n,
            'idTipoProceso':2,'idEstado':8,'prefijo':'CL','nombreModalidad':'Global'}


def test_api_recovery_splits_saturated_ranges_without_gaps():
    from types import SimpleNamespace
    calls=[]
    def request(endpoint, json, timeout):
        calls.append(json)
        return api_response([api_record(1),api_record(2)],2) if len(calls)==1 else api_response([api_record(len(calls)-1)],2)
    r=c.recover_listing({'links':[],'pages':[],'complete':False,'error':'UI bloqueada'},SimpleNamespace(adjudication_by_url={}), 'clv',lambda *a:None, request=request)
    assert r['complete'] and len(r['links'])==2
    from datetime import datetime,timedelta
    left=calls[1]['filtro'];right=calls[2]['filtro']
    assert datetime.fromisoformat(left['fechaHasta'])+timedelta(milliseconds=1)==datetime.fromisoformat(right['fechaDesde'])


def test_api_outage_is_bounded_retains_ui_rows_and_is_partial():
    from types import SimpleNamespace
    calls=[]
    def request(*a,**kw): calls.append(1);return api_response([],status=0)
    r=c.recover_listing({'links':[url(1)],'pages':[],'complete':False,'error':'UI bloqueada'},SimpleNamespace(adjudication_by_url={}), 'clv',lambda *a:None, request=request)
    assert not r['complete'] and r['links']==[url(1)] and len(calls)==6


def test_api_recovery_updates_modalities_and_removes_stale_state_links():
    from types import SimpleNamespace
    page=SimpleNamespace(adjudication_by_url={})
    r=c.recover_listing({'links':[url(2)],'pages':[],'complete':False,'error':'UI bloqueada'},page,'clv',lambda *a:None,request=lambda *a,**kw:api_response([api_record(1)]))
    assert r['complete'] and len(r['links'])==1 and c.process_key(r['links'][0])==c.process_key(url(1))
    assert page.adjudication_by_url[r['links'][0]]=='Global'


def test_database_api_splits_full_windows_and_does_not_hide_failures():
    from datetime import datetime,timedelta,timezone
    root=Path(__file__).resolve().parents[1]
    path=root/'db/db_api_updater.py'
    if not path.exists(): pytest.skip('Legacy branch has no API database updater')
    tree=ast.parse(path.read_text(encoding='utf-8'))
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in {'api_iso','listing_payload','fetch_listing'}]
    calls=[]
    def request(method,endpoint,payload):
        calls.append(payload)
        return {'result':{'registros':[1,2,3] if len(calls)==1 else [len(calls)]}}
    from typing import Any
    env={'datetime':datetime,'timedelta':timedelta,'timezone':timezone,'Any':Any,'API_PAGE_SIZE':3,
         'LIST_ENDPOINT':'https://example.test','request_json':request,'log':lambda *a:None}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'db_listing_functions','exec'),env)
    start=datetime(2026,9,1,tzinfo=timezone.utc);end=start+timedelta(days=1)
    assert env['fetch_listing'](1011,start,end)==[2,3]
    assert len(calls)==3
    left=calls[1]['filtro'];right=calls[2]['filtro']
    assert datetime.fromisoformat(left['fechaHasta'])+timedelta(milliseconds=1)==datetime.fromisoformat(right['fechaDesde'])
    env['request_json']=lambda *a,**kw: (_ for _ in ()).throw(RuntimeError('API no disponible'))
    with pytest.raises(RuntimeError,match='no disponible'):env['fetch_listing'](1011,start,end)
