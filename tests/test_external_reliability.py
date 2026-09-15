import json
from types import SimpleNamespace

import pytest
import requests

from otras_fuentes.adapters.ungm import UngmAdapter, UngmInternationalAdapter
from otras_fuentes.adapters.acp_sli import AcpSliAdapter
from otras_fuentes.adapters.ensa import EnsaAdapter
from otras_fuentes.adapters.supplier_portals import NaturgyAdapter, AesAdapter
from otras_fuentes.adapters.idb import IdbAdapter
from otras_fuentes.http import ResilientHttpClient
from otras_fuentes.models import Opportunity, SourceFetchResult
from otras_fuentes.storage import OpportunityStore
from otras_fuentes.enrichment import DetailEnricher


def listing(ids, total):
    return ''.join(f'''<div class="dataRow" data-noticeid="{i}">
        <div class="resultTitle"><span class="ungm-title">Suministro chiller {i}</span></div>
        <div class="tableCell">a</div><div class="tableCell">b</div><div class="tableCell">c</div>
        <div class="tableCell">15-Sep-2026</div><div class="tableCell">UNDP</div>
        <div class="tableCell">RFQ</div><div class="tableCell">REF-{i}</div><div class="tableCell">Costa Rica</div>
        <div data-description="Deadline"><span>30-Sep-2099</span></div></div>''' for i in ids) + f'<script>var noticeTotal = "{total}";</script>'


class PagedClient:
    def __init__(self, pages): self.pages = iter(pages); self.calls = []
    def get(self, *a, **kw): return SimpleNamespace(response=SimpleNamespace(text=''))
    def post(self, *a, **kw):
        self.calls.append(kw['json'])
        result = next(self.pages)
        if isinstance(result, Exception): raise result
        return SimpleNamespace(response=SimpleNamespace(text=result))


@pytest.mark.parametrize('repeat', [False,True])
def test_acp_page_one_alias_is_not_a_new_page_but_repeated_page_two_is_detected(repeat):
    base='https://apps.pancanal.com/sli/LicitacionesBusqueda/BusquedaLicitacionesResultados'
    def page(code, target):
        return f'''<div><a href="/sli/LicitacionesBusqueda/RedirectLicitaciones?NumeroLicitacion={code}">{code}</a>
          Chiller Agente de compras Ana Fecha de publicación 15-sep-2026 Última revisión 15-sep-2026
          Fecha y hora de cierre 30-sep-2099 11:00 AM Unidad de compras A # Enmienda 0</div>
          <a href="/sli/LicitacionesBusqueda/BusquedaLicitacionesResultados?pagina={target}">Página</a>'''
    calls=[]
    def get(url,**kw):
        calls.append(url)
        body='<form id="frmBusqueda3" action="/search"></form>' if url==AcpSliAdapter.url else page('1' if repeat else '2',1)
        return SimpleNamespace(response=SimpleNamespace(text=body,url=url))
    def post(url,**kw):
        body=page('1',2) if kw['data']['status']=='AN' else 'No hay licitaciones'
        return SimpleNamespace(response=SimpleNamespace(text=body,url=base))
    result=AcpSliAdapter(SimpleNamespace(get=get,post=post)).fetch()
    assert result.status==('partial' if repeat else 'success')
    assert not any('pagina=1' in u for u in calls)
    assert len(result.opportunities)==(1 if repeat else 2)


def test_ungm_reads_beyond_old_limit_and_verifies_official_total():
    pages = [listing(range(n, min(n+15, 317)),317) for n in range(0,317,15)]
    result = UngmAdapter(PagedClient(pages)).fetch()
    assert result.status == 'success' and len(result.opportunities) == 317
    assert result.pages_fetched == 22


@pytest.mark.parametrize('ending', [requests.HTTPError('429'), listing(range(15),40), '<html>Temporary maintenance</html>'])
def test_ungm_mid_capture_failure_keeps_first_page(ending, tmp_path):
    result = UngmAdapter(PagedClient([listing(range(15),40), ending])).fetch()
    assert result.status == 'partial' and len(result.opportunities) == 15
    store=OpportunityStore.sqlite(tmp_path/'test.db')
    store.ingest_source('run','start','end',result)
    row=store.connection.execute('SELECT baseline_completed,last_success_at FROM external_sources').fetchone()
    assert row[0] == 0 and row[1] is None
    assert store.connection.execute('SELECT count(*) FROM external_opportunities').fetchone()[0] == 15
    store.close()


def test_ungm_limit_never_claims_complete(monkeypatch):
    monkeypatch.setenv('OTRAS_FUENTES_UNGM_PAGES','1')
    result=UngmAdapter(PagedClient([listing(range(15),30)])).fetch()
    assert result.status == 'partial' and 'límite' in result.coverage


def test_ungm_official_zero_is_success_but_empty_html_is_not():
    assert UngmAdapter(PagedClient([listing([],0)])).fetch().status == 'success'
    assert UngmAdapter(PagedClient([''])).fetch().status != 'success'


def test_ungm_global_failure_does_not_discard_regional_capture():
    result=UngmInternationalAdapter(PagedClient([listing([1,2],2), requests.HTTPError('429')])).fetch()
    assert result.status == 'partial' and len(result.opportunities)==2
    assert 'global' in result.error


def response(code, headers=None):
    r=requests.Response();r.status_code=code;r.headers.update(headers or {});r._content=b'';r._content_consumed=True
    r.url='https://example.test';return r


def test_http_honors_retry_after_and_keeps_custom_timeout():
    replies=iter([response(429,{'Retry-After':'35'}),response(200)])
    calls=[];sleeps=[]
    session=SimpleNamespace(headers={},request=lambda *a,**kw:(calls.append(kw) or next(replies)))
    client=ResilientHttpClient(session=session,sleeper=sleeps.append,minimum_interval=0)
    client.get('https://example.test',timeout=9)
    assert sleeps == [35.0] and [c['timeout'] for c in calls]==[9,9]


@pytest.mark.parametrize('code',[400,403,409])
def test_http_does_not_retry_permanent_client_errors(code):
    calls=[]
    session=SimpleNamespace(headers={},request=lambda *a,**kw:(calls.append(1) or response(code)))
    with pytest.raises(requests.HTTPError):
        ResilientHttpClient(session=session,sleeper=lambda _:pytest.fail('No retry'),minimum_interval=0).get('https://example.test')
    assert len(calls)==1


def test_ungm_clients_share_rate_limit(monkeypatch):
    import otras_fuentes.http as http
    now=[100.0]; waits=[]
    monkeypatch.setattr(http.time,'monotonic',lambda:now[0])
    monkeypatch.setattr(ResilientHttpClient,'_host_last',{})
    monkeypatch.setattr(ResilientHttpClient,'_host_cooldown',{})
    def sleep(n): waits.append(n);now[0]+=n
    for _ in range(2):
        client=ResilientHttpClient(sleeper=sleep,session=SimpleNamespace(headers={},request=lambda *a,**kw:response(200)))
        client.get('https://www.ungm.org/Public/Notice')
    assert waits==[4.0]


def test_http_long_retry_after_defers_instead_of_retrying_early():
    calls=[]
    session=SimpleNamespace(headers={},request=lambda *a,**kw:(calls.append(1) or response(429,{'Retry-After':'120'})))
    with pytest.raises(requests.HTTPError):
        ResilientHttpClient(session=session,sleeper=lambda _:pytest.fail('No blocking wait'),minimum_interval=0).get('https://example.test')
    assert len(calls)==1


@pytest.mark.parametrize('adapter,html',[(NaturgyAdapter,'Evaluación inicial compraseinstalaciones'),(AesAdapter,'Ariba invitación precalificación')])
def test_supplier_portals_create_no_tenders_or_alerts(adapter,html,tmp_path):
    client=SimpleNamespace(get=lambda *a,**kw:SimpleNamespace(response=SimpleNamespace(text=f'<main>{html}</main>')))
    store=OpportunityStore.sqlite(tmp_path/'access.db')
    for run in ('first','repeat'):
        result=adapter(client).fetch()
        assert result.status=='access_required' and not result.opportunities
        stats=store.ingest_source(run,'start','end',result)
        assert stats.events==stats.new==0
    row=store.connection.execute('SELECT baseline_completed,last_success_at FROM external_sources').fetchone()
    assert row[0]==0 and row[1] is None
    assert store.connection.execute('SELECT count(*) FROM external_opportunities').fetchone()[0]==0
    store.close()


def test_portal_broken_html_is_error_not_access_verified():
    client=SimpleNamespace(get=lambda *a,**kw:SimpleNamespace(response=SimpleNamespace(text='<h1>maintenance</h1>')))
    assert NaturgyAdapter(client).fetch().status=='error'


def test_enrichment_amendment_forces_fresh_detail_and_attachments(tmp_path):
    reads=[]
    def fetch(url):
        reads.append(url)
        return {'status':'ok','text':'Suministro e instalación de chiller con controles',
                'links':[{'title':'Anexo','url':'https://example.test/doc.pdf'}] if not url.endswith('pdf') else []}
    for revision, count in [('1',2),('1',2),('2',4)]:
        item=Opportunity('acp_sli','1','Chiller','https://example.test/1',deadline='2099-01-01',
                         raw_payload={'official_updated_at':revision})
        e=DetailEnricher(tmp_path/'details.db',fetcher=fetch);e.enrich([item]);e.close()
        assert len(reads)==count and item.raw_payload['document_analysis']['attachments_read']==1


def test_retry_failure_keeps_previous_document_text(tmp_path):
    e=DetailEnricher(tmp_path/'details.db',fetcher=lambda u:{'status':'ok','text':'Especificaciones del chiller'})
    e.revisions={'https://test':'1'};e._read_cached(['https://test'])
    e.revisions={'https://test':'2'};e.fetcher=lambda u: {'status':'error','text':''}
    second=e._read_cached(['https://test']);e.close()
    assert second['https://test']['using_previous'] and second['https://test']['source_revision']=='1'


def test_amendment_without_remaining_budget_is_reported_as_pending(tmp_path):
    e=DetailEnricher(tmp_path/'cache.db',fetcher=lambda u:{'status':'ok','text':'Texto anterior'})
    e.revisions={'https://test':'1'};e._read_cached(['https://test'])
    e.revisions={'https://test':'2'};e.budget=0
    result=e._read_cached(['https://test'])['https://test'];e.close()
    assert result['status']=='partial' and result['using_previous']
    assert result['text']=='Texto anterior'


def test_idb_fallback_checks_csv_and_preserves_official_identity():
    from datetime import date
    calls=[]
    def get(url,**kw):
        calls.append(url)
        if 'datastore_search' in url: raise requests.HTTPError('409')
        if 'package_show' in url:
            return SimpleNamespace(response=SimpleNamespace(json=lambda:{'result':{'resources':[
                {'id':IdbAdapter.resource_id,'format':'CSV','url':'https://data.iadb.org/files/download/notice'}]}}))
        return SimpleNamespace(response=SimpleNamespace(status_code=200,
            iter_content=lambda n:iter([f'noticeid,noticetitle,publicationdate,deadline,type\n1,Chiller,{date.today().isoformat()},01/31/2099,SPECIFIC\n'.encode()]),close=lambda:None))
    result=IdbAdapter(SimpleNamespace(get=get)).fetch()
    assert result.status=='success' and result.opportunities[0].external_id=='1'
    assert result.opportunities[0].deadline=='2099-01-31'
    assert len(calls)==3


@pytest.mark.parametrize('value,expected',[('10/31/2022','2022-10-31'),('4/5/2026','2026-04-05'),('NULL',''),('bad','')])
def test_idb_csv_dates_never_become_future_opportunities_by_string_comparison(value,expected):
    assert IdbAdapter._official_date(value,csv_format=True)==expected


def test_idb_stale_csv_cannot_replace_current_source_or_create_tenders():
    def get(url,**kw):
        if 'datastore_search' in url: raise requests.HTTPError('409')
        if 'package_show' in url:
            return SimpleNamespace(response=SimpleNamespace(json=lambda:{'result':{'resources':[
                {'id':IdbAdapter.resource_id,'format':'CSV','url':'https://data.iadb.org/files/download/notice'}]}}))
        return SimpleNamespace(response=SimpleNamespace(status_code=200,iter_content=lambda n:iter([
            b'noticeid,noticetitle,publicationdate,deadline,type\n1,Chiller,2020-01-01,8/31/2020,SPECIFIC\n']),close=lambda:None))
    result=IdbAdapter(SimpleNamespace(get=get)).fetch()
    assert result.status=='error' and not result.opportunities and 'desactualizado' in result.error


def test_idb_empty_broken_datastore_is_not_reported_as_success():
    client=SimpleNamespace(get=lambda *a,**kw:SimpleNamespace(response=SimpleNamespace(json=lambda:{
        'success':True,'result':{'records':[],'fields':[{'id':'_id'}],'total':0}})))
    assert IdbAdapter(client).fetch().status=='error'


def test_idb_ancient_undated_general_notices_are_not_imported_as_active():
    payload={'success':True,'result':{'records':[{'noticeid':'1','noticetitle':'Aviso antiguo',
        'publicationdate':'2020-01-01','deadline':'NULL','type':'GENERAL'}]}}
    client=SimpleNamespace(get=lambda *a,**kw:SimpleNamespace(response=SimpleNamespace(json=lambda:payload)))
    result=IdbAdapter(client).fetch()
    assert result.status=='success' and not result.opportunities


def test_late_document_match_alerts_once_without_listing_change(tmp_path):
    store=OpportunityStore.sqlite(tmp_path/'late.db')
    item=Opportunity('ungm','1','Proceso de contratación','https://test/1',deadline='2099-01-01',
                     raw_payload={'qualification':{'bucket':'review','deadline_date':'2099-01-01'}})
    store.ingest_source('baseline','start','end',SourceFetchResult('ungm',[item]))
    original_hash=item.content_hash
    item.matched_company='RS/SP'
    item.raw_payload['qualification']['bucket']='relevant'
    item.raw_payload['document_analysis']={'status':'ok','text':'Suministro de chiller y agua helada'}
    assert item.content_hash==original_hash
    first=store.ingest_source('enriched','start','end',SourceFetchResult('ungm',[item]))
    assert first.changed==0 and first.events==1
    assert store.ingest_source('repeat','start','end',SourceFetchResult('ungm',[item])).events==0
    item.matched_company='';item.raw_payload['qualification']['bucket']='review'
    store.ingest_source('unmatched','start','end',SourceFetchResult('ungm',[item]))
    item.matched_company='RS/SP';item.raw_payload['qualification']['bucket']='relevant'
    assert store.ingest_source('matched_again','start','end',SourceFetchResult('ungm',[item])).events==0
    store.close()
