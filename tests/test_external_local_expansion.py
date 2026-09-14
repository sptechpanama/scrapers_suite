import json
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from otras_fuentes.adapters.acp_sli import AcpSliAdapter
from otras_fuentes.adapters.ciudad_saber import CiudadSaberAdapter
from otras_fuentes.adapters.ensa import EnsaAdapter
from otras_fuentes.adapters.ena import EnaAdapter
from otras_fuentes.adapters.ifrc import IfrcAdapter
from otras_fuentes.adapters.public_pages import official_date
from otras_fuentes.acp_documents import acp_detail
from otras_fuentes.classifier import classify_opportunity
from otras_fuentes.enrichment import DetailEnricher
from otras_fuentes.models import Opportunity, SourceDocument, SourceFetchResult
from otras_fuentes.storage import OpportunityStore


class Client:
    def __init__(self, response): self.response = response
    def get(self, url, **kwargs):
        return SimpleNamespace(response=SimpleNamespace(text=self.response, json=lambda: self.response))


@pytest.mark.parametrize('value,expected', [('07-sep-2026 11:20 AM','2026-09-07'),
    ('21/07/26','2026-07-21'), ('septiembre 16, 2026','2026-09-16'),
    ('05 October 2026','2026-10-05'), ('por definir',''), ('31/02/2026','')])
def test_official_dates_never_invent(value, expected):
    assert official_date(value) == expected


def test_acp_card_and_native_panama_closing_time():
    soup = BeautifulSoup('''<div><a href="/sli/LicitacionesBusqueda/RedirectLicitaciones?NumeroLicitacion=123&amp;HeaderId=456">123</a>
        Chiller de agua helada Agente de compras Ana Fecha de publicación 07-sep-2026 11:20 AM
        Última revisión 08-sep-2026 10:00 AM Fecha y hora de cierre 15-sep-2026 08:30 AM
        Unidad de compras Mecánica # Enmienda 1</div>''', 'html.parser')
    item = AcpSliAdapter.parse_listing(soup, 'EN')[0]
    assert item.title == 'Chiller de agua helada'
    assert item.status == 'Enmendada'
    assert item.raw_payload['deadline_raw'] == '2026-09-15T08:30:00-05:00'
    quality = classify_opportunity(item).raw_payload['qualification']
    assert quality['deadline_date'] == '2026-09-15'
    assert quality['deadline_at'] == '2026-09-15T13:30:00+00:00'


def test_acp_detail_excludes_all_category_sidebar_and_exposes_documents():
    soup = BeautifulSoup('''<form><select><option>Medical Equipment HVAC</option></select></form>
        <main>Licitación No.123 Suministro de taladros
        <a class="Modal_PC_1_2">Pliego</a><a class="selectNumberAttachment" data-anexos="123-1">Anexo</a>
        <a class="selectAI" data-poline="33" data-linedesc="Taladro">Información adicional</a></main>''', 'html.parser')
    body, links = acp_detail(soup, 'https://apps.pancanal.com/sli/LicitacionesBusqueda/RedirectLicitaciones?NumeroLicitacion=123&HeaderId=456')
    assert 'Medical Equipment' not in body and 'HVAC' not in body
    assert len(links) == 3
    assert any('po_lineid=33' in d['url'] for d in links)
    assert not classify_opportunity(Opportunity('acp_sli','123','Taladros','https://test/123',raw_payload={'document_analysis':{'text':body}})).matched_company


def test_ensa_primary_listing_not_limited_by_feed():
    html = '''<article><a href="/licitaciones/chiller-a">Chiller para edificio A</a>Fecha final para licitar: 20/09/2026 10:00 am</article>
              <article><a href="/licitaciones/chiller-b">Chiller para edificio B</a>Fecha final para licitar: 21/09/2026 11:00 am</article>'''
    result = EnsaAdapter(client=Client(html)).fetch()
    assert result.status == 'success'
    assert len(result.opportunities) == 2
    assert result.opportunities[0].deadline == '2026-09-20'


def test_cds_repeated_page_preserves_partial_rows(monkeypatch):
    monkeypatch.setenv('OTRAS_FUENTES_CDS_PAGE_SIZE','9')
    items = [{'id':i, 'title':'Suministro médico '+str(i),'slug':str(i)} for i in range(9)]
    result = CiudadSaberAdapter(client=Client({'data':items,'meta':{'pagination':{'pageCount':3}}})).fetch()
    assert result.status == 'partial' and len(result.opportunities) == 9
    assert 'página' in result.error


def test_partial_source_is_persisted_without_claiming_completed_baseline(tmp_path):
    store = OpportunityStore.sqlite(tmp_path/'test.db')
    item = Opportunity('acp_sli','1','Chiller','https://test/1',deadline='2099-10-01')
    partial = SourceFetchResult('acp_sli',[classify_opportunity(item)],status='partial',error='página 2 falló')
    stats = store.ingest_source('one','2026-09-14','2026-09-14',partial)
    assert stats.new == 1 and stats.events == 0 and not stats.baseline_created
    assert store.connection.execute('SELECT baseline_completed FROM external_sources').fetchone()[0] == 0
    stats = store.ingest_source('two','2026-09-15','2026-09-15',SourceFetchResult('acp_sli',[item]))
    assert stats.new == stats.events == 0 and stats.baseline_created
    store.close()


def test_profiles_preserve_explicit_zero_terms_and_match_only_explicit_fichas():
    profiles = {'rs':[], 'negative':[], 'fichas':['43358']}
    generic = classify_opportunity(Opportunity('acp_sli','1','Chiller UNSPSC 43358','https://test/1'), profiles)
    assert not generic.matched_company
    explicit = classify_opportunity(Opportunity('acp_sli','2','Ficha técnica Nº 43358 circuito de paciente','https://test/2'), profiles)
    assert explicit.matched_company == 'RIR'
    assert explicit.raw_payload['watched_fichas'] == ['43358']


def test_attachment_cache_resumes_and_second_run_is_silent(tmp_path):
    docs = [SourceDocument('Anexo', 'https://test/document.pdf')]
    fetches = []
    def read(url):
        fetches.append(url)
        if url.endswith('pdf'): return {'status':'ok', 'text':'Suministro de dispositivos médicos para pacientes'}
        return {'status':'ok','text':'Convocatoria No 1', 'links':[{'title':d.title,'url':d.url} for d in docs]}
    store = OpportunityStore.sqlite(tmp_path/'test.db')
    for run in ('first','second'):
        item = Opportunity('acp_sli','1','Convocatoria de suministros','https://test/notice',deadline='2099-10-01')
        e = DetailEnricher(tmp_path/'cache.db',fetcher=read)
        e.enrich([item]);e.close();classify_opportunity(item)
        assert item.matched_company == 'RIR'
        assert item.raw_payload['document_analysis']['attachments_read'] == 1
        stats = store.ingest_source(run,'2026-09-14','2026-09-14',SourceFetchResult('acp_sli',[item]))
        assert stats.events == 0
        if run == 'second': assert stats.new == stats.changed == 0
    assert len(fetches) == 2
    store.close()


def test_ena_old_annex_is_linked_to_official_card_without_deletion(tmp_path):
    store = OpportunityStore.sqlite(tmp_path/'test.db')
    old = Opportunity('ena','old-file','Aviso','https://test/notice.pdf')
    new = Opportunity('ena','COT 1-26','Chiller','https://test/bases.pdf',
                      documents=[SourceDocument('Aviso',old.source_url)], raw_payload={'official_number':'COT 1-26'})
    store.ingest_source('old','2026-09-01','2026-09-01',SourceFetchResult('ena',[old]),emit_events=False)
    for run in ('new','repeat'):
        store.ingest_source(run,'2026-09-14','2026-09-14',SourceFetchResult('ena',[new]),emit_events=False)
    rows = store.connection.execute('SELECT id,raw_payload_json FROM external_opportunities').fetchall()
    assert len(rows) == 2
    assert json.loads(next(r['raw_payload_json'] for r in rows if r['id']==old.id))['superseded_by'] == new.id
    assert store.connection.execute('SELECT COUNT(*) FROM external_alert_events').fetchone()[0] == 0
    store.close()


def test_profile_failure_keeps_last_valid_and_empty_list_is_respected(tmp_path):
    from otras_fuentes.profiles import load_profiles
    path=tmp_path/'profiles.json'
    rows={'pc_palabras_clave':[['Palabra clave'],['fotovolta*']], 'pc_palabras_negativas':[['Palabra clave']],
          'ct_rir_fichas':[['Ficha'],['43358']]}
    first=load_profiles(path,reader=lambda sheet:rows[sheet])
    assert first['status']=='updated' and first['negative']==[] and first['rs']==['fotovolta*']
    def fail(sheet): raise TimeoutError('offline')
    cached=load_profiles(path,reader=fail)
    assert cached['status']=='cached' and cached['fichas']==['43358']
    rows['pc_palabras_clave']=[['Palabra clave']]
    assert load_profiles(path,reader=lambda sheet:rows[sheet])['rs']==[]


def test_unknown_budget_is_review_not_lost_and_known_below_threshold_is_excluded():
    rules={'rs':['aire acondicion*>8k'],'negative':[],'fichas':[]}
    item=Opportunity('acp_sli','1','Instalación de aire acondicionado','https://test/1',deadline='2099-10-01')
    classify_opportunity(item,rules)
    assert item.matched_company=='RS/SP'
    assert item.raw_payload['qualification']['bucket']=='review'
    assert item.raw_payload['budget_rules_pending']
    item.estimated_value=7000
    classify_opportunity(item,rules)
    assert not item.matched_company


def test_acp_document_initializes_public_tender_context(monkeypatch):
    import fitz
    import otras_fuentes.enrichment as module
    doc=fitz.open();page=doc.new_page();page.insert_text((30,30),'Tender specifications for supply of industrial pumps and accessories. '*2)
    pdf=doc.tobytes();doc.close()
    calls=[]
    class Http:
        def __init__(self, **kw): self.session=SimpleNamespace(mount=lambda *a:None,close=lambda:None)
        def get(self,url,**kwargs):
            calls.append((url,kwargs))
            return SimpleNamespace(response=SimpleNamespace(headers={'Content-Type':'application/pdf'},
                iter_content=lambda size:iter([pdf]),close=lambda:None))
    monkeypatch.setattr(module,'ResilientHttpClient',Http)
    context='https://apps.pancanal.com/sli/LicitacionesBusqueda/RedirectLicitaciones?NumeroLicitacion=1'
    url='https://apps.pancanal.com/sli/Comunes/ImpresionInformacionAdicional?po_lineid=1'
    result=module.read_document(url,context_url=context)
    assert result['status']=='ok'
    assert [u for u,kw in calls]==[context,url]
    assert 'Referer' in calls[-1][1]['headers']
