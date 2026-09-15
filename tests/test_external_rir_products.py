import json
import pytest

from otras_fuentes.classifier import classify_opportunity
from otras_fuentes.models import Opportunity, SourceFetchResult
from otras_fuentes.profiles import load_profiles, read_existing_profiles, read_catalog_names
from otras_fuentes.rir_products import match_products
from otras_fuentes.storage import OpportunityStore


@pytest.mark.parametrize('name,notice', [
    ('KIT DE CIRCUITO DE PACIENTE PARA MAQUINA DE ANESTE...', 'Compra de circuitos de anestesia'),
    ('KIT DE CIRCUITO DE PACIENTE PARA MAQUINA DE ANESTE...', 'Breathing circuits for anaesthesia'),
    ('MASCARILLA PARA TRAQUEOSTOMIA', 'Mascarillas desechables de traqueostomía'),
    ('MASCARILLA PARA TRAQUEOSTOMIA', 'Tracheostomy masks'),
    ('TUBO ENDOTRAQUEAL CON BALON.', 'Tubos endotraqueales con balón'),
    ('TUBO ENDOTRAQUEAL CON BALON.', 'Endotracheal cuffed tubes'),
    ('CIRCUITO RESPIRATORIO DE ANESTESIA PEDIATRICO EXPA...', 'Circuitos respiratorios'),
    ('CIRCUITO RESPIRATORIO DE ANESTESIA PARA ADULTO CON...', 'Adult anesthesia circuits'),
    ('RESPIRADOR FACIAL CONTRA PARTICULAS DE ALTA FILTRA...', 'Mascarillas N95'),
    ('CIRCUITO DE VENTILADOR CON SISTEMA DE CALEFACCIÓN', 'Tubuladura y circuito de ventilador'),
    ('PAQUETE DE PRUEBA DE PENETRACION Y EXTRACCION DE A...', 'Bowie-Dick test packs'),
    ('KIT DE MICRONEBULIZADOR PEDIATRICO Y ADULTO', 'Nebulizadores con accesorios'),
    ('HUMIDIFICADOR DE ALTO FLUJO', 'Humificadores de alto flujo'),
    ('EQUIPO PARA NEBULIZACION DE ADULTOS Y NIÑOS.', 'Nebuliser kits'),
    ('CIRCUITO RESPIRATORIO NEONATAL PARA ANESTESIA', 'Circuitos respiratorios neonatales'),
    ('CIRCUITO UNIVERSAL PARA VENTILADOR DE 72" DE LARGO...', 'Ventilator circuits'),
    ('ENVOLTURA PARA ESTERILIZAR NO TEJIDA SMS', 'Envolturas SMS'),
    ('ENVOLTURA PARA ESTERILIZAR NO TEJIDA SMS', 'Sterilization wraps'),
    ('BOLSA TERMOSELLABLE PARA ESTERILIZACION', 'Bolsas termosellables para esterilizacion'),
])
def test_product_names_detect_without_number(name, notice):
    products = [{'ficha':'43358','name':name}]
    item = Opportunity('acp_sli','notice',notice,'https://test/notice')
    classify_opportunity(item, {'rs':[], 'negative':[], 'fichas':['43358'], 'rir_products':products})
    assert item.matched_company == 'RIR'
    assert item.raw_payload['rir_product_matches'][0]['ficha'] == '43358'
    assert item.raw_payload['explicit_fichas'] == []
    assert 'Ficha 43358' not in item.matched_keywords


@pytest.mark.parametrize('name,notice', [
    ('KIT DE CIRCUITO DE PACIENTE PARA MAQUINA DE ANESTE...', 'Reparación de circuitos eléctricos'),
    ('CIRCUITO UNIVERSAL PARA VENTILADOR DE 72" DE LARGO...', 'Ventiladores de techo'),
    ('CIRCUITO UNIVERSAL PARA VENTILADOR DE 72" DE LARGO...', 'Todos los circuitos de control se desactivaran y el ventilador de suministro no funcionara'),
    ('CIRCUITO UNIVERSAL PARA VENTILADOR DE 72" DE LARGO...', 'Circuitos electricos del ventilador de techo'),
    ('MASCARILLA PARA TRAQUEOSTOMIA', 'Mascarillas para pintura automotriz'),
    ('EQUIPO PARA NEBULIZACION DE ADULTOS Y NIÑOS.', 'Nebulizadores para fumigación de mosquitos'),
    ('EQUIPO PARA NEBULIZACION DE ADULTOS Y NIÑOS.', 'Nebulización de plaguicidas agrícolas'),
    ('EQUIPO PARA NEBULIZACION DE ADULTOS Y NIÑOS.', 'Salbutamol 5mg/ml solución para nebulizar'),
    ('KIT DE MICRONEBULIZADOR PEDIATRICO Y ADULTO', 'Bromuro de ipratropio 250 mcg/ml sol para nebulizar'),
    ('HUMIDIFICADOR DE ALTO FLUJO', 'Humidificador ambiental para oficinas'),
    ('ENVOLTURA PARA ESTERILIZAR NO TEJIDA SMS', 'Mensajería SMS'),
    ('CILINDRO', 'Cilindros hidráulicos'),
    ('CUÑA', 'Cuñas para ruedas'),
    ('LENTE', 'Lentes para cámaras'),
    ('Ficha técnica 43358', 'Ficha técnica 43358'),
    ('MASCARILLA PARA TRAQUEOSTOMIA', 'Mascarilla ' + 'especificaciones generales '*50 + 'traqueostomía'),
])
def test_product_guards(name,notice):
    assert not match_products(notice,[{'ficha':'1','name':name}],'titulo')


def test_title_description_and_attachment_are_checked_but_no_cross_field_word_join():
    profile={'rs':[],'negative':[],'fichas':['22241'],'rir_products':[{'ficha':'22241','name':'TUBO ENDOTRAQUEAL CON BALON'}]}
    item=Opportunity('acp_sli','n','Tubo industrial','https://test/n',description='Código endotraqueal')
    classify_opportunity(item,profile)
    assert not item.raw_payload['rir_product_matches']
    item.raw_payload['document_analysis']={'text':'Compra de tubos endotraqueales','status':'ok'}
    classify_opportunity(item,profile)
    assert item.raw_payload['rir_product_matches'][0]['field']=='documento'


def test_contractor_safety_clause_is_not_a_medical_purchase():
    products=[{'ficha':'104747','name':'RESPIRADOR FACIAL CONTRA PARTICULAS'}]
    assert not match_products('El personal del contratista debe utilizar respiradores N95 durante los trabajos.', products, 'documento')
    assert match_products('Suministro de respiradores N95 para personal de salud.', products, 'titulo')
    assert match_products('El personal del contratista usará respiradores N95. ' + 'Otras condiciones generales. '*20 + 'Suministro de respiradores N95 para personal de salud.', products, 'documento')


def test_names_cache_resolution_removal_and_empty_watchlist(tmp_path):
    rows={'pc_palabras_clave':[['Palabra clave'],['solar*']], 'pc_palabras_negativas':[['Palabra clave']],
          'ct_rir_fichas':[['Ficha #','Nombre ficha'],['43358','Circuito de anestesia'],['22287','Ficha tecnica 22287']]}
    path=tmp_path/'profiles.json'
    result=load_profiles(path,reader=rows.get,name_resolver=lambda codes:{'22287':'Circuito universal para ventilador'})
    assert len(result['rir_products'])==2 and not result['rir_unresolved_fichas']
    def offline(*args):raise TimeoutError('offline')
    cached=load_profiles(path,reader=offline)
    assert cached['rir_products']==result['rir_products'] and cached['status']=='cached'
    fallback=load_profiles(path,reader=rows.get,name_resolver=offline)
    assert len(fallback['rir_products'])==2
    rows['ct_rir_fichas']=rows['ct_rir_fichas'][:2]
    result=load_profiles(path,reader=rows.get)
    assert [p['ficha'] for p in result['rir_products']]==['43358']
    rows['ct_rir_fichas']=rows['ct_rir_fichas'][:1]
    result=load_profiles(path,reader=rows.get)
    assert result['fichas']==result['rir_products']==[]


def test_unresolved_is_visible_not_numeric_fallback(tmp_path):
    rows={'pc_palabras_clave':[['Palabra clave']], 'pc_palabras_negativas':[['Palabra clave']],
          'ct_rir_fichas':[['Ficha','Nombre'],['99999','Ficha tecnica 99999']]}
    p=load_profiles(tmp_path/'p.json',reader=rows.get,name_resolver=lambda codes:{})
    assert p['rir_unresolved_fichas']==['99999']
    item=classify_opportunity(Opportunity('acp_sli','n','Ficha 99999','https://test/n'),p)
    assert not item.matched_company
    assert item.raw_payload['company_profile']['rir_unresolved_fichas']==['99999']


def test_headerless_catalog_reads_first_row(tmp_path,monkeypatch):
    import openpyxl
    path=tmp_path/'catalog.xlsx';wb=openpyxl.Workbook()
    wb.active.append(['22287','Circuito universal para ventilador']);wb.save(path);wb.close()
    monkeypatch.setenv('FICHAS_CATALOG_PATHS',str(path))
    assert read_catalog_names({'22287'})=={'22287':'Circuito universal para ventilador'}


def test_changed_rules_and_two_identical_runs_do_not_duplicate_or_create_alerts(tmp_path):
    item=Opportunity('acp_sli','n','Tubos endotraqueales con balon','https://test/n',deadline='2099-01-01')
    store=OpportunityStore.sqlite(tmp_path/'db.sqlite')
    classify_opportunity(item,{'rs':[],'fichas':[]})
    original_hash=item.content_hash
    store.ingest_source('baseline','2026-09-14','2026-09-14',SourceFetchResult('acp_sli',[item]))
    for run in ['new_rules','repeat']:
        classify_opportunity(item,{'rs':[],'fichas':['22241'],'rir_products':[{'ficha':'22241','name':item.title}]})
        assert item.content_hash==original_hash and item.matched_company=='RIR'
        stats=store.ingest_source(run,'2026-09-14','2026-09-14',SourceFetchResult('acp_sli',[item]))
        assert stats.new==stats.changed==stats.events==0
    assert store.connection.execute('SELECT count(*) FROM external_opportunities').fetchone()[0]==1
    store.close()
