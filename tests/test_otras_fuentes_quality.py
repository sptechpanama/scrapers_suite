import json
from datetime import datetime, timezone
from dataclasses import replace
import pytest

from otras_fuentes.classifier import classify_opportunity, should_alert
from otras_fuentes.models import Opportunity, SourceDocument, SourceFetchResult
from otras_fuentes.qualification import deadline_info, effective_bucket
from otras_fuentes.enrichment import DetailEnricher, extract_deadline
from otras_fuentes.storage import OpportunityStore
from otras_fuentes.reclassify import reclassify_store


def notice(title='Chiller HVAC', **kwargs):
    return classify_opportunity(Opportunity(source='ungm', external_id='42', title=title,
        source_url='https://www.ungm.org/Public/Notice/42', deadline='2099-10-01', **kwargs))


@pytest.mark.parametrize('raw,expected', [
    ('julio 3, 2026','2026-07-03'), ('30 de ABRIL de 2026','2026-04-30'),
    ('18-Sep-2026 16:00 (GMT -4.00)','2026-09-18'), ('24/09/2026 11:00 am','2026-09-24'),
    ('2099-10-01','2099-10-01'), ('31/02/2026',''), ('Por confirmar',''), ('',''),
])
def test_deadlines_are_validated_not_guessed(raw, expected):
    assert deadline_info(raw)[0] == expected


def test_expiry_uses_explicit_hour_and_retains_date_only_until_end_of_day():
    item = notice(raw_payload={'deadline_raw':'18-Sep-2026 16:00 (GMT -4.00)'})
    q = item.raw_payload['qualification']
    assert q['deadline_at'] == '2026-09-18T20:00:00+00:00'
    assert effective_bucket(q, now=datetime(2026,9,18,19,59,tzinfo=timezone.utc)) == 'relevant'
    assert effective_bucket(q, now=datetime(2026,9,18,20,1,tzinfo=timezone.utc)) == 'historical'
    q.update(deadline_at='',deadline_date='2026-09-18')
    assert effective_bucket(q, now=datetime(2026,9,19,2,tzinfo=timezone.utc)) == 'relevant'
    assert effective_bucket(q, now=datetime(2026,9,19,5,1,tzinfo=timezone.utc)) == 'historical'


@pytest.mark.parametrize('title', [
    'Psychological testing for security officers in New York',
    'Servicios Laboratorio de Suelos', 'Diagnóstico archivístico de documentación',
    'Consultoría diagnóstico de cultura organizacional',
])
def test_known_false_positives_are_not_alerts(title):
    item = notice(title)
    assert not item.matched_company
    assert not should_alert(item)


@pytest.mark.parametrize('title,company', [
    ('Chiller York y manejadoras de agua helada','RS/SP'),
    ('Equipos médicos para diagnóstico clínico','RIR'),
    ('Supply of medical devices and laboratory equipment','RIR'),
    ('Sistema fotovoltaico','RS/SP'), ('Cambio de aires acondicionados','RS/SP'),
])
def test_specific_opportunities_survive(title, company):
    item = notice(title)
    assert company in item.matched_company
    assert should_alert(item)


def test_buyer_alone_cannot_make_it_a_medical_supply():
    item = notice('Licencias software financiero', buyer='Hospital de diagnostico')
    assert not item.matched_company
    assert not should_alert(item)


def test_unknown_date_is_review_and_extension_reopens():
    item = replace(notice(), deadline='por definir')
    classify_opportunity(item)
    assert item.raw_payload['qualification']['bucket'] == 'review'
    assert not should_alert(item)
    item.deadline = '2020-01-01'; classify_opportunity(item)
    assert effective_bucket(item.raw_payload['qualification']) == 'historical'
    item.deadline = '2099-01-01'; classify_opportunity(item)
    assert should_alert(item)


def test_foreign_restriction_is_review_not_deleted():
    item = notice('Sistemas fotovoltaicos - Solo Proveedores Nacionales', country='Honduras')
    assert item.matched_company == 'RS/SP'
    assert item.raw_payload['qualification']['local_only']
    assert effective_bucket(item.raw_payload['qualification']) == 'review'
    assert not should_alert(item)


def test_recent_local_only_notice_remains_eligible_for_reviewing_costs():
    item = notice('Sistemas fotovoltaicos - Solo Proveedores Nacionales')
    assert effective_bucket(item.raw_payload['qualification']) == 'relevant'


def test_generic_pdf_recovered_and_official_deadline_extracted(tmp_path):
    content = 'Relacionado a: CAMBIO DE UNA UNIDAD DE AIRE ACONDICIONADO. Fecha de entrega de sus respuestas: a más tardar el 30 de ABRIL de 2026.'
    day = extract_deadline(content)
    assert day == '2026-04-30'
    assert extract_deadline('Ley del 30 de abril de 2026') == ''
    assert extract_deadline('Fecha de cierre 10/09/2026. Fecha límite 12/09/2026.') == ''
    assert extract_deadline('Fecha final para licitar: 20/08/2026 11:00 am. Lugar de Presentación de Propuestas: No aplica Fecha de Subsanación: 13/08/2026') == '2026-08-20'
    item = replace(notice(),source='ena',title='SOLICITUD DE INFORMACIÓN',deadline='',
        documents=[SourceDocument('Documento','https://ena.com.pa/a.pdf')])
    classify_opportunity(item)
    enricher = DetailEnricher(tmp_path/'cache.db',fetcher=lambda url: {'status':'ok','text':content,'deadline':day})
    enricher.enrich([item]); classify_opportunity(item)
    assert item.matched_company == 'RS/SP'
    assert effective_bucket(item.raw_payload['qualification']) == 'historical'
    enricher.close()


def test_automotive_hvac_does_not_enter_building_services():
    item = notice('Mantenimiento de aire acondicionado flota ENSA')
    assert effective_bucket(item.raw_payload['qualification']) == 'no_match'
    assert not should_alert(item)


def test_integral_construction_remains_available_for_review():
    item = notice('Construcción hospital con equipos médicos')
    assert item.matched_company == 'RIR'
    assert effective_bucket(item.raw_payload['qualification']) == 'review'


def test_cached_text_survives_failed_refresh_and_is_marked_for_review(tmp_path):
    item = notice()
    e = DetailEnricher(tmp_path/'cache.db',fetcher=lambda url:{'status':'ok','text':'Chiller HVAC','deadline':'2099-10-01'})
    e.enrich([item])
    e.connection.execute('UPDATE detail_cache SET attempted=0');e.connection.commit()
    def fail(url): raise TimeoutError('network')
    e.fetcher=fail
    e.enrich([item]);classify_opportunity(item)
    assert item.raw_payload['document_analysis']['text'] == 'Chiller HVAC'
    assert item.raw_payload['document_analysis']['using_previous']
    assert effective_bucket(item.raw_payload['qualification']) == 'review'
    e.close()


def test_silent_reclassification_preserves_dates_and_is_idempotent(tmp_path):
    store = OpportunityStore.sqlite(tmp_path/'data.db')
    item = notice()
    store.ingest_source('a','2026-09-01','2026-09-01',SourceFetchResult('ungm',[item]))
    before = dict(store.connection.execute('select * from external_opportunities').fetchone())
    reclassify_store(store,apply=True)
    second = reclassify_store(store,apply=True)
    after = dict(store.connection.execute('select * from external_opportunities').fetchone())
    assert second['changes'] == 0
    assert before['last_seen_at'] == after['last_seen_at']
    assert before['first_seen_at'] == after['first_seen_at']
    assert store.connection.execute('select count(*) from external_alert_events').fetchone()[0] == 0
    store.close()


def test_same_official_notice_has_one_alert_across_sources(tmp_path):
    store = OpportunityStore.sqlite(tmp_path/'data.db')
    for src in ['ungm','ungm_international']:
        store.ingest_source('base'+src,'2026-09-01','2026-09-01',SourceFetchResult(src,[]))
    for src in ['ungm','ungm_international']:
        item = replace(notice(),source=src)
        store.ingest_source('new'+src,'2026-09-02','2026-09-02',SourceFetchResult(src,[item]))
    assert store.connection.execute('select count(*) from external_opportunities').fetchone()[0] == 2
    assert store.connection.execute('select count(*) from external_alert_events').fetchone()[0] == 1
    store.close()
