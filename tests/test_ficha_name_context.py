import pandas as pd
import pytest

from common.ficha_name_context import catalog_name_context_allowed
from common.ficha_utils import detectar_fichas_tokens
from common.keyword_watch import match_keyword_fields, negative_keywords_in_matching_context


@pytest.mark.parametrize('text', ['soldadura de plata al 5', 'soldaduras de plata', 'soldadura plata', 'soldadura de plata y soldadura plata'])
def test_silver_solder_is_not_a_medical_name(text):
    assert not catalog_name_context_allowed('plata',text)


@pytest.mark.parametrize('text', ['plata coloidal', 'nitrato de plata', 'soldadura de plata y nitrato de plata', 'plata para uso medico'])
def test_medical_mentions_are_retained_even_in_mixed_acts(text):
    assert catalog_name_context_allowed('plata',text)


@pytest.fixture
def catalog(tmp_path):
    path=tmp_path/'catalog.xlsx'
    pd.DataFrame({'Ficha':['40229','43358'],'Nombre':['PLATA','KIT DE CIRCUITO DE PACIENTE PARA MAQUINA DE ANESTESIA']}).to_excel(path,index=False)
    return path


def test_full_detector_only_suppresses_industrial_name(catalog):
    assert detectar_fichas_tokens('SOLDADURA DE PLATA AL 5%',path=catalog)==[]
    assert detectar_fichas_tokens('Ficha técnica 40229: soldadura de plata',path=catalog)==['40229']
    assert detectar_fichas_tokens('Soldadura de plata y plata coloidal',path=catalog)==['* 40229']
    assert detectar_fichas_tokens('KIT DE CIRCUITO DE PACIENTE PARA MAQUINA DE ANESTESIA',path=catalog)==['* 43358']


def test_atlapa_uses_existing_hvac_keywords_and_amount_without_medical_ficha(catalog):
    title='ADQUISICIÓN E INSTALACIÓN DE DIEZ (10) AIRES ACONDICIONADOS PARA EL CENTRO DE CONVENCIONES ATLAPA'
    fields=[('titulo',title),('descripcion',title),('item_16','SOLDADURA DE PLATA AL 5%'),('item_26','SERVICIO DE INSTALACIÓN DE 10 AIRES ACONDICIONADOS')]
    assert detectar_fichas_tokens(' '.join(value for _,value in fields),path=catalog)==[]
    result=match_keyword_fields(fields,['aires acondicion*>8k'],reference_amount='B/. 24,088.06',adjudication_type='Global')
    assert result.terms==('aires acondicion*>8k',)
    assert not negative_keywords_in_matching_context(title=title,matched_field_values=result.field_values,negative_keywords=['automotriz','protector solar'])
    assert not match_keyword_fields(fields,['aires acondicion*>8k'],reference_amount=8000).terms


def test_solder_alone_does_not_become_an_rs_sp_opportunity(catalog):
    assert detectar_fichas_tokens('SOLDADURA DE PLATA',path=catalog)==[]
    assert not match_keyword_fields([('titulo','Soldadura de plata')],['aires acondicion*>8k'],reference_amount=25000).terms
