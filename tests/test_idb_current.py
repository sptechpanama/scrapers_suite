from types import SimpleNamespace
import pytest
import requests
from otras_fuentes.adapters.idb_current import IdbCurrentAdapter
from otras_fuentes.adapters.idb import IdbAdapter


def record(identifier=1):
    return {'id':identifier,'status':'approved','date_created':'2026-09-15T20:00:00Z',
        'date_close':'2099-09-29T02:59:00Z','date_updated':'2026-09-15T21:00:00Z',
        'amount':1234,'undb_id':'PN-L1148-P00090','tender_url':f'https://tenders-bidfa-app.connectamericas.com/{identifier}.pdf',
        'country':{'translations':[{'languages_code':'es','name':'Panamá'}]},
        'translations':[{'languages_code':'en-US','name':'Chiller'},
                        {'languages_code':'es','name':'Suministro de chiller','executing_unit':'IDAAN','contract_object':'Sistema de agua helada'}]}


class Client:
    def __init__(self,pages):self.pages=iter(pages);self.calls=[]
    def get(self,url,**kw):
        self.calls.append((url,kw));v=next(self.pages)
        if isinstance(v,Exception):raise v
        return SimpleNamespace(response=SimpleNamespace(json=lambda:v))


def test_current_public_bids_use_spanish_title_original_identity_documents_and_exact_instant():
    c=Client([{'data':[record()],'meta':{'filter_count':1}}])
    r=IdbAdapter(c).fetch()
    assert r.status=='success' and len(r.opportunities)==1
    item=r.opportunities[0]
    assert item.external_id=='bidfa:1' and item.title=='Suministro de chiller'
    assert item.country=='Panamá' and item.buyer=='IDAAN'
    assert item.deadline=='2099-09-28'
    assert item.raw_payload['deadline_raw']=='2099-09-28T21:59:00-05:00'
    assert item.estimated_value is None  # API does not label the currency
    assert len(item.documents)==1
    assert len(c.calls)==1 and 'connectamericas.com' in c.calls[0][0]


@pytest.mark.parametrize('mode',['ok','failure','repeat','empty'])
def test_official_count_pagination_and_partial_retention(mode):
    first={'data':[record(i) for i in range(1,101)],'meta':{'filter_count':101}}
    second={'data':[record(101)],'meta':{'filter_count':101}}
    if mode=='failure':second=requests.ReadTimeout('offline')
    if mode=='repeat':second=first
    if mode=='empty':second={'data':[],'meta':{'filter_count':101}}
    c=Client([first,second]);r=IdbCurrentAdapter(c).fetch()
    assert r.status==('success' if mode=='ok' else 'partial')
    assert len(r.opportunities)==(101 if mode=='ok' else 100)
    assert c.calls[1][1]['params']['offset']==100


@pytest.mark.parametrize('payload',[{}, {'data':[]}, {'data':[],'meta':{'filter_count':'error'}}])
def test_invalid_api_response_does_not_look_like_zero_opportunities(payload):
    assert IdbCurrentAdapter(Client([payload])).fetch().status=='error'


def test_explicit_zero_official_approved_count_is_valid():
    r=IdbCurrentAdapter(Client([{'data':[],'meta':{'filter_count':0}}])).fetch()
    assert r.status=='success' and not r.opportunities


def test_bad_date_is_rejected_instead_of_inventing_a_deadline():
    with pytest.raises(ValueError):IdbCurrentAdapter().map_record({**record(),'date_close':'bad'})
