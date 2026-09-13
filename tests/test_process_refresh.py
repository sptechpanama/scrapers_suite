import base64
import ast
import copy
import json
import re
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from common.process_refresh import latest_links, parse_official_detail, process_code, refresh_processes

CODE = '2026-1-10-01-08-LP-000209'
NOW = datetime(2026, 9, 13, 14)


@pytest.mark.parametrize('job', ['clv', 'clrir', 'rir1'])
def test_each_scraper_refreshes_before_purge_and_invalidates_sheet_caches(job, tmp_path):
    path = Path(__file__).resolve().parents[1]/job/f'{job}.py'
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    calls = sorted((node.lineno, node.func.id) for node in ast.walk(functions['main'])
                   if isinstance(node, ast.Call) and isinstance(node.func, ast.Name))
    refresh_line = next(line for line, name in calls if name == 'refresh_known_processes')
    assert all(line > refresh_line for line, name in calls if name in ('purge_all', 'purge_by_fecha'))
    assert not any(name == 'update_fechas_sheet' for _, name in calls)
    observed = []
    def refresh(*args):
        observed.append(args)
        return {'protected_codes': {CODE}, 'changed': [], 'failed': {CODE: 'Timeout'}, 'checked': 0}
    namespace = {'CFG': {'sheets_data': ['source', 'priority'], 'sheet_ct_rir': 'ct_rir'},
        'GSVC': object(), 'SSID': 'test', 'DATA_DIR': tmp_path, 'process_code': process_code,
        'refresh_google_sheets': refresh, 'LOG': lambda *a: None, 'json': json,
        'SHEET_CACHE': {'source': ['old']}, '_GS_CACHE': {'source': ['old']}}
    exec(compile(ast.Module(body=[functions['refresh_known_processes']], type_ignores=[]), str(path), 'exec'), namespace)
    namespace['refresh_known_processes']([url(1037777)])
    assert namespace['PURGE_PROTECTED_CODES'] == {CODE}
    assert not namespace['SHEET_CACHE'] and not namespace['_GS_CACHE']
    assert observed[0][2] == ['source', 'priority', 'ct_rir']


def url(flow, code=CODE, kind=7):
    token = base64.b64encode(json.dumps({'i':flow,'tp':kind},separators=(',',':')).encode()).decode().rstrip('=')[::-1]
    route = 'solicitud-de-cotizacion' if kind==2 else 'pliego-de-cargos'
    return f'https://www.panamacompra.gob.pa/Inicio/#/{route}/{code}/{token}'


def response(code=CODE, date='24-09-2026 hasta 09:30 AM'):
    return {'status':1,'result':{'pageComponentes':[{'tipo':'componentInfoGeneral','value':[
        {'nombre':'Numero de Proceso','value':code},
        {'nombre':'Fecha y hora presentacion de propuestas','value':date},
        {'nombre':'Precio de referencia','value':'B/. 98,573.32'},
    ]}]}}


class Sheets:
    def __init__(self, sheets=1):
        self.data = {f'ap_{i}':[
            ['enlace','fecha','precio_referencia','Prioritario','Descartar','Notas','Fecha de Actualizacion'],
            [url(1005713),'14-09-2026 hasta 09:30 AM','98573.32',True,False,'Conservar nota','2026-09-12'],
        ] for i in range(sheets)}
        self.writes=[]

    def read(self):return copy.deepcopy(self.data)

    def write(self, changes):
        self.writes.extend(changes)
        for change in changes:
            match=re.fullmatch(r"'([^']+)'!([A-Z]+)(\d+)",change['range'])
            sheet, col, row=match.groups();column=0
            for char in col:column=column*26+ord(char)-64
            self.data[sheet][int(row)-1][column-1]=change['values'][0][0]


def run(grid,tmp_path,*,flow=1037777,fetch=None,now=NOW,force=False,writer=None):
    return refresh_processes(listing_links=[url(flow)],read_sheets=grid.read,write_cells=writer or grid.write,
        checkpoint_path=tmp_path/'refresh.json',fetch=fetch or (lambda u:parse_official_detail(response(),u)),now=now,force=force)


def test_real_adenda_updates_both_copies_preserves_flags_and_never_appends(tmp_path):
    grid=Sheets(2);calls=[]
    def fetch(u):calls.append(u);return parse_official_detail(response(),u)
    first=run(grid,tmp_path,fetch=fetch)
    assert first['checked']==1 and len(first['changed'])==2 and not first['failed']
    assert len(calls)==1
    for data in grid.data.values():
        assert len(data)==2
        assert data[1][:3]==[url(1037777),'24-09-2026 hasta 09:30 AM','98573.32']
        assert data[1][3:6]==[True,False,'Conservar nota']
    first_writes=len(grid.writes)
    again=run(grid,tmp_path,fetch=fetch)
    assert not again['changed'] and again['checked']==0
    assert len(grid.writes)==first_writes and len(calls)==1


def test_new_revision_bypasses_daily_cache(tmp_path):
    grid=Sheets();run(grid,tmp_path)
    result=run(grid,tmp_path,flow=1040000,now=NOW+timedelta(minutes=10))
    assert result['checked']==1 and grid.data['ap_0'][1][0]==url(1040000)


def test_unchanged_url_date_change_is_checked_daily(tmp_path):
    grid=Sheets();run(grid,tmp_path)
    result=run(grid,tmp_path,now=NOW+timedelta(days=1),fetch=lambda u:parse_official_detail(response(date='28-09-2026 hasta 09:30 AM'),u))
    assert result['changed'][0]['changes']['fecha']['before']=='24-09-2026 hasta 09:30 AM'
    assert grid.data['ap_0'][1][1]=='28-09-2026 hasta 09:30 AM'


def test_near_deadline_is_rechecked_after_two_hours(tmp_path):
    grid=Sheets();fetch=lambda u:parse_official_detail(response(date='14-09-2026 hasta 09:30 AM'),u)
    run(grid,tmp_path,fetch=fetch)
    assert run(grid,tmp_path,fetch=fetch,now=NOW+timedelta(hours=1))['checked']==0
    assert run(grid,tmp_path,fetch=fetch,now=NOW+timedelta(hours=2))['checked']==1


@pytest.mark.parametrize('payload',[
    {'status':0,'result':'ERROR'},response(code='2026-1-39-01-08-CM-000590'),
    response(date='No Disponible'),response(date='99-09-2026'),
])
def test_invalid_official_data_keeps_rows_and_retries(tmp_path,payload):
    grid=Sheets();before=grid.read()
    result=run(grid,tmp_path,fetch=lambda u:parse_official_detail(payload,u))
    assert CODE in result['protected_codes'] and CODE in result['failed']
    assert grid.read()==before and not grid.writes
    assert not (tmp_path/'refresh.json').exists()


def test_sheet_write_failure_never_advances_checkpoint(tmp_path):
    grid=Sheets()
    def fail(_):raise ConnectionError('Sheets temporalmente caido')
    result=run(grid,tmp_path,writer=fail)
    assert result['failed'] and not result['changed'] and CODE in result['protected_codes']
    assert not (tmp_path/'refresh.json').exists()
    assert run(grid,tmp_path)['checked']==1


def test_concurrent_manual_notes_and_row_reordering_survive(tmp_path):
    grid=Sheets()
    def fetch(u):
        grid.data['ap_0'].insert(1,['https://example.org','',0,False,False,'otro',''])
        grid.data['ap_0'][2][5]='Nota actualizada por usuario'
        return parse_official_detail(response(),u)
    run(grid,tmp_path,fetch=fetch)
    assert grid.data['ap_0'][2][1]=='24-09-2026 hasta 09:30 AM'
    assert grid.data['ap_0'][2][5]=='Nota actualizada por usuario'
    assert grid.data['ap_0'][1][0]=='https://example.org'


def test_user_concurrent_deadline_edit_is_not_overwritten(tmp_path):
    grid=Sheets()
    def fetch(u):
        grid.data['ap_0'][1][1]='25-09-2026'
        return parse_official_detail(response(),u)
    result=run(grid,tmp_path,fetch=fetch)
    assert result['failed'] and not grid.writes
    assert grid.data['ap_0'][1][1]=='25-09-2026'


def test_latest_flow_selected_regardless_of_listing_order():
    assert latest_links([url(1037777),url(1005713)])=={CODE:url(1037777)}


@pytest.mark.parametrize('code,kind',[(CODE,7),('2026-1-10-01-06-CM-024002',6),('2026-1-10-01-09-CL-050473',2)])
def test_identity_and_public_response_support_all_three_process_types(code,kind):
    link=url(12345,code=code,kind=kind)
    assert process_code(link.lower())==code
    assert parse_official_detail(response(code=code),link)['enlace']==link
