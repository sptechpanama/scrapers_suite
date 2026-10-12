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
    if job == 'clv':
        assert '_load_open_listing' in calls
        loader = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == '_load_open_listing')
        calls.update(node.func.id for node in ast.walk(loader) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name))
    assert {'collect_listing','DetailCoverage','verify_detail','technical_discard'}<=calls
    assert 'skip_timeout_xpath' not in ast.unparse(main)


def api_response(records, size=5000, status=1, cursor=None):
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'status':status,'result':{'registros':records,'registrosPorPagina':size,'valorInicial':cursor}}
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


def test_open_api_uses_small_batches_and_follows_cursor_until_empty():
    from types import SimpleNamespace
    calls = []
    responses = iter([
        api_response([api_record(1), api_record(2)], size=2, cursor='first'),
        api_response([api_record(3), api_record(4)], size=2, cursor='second'),
        api_response([], size=2),
    ])
    def request(endpoint, json, timeout):
        calls.append(dict(json))
        return next(responses)
    result = c.recover_listing({'links': [], 'pages': [], 'complete': False, 'error': 'UI'},
                              SimpleNamespace(), 'clv', lambda *args: None, request=request)
    assert result['complete'] and len(result['links']) == 4
    assert [payload['valorSiguiente'] for payload in calls] == ['', 'first', 'second']
    assert all(payload['registrosPorPagina'] == 50 for payload in calls)


def test_open_cursor_failure_preserves_all_successful_batches():
    from types import SimpleNamespace
    calls = []
    def request(endpoint, json, timeout):
        calls.append(dict(json))
        if len(calls) == 1:
            return api_response([api_record(1), api_record(2)], size=2, cursor='first')
        return api_response([], status=0)
    result = c.recover_listing({'links': [url(3)], 'pages': [], 'complete': False, 'error': 'UI'},
                              SimpleNamespace(), 'clv', lambda *args: None, request=request)
    assert not result['complete']
    assert {c.process_key(link) for link in result['links']} == {c.process_key(url(number)) for number in (1, 2, 3)}
    assert len(calls) == 7
    assert calls[1]['valorSiguiente'] == calls[2]['valorSiguiente'] == calls[3]['valorSiguiente'] == 'first'


def test_open_cursor_duplicate_versions_keep_latest_official_route():
    from types import SimpleNamespace
    older, newer = api_record(1), api_record(1)
    newer['idProcesosContratacionFlujos'] = 99
    responses = iter([
        api_response([newer, api_record(2)], size=2, cursor='first'),
        api_response([older], size=2),
    ])
    result = c.recover_listing({'links': [], 'pages': [], 'complete': False, 'error': 'UI'},
                              SimpleNamespace(), 'clv', lambda *args: None,
                              request=lambda *args, **kwargs: next(responses))
    from common.process_refresh import route_payload
    assert result['complete'] and len(result['links']) == 2
    assert route_payload(result['links'][0])[0] == 99


def test_full_api_batch_from_another_state_is_not_accepted():
    from types import SimpleNamespace
    wrong = api_record(1)
    wrong['idEstado'] = 15
    result = c.recover_listing({'links': [url(2)], 'pages': [], 'complete': False, 'error': 'UI'},
                              SimpleNamespace(), 'clv', lambda *args: None,
                              request=lambda *args, **kwargs: api_response([wrong], size=1))
    assert not result['complete'] and result['links'] == [url(2)]


def test_deferred_details_survive_restart_without_fake_attempts(tmp_path, capsys):
    listing = {'links': [url(1), url(2)], 'pages': [], 'complete': True, 'error': ''}
    coverage = c.DetailCoverage(tmp_path, 'clv', listing)
    coverage.failure(url(1), 'timeout')
    coverage.defer_remaining(listing['links'], 'Fuente temporalmente caída')
    restarted = c.DetailCoverage(tmp_path, 'clv', listing)
    assert restarted.pending[c.process_key(url(1))]['attempts'] == 1
    assert restarted.pending[c.process_key(url(2))]['attempts'] == 0
    coverage.finish()
    assert '2 detalles pendientes' in c.coverage_problem(capsys.readouterr().out)


def test_open_detail_waits_for_slow_server_all_price_variants_and_items():
    from types import SimpleNamespace
    from selenium.common.exceptions import StaleElementReferenceException
    started = c.time.monotonic()
    class Driver:
        stale = True
        def execute_script(self, *args):
            return c.process_key(url(1)) if c.time.monotonic() - started >= 32 else 'Cargando...'
        def find_elements(self, by, xpath):
            if self.stale:
                self.stale = False
                raise StaleElementReferenceException()
            return [SimpleNamespace(text='B/. 1,500.00' if xpath == 'fallback-price' else 'Ficha técnica: 43358')] if xpath != 'first-price' else []
    c.wait_open_detail(Driver(), url(1), ['first-price', 'fallback-price'], ['items'])
    assert 32 <= c.time.monotonic() - started < 60


@pytest.mark.parametrize('body,items', [(c.process_key(url(2)), True), (c.process_key(url(1)), False)])
def test_open_detail_does_not_accept_wrong_act_or_missing_items(body, items):
    from types import SimpleNamespace
    from selenium.common.exceptions import TimeoutException
    class Driver:
        def execute_script(self, *args): return body
        def find_elements(self, by, xpath):
            return [SimpleNamespace(text='10.00')] if xpath == 'price' or items else []
    with pytest.raises(TimeoutException):
        c.wait_open_detail(Driver(), url(1), ['price'], ['items'], timeout=2)


def test_open_scraper_does_not_refresh_and_defers_on_persistent_failure():
    path = Path(__file__).resolve().parents[1] / 'clv' / 'clv.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    assert not any(isinstance(node, ast.Attribute) and node.attr == 'refresh' for node in ast.walk(functions['scrape']))
    assert any(isinstance(node, ast.Name) and node.id == 'wait_open_detail' for node in ast.walk(functions['scrape']))
    assert any(isinstance(node, ast.Attribute) and node.attr == 'defer_remaining' for node in ast.walk(functions['main']))


def open_detail_url():
    token = c.base64.b64encode(b'{"i":1,"tp":2}').decode().rstrip('=')[::-1]
    return url(1).replace('/token', '/' + token)


def open_detail_response(*, number=None, items=True, price=True, title='Circuito de anestesia'):
    values = [
        {'nombre': 'Número de Proceso', 'value': number or c.process_key(url(1))},
        {'nombre': 'Título', 'value': title},
        {'nombre': 'Entidad', 'value': 'Caja de Seguro Social'},
        {'nombre': 'Unidad de Compra', 'value': 'Hospital de prueba'},
        {'nombre': 'Fecha y hora presentación de cotizaciones', 'value': '12-10-2026 a 15-10-2026'},
        {'nombre': 'Fecha de Publicación', 'value': '09-10-2026 - 10:00 AM'},
    ]
    if price:
        values.append({'nombre': 'Precio estimado', 'value': 'B/. 37,500.00'})
    class Response:
        def raise_for_status(self): pass
        def json(self):
            return {'status': 1, 'result': {'pageComponentes': [
                {'tipo': 'componentInfoProcesos', 'value': values},
                {'tipo': 'componentItems', 'value': [{'descripcion': 'Kit de circuito. Ficha técnica: 43358'}] if items else []},
            ]}}
    return Response()


def test_open_api_detail_is_complete_and_does_not_need_a_loaded_browser():
    observed = []
    def request(endpoint, timeout):
        observed.append((endpoint, timeout))
        return open_detail_response()
    detail = c.fetch_open_detail(open_detail_url(), request=request)
    c.verify_detail(None, open_detail_url(), detail)
    assert detail['titulo'] == 'Circuito de anestesia'
    assert detail['precio_referencia'] == 'B/. 37,500.00'
    assert detail['unidad solicitante'] == 'Hospital de prueba'
    assert detail['publicacion'] == '09-10-2026'
    assert detail['items'] == ['Kit de circuito. Ficha técnica: 43358']
    assert observed[0][1] == (8, 45)


@pytest.mark.parametrize('changes', [
    {'number': c.process_key(url(2))}, {'items': False}, {'price': False}, {'title': ''},
])
def test_open_api_rejects_wrong_act_and_missing_required_values(changes):
    calls = []
    def request(*args, **kwargs):
        calls.append(1)
        return open_detail_response(**changes)
    with pytest.raises(c.ListingIncomplete):
        c.fetch_open_detail(open_detail_url(), request=request, sleeper=lambda delay: None)
    assert len(calls) == 2


def test_open_api_retries_transient_error_and_validates_after_recovery():
    calls = []
    def request(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise TimeoutError('Fuente lenta')
        return open_detail_response()
    assert c.fetch_open_detail(open_detail_url(), request=request, sleeper=lambda delay: None)['items']
    assert len(calls) == 2


def test_open_api_classification_keeps_existing_ficha_logic():
    from types import SimpleNamespace
    path = Path(__file__).resolve().parents[1] / 'clv' / 'clv.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    scrape = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'scrape')
    detail = c.fetch_open_detail(open_detail_url(), request=lambda *args, **kwargs: open_detail_response())
    namespace = {
        'PageTools': SimpleNamespace,
        'CFG': {'xpath_map': {}}, 'fetch_open_detail': lambda link: detail,
        'detectar_fichas_tokens': lambda text: ['43358'] if '43358' in text else [],
        'FICHAS_CON_CT': ['43358'], 'FICHAS_SIN_REQ': [], 'FICHAS_CON_RS': [],
    }
    if any(isinstance(node, ast.Name) and node.id == 'parse_purchase_unit_components' for node in ast.walk(scrape)):
        from common import no_requirements, purchase_unit_details, process_law
        namespace.update(vars(no_requirements))
        namespace.update(vars(purchase_unit_details))
        namespace.update(vars(process_law))
    exec(compile(ast.Module(body=[scrape], type_ignores=[]), str(path), 'exec'), namespace)
    info = namespace['scrape'](SimpleNamespace(d=None), open_detail_url())
    assert info['ficha_detectada'] == '43358' and info['has_ct']
    assert info['items'] == detail['items']
    c.verify_detail(None, open_detail_url(), info)


@pytest.fixture
def open_bootstrap():
    from types import SimpleNamespace
    from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
    path = Path(__file__).resolve().parents[1] / 'clv' / 'clv.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name in {'_load_open_listing', '_restart_scrape_driver', 'scrape'}]
    calls = {'api': 0, 'collected': 0, 'navigated': 0, 'finished': 0}
    class Driver:
        problem = ''
        quits = 0
        def get(self, address):
            calls['navigated'] += 1
            if self.problem == 'navigation': raise WebDriverException('Navegación fallida')
        def execute_script(self, *args): pass
        def quit(self): self.quits += 1
    driver = Driver()
    def start_browser():
        if driver.problem == 'browser': raise WebDriverException('Chrome no disponible')
        return driver
    class Wait:
        def __init__(self, *args): pass
        def until(self, condition):
            if driver.problem == condition:
                raise TimeoutException('Control no disponible')
            return SimpleNamespace()
    class Select:
        def __init__(self, *args): pass
        def select_by_visible_text(self, text):
            if driver.problem == 'option': raise NoSuchElementException('50 no disponible')
            assert text == '50'
    def collect(*args, **kwargs):
        calls['collected'] += 1
        return {'links': [url(1)], 'pages': [], 'complete': True, 'error': ''}
    def request(*args, **kwargs):
        calls['api'] += 1
        return api_response([api_record(1)])
    namespace = {
        'CFG': {'url_list': 'https://www.panamacompra.gob.pa/Inicio/', 'css_links': 'a'},
        'SimpleNamespace': SimpleNamespace, 'time': c.time, 'LOG': lambda *args: None,
        'start_browser': start_browser, 'PageTools': lambda current: SimpleNamespace(d=current, close_popup=lambda: None),
        'WebDriverException': WebDriverException, 'WebDriverWait': Wait, 'Select': Select,
        'By': SimpleNamespace(XPATH='xpath'), 'EC': SimpleNamespace(element_to_be_clickable=lambda locator: 'tab',
        presence_of_element_located=lambda locator: 'selector'),
        'collect_listing': collect, 'normalize_url': str,
        'recover_listing': lambda *args, **kwargs: c.recover_listing(*args, **kwargs, request=request),
    }
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), 'exec'), namespace)
    return namespace, driver, calls


@pytest.mark.parametrize('problem', ['selector', 'tab', 'option', 'navigation', 'browser'])
def test_open_bootstrap_failures_reach_api_instead_of_aborting(open_bootstrap, problem):
    namespace, browser, calls = open_bootstrap
    browser.problem = problem
    driver, page, listing = namespace['_load_open_listing']()
    assert driver is None and page.d is None
    assert listing['complete'] and len(listing['links']) == 1
    assert calls['api'] == 1 and calls['collected'] == 0
    assert browser.quits == (0 if problem == 'browser' else 1)
    assert calls['navigated'] == (5 if problem == 'navigation' else 0 if problem == 'browser' else 1)


def test_working_open_ui_does_not_need_api_recovery(open_bootstrap):
    namespace, browser, calls = open_bootstrap
    driver, page, listing = namespace['_load_open_listing']()
    assert driver is browser and page.d is browser and listing['complete']
    assert calls['api'] == 0 and calls['collected'] == 1 and browser.quits == 0


def test_bootstrap_and_api_outage_remain_partial(open_bootstrap):
    namespace, browser, calls = open_bootstrap
    browser.problem = 'selector'
    namespace['recover_listing'] = lambda *args, **kwargs: c.recover_listing(
        *args, **kwargs, request=lambda *request_args, **request_kwargs: api_response([], status=0))
    driver, page, listing = namespace['_load_open_listing']()
    assert driver is None and page.d is None
    assert not listing['complete'] and not listing['links']
    assert listing['failed_windows'] and browser.quits == 1


def test_detail_browser_failure_does_not_block_public_api(open_bootstrap):
    namespace, browser, calls = open_bootstrap
    browser.problem = 'browser'
    driver, page = namespace['_restart_scrape_driver'](None)
    assert driver is None and page.d is None


def test_api_error_without_browser_preserves_original_error(open_bootstrap):
    from types import SimpleNamespace
    namespace, browser, calls = open_bootstrap
    namespace['CFG']['xpath_map'] = {}
    def unavailable(link): raise c.ListingIncomplete('Fuente pública no disponible')
    namespace['fetch_open_detail'] = unavailable
    with pytest.raises(c.ListingIncomplete, match='Fuente pública no disponible'):
        namespace['scrape'](SimpleNamespace(d=None), open_detail_url())


def test_empty_unverified_listing_returns_before_maintenance(tmp_path):
    from types import SimpleNamespace
    path = Path(__file__).resolve().parents[1] / 'clv' / 'clv.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    main = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'main')
    stop = next(index for index, node in enumerate(main.body)
                if isinstance(node, ast.If) and isinstance(node.test, ast.BoolOp)
                and isinstance(node.test.op, ast.And)
                and {value.operand.id for value in node.test.values if isinstance(value, ast.UnaryOp)
                     and isinstance(value.op, ast.Not) and isinstance(value.operand, ast.Name)} == {'links', 'listing_complete'})
    main.body = main.body[:stop + 1] + [ast.Raise(exc=ast.Call(func=ast.Name(id='AssertionError', ctx=ast.Load()),
                                                          args=[ast.Constant('No maintenance after an unverified list')], keywords=[]))]
    ast.fix_missing_locations(main)
    finished = []
    listing = {'links': [], 'pages': [], 'complete': False, 'error': 'API caída'}
    namespace = {
        'CFG': {'sheet_ct_rir': 'ct', 'sheet_419_sfd': '419', 'sheets_data': [], 'sheet_desc': 'discard'},
        'ensure_sheet_exists': lambda *args: None, 'load_ct_rir_fichas': lambda: set(),
        'load_rs_sp_keywords': lambda: [], 'load_rs_sp_negative_keywords': lambda: [],
        '_flush_failed_appends': lambda: None, 'read_links_from_sheets': lambda sheets: ({}, set()),
        'gs_get': lambda *args: [], 'LOG': lambda *args: None,
        '_load_open_listing': lambda: (None, SimpleNamespace(adjudication_by_url={}), listing),
        'DATA_DIR': tmp_path, 'DetailCoverage': lambda *args: SimpleNamespace(finish=lambda: finished.append(True)),
    }
    exec(compile(ast.Module(body=[main], type_ignores=[]), str(path), 'exec'), namespace)
    namespace['main']()
    assert finished == [True]


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
