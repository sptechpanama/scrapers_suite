import importlib.util
import json
import os
from pathlib import Path
from unittest import mock

import pytest

spec=importlib.util.spec_from_file_location('pc_preflight_pipeline',Path(__file__).resolve().parents[1]/'orquestador/database_pipeline.py')
pipeline=importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)


def builder(tmp_path, *, lifecycle=True, acts=True, proposals=True):
    code='PC_ACT_COLUMNS='+repr(['numero_proceso','source_layer','resultado_provisional'] if acts else ['acto_key'])+'\n'
    code+='PC_PROPOSAL_COLUMNS='+repr(['resultado_empresa','fuente_resultado','monto_ganado_fuente'] if proposals else ['proveedor'])+'\n'
    if lifecycle: code+='def _prepare_lifecycle(): pass\n'
    target=tmp_path/'build_inteligencia_pc.py'
    target.write_text(code,encoding='utf-8')
    return target


def test_current_participation_contract_passes(tmp_path):
    pipeline.validate_pc_builder(builder(tmp_path))


@pytest.mark.parametrize('missing',['lifecycle','acts','proposals'])
def test_legacy_builder_is_rejected(tmp_path,missing):
    with pytest.raises(RuntimeError,match='desactualizado'):
        pipeline.validate_pc_builder(builder(tmp_path,**{missing:False}))


def test_server_config_selects_the_deployed_app_checkout(tmp_path):
    (tmp_path/'db').mkdir()
    (tmp_path/'db/update_config.json').write_text(json.dumps({'geapp_root':str(tmp_path/'current-app')}),encoding='utf-8')
    with mock.patch.dict(os.environ,{},clear=True), mock.patch.object(pipeline,'REPO_ROOT',tmp_path):
        assert pipeline.pc_analytics_builder()==tmp_path/'current-app/scripts/build_inteligencia_pc.py'


def test_explicit_environment_root_takes_priority(tmp_path):
    (tmp_path/'db').mkdir()
    (tmp_path/'db/update_config.json').write_text(json.dumps({'geapp_root':str(tmp_path/'old-app')}),encoding='utf-8')
    with mock.patch.dict(os.environ,{'GEAPP_ROOT':str(tmp_path/'current-app')}), mock.patch.object(pipeline,'REPO_ROOT',tmp_path):
        assert pipeline.pc_analytics_builder()==tmp_path/'current-app/scripts/build_inteligencia_pc.py'


def test_bad_builder_stops_before_data_changes(tmp_path):
    target=builder(tmp_path,lifecycle=False)
    with mock.patch.dict(os.environ,{'SUPABASE_DB_URL':'test-only'}), \
         mock.patch.object(pipeline,'pc_analytics_builder',return_value=target), \
         mock.patch.object(pipeline,'_run') as run, \
         mock.patch.object(pipeline,'emit_component') as emit:
        assert pipeline.run_pipeline('incremental')==2
    run.assert_not_called()
    assert [(c.args[0],c.args[1]) for c in emit.call_args_list]==[
        ('db_local','blocked'),('supabase_operational','blocked'),('analytics','error')]
    assert all('desactualizado' in c.args[-1] for c in emit.call_args_list)
