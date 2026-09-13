from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path


ORCHESTRATOR_DIR = Path(__file__).resolve().parents[1] / "orquestador"
if str(ORCHESTRATOR_DIR) not in sys.path:
    sys.path.insert(0, str(ORCHESTRATOR_DIR))

import main as orchestrator  # noqa: E402


def test_amount_formatting_is_not_a_new_revision():
    first = {'fecha': '24-09-2026 hasta 09:30 AM', 'precio_referencia': '$98,573.32'}
    second = {'fecha': '24-09-2026 hasta 09:30 am', 'precio_referencia': 98573.32}
    assert orchestrator._rs_sp_revision_key(first) == orchestrator._rs_sp_revision_key(second)


def test_duplicate_versions_use_only_latest_and_notify_once(monkeypatch):
    from test_process_refresh import url
    old = {'enlace': url(1005713), 'fecha': '14-09-2026 hasta 09:30 AM', 'precio_referencia': '98573.32'}
    latest = dict(old, enlace=url(1037777), fecha='24-09-2026 hasta 09:30 AM')
    state = {'rs_sp_module_seen_keys': {orchestrator._build_module_act_key(old): '2026-09-01'},
             'rs_sp_context_rules_baseline_version': orchestrator.RS_SP_CONTEXT_RULES_VERSION,
             'rs_sp_module_revisions': {'2026-1-10-01-08-LP-000209': {'fingerprint': orchestrator._rs_sp_revision_key(old), 'fecha': old['fecha']}}}
    monkeypatch.setattr(orchestrator, 'load_state', lambda: state)
    monkeypatch.setattr(orchestrator, 'save_state', lambda s: None)
    monkeypatch.setattr(orchestrator, '_scan_rs_sp_candidates', lambda: [latest, old, dict(latest)])
    monkeypatch.setattr(orchestrator, '_rs_sp_recent_date_changes', lambda: {})
    assert orchestrator._queue_scan_based_notifications('rir1', 'rs_sp', datetime(2026, 9, 13)) == 1
    assert state['rs_sp_email_pending'][0]['enlace'] == latest['enlace']
    assert orchestrator._queue_scan_based_notifications('rir1', 'rs_sp', datetime(2026, 9, 13)) == 0


def test_date_change_notifies_once_even_when_url_does_not_change(monkeypatch):
    old={'enlace':'https://www.panamacompra.gob.pa/Inicio/#/pliego-de-cargos/2026-1-10-01-08-LP-000209/token',
         'fecha':'14-09-2026 hasta 09:30 AM','precio_referencia':'98573.32','palabras_clave':'aire acondicion*>8k'}
    current=dict(old)
    state={'rs_sp_module_seen_keys':{orchestrator._build_module_act_key(old):'2026-09-01T00:00:00'},
           'rs_sp_context_rules_baseline_version':orchestrator.RS_SP_CONTEXT_RULES_VERSION}
    monkeypatch.setattr(orchestrator,'load_state',lambda:state)
    monkeypatch.setattr(orchestrator,'save_state',lambda s:None)
    monkeypatch.setattr(orchestrator,'_scan_rs_sp_candidates',lambda:[current])
    monkeypatch.setattr(orchestrator,'_rs_sp_recent_date_changes',lambda:{})
    now=datetime(2026,9,13,14)
    assert orchestrator._queue_scan_based_notifications('rir1','rs_sp',now)==0
    current['fecha']='24-09-2026 hasta 09:30 AM'
    assert orchestrator._queue_scan_based_notifications('rir1','rs_sp',now)==1
    assert state['rs_sp_email_pending'][0]['fecha_anterior']==old['fecha']
    assert state['rs_sp_email_pending'][0]['tipo_evento']=='Actualizado'
    assert orchestrator._queue_scan_based_notifications('rir1','rs_sp',now)==0
    assert len(state['rs_sp_email_pending'])==1


def test_first_recovery_of_legacy_adenda_is_not_silenced(monkeypatch):
    old='https://www.panamacompra.gob.pa/Inicio/#/pliego-de-cargos/2026-1-10-01-08-LP-000209/old-token'
    entry={'enlace':old.replace('old-token','new-token'),'fecha':'24-09-2026 hasta 09:30 AM','precio_referencia':'98573.32'}
    state={'rs_sp_module_seen_keys':{orchestrator._normalize_url_key(old):'2026-09-01'},'rs_sp_context_rules_baseline_version':1}
    monkeypatch.setattr(orchestrator,'load_state',lambda:state)
    monkeypatch.setattr(orchestrator,'save_state',lambda s:None)
    monkeypatch.setattr(orchestrator,'_scan_rs_sp_candidates',lambda:[entry])
    monkeypatch.setattr(orchestrator,'_rs_sp_recent_date_changes',lambda:{'2026-1-10-01-08-LP-000209':{'before':'14-09-2026 hasta 09:30 AM','after':entry['fecha']}})
    assert orchestrator._queue_scan_based_notifications('rir1','rs_sp',datetime(2026,9,13))==1
    assert state['rs_sp_email_pending'][0]['tipo_evento']=='Actualizado'
    assert orchestrator._queue_scan_based_notifications('rir1','rs_sp',datetime(2026,9,13))==0


def test_rs_sp_scanner_applies_amount_rule_using_normalized_sheet_header(monkeypatch):
    monkeypatch.setattr(orchestrator, "PANAMACOMPRA_RS_SP_SCAN_SHEETS", ["prueba"])
    monkeypatch.setattr(
        orchestrator,
        "_load_rs_sp_keywords_for_notifications",
        lambda: ["aires acondicion*>15k"],
    )
    monkeypatch.setattr(
        orchestrator,
        "_load_rs_sp_negative_keywords_for_notifications",
        lambda: [],
    )
    monkeypatch.setattr(
        orchestrator,
        "_read_panamacompra_sheet",
        lambda _sheet: [
            ["titulo", "descripcion", "precio_referencia", "enlace"],
            ["Aires acondicionados split", "Suministro", "$14,999.99", "low"],
            ["Aires acondicionados", "Suministro", "$25,000.00", "high"],
        ],
    )

    rows = orchestrator._scan_rs_sp_candidates()

    assert len(rows) == 1
    assert rows[0]["enlace"] == "high"
    assert rows[0]["precio_referencia"] == "$25,000.00"
    assert rows[0]["palabras_clave"] == "aires acondicion*>15k"


def test_rs_sp_scanner_excludes_only_obvious_negative_contexts(monkeypatch):
    monkeypatch.setattr(orchestrator, "PANAMACOMPRA_RS_SP_SCAN_SHEETS", ["prueba"])
    monkeypatch.setattr(
        orchestrator,
        "_load_rs_sp_keywords_for_notifications",
        lambda: ["aire acondicion*>15k", "solar"],
    )
    monkeypatch.setattr(
        orchestrator,
        "_load_rs_sp_negative_keywords_for_notifications",
        lambda: ["automotriz", "habitacion de hotel", "protector solar"],
    )
    monkeypatch.setattr(
        orchestrator,
        "_read_panamacompra_sheet",
        lambda _sheet: [
            ["titulo", "descripcion", "item_1", "precio_referencia", "enlace"],
            ["Mantenimiento automotriz", "Servicio", "Aire acondicionado", "25000", "vehicle"],
            ["Alquiler de habitaciones (hotel)", "Aire acondicionado", "", "25000", "hotel"],
            ["Compra de protector solar", "Protector solar", "", "25000", "sunblock"],
            ["Sistema para edificio", "Aire acondicionado central", "", "25000", "valid"],
        ],
    )

    rows = orchestrator._scan_rs_sp_candidates()

    assert [row["enlace"] for row in rows] == ["valid"]


def test_negative_loader_respects_an_existing_header_only_sheet(monkeypatch):
    monkeypatch.setattr(
        orchestrator,
        "_read_panamacompra_sheet",
        lambda _sheet: [["Palabra clave", "Actualizado por", "Actualizado"]],
    )
    assert orchestrator._load_rs_sp_negative_keywords_for_notifications() == []


def test_rs_sp_scanner_suppresses_contextual_item_in_global_mixed_act(monkeypatch):
    monkeypatch.setattr(orchestrator, "PANAMACOMPRA_RS_SP_SCAN_SHEETS", ["prueba"])
    monkeypatch.setattr(
        orchestrator,
        "_load_rs_sp_keywords_for_notifications",
        lambda: ["generador electric*"],
    )
    monkeypatch.setattr(
        orchestrator,
        "_load_rs_sp_negative_keywords_for_notifications",
        lambda: [],
    )
    monkeypatch.setattr(
        orchestrator,
        "_read_panamacompra_sheet",
        lambda _sheet: [
            ["titulo", "item_1", "item_2", "Tipo de adjudicacion", "enlace"],
            ["Compra mixta", "Aceite para generador electrico", "Alimentos", "Global", "noise"],
            ["Compra mixta", "Aceite para generador electrico", "Alimentos", "Por renglon", "valid"],
        ],
    )

    rows = orchestrator._scan_rs_sp_candidates()

    assert [row["enlace"] for row in rows] == ["valid"]


def test_contextual_rules_get_one_silent_baseline_without_hiding_old_rules(monkeypatch):
    contextual = {
        "palabras_clave": "planta electric*",
        "titulo": "Planta electrica",
        "enlace": "https://example.test/contextual",
    }
    existing_rule = {
        "palabras_clave": "chiller",
        "titulo": "Chiller",
        "enlace": "https://example.test/existing",
    }
    state = {"rs_sp_module_seen_keys": {"already-seen": "2026-09-01T00:00:00"}}
    saved: list[dict] = []
    monkeypatch.setattr(
        orchestrator,
        "_scan_rs_sp_candidates",
        lambda: [contextual, existing_rule],
    )
    monkeypatch.setattr(orchestrator, "load_state", lambda: state)
    monkeypatch.setattr(orchestrator, "save_state", lambda value: saved.append(dict(value)))

    queued = orchestrator._queue_scan_based_notifications(
        "clv",
        "rs_sp",
        datetime(2026, 9, 8, 8, 0, 0),
    )

    contextual_key = orchestrator._build_module_act_key(contextual)
    assert queued == 1
    assert contextual_key in state["rs_sp_module_seen_keys"]
    assert state["rs_sp_context_rules_baseline_version"] == orchestrator.RS_SP_CONTEXT_RULES_VERSION
    assert [item["enlace"] for item in state["rs_sp_email_pending"]] == [
        "https://example.test/existing"
    ]
    assert saved
