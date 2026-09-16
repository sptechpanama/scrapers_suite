from __future__ import annotations

import importlib.util
import json
import sys
from types import SimpleNamespace
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "orquestador"))
SPEC = importlib.util.spec_from_file_location("cl_stages_under_test", ROOT / "orquestador/main.py")
o = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = o
SPEC.loader.exec_module(o)
NOW = datetime(2026, 9, 16, 15, 0)
URL = "https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-0-12-18-08-CL-041711/token"


@pytest.fixture
def state(monkeypatch, tmp_path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"last_run": {}}), encoding="utf-8")
    monkeypatch.setattr(o, "STATE_PATH", path)
    monkeypatch.setattr(o, "_rs_sp_recent_date_changes", lambda: {})
    # Accidental transport calls make these tests fail, never send real mail.
    monkeypatch.setattr(o.smtplib, "SMTP_SSL", lambda *a, **k: pytest.fail("unexpected SMTP"))
    monkeypatch.setattr(o, "_read_panamacompra_sheet", lambda *a: pytest.fail("unexpected Sheets read"))
    return path


def row(stage="programada", **overrides):
    data = dict(hoja_origen="cl_prog_ct_rir" if stage == "programada" else "cl_abiertas_ct_rir",
                enlace=URL, titulo="Circuito de paciente", ficha_detectada="43358",
                palabras_clave="chiller", fecha="16-09-2026 a 21-09-2026", precio_referencia="20,000.00")
    data.update(overrides)
    return data


def summary(module, entry):
    prefix = "CT_RIR" if module == "ct_rir" else "RS_SP"
    job = "clrir" if o.cl_stage(entry) == "programada" else "clv"
    payload = json.dumps({"sheet": entry["hoja_origen"], "rows": [entry]})
    return getattr(o, f"_queue_{module}_notifications")(job, prefix + "_SUMMARY_JSON=" + payload, NOW)


def scan(monkeypatch, module, rows):
    monkeypatch.setattr(o, "_scan_" + module + "_candidates", lambda **kw: rows)
    return o._queue_scan_based_notifications("clv", module, NOW)


@pytest.mark.parametrize("module", ["ct_rir", "rs_sp"])
def test_scheduled_then_open_queues_two_and_repeated_scans_do_not_duplicate(state, monkeypatch, module):
    assert summary(module, row()) == 1
    assert summary(module, row()) == 0
    assert scan(monkeypatch, module, [row(), row("abierta")]) == 1
    assert summary(module, row("abierta")) == 0
    assert scan(monkeypatch, module, [row("abierta"), row()]) == 0
    stored = json.loads(state.read_text())
    pending = stored[module + "_email_pending"]
    assert len(pending) == 2
    assert {r["tipo_evento"] for r in pending} == {"CL programada", "CL abierta"}
    assert stored[module + "_email_sent_keys"] == {}


@pytest.mark.parametrize("module", ["ct_rir", "rs_sp"])
def test_first_scan_is_silent_but_opening_after_baseline_is_not(state, monkeypatch, module):
    assert scan(monkeypatch, module, [row()]) == 0
    assert scan(monkeypatch, module, [row("abierta")]) == 1
    assert scan(monkeypatch, module, [row("abierta")]) == 0


@pytest.mark.parametrize("module", ["ct_rir", "rs_sp"])
def test_deployment_does_not_replay_legacy_current_alerts(state, monkeypatch, module):
    original = {"last_run": {}, module + "_module_seen_keys": {o._build_module_act_key(row()): "2026-09-12"},
                module + "_email_sent_keys": {"old-history": "2026-09-12"}}
    state.write_text(json.dumps(original))
    assert summary(module, row()) == 0
    assert scan(monkeypatch, module, [row()]) == 0
    assert summary(module, row("abierta")) == 1
    saved = json.loads(state.read_text())
    assert saved[module + "_email_sent_keys"] == original[module + "_email_sent_keys"]


@pytest.mark.parametrize("module", ["ct_rir", "rs_sp"])
def test_stale_programmed_row_after_open_does_not_alert(state, module):
    assert summary(module, row("abierta")) == 1
    assert summary(module, row()) == 0


@pytest.mark.parametrize("module", ["ct_rir", "rs_sp"])
def test_route_version_and_sheet_classification_do_not_repeat_same_stage(state, module):
    assert summary(module, row("abierta")) == 1
    updated = row("abierta", enlace=URL.replace("/token", "/other-token"), hoja_origen="cl_abiertas_rir_con_ct")
    assert summary(module, updated) == 0


def test_modules_keep_independent_alert_histories(state):
    assert summary("ct_rir", row()) == 1
    assert summary("rs_sp", row()) == 1
    assert summary("ct_rir", row("abierta")) == 1
    assert summary("rs_sp", row("abierta")) == 1


def test_rs_sp_date_changes_still_alert_once_per_stage(state, monkeypatch):
    assert summary("rs_sp", row()) == 1
    assert summary("rs_sp", row("abierta")) == 1
    changed = row("abierta", fecha="16-09-2026 a 25-09-2026")
    assert summary("rs_sp", changed) == 1
    assert scan(monkeypatch, "rs_sp", [row(), changed]) == 0
    entries = json.loads(state.read_text())["rs_sp_email_pending"]
    assert entries[-1]["tipo_evento"] == "Actualizado"
    assert entries[-1]["fecha_anterior"] == row()["fecha"]


@pytest.mark.parametrize("module", ["ct_rir", "rs_sp"])
def test_pending_queue_is_preserved_and_no_500_entry_truncation(state, module):
    pending = [{"unique_key": f"older-{n}"} for n in range(501)]
    state.write_text(json.dumps({"last_run": {}, module + "_email_pending": pending}))
    assert summary(module, row("abierta")) == 1
    saved = json.loads(state.read_text())[module + "_email_pending"]
    assert saved[:501] == pending
    assert len(saved) == 502


@pytest.mark.parametrize("module", ["ct_rir", "rs_sp"])
def test_scanner_preserves_programmed_and_open_rows(state, monkeypatch, module):
    sheets = ["cl_prog_ct_rir", "cl_abiertas_ct_rir"]
    monkeypatch.setattr(o, "PANAMACOMPRA_CT_RIR_SCAN_SHEETS", sheets)
    monkeypatch.setattr(o, "PANAMACOMPRA_RS_SP_SCAN_SHEETS", sheets)
    monkeypatch.setattr(o, "_load_ct_rir_fichas_for_notifications", lambda: {"43358"})
    if hasattr(o, "_ct_rir_semantic_anchor_tokens"):
        monkeypatch.setattr(o, "_ct_rir_semantic_anchor_tokens", lambda *a: set())
    monkeypatch.setattr(o, "_load_rs_sp_keywords_for_notifications", lambda: ["chiller"])
    monkeypatch.setattr(o, "_load_rs_sp_negative_keywords_for_notifications", lambda: [])
    monkeypatch.setattr(o, "_read_panamacompra_sheet", lambda *a: [
        ["titulo", "ficha_detectada", "enlace", "precio_referencia"], ["chiller", "43358", URL, "20000"]])
    rows = getattr(o, "_scan_" + module + "_candidates")()
    assert {o.cl_stage(r) for r in rows} == {"programada", "abierta"}


def test_existing_ap_keys_are_unchanged(state):
    entry = row(hoja_origen="ap_ct_rir", enlace="https://example.test/acto")
    assert o._build_ct_rir_unique_key(entry) == "ap_ct_rir|https://example.test/acto"


def test_scanning_job_does_not_imply_stage_for_other_sources():
    assert o.cl_stage({"job": "clv", "enlace": URL}) == ""
    assert o.cl_stage({"job": "clv", "hoja_origen": "ap_sin_ficha"}) == ""


def test_context_rule_baseline_also_applies_to_cl(state, monkeypatch):
    state.write_text(json.dumps({"rs_sp_module_seen_keys": {"previous": "2026-09-01"}}))
    entries = [row(palabras_clave="planta electric*"),
               row(enlace=URL.replace("041711", "041712"), palabras_clave="chiller")]
    assert scan(monkeypatch, "rs_sp", entries) == 1
    queued = json.loads(state.read_text())["rs_sp_email_pending"]
    assert queued[0]["palabras_clave"] == "chiller"


def test_successful_scraper_retries_existing_rs_sp_mail_without_new_matches(state, monkeypatch):
    state.write_text(json.dumps({"rs_sp_email_pending": [{"unique_key": "pending"}]}))
    calls = []
    monkeypatch.setattr(o.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout="", stderr=""))
    monkeypatch.setattr(o, "update_last_run", lambda *a, **kw: None)
    if hasattr(o, "record_component_states"):
        monkeypatch.setattr(o, "record_component_states", lambda *a, **kw: None)
    monkeypatch.setattr(o, "_queue_ct_rir_notifications", lambda *a: 0)
    monkeypatch.setattr(o, "_send_pending_ct_rir_email", lambda: (True, "", 0))
    monkeypatch.setattr(o, "_queue_rs_sp_notifications", lambda *a: 0)
    monkeypatch.setattr(o, "_queue_scan_based_notifications", lambda *a: 0)
    def deliver():
        calls.append(True)
        state.write_text(json.dumps({"rs_sp_email_pending": []}))
        return True, "", 1
    monkeypatch.setattr(o, "_send_pending_rs_sp_email", deliver)
    job = o.JobConfig(name="clv", python="python", script="clv.py", days_of_week=["wed"], times=["08:00"])
    assert o.run_job(job)[0] == "success"
    assert len(calls) == 1


@pytest.mark.parametrize("module", ["ct_rir", "rs_sp"])
def test_smtp_failure_preserves_open_alert_then_success_marks_delivered(state, monkeypatch, module):
    assert summary(module, row("abierta")) == 1
    monkeypatch.setattr(o, f"_{module}_email_config", lambda: ("test@example.test", "test", ["to@example.test"]))
    if hasattr(o, "CT_RIR_EMAIL_RETRY_DELAYS_SECONDS"):
        monkeypatch.setattr(o, "CT_RIR_EMAIL_RETRY_DELAYS_SECONDS", (0,))
    class SMTP:
        fail = True
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def login(self, *a): pass
        def send_message(self, message):
            assert "Etapa: CL abierta" in message.get_content()
            if self.fail: raise TimeoutError("simulated failure")
    monkeypatch.setattr(o.smtplib, "SMTP_SSL", SMTP)
    sender = getattr(o, "_send_pending_" + module + "_email")
    try:
        assert not sender()[0]
    except TimeoutError:
        pass  # legacy RS/SP caller catches this; its queue must still survive.
    assert len(json.loads(state.read_text())[module + "_email_pending"]) == 1
    assert summary(module, row("abierta")) == 0
    SMTP.fail = False
    assert sender()[2] == 1
    assert not json.loads(state.read_text())[module + "_email_pending"]
    assert summary(module, row("abierta")) == 0
