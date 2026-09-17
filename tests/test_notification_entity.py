from __future__ import annotations

import ast
import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "orquestador")]
from common.notification_entity import notification_location, notification_location_from_row, notification_location_lines
from common.keyword_watch import summarize_keyword_rows

SPEC = importlib.util.spec_from_file_location("location_orchestrator_test", ROOT / "orquestador/main.py")
o = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = o
SPEC.loader.exec_module(o)

HOSPITAL = "CSS - Hospital de Especialidades Pediátricas Omar Torrijos Herrera"
ENTRY = {"titulo": "chiller", "ficha_detectada": "43358", "entidad": "Caja de Seguro Social",
         "unidad solicitante": HOSPITAL, "unidad de compra": HOSPITAL, "dependencia": "CSS - Sede",
         "palabras_clave": "chiller", "fecha": "18-09-2026", "precio_referencia": "20000"}


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"last_run": {}}))
    monkeypatch.setattr(o, "STATE_PATH", path)
    monkeypatch.setattr(o.smtplib, "SMTP_SSL", lambda *a, **k: pytest.fail("Unexpected real SMTP"))
    monkeypatch.setattr(o, "_read_panamacompra_sheet", lambda *a: pytest.fail("Unexpected Sheets read"))


@pytest.mark.parametrize("alias", ["unidad solicitante", "unidad_solicitante", "UNIDAD DE COMPRA", "nombreUnidadCompra", "unidad_solic"])
def test_aliases_preserve_hospital_and_avoid_repeating_entity(alias):
    record = {"entidad": "Caja de Seguro Social", alias: HOSPITAL, "dependencia": "CSS - Sede"}
    body = "\n".join(notification_location_lines(record))
    assert body.splitlines() == ["Entidad: Caja de Seguro Social", "Hospital: " + HOSPITAL]
    assert body.count(HOSPITAL) == 1


def test_no_inferred_hospital_and_no_none_or_nan_in_mail():
    record = {"entidad": "CSS", "unidad de compra": None, "hospital": float("nan"),
              "titulo": "Hospital inventado", "provincia": "Panamá"}
    body = "\n".join(notification_location_lines(record))
    assert "Hospital: No especificado" in body
    assert "inventado" not in body and "nan" not in body and "None" not in body
    assert notification_location_from_row(["entidad", "hospital"], ["MINSA"]) == {"entidad": "MINSA"}


def test_only_hospital_is_shown_below_entity():
    lines = notification_location_lines({**ENTRY, "unidad de compra": "Dirección Nacional de Compras"})
    assert lines == ["Entidad: Caja de Seguro Social", "Hospital: " + HOSPITAL]


@pytest.mark.parametrize("record,expected", [
    ({"entidad": "MINSA", "unidad solicitante": "HSMA Compras", "dependencia": "Hospital San Miguel Arcangel"}, "Hospital San Miguel Arcangel"),
    ({"entidad": "MINSA", "unidad de compra": "MINSA Bocas del Toro - Compras", "dependencia": "Region de Salud de Bocas del Toro"}, "No especificado"),
    ({"entidad": "MINSA", "unidad solicitante": "Departamento de Compras de Medicamentos e Insumos para la Salud"}, "No especificado"),
    ({"entidad": "MINSA", "unidad de compra": "Departamento De Compras / Instituto Oncologico Nacional"}, "Departamento De Compras / Instituto Oncologico Nacional"),
])
def test_department_is_not_mislabeled_as_a_hospital(record, expected):
    assert notification_location_lines(record) == ["Entidad: MINSA", "Hospital: " + expected]


def test_keyword_summary_preserves_official_unit():
    result = summarize_keyword_rows(rows=[list(ENTRY.values())], cols=list(ENTRY), keyword_terms=["chiller"],
                                    source_sheet="cl_abiertas", job_name="clv")
    assert result["rows"][0]["unidad solicitante"] == HOSPITAL
    assert result["rows"][0]["dependencia"] == "CSS - Sede"


@pytest.mark.parametrize("scraper", ["clv", "clrir", "rir1"])
def test_real_scraper_summary_block_keeps_unit(scraper):
    tree = ast.parse((ROOT / scraper / (scraper + ".py")).read_text(encoding="utf-8"))
    block = next(node for node in ast.walk(tree) if isinstance(node, ast.If)
                 and isinstance(node.test, ast.Name) and node.test.id == "ct_rir_rows"
                 and "CT_RIR_SUMMARY_JSON=" in ast.unparse(node))
    output = []
    env = {"ct_rir_rows": [list(ENTRY.values())], "ct_rir_cols": list(ENTRY), "CFG": {"sheet_ct_rir": "ct_rir"},
           "notification_location_from_row": notification_location_from_row, "json": json,
           "print": lambda text, **kwargs: output.append(text)}
    exec(compile(ast.Module(body=[block], type_ignores=[]), "scraper_summary", "exec"), env)
    result = json.loads(output[-1].split("=", 1)[1])
    assert result["rows"][0]["unidad solicitante"] == HOSPITAL


@pytest.mark.parametrize("module", ["ct_rir", "rs_sp"])
def test_sheet_scanner_preserves_unit(monkeypatch, module):
    monkeypatch.setattr(o, "PANAMACOMPRA_CT_RIR_SCAN_SHEETS", ["ap_ct_rir"])
    monkeypatch.setattr(o, "PANAMACOMPRA_RS_SP_SCAN_SHEETS", ["ap_ct_rir"])
    monkeypatch.setattr(o, "_load_ct_rir_fichas_for_notifications", lambda: {"43358"})
    monkeypatch.setattr(o, "_load_rs_sp_keywords_for_notifications", lambda: ["chiller"])
    if hasattr(o, "_load_rs_sp_negative_keywords_for_notifications"):
        monkeypatch.setattr(o, "_load_rs_sp_negative_keywords_for_notifications", lambda: [])
    if hasattr(o, "_ct_rir_semantic_anchor_tokens"):
        monkeypatch.setattr(o, "_ct_rir_semantic_anchor_tokens", lambda *a: set())
    record = {**ENTRY, "enlace": "https://example.test/act"}
    monkeypatch.setattr(o, "_read_panamacompra_sheet", lambda *a: [list(record), list(record.values())])
    rows = getattr(o, "_scan_" + module + "_candidates")()
    assert len(rows) == 1 and rows[0]["unidad solicitante"] == HOSPITAL


@pytest.mark.parametrize("module", ["ct_rir", "rs_sp"])
@pytest.mark.parametrize("sheet", ["ap_ct_rir", "cl_prog_ct_rir", "cl_abiertas_ct_rir"])
def test_summary_to_queue_to_smtp_preserves_hospital_without_duplicates(monkeypatch, module, sheet):
    is_cl = sheet.startswith("cl_")
    code = "2026-1-10-01-08-" + ("CL" if is_cl else "LP") + "-050001"
    record = {**ENTRY, "hoja_origen": sheet, "enlace": "https://example.test/" + code + "/token"}
    prefix = "CT_RIR" if module == "ct_rir" else "RS_SP"
    payload = prefix + "_SUMMARY_JSON=" + json.dumps({"rows": [record], "sheet": sheet})
    queue = getattr(o, "_queue_" + module + "_notifications")
    assert queue("clv" if is_cl else "rir1", payload, datetime(2026, 9, 16, 18)) == 1
    assert queue("clv" if is_cl else "rir1", payload, datetime(2026, 9, 16, 18)) == 0
    monkeypatch.setattr(o, "_" + module + "_email_config", lambda: ("sender@example.test", "password", ["recipient@example.test"]))
    messages = []
    class SMTP:
        def __init__(self, *a, **k): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def login(self, *a): pass
        def send_message(self, msg): messages.append(msg); return {}
    monkeypatch.setattr(o.smtplib, "SMTP_SSL", SMTP)
    assert getattr(o, "_send_pending_" + module + "_email")()[2] == 1
    body = messages[0].get_content()
    assert "   Entidad: Caja de Seguro Social\n   Hospital: " + HOSPITAL + "\n" in body
    assert "CSS - Sede" not in body and "Dependencia:" not in body and "Unidad de compra:" not in body
    assert body.count(HOSPITAL) == 1
    assert queue("clv" if is_cl else "rir1", payload, datetime(2026, 9, 16, 18)) == 0


def test_reminder_enriches_hospital_from_fresh_official_detail(monkeypatch):
    if not (ROOT / "orquestador/ficha_deadline_reminders.py").exists():
        pytest.skip("Reminder module lives on master")
    import ficha_deadline_reminders as rem
    code = "2026-1-10-01-03-CL-050508"
    def fake(method, url, **kw):
        if method == "POST":
            return {"registros": [{"numProceso": code, "idEstado": 8, "idProcesosContratacionFlujos": 1,
                                   "idTipoProceso": 2, "nombreEntidad": "Caja de Seguro Social",
                                   "nombreUnidadCompra": "Unidad antigua", "nombreRealizado": "Abierta"}]}
        return {"pageComponentes": [{"tipo": "componentInfo", "value": [
            {"nombre": "Número de acto", "value": code}, {"nombre": "Unidad de Compra", "value": HOSPITAL},
            {"nombre": "Fecha y hora presentación", "value": "16-09-2026 hasta 11:00 AM"}]}]}
    monkeypatch.setattr(rem, "_json_request", fake)
    verified = rem.verify_public_entry({"enlace": "https://example.test/" + code})
    assert verified["unidad de compra"] == HOSPITAL
    body = rem.message_body([{**verified, "numero_proceso": code, "deadline_exact": True}])
    assert HOSPITAL in body and "Caja de Seguro Social" in body
