from __future__ import annotations

import sys
from pathlib import Path


ORCHESTRATOR_DIR = Path(__file__).resolve().parents[1] / "orquestador"
if str(ORCHESTRATOR_DIR) not in sys.path:
    sys.path.insert(0, str(ORCHESTRATOR_DIR))

import main as orchestrator  # noqa: E402


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
