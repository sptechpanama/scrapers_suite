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
