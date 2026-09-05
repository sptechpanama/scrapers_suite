from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRAPERS = {
    "clv/clv.py": "cl_abiertas_419_sfd",
    "clrir/clrir.py": "cl_prog_419_sfd",
    "rir1/rir1.py": "ap_419_sfd",
}


def test_all_scrapers_extract_route_and_publish_process_law() -> None:
    for relative_path, target_sheet in SCRAPERS.items():
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        ast.parse(source)
        assert "extract_process_law(page.d)" in source
        assert "is_law_419_without_ficha(" in source
        assert 'return "419_sfd"' in source
        assert "PROCESS_LAW_COLUMN," in source
        assert target_sheet in source
        assert "Ley419SinFicha=" in source


def test_windows_console_logs_do_not_use_unicode_move_arrow() -> None:
    for relative_path in SCRAPERS:
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert " → " not in source


def test_law_419_rule_precedes_legacy_no_requirements_routing() -> None:
    for relative_path in SCRAPERS:
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        function_start = source.index("def clasifica(info):")
        function_end = source.index("\n# =========================", function_start)
        function_source = source[function_start:function_end]
        assert function_source.index("is_law_419_without_ficha") < function_source.index(
            '_eligible_sin_requisitos'
        )


def test_database_updater_persists_process_law_in_dynamic_schema() -> None:
    source = (ROOT / "db" / "db_api_updater.py").read_text(encoding="utf-8")
    ast.parse(source)
    assert '"ley_proceso": "TEXT"' in source
    assert '"ley_proceso": process_law' in source


def test_orchestrator_rs_sp_scans_new_sheets_without_mixing_ct_rir() -> None:
    source = (ROOT / "orquestador" / "main.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    assignments = {
        node.targets[0].id: ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and node.targets[0].id
        in {"PANAMACOMPRA_RS_SP_SCAN_SHEETS", "PANAMACOMPRA_CT_RIR_SCAN_SHEETS"}
    }
    expected = set(SCRAPERS.values())
    assert expected.issubset(assignments["PANAMACOMPRA_RS_SP_SCAN_SHEETS"])
    assert expected.isdisjoint(assignments["PANAMACOMPRA_CT_RIR_SCAN_SHEETS"])
