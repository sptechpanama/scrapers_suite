"""Recovery scanning must include law-419 sheets, without matching by law alone."""
import ast
from pathlib import Path


def test_419_sources_are_scanned_but_never_unconditionally_accepted():
    source = (Path(__file__).resolve().parents[1] / 'orquestador/main.py').read_text(encoding='utf-8-sig')
    tree = ast.parse(source)
    values = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            if node.targets[0].id in {'PANAMACOMPRA_CT_RIR_SCAN_SHEETS','PANAMACOMPRA_CT_RIR_DIRECT_SHEETS'}:
                values[node.targets[0].id] = set(ast.literal_eval(node.value))
    new = {'cl_abiertas_419_sfd','cl_prog_419_sfd','ap_419_sfd'}
    assert new <= values['PANAMACOMPRA_CT_RIR_SCAN_SHEETS']
    assert not new & values['PANAMACOMPRA_CT_RIR_DIRECT_SHEETS']
