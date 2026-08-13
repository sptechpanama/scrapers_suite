from __future__ import annotations

import unittest
import sys
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "orquestador"))

from orquestador import main as orchestrator


class CtRirRecoveryTests(unittest.TestCase):
    def test_scan_recovers_existing_no_detectada_row(self) -> None:
        rows = [
            [
                "titulo", "descripcion", "item_1", "enlace", "entidad",
                "fecha", "precio_referencia", "ficha_detectada", "descartar",
            ],
            [
                "KIT DE CIRCUITO DE PACIENTES PARA MAQUINA DE ANESTESIA",
                "SE UTILIZA PARA ADMINISTRAR GASES ANESTESICOS",
                "KIT DE CIRCUITO DE PACIENTES PARA MAQUINA DE ANESTESIA",
                "https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/2026-1-10-01-08-CL-048336/x",
                "CAJA DE SEGURO SOCIAL",
                "2026-08-13",
                "3750",
                "No Detectada",
                "",
            ],
        ]
        with (
            patch.object(orchestrator, "PANAMACOMPRA_CT_RIR_SCAN_SHEETS", ["cl_abiertas"]),
            patch.object(orchestrator, "_load_ct_rir_fichas_for_notifications", return_value={"43358"}),
            patch.object(orchestrator, "_read_panamacompra_sheet", return_value=rows),
        ):
            candidates = orchestrator._scan_ct_rir_candidates()

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["ficha_detectada"], "* 43358")
        self.assertIn("048336", candidates[0]["enlace"])

    def test_scan_does_not_recover_generic_text(self) -> None:
        rows = [
            ["titulo", "descripcion", "enlace", "ficha_detectada", "descartar"],
            [
                "COMPRA DE KITS PARA PACIENTES",
                "INSUMOS GENERALES DE ANESTESIA",
                "https://example.test/generic",
                "No Detectada",
                "",
            ],
        ]
        with (
            patch.object(orchestrator, "PANAMACOMPRA_CT_RIR_SCAN_SHEETS", ["cl_abiertas"]),
            patch.object(orchestrator, "_load_ct_rir_fichas_for_notifications", return_value={"43358"}),
            patch.object(orchestrator, "_read_panamacompra_sheet", return_value=rows),
        ):
            self.assertEqual(orchestrator._scan_ct_rir_candidates(), [])


if __name__ == "__main__":
    unittest.main()
