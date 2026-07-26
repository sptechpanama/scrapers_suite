from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "db" / "db_api_updater.py"
SPEC = importlib.util.spec_from_file_location("db_api_updater_item_details_test", MODULE_PATH)
assert SPEC and SPEC.loader
updater = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(updater)


class ItemDetailTests(unittest.TestCase):
    def test_same_ficha_keeps_every_detected_item_position(self) -> None:
        best, detailed = updater._detect_fichas_with_line_evidence(
            {
                "titulo": "COMPRA DE INSUMOS",
                "item_1": "FICHA TECNICA 43358 KIT A",
                "item_2": "PRODUCTO NO RELACIONADO",
                "item_3": "FICHA TECNICA 43358 KIT B",
            }
        )
        self.assertEqual([match.code for match in best], ["43358"])
        self.assertEqual(
            {match.field for match in detailed if match.code == "43358"},
            {"item_1", "item_3"},
        )

    def test_legacy_reference_is_kept_as_line_total(self) -> None:
        record = updater._item_detail_record(
            {
                "descripcion": "KIT DE CIRCUITO",
                "numRenglon": 3,
                "cantidad": 10,
                "precioReferencia": "B/. 1,000.00",
            }
        )
        self.assertEqual(record["numero_renglon"], "3")
        self.assertEqual(record["precio_referencia_total"], 1000.0)
        self.assertEqual(record["precio_referencia_unitario"], 100.0)

    def test_total_is_calculated_from_explicit_unit_price_and_quantity(self) -> None:
        record = updater._item_detail_record(
            {
                "descripcion": "CINTA TESTIGO",
                "numeroRenglon": 2,
                "cantidadSolicitada": 20,
                "precioReferenciaUnitario": 12.5,
            }
        )
        self.assertEqual(record["precio_referencia_total"], 250.0)
        self.assertEqual(record["precio_referencia_unitario"], 12.5)


if __name__ == "__main__":
    unittest.main()
