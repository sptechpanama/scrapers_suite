from __future__ import annotations

import importlib.util
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "db" / "build_intelligence_tables.py"
SPEC = importlib.util.spec_from_file_location("build_intelligence_tables", MODULE_PATH)
assert SPEC and SPEC.loader
builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(builder)


class BuilderUnitTests(unittest.TestCase):
    def test_money_formats(self) -> None:
        cases = {
            "B/. 12,345.67": 12345.67,
            "$12.345,67": 12345.67,
            "12345,67": 12345.67,
            950.5: 950.5,
            "": 0.0,
        }
        for raw, expected in cases.items():
            self.assertAlmostEqual(builder.parse_number(raw), expected, places=2)

    def test_iso_and_local_dates_are_not_swapped(self) -> None:
        self.assertEqual(builder.parse_date("2026-07-12"), "2026-07-12")
        self.assertEqual(builder.parse_date("12/07/2026"), "2026-07-12")

    def test_repeated_ficha_is_one_distinct_ficha(self) -> None:
        row = {
            "id": 1,
            "enlace": "https://example.test/acto/1",
            "ficha_detectada": "* 43358",
            "fichas_detectadas_json": json.dumps(
                [
                    {"code": "43358", "method": "nombre_compacto", "score": 90},
                    {"code": "43358", "method": "codigo_contextual", "score": 100},
                ]
            ),
            "estado": "Adjudicado",
            "precio_referencia": "B/. 4,000.00",
            "razon_social": "BIOMEDICAL AND TECHNOLOGIES SUPPORT PANAMA, S.A",
            "nombre_comercial": "BTS, PANAMA",
            "Proponente 1": "BTS, PANAMA",
            "Precio Proponente 1": "3,700.00",
        }
        facts, proponents = builder.row_to_records(row)
        self.assertEqual(len(facts), 1)
        self.assertEqual(facts[0]["ficha"], "43358")
        self.assertEqual(facts[0]["detected_ficha_count"], 1)
        self.assertEqual(facts[0]["is_unique_ficha"], 1)
        self.assertEqual(facts[0]["detection_score"], 100)
        self.assertEqual(facts[0]["award_amount"], 3700.0)
        self.assertEqual(proponents[0]["is_winner"], 1)

    def test_two_distinct_fichas_are_not_unique(self) -> None:
        row = {
            "id": 2,
            "enlace": "https://example.test/acto/2",
            "ficha_detectada": "43358, * 103169",
            "fichas_detectadas_json": json.dumps(
                [{"code": "43358", "score": 96}, {"code": "103169", "score": 90}]
            ),
        }
        facts, _ = builder.row_to_records(row)
        self.assertEqual({fact["ficha"] for fact in facts}, {"43358", "103169"})
        self.assertTrue(all(fact["is_unique_ficha"] == 0 for fact in facts))
        self.assertTrue(all(fact["detected_ficha_count"] == 2 for fact in facts))

    def test_winner_match_uses_legal_or_trade_name(self) -> None:
        self.assertTrue(builder.provider_matches_winner("BTS, PANAMA", ["BIOMEDICAL AND TECHNOLOGIES SUPPORT PANAMA, S.A", "BTS, PANAMA"]))
        self.assertFalse(builder.provider_matches_winner("OTRA EMPRESA", ["BTS, PANAMA"]))


class BuilderIntegrationTests(unittest.TestCase):
    def test_builds_verified_normalized_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_db = root / "source.db"
            output_db = root / "analytics.db"
            metadata_xlsx = root / "metadata.xlsx"
            catalog_xlsx = root / "catalog.xlsx"
            aliases_json = root / "aliases.json"

            connection = sqlite3.connect(source_db)
            columns = [
                'id INTEGER PRIMARY KEY', 'enlace TEXT', 'titulo TEXT', 'entidad TEXT', 'unidad_solic TEXT',
                'estado TEXT', 'publicacion TEXT', 'fecha TEXT', 'fecha_adjudicacion TEXT',
                'fecha_actualizacion TEXT', 'precio_referencia TEXT', 'total_items_ofertados TEXT',
                'num_participantes TEXT', 'razon_social TEXT', 'nombre_comercial TEXT',
                'ficha_detectada TEXT', 'fichas_detectadas_json TEXT', 'ficha_detector_version TEXT',
                'ficha_catalogo_version TEXT',
            ]
            for index in range(1, 15):
                columns.extend([f'"Proponente {index}" TEXT', f'"Precio Proponente {index}" TEXT'])
            connection.execute(f"CREATE TABLE actos_publicos ({', '.join(columns)})")
            connection.execute(
                '''INSERT INTO actos_publicos (
                    id,enlace,titulo,entidad,estado,publicacion,fecha,fecha_adjudicacion,fecha_actualizacion,
                    precio_referencia,total_items_ofertados,num_participantes,razon_social,nombre_comercial,
                    ficha_detectada,fichas_detectadas_json,ficha_detector_version,"Proponente 1","Precio Proponente 1"
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (
                    1, "https://example.test/1", "KIT CIRCUITO PACIENTE", "CSS", "Adjudicado",
                    "15-01-2026", "20-01-2026", "25-01-2026", "26-01-2026", "10,000.00",
                    "9,000.00", "1", "BTS PANAMA", "BTS", "* 43358",
                    json.dumps([{"code": "43358", "method": "nombre_exacto", "score": 96}]),
                    "3.1.0", "BTS", "9,000.00",
                ),
            )
            connection.commit()
            connection.close()

            pd.DataFrame(
                [{
                    "Número Ficha": 43358,
                    "Nombre Genérico": "KIT DE CIRCUITO DE PACIENTE PARA MÁQUINA DE ANESTESIA",
                    "Criterio": "SI",
                    "Registro Sanitario": "NO",
                    "enlace_ficha_tecnica": "https://ctni.test/43358",
                }]
            ).to_excel(metadata_xlsx, index=False)
            pd.DataFrame(
                [{"N° Ficha CTNI": 43358, "Oferente": "PROVEEDOR A", "Contacto": "Ana", "Correo": "a@test.local"}]
            ).to_excel(catalog_xlsx, index=False)
            aliases_json.write_text("{}", encoding="utf-8")

            result = builder.build_local_analytics(source_db, output_db, metadata_xlsx, catalog_xlsx, aliases_json)
            verified = builder.verify_analytics(output_db)
            self.assertEqual(result["fact_rows"], 1)
            self.assertEqual(verified["intel_actos_fichas"], 1)
            self.assertEqual(result["monthly_rows"], 12)
            self.assertEqual(verified["intel_metricas_ficha_mes"], 12)
            with closing(sqlite3.connect(output_db)) as analytics:
                fact = analytics.execute(
                    "SELECT ficha,is_unique_ficha,reference_amount,award_amount,search_text_norm FROM intel_actos_fichas"
                ).fetchone()
                self.assertEqual(fact[:4], ("43358", 1, 10000.0, 9000.0))
                self.assertIn("kit circuito paciente", fact[4])
                monthly = analytics.execute(
                    "SELECT actos,actos_ficha_unica,monto_referencia,monto_adjudicado "
                    "FROM intel_metricas_ficha_mes WHERE date_basis='publicacion' "
                    "AND period_month='2026-01' AND detection_profile='moderado' AND ficha='43358'"
                ).fetchone()
                self.assertEqual(monthly, (1, 1, 10000.0, 9000.0))


if __name__ == "__main__":
    unittest.main()
