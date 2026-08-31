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
    def test_postgres_indexes_cover_interactive_analytics_paths(self) -> None:
        statements = dict(builder.POSTGRES_ANALYTICS_INDEXES)
        expected = {
            "ux_intel_actos_fichas",
            "ix_intel_iaf_acto_score",
            "ix_intel_iaf_publication",
            "ix_intel_iaf_publication_profile",
            "ix_intel_iaf_score",
            "ix_intel_iaf_estado",
            "ix_intel_iaf_entidad",
            "ix_intel_iap_acto",
            "ix_intel_iap_provider",
            "ix_intel_iap_winner",
            "ix_intel_ifm_basis_profile_month_ficha",
            "ix_intel_ifc_ficha",
            "ix_intel_ifc_oferente",
            "ix_intel_ifmeta_ficha",
            "ix_intel_ifmeta_rs",
            "ix_intel_ifmeta_risk",
        }
        self.assertTrue(expected.issubset(statements))
        self.assertEqual(len(statements), len(builder.POSTGRES_ANALYTICS_INDEXES))
        self.assertTrue(all("IF NOT EXISTS" in statement for statement in statements.values()))

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

    def test_sqlite_batch_never_exceeds_bound_parameter_budget(self) -> None:
        rows = builder._sqlite_multi_chunksize(len(builder.FACT_COLUMNS))
        self.assertLessEqual(rows * len(builder.FACT_COLUMNS), builder.SQLITE_MAX_BOUND_PARAMETERS)
        self.assertGreater(rows, 0)

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
        self.assertEqual(facts[0]["reference_amount_context"], 4000.0)
        self.assertEqual(facts[0]["reference_amount_attributed"], 0.0)
        self.assertEqual(facts[0]["award_amount_attributed"], 0.0)
        self.assertEqual(facts[0]["reference_amount_attribution_source"], "sin_detalle_renglones")
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

    def test_single_line_unique_ficha_uses_whole_act_amount_safely(self) -> None:
        row = {
            "id": 3,
            "enlace": "https://example.test/acto/3",
            "ficha_detectada": "* 43358",
            "fichas_detectadas_json": json.dumps(
                [{"code": "43358", "field": "item_1", "score": 96}]
            ),
            "items_json": json.dumps(
                [
                    {
                        "descripcion": "KIT CIRCUITO PACIENTE",
                        "numero_renglon": "1",
                        "cantidad": 10,
                        "precio_referencia_total": 4000,
                    }
                ]
            ),
            "precio_referencia": 4000,
            "estado": "Adjudicado",
            "razon_social": "BTS",
            "Proponente 1": "BTS",
            "Precio Proponente 1": 3700,
        }
        facts, _ = builder.row_to_records(row)
        fact = facts[0]
        self.assertEqual(fact["source_line_count"], 1)
        self.assertEqual(fact["reference_amount_attributed"], 4000.0)
        self.assertEqual(fact["award_amount_attributed"], 3700.0)
        self.assertEqual(fact["reference_amount_attribution_source"], "api_renglon_detectado")
        self.assertEqual(fact["award_amount_attribution_source"], "acto_un_renglon_ficha_unica")

    def test_multiline_act_attributes_only_the_detected_line_amount(self) -> None:
        row = {
            "id": 4,
            "enlace": "https://example.test/acto/4",
            "ficha_detectada": "* 43358, * 103169",
            "fichas_detectadas_json": json.dumps(
                [
                    {"code": "43358", "field": "item_1", "score": 96},
                    {"code": "103169", "field": "item_2", "score": 96},
                ]
            ),
            "items_json": json.dumps(
                [
                    {"descripcion": "CIRCUITO", "precio_referencia_total": 1000},
                    {"descripcion": "EQUIPO AJENO", "precio_referencia_total": 100000},
                    {"descripcion": "OTRO EQUIPO", "precio_referencia_total": 100000},
                ]
            ),
            "precio_referencia": 201000,
            "estado": "Adjudicado",
            "razon_social": "GANADOR",
            "Proponente 1": "GANADOR",
            "Precio Proponente 1": 190000,
        }
        facts, _ = builder.row_to_records(row)
        by_ficha = {fact["ficha"]: fact for fact in facts}
        self.assertEqual(by_ficha["43358"]["reference_amount_attributed"], 1000.0)
        self.assertEqual(by_ficha["103169"]["reference_amount_attributed"], 100000.0)
        self.assertEqual(by_ficha["43358"]["reference_amount_context"], 201000.0)
        self.assertEqual(by_ficha["43358"]["award_amount_attributed"], 0.0)
        self.assertEqual(
            by_ficha["43358"]["award_amount_attribution_source"],
            "sin_adjudicacion_por_renglon_confirmada",
        )

    def test_repeated_ficha_in_multiple_lines_sums_only_its_lines(self) -> None:
        row = {
            "id": 41,
            "enlace": "https://example.test/acto/41",
            "ficha_detectada": "* 43358",
            "fichas_detectadas_json": json.dumps(
                [
                    {"code": "43358", "field": "item_1", "score": 96},
                    {"code": "43358", "field": "item_3", "score": 96},
                ]
            ),
            "items_json": json.dumps(
                [
                    {"descripcion": "CIRCUITO MEDIDA A", "precio_referencia_total": 1000},
                    {"descripcion": "EQUIPO AJENO", "precio_referencia_total": 100000},
                    {"descripcion": "CIRCUITO MEDIDA B", "precio_referencia_total": 2000},
                ]
            ),
            "precio_referencia": 103000,
            "estado": "Adjudicado",
            "razon_social": "GANADOR",
            "Proponente 1": "GANADOR",
            "Precio Proponente 1": 95000,
        }
        facts, _ = builder.row_to_records(row)
        fact = facts[0]
        self.assertEqual(fact["detected_ficha_count"], 1)
        self.assertEqual(fact["is_unique_ficha"], 1)
        self.assertEqual(fact["attributed_line_count"], 2)
        self.assertEqual(fact["reference_amount_attributed"], 3000.0)
        self.assertEqual(fact["reference_amount_context"], 103000.0)
        self.assertEqual(fact["award_amount_attributed"], 0.0)

    def test_confirmed_deep_study_amount_has_priority(self) -> None:
        row = {
            "id": 5,
            "enlace": "https://example.test/acto/5",
            "ficha_detectada": "* 43358",
            "fichas_detectadas_json": json.dumps(
                [{"code": "43358", "field": "titulo", "score": 96}]
            ),
            "items_json": json.dumps(
                [
                    {"descripcion": "RENGLON A"},
                    {"descripcion": "RENGLON B"},
                ]
            ),
            "precio_referencia": 201000,
        }
        facts, _ = builder.row_to_records(
            row,
            confirmed_line_amounts={
                ("https://example.test/acto/5", "43358"): (1250.5, 1)
            },
        )
        self.assertEqual(facts[0]["reference_amount_attributed"], 1250.5)
        self.assertEqual(
            facts[0]["reference_amount_attribution_source"],
            "estudio_renglon_confirmado",
        )
        self.assertEqual(facts[0]["attributed_line_count"], 1)

    def test_confirmed_deep_study_attributes_only_the_winner_lines(self) -> None:
        row = {
            "id": 6,
            "enlace": "https://example.test/acto/6",
            "ficha_detectada": "* 43358",
            "fichas_detectadas_json": json.dumps(
                [{"code": "43358", "field": "titulo", "score": 96}]
            ),
            "items_json": json.dumps(
                [{"descripcion": "RENGLON A"}, {"descripcion": "RENGLON B"}]
            ),
            "precio_referencia": 201000,
            "estado": "Adjudicado",
            "razon_social": "BIOMEDICAL AND TECHNOLOGIES SUPPORT PANAMA, S.A",
            "nombre_comercial": "BTS PANAMA",
            "Proponente 1": "BTS PANAMA",
            "Precio Proponente 1": 190000,
        }
        facts, _ = builder.row_to_records(
            row,
            confirmed_line_amounts={
                ("https://example.test/acto/6", "43358"): {
                    "reference_amount": 1250.5,
                    "line_count": 2,
                    "award_by_provider": {
                        "BTS PANAMA": 1100.0,
                        "OTRO PROPONENTE": 900.0,
                    },
                }
            },
        )
        self.assertEqual(facts[0]["reference_amount_attributed"], 1250.5)
        self.assertEqual(facts[0]["award_amount_attributed"], 1100.0)
        self.assertEqual(
            facts[0]["award_amount_attribution_source"],
            "estudio_renglon_ganador_confirmado",
        )

    def test_load_confirmed_lines_deduplicates_reference_by_line_and_provider(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.execute(
            """
            CREATE TABLE intel_ficha_line_amounts (
                ficha TEXT, acto_url TEXT, line_key TEXT, renglon_id TEXT,
                renglon_numero TEXT, reference_total REAL,
                participation_total REAL, provider TEXT, requires_review INTEGER
            )
            """
        )
        connection.executemany(
            "INSERT INTO intel_ficha_line_amounts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("43358", "https://acto/1", "1", "1", "1", 1000, 900, "BTS", 0),
                ("43358", "https://acto/1", "1", "1", "1", 1000, 850, "OTRO", 0),
                ("43358", "https://acto/1", "2", "2", "2", 500, 450, "BTS", 0),
                ("43358", "https://acto/1", "3", "3", "3", 9999, 9999, "BTS", 1),
            ],
        )
        amounts = builder.load_confirmed_line_amounts(connection)
        connection.close()
        record = amounts[("https://acto/1", "43358")]
        self.assertEqual(record["reference_amount"], 1500.0)
        self.assertEqual(record["line_count"], 2)
        self.assertEqual(record["award_by_provider"]["BTS"], 1350.0)
        self.assertEqual(record["award_by_provider"]["OTRO"], 850.0)

    def test_winner_match_uses_legal_or_trade_name(self) -> None:
        self.assertTrue(builder.provider_matches_winner("BTS, PANAMA", ["BIOMEDICAL AND TECHNOLOGIES SUPPORT PANAMA, S.A", "BTS, PANAMA"]))
        self.assertFalse(builder.provider_matches_winner("OTRA EMPRESA", ["BTS, PANAMA"]))

    def test_metadata_is_completed_from_scraper_classification(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            metadata_xlsx = root / "metadata.xlsx"
            classification_xlsx = root / "todas_las_fichas.xlsx"
            risk_class_xlsx = root / "risk_classes.xlsx"
            aliases_json = root / "aliases.json"
            pd.DataFrame(
                [{"Numero Ficha": 43358, "Nombre Generico": "CIRCUITO", "Criterio": "SI", "Registro Sanitario": "NO"}]
            ).to_excel(metadata_xlsx, index=False)
            pd.DataFrame(
                [
                    [43358, "SI", "NO"],
                    [100523, "SI", "NO"],
                    [22241, "NO", "SI RS LCRSP"],
                ]
            ).to_excel(classification_xlsx, index=False, header=False)
            pd.DataFrame(
                [
                    {"Numero Ficha": 43358, "Clase de Riesgo": "A"},
                    {"Numero Ficha": 100523, "Clase de Riesgo": "B"},
                    {"Numero Ficha": 22241, "Clase de Riesgo": "D"},
                    {"Numero Ficha": 99999, "Clase de Riesgo": "No Aplica"},
                ]
            ).to_excel(risk_class_xlsx, index=False)
            aliases_json.write_text(json.dumps({"100523": ["FICHA DE PRUEBA"]}), encoding="utf-8")

            result = builder.load_metadata(
                metadata_xlsx,
                aliases_json,
                classification_xlsx,
                risk_class_xlsx,
            ).set_index("ficha")
            self.assertIn("100523", result.index)
            self.assertEqual(result.loc["100523", "tiene_ct"], "Si")
            self.assertEqual(result.loc["100523", "registro_sanitario"], "No")
            self.assertEqual(result.loc["100523", "nombre_ficha"], "FICHA DE PRUEBA")
            self.assertEqual(result.loc["100523", "clase_riesgo"], "B")
            self.assertEqual(result.loc["22241", "registro_sanitario"], "Si")
            self.assertEqual(result.loc["22241", "clase_riesgo"], "D")
            self.assertEqual(result.loc["99999", "clase_riesgo"], "NO APLICA")
            self.assertIn(classification_xlsx.name, result.loc["100523", "metadata_source"])


class BuilderIntegrationTests(unittest.TestCase):
    def test_builds_verified_normalized_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source_db = root / "source.db"
            output_db = root / "analytics.db"
            metadata_xlsx = root / "metadata.xlsx"
            catalog_xlsx = root / "catalog.xlsx"
            aliases_json = root / "aliases.json"
            classification_xlsx = root / "classification.xlsx"
            risk_class_xlsx = root / "risk_classes.xlsx"

            connection = sqlite3.connect(source_db)
            columns = [
                'id INTEGER PRIMARY KEY', 'enlace TEXT', 'titulo TEXT', 'entidad TEXT', 'unidad_solic TEXT',
                'estado TEXT', 'publicacion TEXT', 'fecha TEXT', 'fecha_adjudicacion TEXT',
                'fecha_actualizacion TEXT', 'precio_referencia TEXT', 'total_items_ofertados TEXT',
                'num_participantes TEXT', 'razon_social TEXT', 'nombre_comercial TEXT',
                'ficha_detectada TEXT', 'fichas_detectadas_json TEXT', 'ficha_detector_version TEXT',
                'ficha_catalogo_version TEXT', 'items_json TEXT',
            ]
            for index in range(1, 15):
                columns.extend([f'"Proponente {index}" TEXT', f'"Precio Proponente {index}" TEXT'])
            connection.execute(f"CREATE TABLE actos_publicos ({', '.join(columns)})")
            connection.execute(
                '''INSERT INTO actos_publicos (
                    id,enlace,titulo,entidad,estado,publicacion,fecha,fecha_adjudicacion,fecha_actualizacion,
                    precio_referencia,total_items_ofertados,num_participantes,razon_social,nombre_comercial,
                    ficha_detectada,fichas_detectadas_json,ficha_detector_version,items_json,
                    "Proponente 1","Precio Proponente 1"
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (
                    1, "https://example.test/1", "KIT CIRCUITO PACIENTE", "CSS", "Adjudicado",
                    "15-01-2026", "20-01-2026", "25-01-2026", "26-01-2026", "10,000.00",
                    "9,000.00", "1", "BTS PANAMA", "BTS", "* 43358",
                    json.dumps([{"code": "43358", "method": "nombre_exacto", "score": 96}]),
                    "3.1.0",
                    json.dumps([{"descripcion": "KIT CIRCUITO", "precio_referencia_total": 10000}]),
                    "BTS", "9,000.00",
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
            pd.DataFrame([[43358, "SI", "NO"]]).to_excel(classification_xlsx, index=False, header=False)

            result = builder.build_local_analytics(
                source_db,
                output_db,
                metadata_xlsx,
                catalog_xlsx,
                aliases_json,
                classification_xlsx=classification_xlsx,
                risk_class_xlsx=risk_class_xlsx,
            )
            verified = builder.verify_analytics(output_db)
            self.assertEqual(result["fact_rows"], 1)
            self.assertEqual(verified["intel_actos_fichas"], 1)
            self.assertEqual(result["monthly_rows"], 16)
            self.assertEqual(verified["intel_metricas_ficha_mes"], 16)
            with closing(sqlite3.connect(output_db)) as analytics:
                fact = analytics.execute(
                    "SELECT ficha,is_unique_ficha,reference_amount_attributed,award_amount_attributed,"
                    "search_text_norm FROM intel_actos_fichas"
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
