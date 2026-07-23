from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "orquestador"))

from intel_ficha_worker import (  # noqa: E402
    _acts_for_ficha,
    _filter_acts_by_payload,
    _study_filters_from_payload,
)


class WorkerFilterTests(unittest.TestCase):
    def test_filter_uses_same_period_confidence_amount_and_search(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "id": 1,
                    "publicacion": "2026-02-15",
                    "fecha": "20-02-2026 a 21-02-2026",
                    "fecha_adjudicacion": "2026-03-01",
                    "fecha_actualizacion": "2026-03-02",
                    "estado": "Adjudicado",
                    "entidad": "CSS",
                    "precio_referencia": "B/. 15,000.00",
                    "total_items_ofertados": "B/. 12,500.00",
                    "titulo": "KIT DE CIRCUITO PARA REFRIGERACI\u00d3N",
                    "descripcion": "Equipo para anestesia",
                    "ficha_detectada": "* 43358",
                    "fichas_detectadas_json": json.dumps([{"code": "43358", "score": 96}]),
                },
                {
                    "id": 2,
                    "publicacion": "2025-02-15",
                    "fecha": "20-02-2025",
                    "fecha_adjudicacion": "2025-03-01",
                    "fecha_actualizacion": "2025-03-02",
                    "estado": "Adjudicado",
                    "entidad": "CSS",
                    "precio_referencia": "20,000.00",
                    "total_items_ofertados": "B/. 8,000.00",
                    "titulo": "KIT DE CIRCUITO",
                    "descripcion": "Anestesia",
                    "ficha_detectada": "43358",
                    "fichas_detectadas_json": json.dumps([{"code": "43358", "score": 90}]),
                },
            ]
        )
        filters = {
            "tipo_fecha": "publicacion",
            "fecha_desde": "2026-01-01",
            "fecha_hasta": "2026-12-31",
            "estados": ["Adjudicado"],
            "entidades": ["CSS"],
            "monto_minimo": 10_000,
            "adjudicado_minimo": 12_000,
            "adjudicado_maximo": 13_000,
            "score_minimo": 96,
            "busqueda": ["refrigeracion", "anestesia"],
            "modo_busqueda": "AND",
        }
        result = _filter_acts_by_payload(frame, filters, "43358")
        self.assertEqual(result["id"].tolist(), [1])

    def test_acts_for_ficha_includes_json_only_detection(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "source.db"
            with closing(sqlite3.connect(database)) as connection:
                connection.execute(
                    """
                    CREATE TABLE actos_publicos (
                        id INTEGER, enlace TEXT, titulo TEXT, entidad TEXT, descripcion TEXT,
                        ficha_detectada TEXT, fichas_detectadas_json TEXT, razon_social TEXT,
                        nombre_comercial TEXT, publicacion TEXT, fecha TEXT,
                        fecha_adjudicacion TEXT, fecha_actualizacion TEXT,
                        precio_referencia TEXT, termino_entrega TEXT, estado TEXT
                    )
                    """
                )
                connection.execute(
                    "INSERT INTO actos_publicos VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        7,
                        "https://example.test/7",
                        "INSUMO RESPIRATORIO",
                        "CSS",
                        "Sin c\u00f3digo visible",
                        "",
                        json.dumps([{"code": "43358", "score": 96, "method": "nombre_exacto"}]),
                        "GANADOR",
                        "GANADOR",
                        "2026-05-01",
                        "2026-05-05",
                        "2026-05-10",
                        "2026-05-11",
                        "1000",
                        "30 d\u00edas",
                        "Adjudicado",
                    ),
                )
                connection.commit()
            result = _acts_for_ficha(database, "43358")
            self.assertEqual(result["id"].tolist(), [7])

    def test_historical_scope_is_default_and_ignores_visual_filters(self) -> None:
        scope, filters = _study_filters_from_payload(
            {
                "filters": {
                    "fecha_desde": "2026-01-01",
                    "fecha_hasta": "2026-12-31",
                }
            }
        )
        self.assertEqual(scope, "historico_completo")
        self.assertEqual(filters, {})

        scope, filters = _study_filters_from_payload(
            {
                "study_scope": "analisis_actual",
                "filters": {"fecha_desde": "2026-01-01"},
            }
        )
        self.assertEqual(scope, "analisis_actual")
        self.assertEqual(filters, {"fecha_desde": "2026-01-01"})

    def test_acts_for_ficha_uses_normalized_analytics_relation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            source_database = Path(temp_dir) / "source.db"
            analytics_database = Path(temp_dir) / "analytics.db"
            with closing(sqlite3.connect(source_database)) as connection:
                connection.execute(
                    """
                    CREATE TABLE actos_publicos (
                        id INTEGER, enlace TEXT, titulo TEXT, entidad TEXT, descripcion TEXT,
                        ficha_detectada TEXT, fichas_detectadas_json TEXT, razon_social TEXT,
                        nombre_comercial TEXT, publicacion TEXT, fecha TEXT,
                        fecha_adjudicacion TEXT, fecha_actualizacion TEXT,
                        precio_referencia TEXT, termino_entrega TEXT, estado TEXT
                    )
                    """
                )
                connection.execute(
                    "INSERT INTO actos_publicos VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        99,
                        "https://example.test/99",
                        "INSUMO RESPIRATORIO",
                        "CSS",
                        "La fila operacional aún no fue reclasificada",
                        "",
                        "",
                        "GANADOR",
                        "GANADOR",
                        "2025-05-01",
                        "2025-05-05",
                        "2025-05-10",
                        "2025-05-11",
                        "1000",
                        "30 días",
                        "Adjudicado",
                    ),
                )
                connection.commit()
            with closing(sqlite3.connect(analytics_database)) as connection:
                connection.execute(
                    """
                    CREATE TABLE intel_actos_fichas (
                        source_id TEXT, enlace TEXT, ficha TEXT
                    )
                    """
                )
                connection.execute(
                    "INSERT INTO intel_actos_fichas VALUES (?,?,?)",
                    ("99", "https://example.test/99", "43358"),
                )
                connection.commit()

            result = _acts_for_ficha(
                source_database,
                "43358",
                analytics_db=analytics_database,
            )
            self.assertEqual(result["id"].tolist(), [99])


if __name__ == "__main__":
    unittest.main()
