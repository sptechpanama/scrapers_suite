from __future__ import annotations

import sqlite3
from pathlib import Path

from db import publish_rir_price_sheets as publisher


def test_merge_research_headers_preserves_external_columns_and_is_idempotent() -> None:
    first = publisher.merge_research_headers(["id_estable", "columna_chatgpt"])
    second = publisher.merge_research_headers(first)
    assert first == second
    assert first[:2] == ["id_estable", "columna_chatgpt"]
    assert "numero_acto" in first
    assert "precio_competitivo_historico" in first


def test_load_historical_rows_uses_stable_numeric_order(tmp_path: Path) -> None:
    database = tmp_path / "analytics.db"
    connection = sqlite3.connect(database)
    connection.execute(
        """
        CREATE TABLE intel_ficha_price_benchmarks (
            ficha TEXT, nombre_ficha TEXT, unidad_comparable TEXT,
            precio_referencia_tipico REAL, precio_participacion_tipico REAL,
            precio_competitivo_historico REAL, actos_con_muestra INTEGER,
            muestras_referencia INTEGER, muestras_participacion INTEGER,
            muestras_ganadoras INTEGER, unidad_dominante_pct REAL,
            mapeo_explicito_pct REAL, ultima_muestra TEXT,
            nivel_confianza TEXT, confianza_precio TEXT, updated_at TEXT
        )
        """
    )
    connection.executemany(
        "INSERT INTO intel_ficha_price_benchmarks VALUES "
        "(?, ?, 'unidad', 12, 10, 9, 2, 2, 3, 1, 100, 100, "
        "'2026-08-01', 'Media', 'Media (2 actos)', '2026-09-04')",
        [("100", "Ficha 100"), ("20", "Ficha 20")],
    )
    connection.commit()
    connection.close()

    rows = publisher.load_historical_rows(database)
    assert [row[0] for row in rows] == ["20", "100"]
    assert rows[0][4] == 10.0
