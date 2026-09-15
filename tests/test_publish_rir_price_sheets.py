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


class FakeSheets:
    def __init__(self, rows, *, fail=False):
        self.rows = rows
        self.fail = fail
        self.writes = []
        self.pending = None

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def batchGet(self, **kwargs):
        self.pending = lambda: {"valueRanges": [{"values": self.rows}, {"values": [publisher.RESEARCH_HEADERS]}]}
        return self

    def batchUpdate(self, **kwargs):
        def update():
            if self.fail:
                raise ConnectionError("publication unavailable")
            self.writes.append(kwargs['body'])
            self.rows = kwargs['body']['data'][0]['values']
            return {}
        self.pending = update
        return self

    def get(self, **kwargs):
        self.pending = lambda: {"values": self.rows}
        return self

    def execute(self):
        return self.pending()


def test_prices_replace_atomically_without_touching_research_rows(monkeypatch):
    monkeypatch.setattr(publisher, '_ensure_sheets', lambda *args: None)
    monkeypatch.setattr(publisher, '_call_with_backoff', lambda action: action())
    width = len(publisher.HISTORICAL_HEADERS)
    row = ['123'] + [''] * (width - 1)
    old = [publisher.HISTORICAL_HEADERS, row, ['obsolete'] + [''] * (width - 1)]
    service = FakeSheets(old)
    result = publisher.publish_verified(service, 'spreadsheet', [row])
    assert result['historical_rows'] == 1
    assert len(service.writes) == 1
    assert len(service.writes[0]['data']) == 1
    assert publisher.HISTORICAL_SHEET in service.writes[0]['data'][0]['range']
    assert service.rows[-1] == [''] * width


def test_failed_publication_preserves_previous_prices(monkeypatch):
    import pytest
    monkeypatch.setattr(publisher, '_ensure_sheets', lambda *args: None)
    monkeypatch.setattr(publisher, '_call_with_backoff', lambda action: action())
    old = [publisher.HISTORICAL_HEADERS, ['old']]
    service = FakeSheets(old, fail=True)
    with pytest.raises(ConnectionError):
        publisher.publish_verified(service, 'spreadsheet', [['new'] + [''] * 15])
    assert service.rows is old
    assert service.writes == []


def test_database_hook_runs_price_sync_only_after_success():
    from types import SimpleNamespace
    from orquestador.run_database_with_prices import main
    calls = []
    def runner(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0)
    assert main(['--mode', 'incremental'], pipeline=lambda argv: 2, runner=runner) == 2
    assert not calls
    seen = []
    def pipeline(argv):
        seen.append(argv)
        return 0
    assert main(['--mode', 'full'], pipeline=pipeline, runner=runner) == 0
    assert seen == [['--mode', 'full']]
    assert calls[0][0][-1].endswith('publish_rir_price_sheets.py')


def test_database_hook_reports_price_publication_failure():
    from types import SimpleNamespace
    from orquestador.run_database_with_prices import main
    assert main([], pipeline=lambda argv: 0, runner=lambda *args, **kwargs: SimpleNamespace(returncode=7)) == 7
