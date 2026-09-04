from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest import mock

from db import backfill_rir_price_evidence as backfill


def _analytics_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE intel_actos_fichas (ficha TEXT, enlace TEXT);
        CREATE TABLE intel_ficha_metadata (
            ficha TEXT, tiene_ct TEXT, registro_sanitario TEXT
        );
        INSERT INTO intel_ficha_metadata VALUES ('100', 'No', 'No');
        INSERT INTO intel_ficha_metadata VALUES ('200', 'Si', 'No');
        INSERT INTO intel_actos_fichas VALUES ('100', 'https://acto/1');
        INSERT INTO intel_actos_fichas VALUES ('100', 'https://acto/1');
        INSERT INTO intel_actos_fichas VALUES ('200', 'https://acto/2');
        """
    )
    connection.commit()
    connection.close()


def _source_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE actos_publicos (
            enlace TEXT UNIQUE,
            source_flow_id TEXT,
            source_tipo_proceso TEXT
        )
        """
    )
    connection.execute(
        "INSERT INTO actos_publicos VALUES ('https://acto/1', '123', '7')"
    )
    connection.commit()
    connection.close()


def test_scope_is_deduplicated_and_restricted_to_no_ct_no_rs(tmp_path: Path) -> None:
    analytics = tmp_path / "analytics.db"
    _analytics_db(analytics)
    assert backfill.eligible_links(analytics) == ["https://acto/1"]


def test_source_ids_are_recovered_from_official_route_token(tmp_path: Path) -> None:
    link = (
        "https://www.panamacompra.gob.pa/Inicio/#/pliego-de-cargos/"
        "2025-0-27-01-08-CM-001797/Qf0ojIwRnIsUDOwkzN2ojIpJye"
    )
    assert backfill.source_ids_from_link(link) == (679085, 4)

    source = tmp_path / "source.db"
    connection = sqlite3.connect(source)
    connection.execute(
        "CREATE TABLE actos_publicos (enlace TEXT UNIQUE, source_flow_id TEXT, source_tipo_proceso TEXT)"
    )
    connection.execute("INSERT INTO actos_publicos(enlace) VALUES (?)", (link,))
    connection.commit()
    connection.close()

    targets, skipped = backfill.target_acts(source, [link])
    assert targets == [backfill.TargetAct(link, 679085, 4)]
    assert skipped == 0
    connection = sqlite3.connect(source)
    stored = connection.execute(
        "SELECT source_flow_id, source_tipo_proceso FROM actos_publicos WHERE enlace=?",
        (link,),
    ).fetchone()
    connection.close()
    assert stored == ("679085", "4")


def test_completed_rows_are_resumable(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    _source_db(source)
    targets, skipped = backfill.target_acts(source, ["https://acto/1"])
    assert targets == [backfill.TargetAct("https://acto/1", 123, 7)]
    assert skipped == 0

    result = backfill.FetchResult(
        targets[0],
        json.dumps([{"renglon": "1", "precio_participacion_unitario": 9.5}]),
        "completo",
        json.dumps(
            [
                {
                    "descripcion": "INSUMO",
                    "numero_renglon": "1",
                    "cantidad": 10,
                    "unidad": "Unidad",
                    "precio_referencia_unitario": 10,
                    "precio_referencia_total": 100,
                }
            ]
        ),
    )
    assert backfill.persist_results(source, [result]) == (1, 0, 0)
    targets, skipped = backfill.target_acts(source, ["https://acto/1"])
    assert targets == []
    assert skipped == 1
    verification = backfill.verify(source, ["https://acto/1"])
    assert verification["current_version"] == 1
    assert verification["with_line_offers"] == 1
    connection = sqlite3.connect(source)
    stored_items = connection.execute(
        "SELECT items_json FROM actos_publicos WHERE enlace='https://acto/1'"
    ).fetchone()[0]
    connection.close()
    assert json.loads(stored_items)[0]["cantidad"] == 10


def test_fetch_uses_detail_and_official_result_components() -> None:
    target = backfill.TargetAct("https://acto/1", 123, 7)
    detail = {"result": {"pageComponentes": [{"tipo": "componentInfoGeneral"}]}}
    offer = {"renglon": "2", "precio_participacion_unitario": 15.0}
    with (
        mock.patch.object(backfill.updater, "request_json", return_value=detail),
        mock.patch.object(
            backfill.updater,
            "_official_result_pages",
            return_value=([{"tipo": "componentOfertasAdjudicadasProponentes"}], 1, 0),
        ),
        mock.patch.object(backfill.updater, "_offer_item_details", return_value=[offer]),
    ):
        result = backfill.fetch_offer_evidence(target)
    assert result.status == "completo"
    assert json.loads(result.payload_json) == [offer]


def test_fetch_also_enriches_requested_items_for_unit_comparison() -> None:
    target = backfill.TargetAct("https://acto/1", 123, 7)
    detail = {
        "result": {
            "pageComponentes": [
                {
                    "tipo": "componentItems",
                    "value": [
                        {
                            "numRenglon": 2,
                            "descripcion": "INSUMO MEDICO",
                            "cantidad": 20,
                            "unidadMedida": "Unidad",
                            "precioReferencia": 500,
                        }
                    ],
                }
            ]
        }
    }
    with (
        mock.patch.object(backfill.updater, "request_json", return_value=detail),
        mock.patch.object(
            backfill.updater,
            "_official_result_pages",
            return_value=([], 0, 0),
        ),
        mock.patch.object(backfill.updater, "_offer_item_details", return_value=[]),
    ):
        result = backfill.fetch_offer_evidence(target)
    item = json.loads(result.items_json)[0]
    assert item["numero_renglon"] == "2"
    assert item["cantidad"] == 20.0
    assert item["unidad"] == "Unidad"
    assert item["precio_referencia_unitario"] == 25.0
