from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parents[1] / "db" / "db_api_updater.py"
SPEC = importlib.util.spec_from_file_location("db_api_updater_official_results_test", MODULE_PATH)
assert SPEC and SPEC.loader
updater = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(updater)


class _Matcher:
    def classify(self, _fields):
        return {
            "ficha_detectada": "No Detectada",
            "fichas_detectadas_json": "[]",
            "ficha_detector_version": "test",
            "ficha_catalogo_version": "test",
            "ficha_detectada_at": "",
        }


def _record() -> dict[str, object]:
    return {
        "idProcesosContratacionFlujos": 72000,
        "idTipoProceso": 7,
        "numProceso": "2026-1-10-01-13-LP-072000",
        "prefijo": "LP",
        "titulo": "Adecuacion HVAC de policlinica",
        "nombreEntidad": "Caja de Seguro Social",
        "nombreUnidadCompra": "Policlinica",
        "fechaPublicacion": "2026-08-01T12:00:00Z",
        "fechaEstado": "2026-08-20T12:00:00Z",
    }


def _main_page(*, stale_winner: bool = False) -> dict[str, object]:
    labels = [
        {"nombre": "Título", "value": "Adecuacion HVAC de policlinica"},
        {"nombre": "Precio de referencia", "value": "75,000.00"},
    ]
    if stale_winner:
        labels.extend(
            [
                {"nombre": "Razón Social", "value": "GANADOR OBSOLETO"},
                {"nombre": "Nombre Comercial", "value": "GANADOR OBSOLETO"},
            ]
        )
    return {
        "status": 1,
        "result": {
            "pageComponentes": [
                {"tipo": "componentInfoGeneral", "value": labels},
                {
                    "tipo": "componentProcesosActasPliego",
                    "value": [
                        {
                            "nombre": "Acta de apertura",
                            "rutaNueva": "/ps/ver-acta-apertura/publico",
                            "paramsBody": {"idProcesosContratacionRegistroActas": 1},
                        },
                        {
                            "nombre": "Resolución de Adjudicación",
                            "rutaNueva": "/ps/adjudicacion/acta/preview-publico",
                            "paramsBody": {"idProcesosContratacionRegistroActas": 2},
                            "fecha": "20-08-2026 - 10:00 AM",
                        },
                    ],
                },
            ]
        },
    }


def _opening_page() -> dict[str, object]:
    return {
        "status": 1,
        "result": {
            "pageComponentes": [
                {
                    "tipo": "componentProcesosActasCuadroCotizacionesOR",
                    "value": [
                        {"nombreComercial": "RS ENGINEERING, S.A.", "ruc": "1", "precioTotal": 72000},
                        {"nombreComercial": "OTRA EMPRESA", "ruc": "2", "precioTotal": 73500},
                    ],
                }
            ]
        },
    }


def _award_page() -> dict[str, object]:
    return {
        "status": 1,
        "result": {
            "pageComponentes": [
                {
                    "tipo": "componentProcesosActasCuadroCotizacionesOR",
                    "value": [
                        {"nombreComercial": "RS ENGINEERING, S.A.", "ruc": "1", "precioTotal": 72000},
                        {"nombreComercial": "OTRA EMPRESA", "ruc": "2", "precioTotal": 73500},
                    ],
                },
                {
                    "tipo": "componentOfertasAdjudicadasProponentes",
                    "value": [
                        {
                            "empresa": {"nombreComercial": "RS ENGINEERING, S.A.", "ruc": "1"},
                            "procesosOfertasItems": [{"precioTotal": 72000}],
                        }
                    ],
                },
            ]
        },
    }


def test_parse_detail_recovers_all_participants_and_official_winner() -> None:
    def response(method: str, url: str, *, payload=None):
        if method == "GET":
            return _main_page()
        if "ver-acta-apertura" in url:
            assert payload["idTipoProceso"] == 7
            assert payload["idProcesosContratacionFlujos"] == 72000
            return _opening_page()
        return _award_page()

    with mock.patch.object(updater, "request_json", side_effect=response):
        row = updater.parse_detail(_record(), "Adjudicado", "2026-08-26 12:00:00", _Matcher())

    assert row["razon_social"] == "RS ENGINEERING, S.A."
    assert row["nombre_comercial"] == "RS ENGINEERING, S.A."
    assert row["total_items_ofertados"] == "72000.00"
    assert row["num_participantes"] == "2"
    assert row["Proponente 1"] == "RS ENGINEERING, S.A."
    assert row["Precio Proponente 1"] == "72000.00"
    assert row["Proponente 2"] == "OTRA EMPRESA"
    assert row["resultado_fuente_version"] == updater.RESULT_ENRICHMENT_VERSION
    assert row["resultado_fuente_estado"] == "completo_actas_oficiales"
    assert len(json.loads(row["proponentes_json"])) == 2
    assert json.loads(row["ganadores_json"])[0]["nombre"] == "RS ENGINEERING, S.A."
    item_offers = json.loads(row["ofertas_items_json"])
    assert item_offers == [
        {
            "proveedor": "RS ENGINEERING, S.A.",
            "ruc": "1",
            "renglon": "1",
            "descripcion": "",
            "cantidad": 0.0,
            "unidad": "",
            "precio_referencia_unitario": 0.0,
            "precio_referencia_total": 0.0,
            "precio_participacion_unitario": 0.0,
            "precio_participacion_total": 72000.0,
            "es_ganador": 1,
            "fuente": "adjudicacion_oficial",
        }
    ]
    assert row["ofertas_items_version"] == updater.RESULT_ENRICHMENT_VERSION
    assert row["ofertas_items_estado"] == "completo"


def test_deserted_act_keeps_participants_but_never_a_stale_winner() -> None:
    def response(method: str, url: str, *, payload=None):
        if method == "GET":
            return _main_page(stale_winner=True)
        return _opening_page()

    with mock.patch.object(updater, "request_json", side_effect=response):
        row = updater.parse_detail(_record(), "Desierto", "2026-08-26 12:00:00", _Matcher())

    assert row["razon_social"] == ""
    assert row["nombre_comercial"] == ""
    assert row["total_items_ofertados"] == ""
    assert row["num_participantes"] == "2"
    assert json.loads(row["ganadores_json"]) == []
    assert row["resultado_fuente_version"] == updater.RESULT_ENRICHMENT_VERSION


def test_multiwinner_result_is_persisted_without_losing_any_winner() -> None:
    award = _award_page()
    award["result"]["pageComponentes"][1]["value"].append(
        {
            "empresa": {"nombreComercial": "OTRA EMPRESA", "ruc": "2"},
            "procesosOfertasItems": [{"precioTotal": 73500}],
        }
    )

    def response(method: str, url: str, *, payload=None):
        if method == "GET":
            return _main_page()
        if "ver-acta-apertura" in url:
            return _opening_page()
        return award

    with mock.patch.object(updater, "request_json", side_effect=response):
        row = updater.parse_detail(_record(), "Adjudicado", "2026-08-26 12:00:00", _Matcher())

    assert [item["nombre"] for item in json.loads(row["ganadores_json"])] == [
        "RS ENGINEERING, S.A.",
        "OTRA EMPRESA",
    ]
    assert row["total_items_ofertados"] == "145500.00"


def test_offer_item_details_preserve_official_unit_price_and_requested_line() -> None:
    component = {
        "tipo": "componentOfertasAdjudicadasProponentes",
        "value": [
            {
                "empresa": {"nombreComercial": "PROVEEDOR UNO", "ruc": "8-1"},
                "procesosOfertasItems": [
                    {
                        "precio": 12.5,
                        "precioTotal": 133.75,
                        "procesosContratacionItems": {
                            "numRenglon": 3,
                            "cantidad": 10,
                            "unidad": "Unidad",
                            "descripcion": "Producto ficha 43358",
                            "precioReferencia": 160.5,
                        },
                    }
                ],
            }
        ],
    }

    assert updater._offer_item_details([component]) == [
        {
            "proveedor": "PROVEEDOR UNO",
            "ruc": "8-1",
            "renglon": "3",
            "descripcion": "Producto ficha 43358",
            "cantidad": 10.0,
            "unidad": "Unidad",
            "precio_referencia_unitario": 16.05,
            "precio_referencia_total": 160.5,
            "precio_participacion_unitario": 12.5,
            "precio_participacion_total": 133.75,
            "es_ganador": 1,
            "fuente": "adjudicacion_oficial",
        }
    ]
