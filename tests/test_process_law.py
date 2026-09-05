from __future__ import annotations

from common.process_law import (
    PROCESS_LAW_419,
    PROCESS_LAW_UNKNOWN,
    extract_process_law,
    has_detected_ficha,
    is_law_419,
    is_law_419_without_ficha,
    normalize_process_law,
    process_law_from_components,
    process_law_from_detail_text,
    process_law_from_mapping,
    process_law_from_pairs,
)


def test_exact_official_labels_are_supported() -> None:
    assert process_law_from_pairs([["Ley del proceso", "Ley 419"]]) == PROCESS_LAW_419
    assert process_law_from_pairs([["Tipo de Ley del proceso", "Ley N.° 419"]]) == PROCESS_LAW_419


def test_other_law_is_preserved_for_visibility() -> None:
    assert process_law_from_pairs([["Ley del proceso", "Ley 22 de 2006"]]) == "Ley 22 de 2006"


def test_free_text_does_not_create_false_positive() -> None:
    assert process_law_from_pairs([["Descripción", "Compra bajo la Ley 419"]]) == PROCESS_LAW_UNKNOWN
    assert process_law_from_detail_text("Descripción\nCompra bajo la Ley 419") == PROCESS_LAW_UNKNOWN


def test_tipo_proceso_is_only_a_fallback_for_explicit_419() -> None:
    assert process_law_from_pairs([["Tipo de proceso", "Ley 419"]]) == PROCESS_LAW_419
    assert process_law_from_pairs([["Tipo de proceso", "Cotización en línea"]]) == PROCESS_LAW_UNKNOWN


def test_primary_label_wins_over_fallback() -> None:
    rows = [["Tipo de proceso", "Ley 419"], ["Ley del proceso", "Ley 22"]]
    assert process_law_from_pairs(rows) == "Ley 22"


def test_detail_text_requires_anchored_label() -> None:
    assert process_law_from_detail_text("Ley del proceso\nLey 419\nEntidad") == PROCESS_LAW_419
    assert process_law_from_detail_text("Ley del proceso: Ley 419") == PROCESS_LAW_419
    assert process_law_from_detail_text("Documento relacionado con Ley 419") == PROCESS_LAW_UNKNOWN


def test_api_components_are_walked_recursively() -> None:
    components = [
        {
            "tipo": "componentDatosGenerales",
            "value": [{"nombre": "Ley del proceso", "value": "Ley 419"}],
        }
    ]
    assert process_law_from_components(components) == PROCESS_LAW_419


def test_mapping_uses_same_strict_rules() -> None:
    assert process_law_from_mapping({"Ley del proceso": "Ley 419"}) == PROCESS_LAW_419
    assert process_law_from_mapping({"observaciones": "Ley 419"}) == PROCESS_LAW_UNKNOWN


def test_ficha_detection_includes_name_based_tokens() -> None:
    assert has_detected_ficha(["* 43358"])
    assert has_detected_ficha("No Detectada, *43358")
    assert not has_detected_ficha([])
    assert not has_detected_ficha("No Detectada")


def test_419_bucket_requires_both_official_law_and_no_ficha() -> None:
    assert is_law_419_without_ficha("Ley 419", [])
    assert not is_law_419_without_ficha("Ley 419", ["* 43358"])
    assert not is_law_419_without_ficha("Ley 22", [])
    assert not is_law_419_without_ficha(PROCESS_LAW_UNKNOWN, [])


class FakeElement:
    text = "Descripción\nSin ley oficial"


class FakeDriver:
    def execute_script(self, _script: str):
        return [["Entidad", "CSS"], ["Ley del proceso", "Ley 419"]]

    def find_element(self, *_args):
        return FakeElement()


def test_dom_extractor_prefers_structured_rows() -> None:
    assert extract_process_law(FakeDriver()) == PROCESS_LAW_419
    assert is_law_419(normalize_process_law("LEY No. 419"))
