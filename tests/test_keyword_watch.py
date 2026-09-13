from __future__ import annotations


def test_8k_threshold_and_new_hvac_families_without_broad_noise():
    from common.keyword_watch import HVAC_TECHNICAL_KEYWORDS, match_keywords_in_text, DEFAULT_RS_SP_KEYWORDS
    rules = ["aires acondicion*>8k"]
    assert not match_keywords_in_text("Aires acondicionados", rules, reference_amount=8000)
    assert match_keywords_in_text("Aires acondicionados", rules, reference_amount=8000.01) == rules
    assert match_keywords_in_text("Aires acondicionados tipo piso-techo", rules, reference_amount=13755) == rules
    assert match_keywords_in_text("FILTRO PLISADO MERV.13", HVAC_TECHNICAL_KEYWORDS) == ["filtro plisad*", "merv"]
    assert match_keywords_in_text("BLOWERS CENTRIFUGOS 18 X 18", HVAC_TECHNICAL_KEYWORDS) == ["blowers centrifug*"]
    assert match_keywords_in_text("REPUESTOS PARA EL SISTEMA HVAC", HVAC_TECHNICAL_KEYWORDS) == ["hvac"]
    assert not match_keywords_in_text("BLOWER DE PISO Y DESBROZADORA", HVAC_TECHNICAL_KEYWORDS)
    assert not match_keywords_in_text("CENTRIFUGA DE LABORATORIO", HVAC_TECHNICAL_KEYWORDS)
    assert not match_keywords_in_text("FILTRO DE ACEITE PARA VEHICULO", HVAC_TECHNICAL_KEYWORDS)
    assert not any(term.endswith(">15k") for term in DEFAULT_RS_SP_KEYWORDS)

from common.keyword_watch import (
    DEFAULT_RS_SP_NEGATIVE_KEYWORDS,
    DEFAULT_RS_SP_KEYWORDS,
    ENGINEERING_PLAN_KEYWORDS,
    POWER_GENERATION_KEYWORDS,
    RS_SP_CONTEXTUAL_KEYWORDS,
    match_keyword_fields,
    match_keywords_in_text,
    match_negative_keywords_in_text,
    negative_keywords_in_matching_context,
    normalize_keyword_term,
    parse_keyword_rule,
    parse_reference_amount,
    summarize_keyword_rows,
)


def test_exact_root_and_amount_rules_are_backward_compatible():
    assert normalize_keyword_term(" Fotovolta* ") == "fotovolta*"
    assert normalize_keyword_term(" Aire Acondicion*>15K ") == "aire acondicion*>15k"
    assert normalize_keyword_term("split>$15,000.00") == "split>15k"
    assert match_keywords_in_text("panel fotovoltaico", ["fotovolta*"]) == ["fotovolta*"]
    assert match_keywords_in_text("equipo prefotovoltaico", ["fotovolta*"]) == []

    rule = parse_keyword_rule("aires acondicion*>15k")
    assert rule is not None
    assert rule.is_root is True
    assert rule.minimum_amount == 15_000


def test_known_legacy_hvac_rows_are_recovered_safely():
    assert normalize_keyword_term("aire acondicion 15k") == "aire acondicion*>15k"
    assert normalize_keyword_term("split 15k") == "split>15k"
    assert normalize_keyword_term("climatizacion 15k") == "climatizacion*>15k"
    assert normalize_keyword_term("proyecto especial 15k") == "proyecto especial 15k"


def test_threshold_is_strict_and_requires_a_valid_reference_amount():
    keywords = ["aires acondicion*>15k", "split>15k", "vrf>15k"]
    text = "Compra de aires acondicionados tipo split con sistema VRF"
    assert match_keywords_in_text(text, keywords) == []
    assert match_keywords_in_text(text, keywords, reference_amount=15_000) == []
    assert match_keywords_in_text(
        text,
        keywords,
        reference_amount="B/. 15.000,50",
    ) == keywords


def test_common_currency_formats_are_parsed_consistently():
    assert parse_reference_amount("$15,000.50") == 15_000.50
    assert parse_reference_amount("B/. 15.000,50") == 15_000.50
    assert parse_reference_amount("15000,50") == 15_000.50
    assert parse_reference_amount(16_250) == 16_250
    assert parse_reference_amount("") is None


def test_same_term_reports_the_more_specific_passing_rule():
    assert match_keywords_in_text(
        "Servicio para chiller",
        ["chiller", "chiller>15k"],
        reference_amount=20_000,
    ) == ["chiller>15k"]
    assert match_keywords_in_text(
        "Servicio para chiller",
        ["chiller", "chiller>15k"],
        reference_amount=10_000,
    ) == ["chiller"]


def test_summary_filters_low_amount_rows_before_email_payload():
    columns = ["titulo", "descripcion", "precio_referencia", "enlace"]
    rows = [
        ["Aire acondicionado split", "Suministro", "$14,999.99", "low"],
        ["Aire acondicionado split", "Suministro", "$25,000.00", "high"],
        ["Otro equipo", "Sin coincidencia", "$80,000.00", "other"],
    ]
    result = summarize_keyword_rows(
        rows=rows,
        cols=columns,
        keyword_terms=["aire acondicion*>15k", "split>15k"],
        source_sheet="cl_abiertas",
        job_name="clv",
    )
    assert result is not None
    assert result["count"] == 1
    assert result["rows"][0]["enlace"] == "high"
    assert result["rows"][0]["precio_referencia"] == "$25,000.00"


def test_hvac_defaults_include_root_and_amount_rules():
    assert "aire acondicion*>8k" in DEFAULT_RS_SP_KEYWORDS
    assert "aires acondicion*>8k" in DEFAULT_RS_SP_KEYWORDS
    assert "vrf>8k" in DEFAULT_RS_SP_KEYWORDS
    assert "climatizacion*>8k" in DEFAULT_RS_SP_KEYWORDS


def test_new_defaults_are_precise_contextual_phrases():
    assert set(RS_SP_CONTEXTUAL_KEYWORDS).issubset(DEFAULT_RS_SP_KEYWORDS)
    assert set(POWER_GENERATION_KEYWORDS).issubset(DEFAULT_RS_SP_KEYWORDS)
    assert set(ENGINEERING_PLAN_KEYWORDS).issubset(DEFAULT_RS_SP_KEYWORDS)
    assert "mantenimiento" not in DEFAULT_RS_SP_KEYWORDS
    assert "reparacion" not in DEFAULT_RS_SP_KEYWORDS
    assert "planos" not in DEFAULT_RS_SP_KEYWORDS


def test_contextual_item_guard_suppresses_mixed_global_packages():
    result = match_keyword_fields(
        [
            ("titulo", "Compra de materiales diversos"),
            ("item_1", "Aceite para generador electrico"),
            ("item_2", "Alimentos secos"),
        ],
        POWER_GENERATION_KEYWORDS,
        adjudication_type="Global",
    )
    assert result.terms == ()
    assert result.suppressed_terms == ("generador electric*",)
    assert result.context_policy == "mixed_items_suppressed"


def test_contextual_item_guard_accepts_line_awards_and_single_item_acts():
    by_line = match_keyword_fields(
        [
            ("titulo", "Compra de equipos"),
            ("item_1", "Cargador para planta electrica"),
            ("item_2", "Bomba de agua"),
        ],
        POWER_GENERATION_KEYWORDS,
        adjudication_type="Parcial por renglon",
    )
    assert by_line.terms == ("planta electric*",)
    assert by_line.context_policy == "line_adjudication"

    single_item = match_keyword_fields(
        [("titulo", "Compra"), ("item_1", "Digitalizacion de planos")],
        ENGINEERING_PLAN_KEYWORDS,
        adjudication_type="Global",
    )
    assert single_item.terms == ("digitalizacion de plano*",)
    assert single_item.context_policy == "all_items_context"


def test_contextual_guard_preserves_preexisting_keyword_behavior():
    result = match_keyword_fields(
        [
            ("titulo", "Compra de equipos"),
            ("item_1", "Chiller y generador electrico"),
            ("item_2", "Mobiliario"),
        ],
        ["chiller", *POWER_GENERATION_KEYWORDS],
        adjudication_type="Global",
    )
    assert result.terms == ("chiller",)
    assert result.suppressed_terms == ("generador electric*",)


def test_negative_context_aliases_are_precise():
    assert match_negative_keywords_in_text(
        "Alquiler de habitaciones (hotel)",
        DEFAULT_RS_SP_NEGATIVE_KEYWORDS,
    ) == ["habitacion de hotel"]
    assert match_negative_keywords_in_text(
        "Cambio de correas del serpentín del vehículo",
        DEFAULT_RS_SP_NEGATIVE_KEYWORDS,
    ) == ["correa del serpentin"]
    assert match_negative_keywords_in_text(
        "Serpentín de aire acondicionado para hotel",
        DEFAULT_RS_SP_NEGATIVE_KEYWORDS,
    ) == []


def test_summary_excludes_negative_only_in_the_positive_matching_context():
    columns = ["titulo", "descripcion", "item_1", "precio_referencia", "enlace"]
    rows = [
        ["Mantenimiento automotriz", "Servicio", "Aire acondicionado", "20000", "vehicle"],
        ["Sistema del edificio", "Servicio", "Aire acondicionado central", "20000", "building"],
    ]
    result = summarize_keyword_rows(
        rows=rows,
        cols=columns,
        keyword_terms=["aire acondicionado*>15k"],
        negative_terms=DEFAULT_RS_SP_NEGATIVE_KEYWORDS,
        source_sheet="cl_abiertas",
        job_name="clv",
    )
    assert result is not None
    assert result["count"] == 1
    assert result["rows"][0]["enlace"] == "building"


def test_summary_applies_context_guard_before_building_email_payload():
    columns = [
        "titulo",
        "descripcion",
        "item_1",
        "item_2",
        "Tipo de adjudicacion",
        "precio_referencia",
        "enlace",
    ]
    rows = [
        [
            "Compra mixta",
            "Varios insumos",
            "Aceite para generador electrico",
            "Alimentos",
            "Global",
            "20000",
            "suppressed",
        ],
        [
            "Compra mixta por renglon",
            "Varios insumos",
            "Aceite para generador electrico",
            "Alimentos",
            "Por renglon",
            "20000",
            "accepted",
        ],
    ]
    result = summarize_keyword_rows(
        rows=rows,
        cols=columns,
        keyword_terms=POWER_GENERATION_KEYWORDS,
        source_sheet="cl_abiertas",
        job_name="clv",
    )
    assert result is not None
    assert result["count"] == 1
    assert result["rows"][0]["enlace"] == "accepted"


def test_negative_helper_ignores_unrelated_nonmatching_fields():
    assert negative_keywords_in_matching_context(
        title="Proyecto fotovoltaico",
        matched_field_values=["Paneles fotovoltaicos"],
        negative_keywords=DEFAULT_RS_SP_NEGATIVE_KEYWORDS,
    ) == []
