from __future__ import annotations

from common.ficha_line_items import (
    bind_offers_to_matches,
    build_ficha_profile,
    extract_dimensions_mm,
    extract_line_items_from_html,
    extract_offer_lines_from_html,
    match_ficha_to_lines,
    matched_reference_amount,
)


ACT_HTML = """
<table>
  <thead>
    <tr>
      <th>Renglón</th><th>Ficha Técnica</th><th>Especificaciones del comprador</th>
      <th>Cantidad</th><th>Unidad de medida</th><th>Precio de referencia unitario</th>
    </tr>
  </thead>
  <tbody>
    <tr><td>1</td><td>43358</td><td>Cinta testigo para esterilización a vapor de 1/2 pulgada</td><td>100</td><td>ROLLO</td><td>2.10</td></tr>
    <tr><td>2</td><td>43358*</td><td>Cinta testigo para esterilización a vapor de 3/4 pulgada</td><td>80</td><td>ROLLO</td><td>2.45</td></tr>
    <tr><td>3</td><td>99999</td><td>Autoclave industrial</td><td>1</td><td>UND</td><td>100000.00</td></tr>
  </tbody>
</table>
"""


OFFER_HTML = """
<table class="caption-top">
  <caption><a>Proveedor Alfa, S.A.</a></caption>
  <thead>
    <tr><th>Renglón</th><th>Descripción del bien</th><th>Cantidad propuesta</th><th>Unidad de medida</th><th>Precio Unitario</th><th>Precio Total</th></tr>
  </thead>
  <tbody>
    <tr><td>1</td><td>Cinta testigo vapor 1/2 pulgada</td><td>100</td><td>ROLLO</td><td>1.95</td><td>195.00</td></tr>
    <tr><td>2</td><td>Cinta testigo vapor 3/4 pulgada</td><td>80</td><td>ROLLO</td><td>2.30</td><td>184.00</td></tr>
    <tr><td>3</td><td>Autoclave industrial</td><td>1</td><td>UND</td><td>99000</td><td>99000</td></tr>
  </tbody>
</table>
"""


def test_extracts_every_line_and_preserves_exact_ficha_tokens():
    lines = extract_line_items_from_html(ACT_HTML)
    assert len(lines) == 3
    assert lines[0].ficha_codes == ("43358",)
    assert lines[1].ficha_codes == ("43358",)
    assert lines[2].ficha_codes == ("99999",)
    assert lines[0].reference_total == 210.0
    assert lines[1].reference_total == 196.0


def test_exact_ficha_matches_all_applicable_lines_and_not_expensive_other_item():
    lines = extract_line_items_from_html(ACT_HTML)
    profile = build_ficha_profile("43358", "Cinta testigo para esterilización a vapor")
    matches = match_ficha_to_lines(profile, lines)
    assert [match.line.line_number for match in matches] == ["1", "2"]
    assert all(match.method == "ficha_exacta_renglon" for match in matches)
    amount, covered, total = matched_reference_amount(matches)
    assert amount == 406.0
    assert covered == 2
    assert total == 2


def test_dimensions_normalize_fraction_decimal_and_millimeters():
    half = extract_dimensions_mm('Cinta de ½ pulgada')
    decimal = extract_dimensions_mm('Cinta de 0.5"')
    millimeters = extract_dimensions_mm("Cinta de 12.7 mm")
    assert half == decimal == millimeters == frozenset({12.7})


def test_specification_match_rejects_conflicting_measure():
    html = """
    <table><thead><tr><th>Renglón</th><th>Descripción</th></tr></thead><tbody>
      <tr><td>1</td><td>Cinta testigo para esterilización a vapor de 1/2 pulgada</td></tr>
      <tr><td>2</td><td>Cinta testigo para esterilización a vapor de 1 pulgada</td></tr>
    </tbody></table>
    """
    lines = extract_line_items_from_html(html)
    profile = build_ficha_profile(
        "43358",
        "Cinta testigo para esterilización a vapor",
        ["Ancho requerido: 1/2 pulgada"],
    )
    matches = match_ficha_to_lines(profile, lines, minimum_score=0.60)
    assert [match.line.line_number for match in matches] == ["1"]


def test_extracts_and_binds_all_provider_prices_by_line_number():
    profile = build_ficha_profile("43358", "Cinta testigo para esterilización a vapor")
    matches = match_ficha_to_lines(profile, extract_line_items_from_html(ACT_HTML))
    offers = extract_offer_lines_from_html(OFFER_HTML)
    bound = bind_offers_to_matches(matches, offers)
    assert len(bound) == 2
    assert [row.offer.unit_price for row in bound if row.offer] == [1.95, 2.30]
    assert {row.offer.provider for row in bound if row.offer} == {"Proveedor Alfa, S.A."}
    assert all(row.binding_method == "numero_renglon" for row in bound)


def test_missing_offer_is_explicit_and_never_inherits_act_total():
    profile = build_ficha_profile("43358", "Cinta testigo para esterilización a vapor")
    matches = match_ficha_to_lines(profile, extract_line_items_from_html(ACT_HTML))
    bound = bind_offers_to_matches(matches, [])
    assert len(bound) == 2
    assert all(row.offer is None for row in bound)
    assert all(row.binding_method == "sin_precio_renglon_confirmado" for row in bound)


def test_contextual_ficha_in_description_is_exact_evidence():
    html = """
    <table><thead><tr><th>Ítem</th><th>Descripción</th></tr></thead><tbody>
      <tr><td>7</td><td>Ficha técnica N° 43358 - cinta testigo vapor</td></tr>
    </tbody></table>
    """
    profile = build_ficha_profile("43358", "Cinta testigo vapor")
    matches = match_ficha_to_lines(profile, extract_line_items_from_html(html))
    assert len(matches) == 1
    assert matches[0].score == 1.0


def test_same_family_without_exact_code_matches_multiple_variants_auditably():
    html = """
    <table><thead><tr><th>Renglón</th><th>Descripción</th></tr></thead><tbody>
      <tr><td>1</td><td>Cinta testigo para esterilización a vapor 1/2 pulgada</td></tr>
      <tr><td>2</td><td>Cinta testigo para esterilización a vapor 3/4 pulgada</td></tr>
      <tr><td>3</td><td>Indicador biológico para óxido de etileno</td></tr>
    </tbody></table>
    """
    profile = build_ficha_profile("43358", "Cinta testigo para esterilización a vapor")
    matches = match_ficha_to_lines(
        profile,
        extract_line_items_from_html(html),
        minimum_score=0.60,
    )
    assert [match.line.line_number for match in matches] == ["1", "2"]
    assert all(match.method == "especificacion_normalizada" for match in matches)
