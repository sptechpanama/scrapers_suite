from minsa_scraper.scrape_minsa import _extract_ctni_detail_fields_from_html


def test_ctni_detail_extracts_official_risk_class_from_readonly_input() -> None:
    html = """
    <table>
      <tr><td>Nombre Genérico</td><td><input value="HUMIFICADOR PRELLENADO" readonly></td></tr>
      <tr><td>Clase de Riesgo</td><td><input value="B" readonly></td></tr>
    </table>
    """
    assert _extract_ctni_detail_fields_from_html(html) == (
        "HUMIFICADOR PRELLENADO",
        "B",
    )


def test_ctni_detail_accepts_single_quotes_and_selected_option() -> None:
    html = """
    <table>
      <tr><td>Nombre</td><td><input value='KIT DE PRUEBA' readonly></td></tr>
      <tr><td>Clase de Riesgo</td><td><select><option>A</option><option selected>C</option></select></td></tr>
    </table>
    """
    assert _extract_ctni_detail_fields_from_html(html) == ("KIT DE PRUEBA", "C")
