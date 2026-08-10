from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "orquestador"))

from intel_ficha_worker import (  # noqa: E402
    _build_study_ficha_profile,
    _line_detail_rows_for_act,
)


ACT_HTML = """
<table>
  <thead><tr><th>Renglón</th><th>Ficha Técnica</th><th>Descripción</th>
  <th>Cantidad</th><th>Unidad de medida</th><th>Precio de referencia unitario</th></tr></thead>
  <tbody>
    <tr><td>1</td><td>43358</td><td>Cinta testigo vapor 1/2 pulgada</td><td>10</td><td>ROLLO</td><td>5</td></tr>
    <tr><td>2</td><td>99999</td><td>Autoclave</td><td>1</td><td>UND</td><td>100000</td></tr>
  </tbody>
</table>
"""

OFFER_HTML = """
<table>
  <thead><tr><th>Renglón</th><th>Descripción</th><th>Cantidad propuesta</th>
  <th>Precio Unitario</th><th>Precio Total</th></tr></thead>
  <tbody>
    <tr><td>1</td><td>Cinta testigo vapor 1/2 pulgada</td><td>10</td><td>4.5</td><td>45</td></tr>
    <tr><td>2</td><td>Autoclave</td><td>1</td><td>90000</td><td>90000</td></tr>
  </tbody>
</table>
"""


def _run(act_html: str = ACT_HTML, offer_html: str = OFFER_HTML):
    return _line_detail_rows_for_act(
        profile=_build_study_ficha_profile("43358", "Cinta testigo vapor"),
        request_id="req",
        run_id="run",
        ficha="43358",
        nombre="Cinta testigo vapor",
        acto_id="acto-1",
        acto_nombre="Compra de insumos",
        acto_url="https://example.test/acto-1",
        entidad="Entidad",
        act_html=act_html,
        offer_html=offer_html,
        default_provider="Proveedor",
        evidence_url="https://example.test/evidencia",
        created_at="2026-07-25T12:00:00",
        precio_total_acto=100050.0,
        enlace_ficha_minsa="https://ctni.minsa.gob.pa/Utilities/LoadFicha/?idficha=43358",
    )


def test_worker_parallel_layer_uses_only_matching_line_amounts() -> None:
    rows = _run()
    assert len(rows) == 1
    assert rows[0]["renglon_numero"] == "1"
    assert rows[0]["precio_referencia_total"] == 50.0
    assert rows[0]["precio_participacion_total"] == 45.0
    assert rows[0]["precio_referencia_total"] != 100050.0
    assert rows[0]["precio_total_acto"] == 100050.0
    assert rows[0]["enlace_ficha_minsa"].endswith("idficha=43358")


def test_worker_marks_missing_correspondence_for_review() -> None:
    rows = _run(
        act_html="<table><tr><th>Descripción</th></tr><tr><td>Autoclave</td></tr></table>",
        offer_html="",
    )
    assert len(rows) == 1
    assert rows[0]["match_method"] == "sin_correspondencia_renglon"
    assert rows[0]["match_requires_review"] == 1
    assert rows[0]["precio_referencia_total"] == 0.0


def test_worker_marks_missing_html_without_inventing_amounts() -> None:
    rows = _run(act_html="", offer_html="")
    assert rows[0]["match_method"] == "sin_html_acto"
    assert rows[0]["precio_participacion_total"] == 0.0
