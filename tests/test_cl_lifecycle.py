from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from common.cl_lifecycle import (
    PANAMA_TZ,
    ProposalObservation,
    _inspect_current_proposal_page,
    apply_observation,
    is_closed,
    load_known_links,
    parse_cl_deadline,
    parse_money,
    persist_local,
    record_from_mapping,
    should_inspect_cl,
)


def test_parse_money_does_not_truncate_four_digit_reference_amount():
    assert parse_money("3750") == 3750.0
    assert parse_money("B/. 3,750.00") == 3750.0
    assert parse_money("B/. 3.750,00") == 3750.0


class FakeElement:
    def __init__(self, text: str = "", *, attrs=None, children=None):
        self.text = text
        self._attrs = attrs or {}
        self._children = children or {}

    def get_attribute(self, name: str):
        return self._attrs.get(name, "")

    def find_elements(self, _by, selector: str):
        return self._children.get(selector, [])


class FakeDriver:
    def __init__(self, body_text: str, tables=None):
        self.body = FakeElement(body_text)
        self.tables = tables or []

    def find_element(self, _by, _value):
        return self.body

    def find_elements(self, _by, selector: str):
        if selector == "table":
            return self.tables
        return []


def _provider_table(name: str, total: str) -> FakeElement:
    provider = FakeElement(name, attrs={"title": "Proveedor participante"})
    amount = FakeElement(total)
    total_row = FakeElement(
        f"Total {total}",
        children={"th,td": [FakeElement("Total"), amount]},
    )
    return FakeElement(
        children={
            "thead th": [],
            "caption a": [provider],
            "tfoot tr": [total_row],
        }
    )


def test_deadline_uses_last_date_and_time_in_panama():
    deadline = parse_cl_deadline(
        "28/02/2026 08:00 a 30/03/2026 02:00 P.M."
    )
    assert deadline == datetime(2026, 3, 30, 14, 0, tzinfo=PANAMA_TZ)
    assert is_closed(
        "28/02/2026 08:00 a 30/03/2026 02:00 P.M.",
        reference=datetime(2026, 3, 30, 14, 1, tzinfo=PANAMA_TZ),
    )
    assert not is_closed(
        "28/02/2026 08:00 a 30/03/2026 02:00 P.M.",
        reference=datetime(2026, 3, 30, 13, 59, tzinfo=PANAMA_TZ),
    )


def test_same_day_cl_is_reviewed_when_it_disappears_from_complete_open_list():
    url = (
        "https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/"
        "2026-1-10-01-08-CL-047050/token"
    )
    at_three_pm = datetime(2026, 7, 30, 15, 0, tzinfo=PANAMA_TZ)
    assert not should_inspect_cl(
        "23-07-2026 a 30-07-2026",
        url,
        active_listing_links=[url],
        reference=at_three_pm,
    )
    assert should_inspect_cl(
        "23-07-2026 a 30-07-2026",
        url,
        active_listing_links=[],
        reference=at_three_pm,
    )
    # Si la captura no fue completa, una ausencia no es evidencia.
    assert not should_inspect_cl(
        "23-07-2026 a 30-07-2026",
        url,
        active_listing_links=None,
        reference=at_three_pm,
    )
    assert not should_inspect_cl(
        "23-07-2026 a 31-07-2026",
        url,
        active_listing_links=[],
        reference=at_three_pm,
    )


def test_official_zero_is_classified_only_with_explicit_evidence():
    number = "2025-1-10-01-08-CL-034939"
    driver = FakeDriver(
        f"{number}\nProponentes participante 0\nNo se encontró registro"
    )
    result = _inspect_current_proposal_page(
        driver,
        expected_number=number,
        evidence_url="https://example.test/cuadro-de-cotizaciones/zero",
    )
    assert result.status == "cerrada_sin_propuestas"
    assert result.proposal_count == 0
    assert result.confidence == 1.0


def test_missing_or_incomplete_page_never_becomes_zero():
    number = "2025-1-10-01-08-CL-034939"
    driver = FakeDriver(f"{number}\nCargando información...")
    result = _inspect_current_proposal_page(
        driver,
        expected_number=number,
        evidence_url="https://example.test/cuadro-de-cotizaciones/pending",
    )
    assert result.status == "cerrada_pendiente_publicacion"
    assert result.proposal_count is None


def test_all_provider_tables_and_totals_are_extracted():
    number = "2025-1-10-01-08-CL-035286"
    driver = FakeDriver(
        f"{number}\nProponentes participante 2",
        tables=[
            _provider_table("NSK INTERNATIONAL SOLUTIONS", "$ 1,250.50"),
            _provider_table("JOTACE", "B/. 980.00"),
        ],
    )
    result = _inspect_current_proposal_page(
        driver,
        expected_number=number,
        evidence_url="https://example.test/cuadro-de-cotizaciones/two",
    )
    assert result.status == "cerrada_con_propuestas"
    assert result.proposal_count == 2
    assert [(item.name, item.total) for item in result.proponents] == [
        ("NSK INTERNATIONAL SOLUTIONS", 1250.50),
        ("JOTACE", 980.0),
    ]


def test_stale_spa_route_is_rejected_instead_of_misclassified():
    driver = FakeDriver(
        "2025-1-10-01-08-CL-000001\n"
        "Proponentes participante 0\nNo se encontró registro"
    )
    result = _inspect_current_proposal_page(
        driver,
        expected_number="2025-1-10-01-08-CL-999999",
        evidence_url="https://example.test/stale",
    )
    assert result.status == "error_verificacion"
    assert result.proposal_count is None


def test_final_proposal_evidence_is_not_erased_if_cl_reappears(tmp_path: Path):
    db_path = tmp_path / "panamacompra.db"
    mapping = {
        "enlace": (
            "https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/"
            "2025-1-10-01-08-CL-035286/token"
        ),
        "titulo": "PRUEBA",
        "fecha": "01/01/2025 08:00 a 02/01/2025 14:00",
    }
    active = record_from_mapping(mapping, source_sheets=["cl_abiertas"])
    observation = ProposalObservation(
        numero_cl="2025-1-10-01-08-CL-035286",
        cl_url=active["enlace"],
        status="cerrada_con_propuestas",
        proposal_count=2,
        proponents=[],
        evidence_type="contador_oficial_sin_detalle",
        evidence_url="https://example.test/cuadro",
        confidence=0.9,
    )
    final, obs_row = apply_observation(active, observation)
    persist_local([final], [obs_row], db_path=db_path)

    reappeared = record_from_mapping(
        {**mapping, "titulo": "PRUEBA ACTUALIZADA"},
        source_sheets=["cl_prioritarios"],
    )
    persist_local([reappeared], db_path=db_path)

    connection = sqlite3.connect(db_path)
    try:
        row = connection.execute(
            "SELECT estado_derivado, proposal_count, evidence_type, evidence_url, "
            "source_sheets, titulo FROM cl_cotizaciones"
        ).fetchone()
    finally:
        connection.close()
    assert row[0:4] == (
        "cerrada_con_propuestas",
        2,
        "contador_oficial_sin_detalle",
        "https://example.test/cuadro",
    )
    assert set(row[4].split(",")) == {"cl_abiertas", "cl_prioritarios"}
    assert row[5] == "PRUEBA ACTUALIZADA"


def test_retry_keeps_null_proposal_count_and_creates_audit_row(tmp_path: Path):
    db_path = tmp_path / "panamacompra.db"
    record = record_from_mapping(
        {
            "enlace": (
                "https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/"
                "2025-1-10-01-08-CL-035286/token"
            ),
            "fecha": "01/01/2025",
        }
    )
    pending, audit = apply_observation(
        record,
        ProposalObservation(
            numero_cl=record["numero_cl"],
            cl_url=record["enlace"],
            status="cerrada_pendiente_publicacion",
            proposal_count=None,
            error="Cuadro aún no publicado",
        ),
    )
    persist_local([pending], [audit], db_path=db_path)

    connection = sqlite3.connect(db_path)
    try:
        stored = connection.execute(
            "SELECT proposal_count, next_check_at, check_attempts "
            "FROM cl_cotizaciones"
        ).fetchone()
        audit_count = connection.execute(
            "SELECT COUNT(*) FROM cl_cotizaciones_observaciones"
        ).fetchone()[0]
    finally:
        connection.close()
    assert stored[0] is None
    assert stored[1]
    assert stored[2] == 1
    assert audit_count == 1
    assert load_known_links(db_path=db_path) == {record["enlace"]}
