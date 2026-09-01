from __future__ import annotations

import base64
import json

from tools.backfill_purchase_unit_details import col_letter, decode_route_payload, extract_links


def _token(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(raw).decode("ascii").rstrip("=")[::-1]


def test_decodes_route_identifier_without_calling_the_portal() -> None:
    token = _token({"i": 989421, "tp": 2})
    url = f"https://www.panamacompra.gob.pa/Inicio/#/solicitud-de-cotizacion/acto/{token}"
    assert decode_route_payload(url) == (989421, 2)


def test_extract_links_uses_header_in_any_position() -> None:
    rows = [["titulo", "enlace"], ["A", "https://example/a"], ["B", ""], ["C", "https://example/c"]]
    assert extract_links(rows) == ["https://example/a", "https://example/c"]


def test_column_letters_cover_sheet_width() -> None:
    assert [col_letter(value) for value in (1, 26, 27, 52, 53, 702)] == ["A", "Z", "AA", "AZ", "BA", "ZZ"]
