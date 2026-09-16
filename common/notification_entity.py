"""Preserve official buying units from scraper rows through email delivery."""
from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping


def _clean(value: object) -> str:
    text = re.sub(r"\s+", " ", str(value if value is not None else "")).strip()
    return "" if text.casefold() in {"none", "null", "nan", "n/a", "no disponible", "-"} else text


def _key(value: object) -> str:
    text = unicodedata.normalize("NFKD", _clean(value)).casefold()
    return re.sub(r"[^a-z0-9]", "", text)


_ALIASES = {
    "entidad": ("entidad", "nombreEntidad"),
    "dependencia": ("dependencia", "nombreDependencia"),
    "unidad solicitante": ("unidad solicitante", "unidad_solicitante", "unidad_solic"),
    "unidad de compra": ("unidad de compra", "unidad_compra", "nombreUnidadCompra", "purchase_unit"),
    "hospital": ("hospital", "nombreHospital", "hospital solicitante"),
}


def notification_location(record: Mapping) -> dict[str, str]:
    """Only official fields: never infer a hospital from product/title/province."""
    values = {_key(k): _clean(v) for k, v in record.items() if _clean(v)}
    result = {}
    for field, aliases in _ALIASES.items():
        value = next((values[_key(alias)] for alias in aliases if values.get(_key(alias))), "")
        if value:
            result[field] = value
    return result


def notification_location_from_row(headers, row) -> dict[str, str]:
    return notification_location(dict(zip(headers, row)))


def notification_location_lines(record: Mapping, *, indent: str = "") -> list[str]:
    location = notification_location(record)
    entity = location.get("entidad", "No informada en la fuente")
    lines = [f"{indent}Entidad: {entity}"]
    seen = {_key(entity)}
    for field, label in (
        ("hospital", "Hospital"),
        ("unidad solicitante", "Hospital / unidad solicitante"),
        ("unidad de compra", "Unidad de compra"),
        ("dependencia", "Dependencia"),
    ):
        value = location.get(field, "")
        if value and _key(value) not in seen:
            lines.append(f"{indent}{label}: {value}")
            seen.add(_key(value))
    if len(lines) == 1:
        lines.append(f"{indent}Hospital / unidad de compra: No informado en los datos disponibles")
    return lines
