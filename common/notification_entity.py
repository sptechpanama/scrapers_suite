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
    hospital = location.get("hospital", "")
    if not hospital:
        for field in ("unidad solicitante", "unidad de compra", "dependencia"):
            value = location.get(field, "")
            normalized = "".join(
                char for char in unicodedata.normalize("NFKD", value).casefold()
                if not unicodedata.combining(char)
            )
            if re.search(
                r"\b(?:hospital|hosp|complejo hospitalario|policlinica|ulaps|capsi|"
                r"centro de salud|instituto oncologico)\b", normalized,
            ):
                hospital = value
                break
    return [
        f"{indent}Entidad: {entity}",
        f"{indent}Hospital: {hospital or 'No especificado'}",
    ]
