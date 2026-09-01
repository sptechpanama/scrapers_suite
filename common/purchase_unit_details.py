from __future__ import annotations

"""Extraccion compartida del contacto y la unidad compradora de PanamaCompra."""

import re
import unicodedata
from collections.abc import Iterable, Sequence
from typing import Any


CONTACT_NAME_COLUMN = "contacto nombre"
CONTACT_ROLE_COLUMN = "contacto cargo"
CONTACT_PHONE_COLUMN = "contacto telefono"
CONTACT_EMAIL_COLUMN = "contacto correo"
DEPENDENCY_COLUMN = "dependencia"
PURCHASE_UNIT_COLUMN = "unidad de compra"
PROVINCE_COLUMN = "provincia"

PURCHASE_UNIT_DETAIL_COLUMNS = (
    CONTACT_NAME_COLUMN,
    CONTACT_ROLE_COLUMN,
    CONTACT_PHONE_COLUMN,
    CONTACT_EMAIL_COLUMN,
    DEPENDENCY_COLUMN,
    PURCHASE_UNIT_COLUMN,
    PROVINCE_COLUMN,
)


def clean_detail_value(value: object) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if text.casefold() in {"no disponible", "n/a", "nan", "none", "null", "-"}:
        return ""
    return text


def normalize_detail_label(value: object) -> str:
    text = clean_detail_value(value).casefold()
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    # El portal a veces entrega U+FFFD en lugar de vocales acentuadas. Los
    # prefijos estables (tel/correo/inform) permiten clasificar esas etiquetas.
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _row_pairs(rows: Iterable[Sequence[object]]) -> Iterable[tuple[str, str]]:
    for source in rows or ():
        row = list(source or ())
        if len(row) < 2:
            continue
        label = normalize_detail_label(row[0])
        value = clean_detail_value(row[1])
        if label and value:
            yield label, value


def parse_purchase_unit_details(
    contact_rows: Iterable[Sequence[object]],
    entity_rows: Iterable[Sequence[object]],
) -> dict[str, str]:
    details = {column: "" for column in PURCHASE_UNIT_DETAIL_COLUMNS}
    details["entidad"] = ""

    for label, value in _row_pairs(contact_rows):
        if label.startswith("nombre") and not details[CONTACT_NAME_COLUMN]:
            details[CONTACT_NAME_COLUMN] = value
        elif label.startswith("cargo") and not details[CONTACT_ROLE_COLUMN]:
            details[CONTACT_ROLE_COLUMN] = value
        elif label.startswith("tel") and not details[CONTACT_PHONE_COLUMN]:
            details[CONTACT_PHONE_COLUMN] = value
        elif (
            label.startswith("correo") or label.startswith("email")
        ) and not details[CONTACT_EMAIL_COLUMN]:
            details[CONTACT_EMAIL_COLUMN] = value

    for label, value in _row_pairs(entity_rows):
        if label == "entidad" and not details["entidad"]:
            details["entidad"] = value
        elif label.startswith("dependencia") and not details[DEPENDENCY_COLUMN]:
            details[DEPENDENCY_COLUMN] = value
        elif label.startswith("unidad de compra") and not details[PURCHASE_UNIT_COLUMN]:
            details[PURCHASE_UNIT_COLUMN] = value
        elif label.startswith("provincia") and not details[PROVINCE_COLUMN]:
            details[PROVINCE_COLUMN] = value

    return details


def parse_purchase_unit_components(components: Iterable[dict[str, Any]]) -> dict[str, str]:
    """Extrae los mismos datos desde la respuesta publica V3 de PanamaCompra."""

    contact_rows: list[list[object]] = []
    entity_rows: list[list[object]] = []
    for component in components or ():
        if not isinstance(component, dict):
            continue
        title = normalize_detail_label(component.get("titulo"))
        values = component.get("value")
        if not isinstance(values, list):
            continue
        rows = [
            [item.get("nombre"), item.get("value")]
            for item in values
            if isinstance(item, dict) and item.get("nombre") is not None
        ]
        if "contacto" in title and "unidad" in title:
            contact_rows = rows
        elif "inform" in title and "entidad" in title:
            entity_rows = rows
    return parse_purchase_unit_details(contact_rows, entity_rows)


_SECTION_ROWS_SCRIPT = r"""
const tokens = arguments[0] || [];
const normalize = (value) => (value || '')
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim()
  .toLowerCase();
const labels = Array.from(document.querySelectorAll('label, legend, h1, h2, h3, h4, h5, h6'));
const heading = labels.find((node) => {
  const text = normalize(node.innerText || node.textContent || '');
  return tokens.every((token) => text.includes(token));
});
if (!heading) return [];
let container = heading.parentElement;
for (let level = 0; level < 7 && container; level += 1, container = container.parentElement) {
  const rows = Array.from(container.querySelectorAll('tr')).map((row) => {
    const cells = Array.from(row.querySelectorAll(':scope > th, :scope > td'));
    return cells.map((cell) => (cell.innerText || cell.textContent || '').trim());
  }).filter((cells) => cells.length >= 2);
  if (rows.length) return rows;
}
return [];
"""


def extract_purchase_unit_details(driver) -> dict[str, str]:
    """Lee ambos bloques del DOM ya cargado; no hace navegacion ni esperas."""

    try:
        contact_rows = driver.execute_script(_SECTION_ROWS_SCRIPT, ["contacto", "unidad"])
    except Exception:
        contact_rows = []
    try:
        entity_rows = driver.execute_script(_SECTION_ROWS_SCRIPT, ["inform", "entidad"])
    except Exception:
        entity_rows = []
    return parse_purchase_unit_details(contact_rows or [], entity_rows or [])


def has_purchase_unit_details(details: dict[str, object]) -> bool:
    return any(clean_detail_value(details.get(column)) for column in PURCHASE_UNIT_DETAIL_COLUMNS)
