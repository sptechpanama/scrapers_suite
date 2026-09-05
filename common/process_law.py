from __future__ import annotations

"""Extraccion y normalizacion estricta de la ley oficial del proceso.

La clasificacion evita buscar ``Ley 419`` en texto libre. Solo acepta el valor
asociado a una etiqueta oficial del detalle de PanamaCompra, de modo que una
mencion incidental en una descripcion o documento no cambie la categoria.
"""

import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from typing import Any


PROCESS_LAW_COLUMN = "Ley del proceso"
PROCESS_LAW_419 = "Ley 419"
PROCESS_LAW_UNKNOWN = "No identificado"

_PRIMARY_LABELS = {"ley del proceso", "tipo de ley del proceso"}
_FALLBACK_LABEL = "tipo de proceso"


def clean_process_law_value(value: object) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip(" \t\r\n:;-")
    if text.casefold() in {"", "n/a", "na", "nan", "none", "null", "no disponible", "-"}:
        return ""
    return text


def normalize_process_law_text(value: object) -> str:
    text = clean_process_law_value(value).casefold()
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _explicitly_mentions_law_419(value: object) -> bool:
    text = normalize_process_law_text(value)
    return bool(
        re.search(r"\bley(?:\s+(?:n|no|nro|numero))?\s+419\b", text)
        or text == "419"
    )


def normalize_process_law(value: object) -> str:
    """Devuelve una etiqueta visible estable sin inventar leyes ausentes."""

    cleaned = clean_process_law_value(value)
    if not cleaned:
        return PROCESS_LAW_UNKNOWN
    if _explicitly_mentions_law_419(cleaned):
        return PROCESS_LAW_419
    return cleaned


def is_law_419(value: object) -> bool:
    return normalize_process_law(value) == PROCESS_LAW_419


def process_law_from_pairs(rows: Iterable[Sequence[object]]) -> str:
    """Lee pares etiqueta/valor y prioriza exclusivamente campos oficiales."""

    fallback = ""
    for source in rows or ():
        row = list(source or ())
        if len(row) < 2:
            continue
        label = normalize_process_law_text(row[0])
        value = clean_process_law_value(row[1])
        if not value:
            continue
        if label in _PRIMARY_LABELS:
            return normalize_process_law(value)
        if label == _FALLBACK_LABEL and _explicitly_mentions_law_419(value):
            fallback = PROCESS_LAW_419
    return fallback or PROCESS_LAW_UNKNOWN


def process_law_from_mapping(source: Mapping[str, Any] | None) -> str:
    if not source:
        return PROCESS_LAW_UNKNOWN
    rows = [[key, value] for key, value in source.items()]
    return process_law_from_pairs(rows)


def _nested_mappings(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        for nested in value.values():
            yield from _nested_mappings(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _nested_mappings(nested)


def process_law_from_components(components: Iterable[dict[str, Any]]) -> str:
    """Extrae la ley desde cualquier contenedor de componentes de la API V3."""

    primary_rows: list[list[object]] = []
    fallback_rows: list[list[object]] = []
    for mapping in _nested_mappings(list(components or ())):
        if "nombre" not in mapping or "value" not in mapping:
            continue
        label = normalize_process_law_text(mapping.get("nombre"))
        row = [mapping.get("nombre"), mapping.get("value")]
        if label in _PRIMARY_LABELS:
            primary_rows.append(row)
        elif label == _FALLBACK_LABEL:
            fallback_rows.append(row)
    result = process_law_from_pairs(primary_rows)
    if result != PROCESS_LAW_UNKNOWN:
        return result
    return process_law_from_pairs(fallback_rows)


def process_law_from_detail_text(value: object) -> str:
    """Respaldo acotado: solo acepta una etiqueta exacta y su valor vecino."""

    lines = [clean_process_law_value(line) for line in str(value or "").splitlines()]
    lines = [line for line in lines if line]
    fallback_rows: list[list[str]] = []
    for index, line in enumerate(lines):
        normalized = normalize_process_law_text(line)
        for label in (*sorted(_PRIMARY_LABELS), _FALLBACK_LABEL):
            if normalized == label:
                if index + 1 < len(lines):
                    target = [[label, lines[index + 1]]]
                    result = process_law_from_pairs(target)
                    if result != PROCESS_LAW_UNKNOWN:
                        if label in _PRIMARY_LABELS:
                            return result
                        fallback_rows.extend(target)
                continue
            if normalized.startswith(label + " "):
                # Se conserva el texto original aproximadamente por longitud de
                # palabras; el normalizador solo se usa para validar la etiqueta.
                remainder_words = normalized.split()[len(label.split()) :]
                result = process_law_from_pairs([[label, " ".join(remainder_words)]])
                if result != PROCESS_LAW_UNKNOWN:
                    if label in _PRIMARY_LABELS:
                        return result
                    fallback_rows.append([label, " ".join(remainder_words)])
    return process_law_from_pairs(fallback_rows)


_PROCESS_LAW_ROWS_SCRIPT = r"""
return Array.from(document.querySelectorAll('tr')).map((row) => {
  const cells = Array.from(row.querySelectorAll(':scope > th, :scope > td'));
  return cells.map((cell) => (cell.innerText || cell.textContent || '').trim());
}).filter((cells) => cells.length >= 2);
"""


def extract_process_law(driver) -> str:
    """Lee la ley del DOM cargado sin navegar ni hacer una búsqueda amplia."""

    try:
        rows = driver.execute_script(_PROCESS_LAW_ROWS_SCRIPT) or []
    except Exception:
        rows = []
    result = process_law_from_pairs(rows)
    if result != PROCESS_LAW_UNKNOWN:
        return result
    try:
        body_text = driver.find_element("tag name", "body").text
    except Exception:
        body_text = ""
    return process_law_from_detail_text(body_text)


def has_detected_ficha(values: Iterable[object] | object) -> bool:
    if isinstance(values, str):
        candidates: Iterable[object] = re.split(r"[,;|\n]+", values)
    elif isinstance(values, Iterable):
        candidates = values
    else:
        candidates = ()
    for value in candidates:
        token = str(value or "").replace("*", "").strip()
        if re.fullmatch(r"\d{3,8}", token):
            return True
    return False


def is_law_419_without_ficha(law: object, fichas: Iterable[object] | object) -> bool:
    return is_law_419(law) and not has_detected_ficha(fichas)
