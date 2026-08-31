from __future__ import annotations

"""Clasificacion de actos que contienen fichas sin CT ni registro sanitario."""

import re
from collections.abc import Iterable, Sequence


NO_REQUIREMENTS_SCOPE_COLUMN = "Tipo de acto sin requisitos"
NO_REQUIREMENTS_ONLY = "Solo fichas sin requisitos"
NO_REQUIREMENTS_MIXED = "Acto mixto"


def normalize_ficha_code(value: object) -> str:
    text = str(value if value is not None else "").strip().replace("*", "")
    text = re.sub(r"\.0$", "", text)
    return text if re.fullmatch(r"\d{3,8}", text) else ""


def normalize_ficha_codes(values: Iterable[object]) -> set[str]:
    return {code for value in values if (code := normalize_ficha_code(value))}


def ficha_codes_from_label(value: object) -> list[str]:
    """Extrae fichas de la columna ``ficha_detectada`` sin inferir desde texto libre."""

    parts = re.split(r"[,;|\n]+", str(value if value is not None else ""))
    ordered: list[str] = []
    seen: set[str] = set()
    for part in parts:
        code = normalize_ficha_code(part)
        if code and code not in seen:
            ordered.append(code)
            seen.add(code)
    return ordered


def classify_no_requirements_scope(
    fichas: Iterable[object],
    *,
    no_requirements: Iterable[object],
    requires_ct: Iterable[object],
    requires_rs: Iterable[object],
) -> str:
    """Distingue actos puros de actos mixtos sin cambiar su categoria principal.

    Un acto es puro solo cuando todas sus fichas detectadas estan confirmadas en
    el catalogo sin requisitos y ninguna aparece en los catalogos CT/RS. Una
    ficha adicional desconocida se trata de forma conservadora como mezcla.
    """

    detected = normalize_ficha_codes(fichas)
    no_req = normalize_ficha_codes(no_requirements)
    ct = normalize_ficha_codes(requires_ct)
    rs = normalize_ficha_codes(requires_rs)
    if not detected or not detected.intersection(no_req):
        return ""
    pure = all(code in no_req and code not in ct and code not in rs for code in detected)
    return NO_REQUIREMENTS_ONLY if pure else NO_REQUIREMENTS_MIXED


def scope_column_values(
    rows: Sequence[Sequence[object]],
    *,
    no_requirements: Iterable[object],
    requires_ct: Iterable[object],
    requires_rs: Iterable[object],
    ficha_column: str = "ficha_detectada",
    scope_column: str = NO_REQUIREMENTS_SCOPE_COLUMN,
) -> tuple[list[list[str]], int]:
    """Calcula la columna completa para migrar filas historicas de una hoja."""

    if not rows:
        return [], 0
    header = [str(value).strip().lower() for value in rows[0]]
    try:
        ficha_idx = header.index(ficha_column.strip().lower())
        scope_idx = header.index(scope_column.strip().lower())
    except ValueError:
        return [], 0

    values: list[list[str]] = []
    changes = 0
    for source_row in rows[1:]:
        row = list(source_row)
        ficha_value = row[ficha_idx] if ficha_idx < len(row) else ""
        expected = classify_no_requirements_scope(
            ficha_codes_from_label(ficha_value),
            no_requirements=no_requirements,
            requires_ct=requires_ct,
            requires_rs=requires_rs,
        )
        current = str(row[scope_idx]).strip() if scope_idx < len(row) else ""
        values.append([expected])
        if current != expected:
            changes += 1
    return values, changes

