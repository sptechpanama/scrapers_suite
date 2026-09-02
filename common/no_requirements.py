from __future__ import annotations

"""Clasificacion de actos que contienen fichas sin CT ni registro sanitario."""

import re
import unicodedata
from collections.abc import Iterable, Sequence
from dataclasses import dataclass


NO_REQUIREMENTS_SCOPE_COLUMN = "Tipo de acto sin requisitos"
NO_REQUIREMENTS_ONLY = "Solo fichas sin requisitos"
NO_REQUIREMENTS_MIXED = "Acto mixto"
ADJUDICATION_TYPE_COLUMN = "Tipo de adjudicación"
NO_REQUIREMENTS_FICHAS_COLUMN = "Fichas sin requisitos"
REQUIREMENTS_FICHAS_COLUMN = "Fichas con requisitos"
UNCLASSIFIED_FICHAS_COLUMN = "Fichas por verificar"
ADJUDICATION_BY_LINE = "Renglón"
ADJUDICATION_GLOBAL = "Global"
ADJUDICATION_UNKNOWN = "No identificado"


@dataclass(frozen=True)
class NoRequirementsDecision:
    """Resultado auditable de la regla de inclusión en ``sin requisitos``."""

    scope: str
    eligible: bool
    adjudication_type: str
    no_requirements_fichas: tuple[str, ...]
    requirements_fichas: tuple[str, ...]
    unclassified_fichas: tuple[str, ...]
    requirements_label: str
    reason: str


def normalize_ficha_code(value: object) -> str:
    text = str(value if value is not None else "").replace("*", "").strip()
    text = re.sub(r"\.0$", "", text)
    return text if re.fullmatch(r"\d{3,8}", text) else ""


def normalize_ficha_codes(values: Iterable[object]) -> set[str]:
    return {code for value in values if (code := normalize_ficha_code(value))}


def _ordered_ficha_codes(values: Iterable[object]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        code = normalize_ficha_code(value)
        if code and code not in seen:
            ordered.append(code)
            seen.add(code)
    return ordered


def _normalized_text(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value if value is not None else ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", text).strip().casefold()


def normalize_adjudication_type(value: object) -> str:
    """Normaliza la modalidad oficial sin adivinar valores ambiguos.

    PanamáCompra publica actualmente ``Global`` y ``Renglón``. Se aceptan
    variantes textuales equivalentes para tolerar cambios menores del rótulo.
    """

    text = _normalized_text(value)
    if not text:
        return ADJUDICATION_UNKNOWN
    if (
        re.search(r"\brenglon(?:es)?\b", text)
        or re.search(r"\bpor (?:linea|lineas|item|items)\b", text)
        or "parcial" in text
    ):
        return ADJUDICATION_BY_LINE
    if re.search(r"\bglobal\b", text) or re.search(r"\btotal\b", text):
        return ADJUDICATION_GLOBAL
    return ADJUDICATION_UNKNOWN


def adjudication_type_from_cells(values: Sequence[object]) -> str:
    """Busca una modalidad reconocida en las celdas de una fila del listado."""

    for value in reversed(values):
        normalized = normalize_adjudication_type(value)
        if normalized != ADJUDICATION_UNKNOWN:
            return normalized
    return ADJUDICATION_UNKNOWN


def adjudication_type_from_detail_text(value: object) -> str:
    """Extrae la modalidad solo cerca de su etiqueta en el detalle del acto."""

    lines = [line.strip() for line in str(value or "").splitlines() if line.strip()]
    labels = (
        "modalidad de adjudicacion",
        "tipo de adjudicacion",
        "forma de adjudicacion",
    )
    for index, line in enumerate(lines):
        normalized_line = _normalized_text(line)
        if not any(label in normalized_line for label in labels):
            continue
        for candidate in lines[index : index + 3]:
            mode = normalize_adjudication_type(candidate)
            if mode != ADJUDICATION_UNKNOWN:
                return mode
    return ADJUDICATION_UNKNOWN


def resolve_adjudication_type(primary: object, detail_text: object = "") -> str:
    """Prefiere el listado oficial y usa el detalle como respaldo acotado."""

    mode = normalize_adjudication_type(primary)
    if mode != ADJUDICATION_UNKNOWN:
        return mode
    return adjudication_type_from_detail_text(detail_text)


def evaluate_no_requirements(
    fichas: Iterable[object],
    *,
    no_requirements: Iterable[object],
    requires_ct: Iterable[object],
    requires_rs: Iterable[object],
    adjudication_type: object = "",
) -> NoRequirementsDecision:
    """Evalúa composición y modalidad antes de publicar un acto sin requisitos.

    * Un acto puro es elegible con cualquier modalidad.
    * Un acto mixto solo es elegible cuando PanamáCompra confirma ``Renglón``.
    * Una modalidad ausente o desconocida falla de forma conservadora.
    """

    detected = _ordered_ficha_codes(fichas)
    no_req_catalog = normalize_ficha_codes(no_requirements)
    ct_catalog = normalize_ficha_codes(requires_ct)
    rs_catalog = normalize_ficha_codes(requires_rs)

    no_req: list[str] = []
    required: list[str] = []
    unclassified: list[str] = []
    requirement_labels: list[str] = []
    for code in detected:
        has_ct = code in ct_catalog
        has_rs = code in rs_catalog
        is_no_req = code in no_req_catalog
        if is_no_req:
            no_req.append(code)
        if has_ct or has_rs:
            required.append(code)
            requirement = "CT/RS" if has_ct and has_rs else ("CT" if has_ct else "RS")
            requirement_labels.append(f"{code} ({requirement})")
        elif not is_no_req:
            unclassified.append(code)

    mode = normalize_adjudication_type(adjudication_type)
    if not no_req:
        return NoRequirementsDecision(
            scope="",
            eligible=False,
            adjudication_type=mode,
            no_requirements_fichas=(),
            requirements_fichas=tuple(required),
            unclassified_fichas=tuple(unclassified),
            requirements_label=", ".join(requirement_labels),
            reason="No contiene una ficha confirmada sin requisitos",
        )

    mixed = bool(required or unclassified)
    scope = NO_REQUIREMENTS_MIXED if mixed else NO_REQUIREMENTS_ONLY
    eligible = not mixed or mode == ADJUDICATION_BY_LINE
    if not mixed:
        reason = "Todas las fichas detectadas están confirmadas sin requisitos"
    elif eligible:
        reason = "Acto mixto admitido porque la adjudicación es por renglón"
    elif mode == ADJUDICATION_GLOBAL:
        reason = "Acto mixto excluido porque la adjudicación es global"
    else:
        reason = "Acto mixto excluido porque la adjudicación no fue confirmada"

    return NoRequirementsDecision(
        scope=scope,
        eligible=eligible,
        adjudication_type=mode,
        no_requirements_fichas=tuple(no_req),
        requirements_fichas=tuple(required),
        unclassified_fichas=tuple(unclassified),
        requirements_label=", ".join(requirement_labels),
        reason=reason,
    )


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

    return evaluate_no_requirements(
        fichas,
        no_requirements=no_requirements,
        requires_ct=requires_ct,
        requires_rs=requires_rs,
    ).scope


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
