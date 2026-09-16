from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable, Sequence

if __package__:
    from .notification_entity import notification_location_from_row
else:
    from notification_entity import notification_location_from_row

HVAC_OVER_15K_KEYWORDS = (
    "aire acondicion*>15k",
    "aires acondicion*>15k",
    "sistema de aire acondicionado>15k",
    "aire acondicionado central>15k",
    "split>15k",
    "mini split>15k",
    "minisplit>15k",
    "multisplit>15k",
    "aire acondicionado inverter>15k",
    "expansion directa>15k",
    "sistema dx>15k",
    "vrf>15k",
    "vrv>15k",
    "flujo de refrigerante variable>15k",
    "volumen de refrigerante variable>15k",
    "unidad manejadora de aire>15k",
    "unidad manejador de aire>15k",
    "manejadora de aire>15k",
    "manejador de aire>15k",
    "uma>15k",
    "unidad tipo paquete>15k",
    "unidad paquete>15k",
    "rooftop>15k",
    "roof top>15k",
    "fan coil>15k",
    "fancoil>15k",
    "agua helada>15k",
    "enfriador de agua>15k",
    "torre de enfriamiento>15k",
    "chiller>15k",
    "chiler>15k",
    "shiller>15k",
    "unidad condensadora>15k",
    "unidad evaporadora>15k",
    "cassette de aire acondicionado>15k",
    "bomba de calor>15k",
    "climatizacion*>15k",
)
# Keep the old tuple for recovery of legacy rows and imports.
HVAC_OVER_8K_KEYWORDS = tuple(rule.replace(">15k", ">8k") for rule in HVAC_OVER_15K_KEYWORDS)
HVAC_TECHNICAL_KEYWORDS = (
    "hvac",
    "blower centrifug*",
    "blowers centrifug*",
    "filtro plisad*",
    "filtros plisad*",
    "merv",
)
POWER_GENERATION_KEYWORDS = (
    "planta electric*",
    "plantas electric*",
    "panta electric*",
    "planta de emergencia*",
    "plantas de emergencia*",
    "grupo electrogen*",
    "grupos electrogen*",
    "generador electric*",
    "generadores electric*",
    "generador de emergencia*",
    "generadores de emergencia*",
    "tablero de transferencia automat*",
    "tableros de transferencia automat*",
    "sistema de transferencia automat*",
    "sistemas de transferencia automat*",
)
ENGINEERING_PLAN_KEYWORDS = (
    "confeccion de plano*",
    "elaboracion de plano*",
    "digitalizacion de plano*",
    "actualizacion de plano*",
    "diseno arquitectonic*",
    "diseno electric*",
    "diseno electromecanic*",
    "diseno de instalaciones electric*",
    "tramite de aprobacion de plano*",
    "servicio de tramite de plano*",
    "planos as built",
    "plano como construido*",
)
RS_SP_CONTEXTUAL_KEYWORDS = (
    *POWER_GENERATION_KEYWORDS,
    *ENGINEERING_PLAN_KEYWORDS,
)
RS_SP_CONTEXT_RULES_VERSION = 1
KEYWORD_RULES_VERSION = 5
DEFAULT_RS_SP_KEYWORDS = (
    "chiller",
    "york",
    "daikin",
    *HVAC_OVER_8K_KEYWORDS,
    *HVAC_TECHNICAL_KEYWORDS,
    *RS_SP_CONTEXTUAL_KEYWORDS,
)
DEFAULT_RS_SP_NEGATIVE_KEYWORDS = (
    "automotriz",
    "habitacion de hotel",
    "bloqueador solar",
    "protector solar",
    "oracle solaris",
    "correa del serpentin",
    "techo de planta electric*",
)
_NEGATIVE_KEYWORD_ALIASES = {
    "habitacion de hotel": (
        "habitacion de hotel",
        "habitaciones de hotel",
        "habitacion hotel",
        "habitaciones hotel",
    ),
    "correa del serpentin": (
        "correa del serpentin",
        "correas del serpentin",
        "correa de serpentin",
        "correas de serpentin",
    ),
}
_AMOUNT_SUFFIX_RE = re.compile(
    r"^(?P<term>.*?)\s*>\s*(?:usd|us\$|b/?\.?|\$)?\s*"
    r"(?P<amount>[0-9][0-9.,\s]*)\s*(?P<unit>[km]?)\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class KeywordRule:
    term: str
    minimum_amount: float | None = None

    @property
    def is_root(self) -> bool:
        return self.term.endswith("*")

    @property
    def canonical(self) -> str:
        if self.minimum_amount is None:
            return self.term
        return f"{self.term}>{_format_rule_amount(self.minimum_amount)}"


@dataclass(frozen=True)
class KeywordFieldMatch:
    terms: tuple[str, ...]
    fields: tuple[str, ...]
    field_values: tuple[str, ...]
    suppressed_terms: tuple[str, ...] = ()
    context_policy: str = ""


def _normalize_text(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
    )
    text = re.sub(r"[^0-9a-z]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


@lru_cache(maxsize=1)
def _legacy_hvac_rule_aliases() -> dict[str, str]:
    """Recupera las reglas HVAC que un UI anterior guardó como ``... 15k``."""

    return {
        _normalize_text(rule.replace("*", "").replace(">", " ")): rule
        for rule in (*HVAC_OVER_15K_KEYWORDS, *HVAC_OVER_8K_KEYWORDS)
    }


def _format_rule_amount(value: float) -> str:
    amount = float(value)
    if amount >= 1_000_000 and math.isclose(amount % 1_000_000, 0.0, abs_tol=1e-6):
        return f"{amount / 1_000_000:g}m"
    if amount >= 1_000 and math.isclose(amount % 1_000, 0.0, abs_tol=1e-6):
        return f"{amount / 1_000:g}k"
    return f"{amount:g}"


def parse_reference_amount(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        amount = float(value)
        return amount if math.isfinite(amount) else None

    raw = str(value or "").strip()
    if not raw:
        return None
    negative = raw.startswith("(") and raw.endswith(")")
    raw = re.sub(r"(?i)(B/\.?|USD|US\$|PAB|\$)", "", raw)
    raw = re.sub(r"[^0-9,\.\-]", "", raw)
    if not raw or raw in {"-", ".", ","}:
        return None
    if "," in raw and "." in raw:
        raw = (
            raw.replace(".", "").replace(",", ".")
            if raw.rfind(",") > raw.rfind(".")
            else raw.replace(",", "")
        )
    elif "," in raw:
        parts = raw.split(",")
        if len(parts) > 2:
            raw = "".join(parts[:-1]) + (f".{parts[-1]}" if len(parts[-1]) <= 2 else parts[-1])
        elif len(parts[-1]) <= 2:
            raw = ".".join(parts)
        else:
            raw = "".join(parts)
    elif "." in raw:
        parts = raw.split(".")
        if len(parts) > 2:
            raw = "".join(parts[:-1]) + (f".{parts[-1]}" if len(parts[-1]) <= 2 else parts[-1])
        elif len(parts[-1]) == 3:
            raw = "".join(parts)
    try:
        amount = float(raw)
    except (TypeError, ValueError):
        return None
    if negative:
        amount = -abs(amount)
    return amount if math.isfinite(amount) else None


def parse_keyword_rule(value: object) -> KeywordRule | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if ">" not in raw:
        raw = _legacy_hvac_rule_aliases().get(_normalize_text(raw), raw)
    minimum_amount: float | None = None
    amount_match = _AMOUNT_SUFFIX_RE.fullmatch(raw)
    if amount_match:
        raw = amount_match.group("term").strip()
        minimum_amount = parse_reference_amount(amount_match.group("amount"))
        if minimum_amount is None:
            return None
        unit = amount_match.group("unit").lower()
        if unit == "k":
            minimum_amount *= 1_000
        elif unit == "m":
            minimum_amount *= 1_000_000
    elif ">" in raw:
        return None

    root_match = raw.endswith("*")
    normalized = _normalize_text(raw[:-1] if root_match else raw)
    if not normalized:
        return None
    return KeywordRule(
        term=f"{normalized}*" if root_match else normalized,
        minimum_amount=minimum_amount,
    )


def normalize_keyword_term(value: object) -> str:
    rule = parse_keyword_rule(value)
    return rule.canonical if rule else ""


def _normalize_keyword_terms(values: Iterable[object]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        term = normalize_keyword_term(value)
        if term and term not in seen:
            seen.add(term)
            output.append(term)
    return output


def normalize_column_name(value: object) -> str:
    return _normalize_text(value)


@lru_cache(maxsize=512)
def _compiled_keyword_pattern(normalized_term: str):
    rule = parse_keyword_rule(normalized_term)
    if rule is None:
        return None
    normalized_term = rule.term
    root_match = normalized_term.endswith("*")
    term_body = normalized_term[:-1].strip() if root_match else normalized_term
    tokens = [re.escape(token) for token in term_body.split() if token]
    if not tokens:
        return None
    token_pattern = r"\s+".join(tokens)
    if root_match:
        token_pattern += r"[0-9a-z]*"
    pattern = rf"(?<![0-9a-z]){token_pattern}(?![0-9a-z])"
    return re.compile(pattern)


def match_keywords_in_text(
    text: object,
    keywords: Iterable[object],
    *,
    reference_amount: object = None,
) -> list[str]:
    normalized_text = _normalize_text(text)
    if not normalized_text:
        return []

    parsed_amount = parse_reference_amount(reference_amount)
    matches: list[str] = []
    match_index: dict[str, int] = {}
    matched_rules: dict[str, KeywordRule] = {}
    for raw_keyword in keywords:
        rule = parse_keyword_rule(raw_keyword)
        if rule is None:
            continue
        if rule.minimum_amount is not None and (
            parsed_amount is None or parsed_amount <= rule.minimum_amount
        ):
            continue
        pattern = _compiled_keyword_pattern(rule.term)
        if not pattern or not pattern.search(normalized_text):
            continue
        previous = matched_rules.get(rule.term)
        if previous is None:
            match_index[rule.term] = len(matches)
            matched_rules[rule.term] = rule
            matches.append(rule.canonical)
            continue
        previous_minimum = previous.minimum_amount or -math.inf
        current_minimum = rule.minimum_amount or -math.inf
        if current_minimum > previous_minimum:
            matched_rules[rule.term] = rule
            matches[match_index[rule.term]] = rule.canonical
    return matches


def match_negative_keywords_in_text(
    text: object,
    negative_keywords: Iterable[object],
) -> list[str]:
    normalized_text = _normalize_text(text)
    if not normalized_text:
        return []

    matches: list[str] = []
    seen: set[str] = set()
    for raw_keyword in negative_keywords:
        rule = parse_keyword_rule(raw_keyword)
        if rule is None:
            continue
        variants = _NEGATIVE_KEYWORD_ALIASES.get(rule.term, (rule.term,))
        if not any(
            (pattern := _compiled_keyword_pattern(variant)) is not None
            and pattern.search(normalized_text)
            for variant in variants
        ):
            continue
        if rule.canonical not in seen:
            seen.add(rule.canonical)
            matches.append(rule.canonical)
    return matches


def negative_keywords_in_matching_context(
    *,
    title: object,
    matched_field_values: Iterable[object],
    negative_keywords: Iterable[object],
) -> list[str]:
    configured = [
        normalize_keyword_term(term)
        for term in negative_keywords
        if normalize_keyword_term(term)
    ]
    matches: list[str] = []
    seen: set[str] = set()
    for context in (title, *tuple(matched_field_values)):
        for term in match_negative_keywords_in_text(context, configured):
            if term not in seen:
                seen.add(term)
                matches.append(term)
    return matches


@lru_cache(maxsize=1)
def _rs_sp_contextual_term_bodies() -> frozenset[str]:
    return frozenset(
        rule.term
        for raw in RS_SP_CONTEXTUAL_KEYWORDS
        if (rule := parse_keyword_rule(raw)) is not None
    )


def is_rs_sp_contextual_keyword(value: object) -> bool:
    rule = parse_keyword_rule(value)
    return bool(rule and rule.term in _rs_sp_contextual_term_bodies())


def _is_line_or_partial_adjudication(value: object) -> bool:
    normalized = _normalize_text(value)
    if not normalized:
        return False
    return any(
        token in normalized.split()
        for token in ("renglon", "renglones", "item", "items", "linea", "lineas")
    ) or "parcial" in normalized


def match_keyword_fields(
    fields: Sequence[tuple[object, object]],
    keywords: Iterable[object],
    *,
    reference_amount: object = None,
    adjudication_type: object = "",
) -> KeywordFieldMatch:
    """Evalua una fila sin atribuir un item aislado a un paquete global."""

    configured = _normalize_keyword_terms(keywords)
    field_matches: list[tuple[str, str, list[str], bool]] = []
    for raw_name, raw_value in fields:
        name = str(raw_name or "").strip()
        value = str(raw_value or "").strip()
        matches = match_keywords_in_text(
            value,
            configured,
            reference_amount=reference_amount,
        )
        field_matches.append(
            (name, value, matches, normalize_column_name(name).startswith("item"))
        )

    contextual_in_primary = {
        term
        for _name, _value, matches, is_item in field_matches
        if not is_item
        for term in matches
        if is_rs_sp_contextual_keyword(term)
    }
    contextual_in_items = {
        term
        for _name, _value, matches, is_item in field_matches
        if is_item
        for term in matches
        if is_rs_sp_contextual_keyword(term)
    }
    contextual_allowed = bool(contextual_in_primary)
    context_policy = "primary_context" if contextual_allowed else ""
    if contextual_in_items and not contextual_allowed:
        if _is_line_or_partial_adjudication(adjudication_type):
            contextual_allowed = True
            context_policy = "line_adjudication"
        else:
            item_rows = [item for item in field_matches if item[3] and item[1]]
            contextual_allowed = bool(item_rows) and all(
                any(is_rs_sp_contextual_keyword(term) for term in matches)
                for _name, _value, matches, _is_item in item_rows
            )
            context_policy = "all_items_context" if contextual_allowed else "mixed_items_suppressed"

    accepted_terms: list[str] = []
    accepted_fields: list[str] = []
    accepted_values: list[str] = []
    suppressed_terms: list[str] = []
    for name, value, matches, _is_item in field_matches:
        accepted_here = [
            term
            for term in matches
            if not is_rs_sp_contextual_keyword(term) or contextual_allowed
        ]
        rejected_here = [
            term
            for term in matches
            if is_rs_sp_contextual_keyword(term) and not contextual_allowed
        ]
        for term in accepted_here:
            if term not in accepted_terms:
                accepted_terms.append(term)
        for term in rejected_here:
            if term not in suppressed_terms:
                suppressed_terms.append(term)
        if accepted_here:
            accepted_fields.append(name)
            accepted_values.append(value)

    return KeywordFieldMatch(
        terms=tuple(accepted_terms),
        fields=tuple(accepted_fields),
        field_values=tuple(accepted_values),
        suppressed_terms=tuple(suppressed_terms),
        context_policy=context_policy,
    )


def summarize_keyword_rows(
    *,
    rows: list[list[object]],
    cols: list[str],
    keyword_terms: Iterable[object],
    negative_terms: Iterable[object] = (),
    source_sheet: str,
    job_name: str,
    preview_limit: int = 100,
) -> dict[str, object] | None:
    normalized_terms = [normalize_keyword_term(term) for term in keyword_terms if normalize_keyword_term(term)]
    if not rows or not cols or not normalized_terms:
        return None

    col_idx = {str(col): idx for idx, col in enumerate(cols)}
    normalized_cols = {normalize_column_name(col): str(col) for col in cols}
    text_columns = [
        normalized_cols[key]
        for key in normalized_cols
        if key in {"titulo", "descripcion"} or key.startswith("item")
    ]
    if not text_columns:
        return None
    price_column = next(
        (
            normalized_cols[key]
            for key in (
                "precio referencia",
                "precio de referencia",
                "monto referencia",
            )
            if key in normalized_cols
        ),
        "",
    )
    adjudication_column = next(
        (
            normalized_cols[key]
            for key in (
                "tipo de adjudicacion",
                "modalidad de adjudicacion",
                "forma de adjudicacion",
                "modalidad",
            )
            if key in normalized_cols
        ),
        "",
    )

    def row_text(row: list[object], normalized_name: str) -> str:
        column = normalized_cols.get(normalized_name, "")
        idx = col_idx.get(column, -1)
        return str(row[idx] or "").strip() if 0 <= idx < len(row) else ""

    preview_rows: list[dict[str, object]] = []
    match_count = 0
    for row in rows:
        price_idx = col_idx.get(price_column, -1)
        reference_amount = row[price_idx] if 0 <= price_idx < len(row) else None
        adjudication_idx = col_idx.get(adjudication_column, -1)
        adjudication_type = (
            row[adjudication_idx] if 0 <= adjudication_idx < len(row) else ""
        )
        field_match = match_keyword_fields(
            [
                (col, row[idx] if 0 <= idx < len(row) else "")
                for col in text_columns
                for idx in (col_idx.get(col, -1),)
            ],
            normalized_terms,
            reference_amount=reference_amount,
            adjudication_type=adjudication_type,
        )
        if not field_match.terms:
            continue
        if negative_keywords_in_matching_context(
            title=row_text(row, "titulo"),
            matched_field_values=field_match.field_values,
            negative_keywords=negative_terms,
        ):
            continue
        match_count += 1
        if len(preview_rows) >= preview_limit:
            continue
        preview_rows.append(
            {
                "palabras_clave": ", ".join(field_match.terms),
                "titulo": row_text(row, "titulo"),
                "entidad": row_text(row, "entidad"),
                **notification_location_from_row(cols, row),
                "fecha": row_text(row, "fecha"),
                "precio_referencia": (
                    str(reference_amount or "").strip()
                    if reference_amount is not None
                    else ""
                ),
                "enlace": row_text(row, "enlace"),
                "hoja_origen": source_sheet,
            }
        )

    if not match_count:
        return None

    return {
        "job": job_name,
        "sheet": source_sheet,
        "count": match_count,
        "rows": preview_rows,
        "truncated": match_count > preview_limit,
    }
