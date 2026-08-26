from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable

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
KEYWORD_RULES_VERSION = 2
DEFAULT_RS_SP_KEYWORDS = (
    "chiller",
    "york",
    "daikin",
    *HVAC_OVER_15K_KEYWORDS,
)
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
        for rule in HVAC_OVER_15K_KEYWORDS
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


def summarize_keyword_rows(
    *,
    rows: list[list[object]],
    cols: list[str],
    keyword_terms: Iterable[object],
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

    def row_text(row: list[object], normalized_name: str) -> str:
        column = normalized_cols.get(normalized_name, "")
        idx = col_idx.get(column, -1)
        return str(row[idx] or "").strip() if 0 <= idx < len(row) else ""

    preview_rows: list[dict[str, object]] = []
    match_count = 0
    for row in rows:
        parts: list[str] = []
        for col in text_columns:
            idx = col_idx.get(col, -1)
            if idx >= 0 and idx < len(row):
                parts.append(str(row[idx] or ""))
        price_idx = col_idx.get(price_column, -1)
        reference_amount = row[price_idx] if 0 <= price_idx < len(row) else None
        matched = match_keywords_in_text(
            " ".join(parts),
            normalized_terms,
            reference_amount=reference_amount,
        )
        if not matched:
            continue
        match_count += 1
        if len(preview_rows) >= preview_limit:
            continue
        preview_rows.append(
            {
                "palabras_clave": ", ".join(matched),
                "titulo": row_text(row, "titulo"),
                "entidad": row_text(row, "entidad"),
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
