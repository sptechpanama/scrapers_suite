from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from typing import Iterable

DEFAULT_RS_SP_KEYWORDS = ("chiller", "york", "daikin")


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


def normalize_keyword_term(value: object) -> str:
    raw = str(value or "").strip()
    root_match = raw.endswith("*")
    normalized = _normalize_text(raw[:-1] if root_match else raw)
    if normalized and root_match:
        return f"{normalized}*"
    return normalized


def normalize_column_name(value: object) -> str:
    return _normalize_text(value)


@lru_cache(maxsize=512)
def _compiled_keyword_pattern(normalized_term: str):
    normalized_term = normalize_keyword_term(normalized_term)
    if not normalized_term:
        return None
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


def match_keywords_in_text(text: object, keywords: Iterable[object]) -> list[str]:
    normalized_text = _normalize_text(text)
    if not normalized_text:
        return []

    matches: list[str] = []
    seen: set[str] = set()
    for raw_keyword in keywords:
        keyword = normalize_keyword_term(raw_keyword)
        if not keyword or keyword in seen:
            continue
        seen.add(keyword)
        pattern = _compiled_keyword_pattern(keyword)
        if pattern and pattern.search(normalized_text):
            matches.append(keyword)
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

    preview_rows: list[dict[str, object]] = []
    match_count = 0
    for row in rows:
        parts: list[str] = []
        for col in text_columns:
            idx = col_idx.get(col, -1)
            if idx >= 0 and idx < len(row):
                parts.append(str(row[idx] or ""))
        matched = match_keywords_in_text(" ".join(parts), normalized_terms)
        if not matched:
            continue
        match_count += 1
        if len(preview_rows) >= preview_limit:
            continue
        preview_rows.append(
            {
                "palabras_clave": ", ".join(matched),
                "titulo": str(row[col_idx.get("titulo", -1)]).strip() if col_idx.get("titulo", -1) >= 0 else "",
                "entidad": str(row[col_idx.get("entidad", -1)]).strip() if col_idx.get("entidad", -1) >= 0 else "",
                "fecha": str(row[col_idx.get("fecha", -1)]).strip() if col_idx.get("fecha", -1) >= 0 else "",
                "precio_referencia": str(row[col_idx.get("precio_referencia", -1)]).strip() if col_idx.get("precio_referencia", -1) >= 0 else "",
                "enlace": str(row[col_idx.get("enlace", -1)]).strip() if col_idx.get("enlace", -1) >= 0 else "",
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
