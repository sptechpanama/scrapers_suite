from __future__ import annotations

import os
from dataclasses import dataclass

from common.keyword_watch import (
    DEFAULT_RS_SP_KEYWORDS,
    DEFAULT_RS_SP_NEGATIVE_KEYWORDS,
    match_keywords_in_text,
    match_negative_keywords_in_text,
    normalize_keyword_term,
)

from .models import Opportunity, clean_text


RIR_DEFAULT_KEYWORDS = (
    "dispositivo medico",
    "dispositivos medicos",
    "equipo medico",
    "equipos medicos",
    "insumo medico",
    "insumos medicos",
    "medico quirurgico",
    "hospital*",
    "laboratorio",
    "reactivo*",
    "esterilizacion",
    "diagnostico",
    "material sanitario",
)

RS_DEFAULT_KEYWORDS = (
    *DEFAULT_RS_SP_KEYWORDS,
    "fotovolta*",
    "energia solar",
    "refrigeracion",
    "agua helada",
    "manejador* de aire",
    "vrf",
    "vrv",
    "torre de enfriamiento",
    "fan coil",
    "fancoil",
    "electromecanico*",
    "sistema electrico",
    "planta electrica",
)


def _configured_terms(env_name: str, defaults: tuple[str, ...]) -> tuple[str, ...]:
    raw = clean_text(os.environ.get(env_name, ""))
    values = raw.replace("\n", ",").replace(";", ",").split(",") if raw else defaults
    normalized = [normalize_keyword_term(value) for value in values]
    return tuple(dict.fromkeys(value for value in normalized if value))


@dataclass(frozen=True, slots=True)
class Classification:
    company: str
    keywords: tuple[str, ...]
    fields: tuple[str, ...]
    score: float
    priority: str


def classify_opportunity(opportunity: Opportunity) -> Opportunity:
    fields = {
        "titulo": opportunity.title,
        "descripcion": opportunity.description,
        "sector": opportunity.sector,
        "comprador": opportunity.buyer,
    }
    rs_terms = _configured_terms("OTRAS_FUENTES_RS_KEYWORDS", RS_DEFAULT_KEYWORDS)
    rir_terms = _configured_terms("OTRAS_FUENTES_RIR_KEYWORDS", RIR_DEFAULT_KEYWORDS)
    rs_negative = _configured_terms(
        "OTRAS_FUENTES_RS_NEGATIVE_KEYWORDS", DEFAULT_RS_SP_NEGATIVE_KEYWORDS
    )

    matches: dict[str, list[str]] = {"RS/SP": [], "RIR": []}
    matched_fields: list[str] = []
    field_weight = 0.0
    for field_name, value in fields.items():
        rs_matches = match_keywords_in_text(
            value, rs_terms, reference_amount=opportunity.estimated_value
        )
        negatives = match_negative_keywords_in_text(value, rs_negative)
        if negatives:
            rs_matches = []
        rir_matches = match_keywords_in_text(value, rir_terms)
        if rs_matches or rir_matches:
            matched_fields.append(field_name)
            field_weight += 18.0 if field_name == "titulo" else 9.0
        matches["RS/SP"].extend(rs_matches)
        matches["RIR"].extend(rir_matches)

    rs = list(dict.fromkeys(matches["RS/SP"]))
    rir = list(dict.fromkeys(matches["RIR"]))
    companies = [name for name, values in (("RS/SP", rs), ("RIR", rir)) if values]
    all_matches = list(dict.fromkeys([*rs, *rir]))
    score = min(100.0, len(all_matches) * 24.0 + field_weight)
    if score >= 70:
        priority = "Alta"
    elif score >= 35:
        priority = "Media"
    else:
        priority = "Baja"

    opportunity.matched_company = " + ".join(companies)
    opportunity.matched_keywords = all_matches
    opportunity.matched_fields = list(dict.fromkeys(matched_fields))
    opportunity.fit_score = round(score, 1)
    opportunity.priority = priority
    return opportunity


def should_alert(opportunity: Opportunity) -> bool:
    return bool(opportunity.matched_company) and opportunity.priority in {"Alta", "Media"}

