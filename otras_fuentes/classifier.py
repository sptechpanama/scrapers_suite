from __future__ import annotations

import os
import re
from bs4 import BeautifulSoup
from dataclasses import dataclass

from common.keyword_watch import (
    DEFAULT_RS_SP_KEYWORDS,
    DEFAULT_RS_SP_NEGATIVE_KEYWORDS,
    match_keywords_in_text,
    match_negative_keywords_in_text,
    normalize_keyword_term,
    parse_keyword_rule,
)

from .models import Opportunity, clean_text, normalized_text
from .qualification import screen, effective_bucket


RIR_DEFAULT_KEYWORDS = (
    "dispositivo medico",
    "dispositivos medicos",
    "equipo medico",
    "equipos medicos",
    "equipamiento medico",
    "suministro medico",
    "suministros medicos",
    "material medico",
    "insumo medico",
    "insumos medicos",
    "medico quirurgico",
    "hospital*",
    "laboratorio",
    "reactivo*",
    "esterilizacion",
    "diagnostico",
    "material sanitario",
    "insumo hospitalario",
    "insumos hospitalarios",
    "medical device*",
    "medical equipment",
    "medical suppl*",
    "surgical suppl*",
    "hospital equipment",
    "laboratory equipment",
    "reagent*",
    "sterilization",
    "diagnostic*",
    "patient monitor*",
    "anesthesia",
    "anaesthesia",
    "oxygen therapy",
    "medical consumable*",
    "biomedical equipment",
    "dispositivo medico*",
    "equipamento medico*",
    "material hospitalar",
    "insumo hospitalar",
)

RS_DEFAULT_KEYWORDS = (
    *DEFAULT_RS_SP_KEYWORDS,
    "fotovolta*",
    "energia solar",
    "refrigeracion",
    "agua helada",
    "manejador de aire",
    "manejadora de aire",
    "manejadores de aire",
    "manejadoras de aire",
    "vrf",
    "vrv",
    "torre de enfriamiento",
    "fan coil",
    "fancoil",
    "electromecanico*",
    "sistema electrico",
    "planta electrica",
    "hvac",
    "ahu",
    "air handling unit*",
    "chilled water",
    "cooling tower*",
    "chiller*",
    "air conditioning",
    "air conditioner*",
    "solar pv",
    "solar photovoltaic",
    "photovoltaic*",
    "electrical work*",
    "electrical system*",
    "generator*",
    "genset*",
    "water treatment",
    "wastewater treatment",
    "ar condicionado",
    "agua gelada",
    "torre de resfriamento",
    "energia fotovoltaica",
    "sistema eletrico",
    "aire acondicionado",
    "aires acondicionados",
    "generadores electricos",
    "generadores sincronos",
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


def classify_opportunity(opportunity: Opportunity, profiles: dict | None = None) -> Opportunity:
    detail = opportunity.raw_payload.get("document_analysis") or {}
    fields = {
        "titulo": opportunity.title,
        "descripcion": opportunity.description,
        "documento": str(detail.get("text") or ""),
    }
    rs_terms = _configured_terms("OTRAS_FUENTES_RS_KEYWORDS", RS_DEFAULT_KEYWORDS)
    rir_terms = _configured_terms("OTRAS_FUENTES_RIR_KEYWORDS", RIR_DEFAULT_KEYWORDS)
    rs_negative = _configured_terms(
        "OTRAS_FUENTES_RS_NEGATIVE_KEYWORDS", DEFAULT_RS_SP_NEGATIVE_KEYWORDS
    )
    if profiles:
        rs_terms = tuple(profiles.get('rs', rs_terms))
        rs_negative = tuple(profiles.get('negative', rs_negative))
        opportunity.raw_payload['company_profile'] = {k: profiles.get(k, '') for k in ('status', 'loaded_at')}

    matches: dict[str, list[str]] = {"RS/SP": [], "RIR": []}
    matched_fields: list[str] = []
    field_weight = 0.0
    ambiguous = False
    budget_pending = []
    # Evaluate text without claiming any actual budget. Only the resulting
    # review flag is stored; this sentinel is never used as an opportunity amount.
    text_only_budget = max((rule.minimum_amount or 0 for term in rs_terms
                            if (rule := parse_keyword_rule(term))), default=0) + 1
    for field_name, value in fields.items():
        value = BeautifulSoup(value, "html.parser").get_text(" ") if "<" in value else value
        norm = normalized_text(value)
        rs_matches = match_keywords_in_text(
            value, rs_terms, reference_amount=opportunity.estimated_value
        )
        if opportunity.estimated_value is None and not rs_matches:
            possible = match_keywords_in_text(value, rs_terms, reference_amount=text_only_budget)
            if possible:
                rs_matches = possible
                budget_pending.extend(possible)
        negatives = match_negative_keywords_in_text(value, rs_negative)
        if negatives:
            ambiguous = ambiguous or bool(rs_matches)
            rs_matches = []
        rir_matches = match_keywords_in_text(value, rir_terms)
        # Names such as New York and administrative diagnostics are not products.
        hvac = bool(re.search(r'\b(?:hvac|chiller\w*|chiler\w*|refriger\w*|climat\w*|aire[s]? acondicionado[s]?|air conditioning|air conditioner\w*|agua helada|chilled water|manejador\w*)\b', norm))
        medical = bool(re.search(r'\b(?:medic[oa]\w*|medical|patient\w*|paciente\w*|clinico\w*|clinical|quirurg\w*|surgical|biomedic\w*|anestesi\w*|anesthe\w*|anaesthe\w*|esteriliz\w*|steriliz\w*|sangre|blood|hospitalario\w*|hospital equipment)\b', norm))
        safe_rs = [term for term in rs_matches if term not in {'york', 'coil', 'serpentin', 'serpentín'} or hvac]
        rir_ambiguous = {'laboratorio', 'laboratory equipment', 'diagnostico', 'diagnostic*', 'reactivo*', 'reagent*', 'hospital*'}
        safe_rir = [term for term in rir_matches if term not in rir_ambiguous or medical]
        # A hospital mentioned as the location alone is insufficient (software, construction...).
        if safe_rir == ['hospital*']:
            safe_rir = []
        ambiguous = ambiguous or len(safe_rs) < len(rs_matches) or len(safe_rir) < len(rir_matches)
        rs_matches, rir_matches = safe_rs, safe_rir
        if rs_matches or rir_matches:
            matched_fields.append(field_name)
            field_weight += 18.0 if field_name == "titulo" else 9.0
        matches["RS/SP"].extend(rs_matches)
        matches["RIR"].extend(rir_matches)

    rs = list(dict.fromkeys(matches["RS/SP"]))
    rir = list(dict.fromkeys(matches["RIR"]))
    # Only explicit technical-sheet labels count; classification codes never do.
    explicit = sorted(set(re.findall(r'\bficha(?:s)?\s*(?:tecnica(?:s)?)?\s*(?:n(?:o|umero)?\s*)?\s*(\d{4,6})\b', normalized_text(' '.join(fields.values())))))
    watched = sorted(set(explicit).intersection((profiles or {}).get('fichas', [])))
    opportunity.raw_payload['explicit_fichas'] = explicit
    opportunity.raw_payload['watched_fichas'] = watched
    rir.extend('Ficha ' + code for code in watched)
    # En los portales globales la palabra "hospital" aparece también en
    # consultorías, obras civiles o software. Se conserva la regla histórica
    # para las fuentes existentes, pero no basta por sí sola para clasificar
    # una oportunidad internacional nueva como RIR.
    if opportunity.source in {"idb", "world_bank", "ungm_international", "unicef"}:
        if rir == ["hospital*"]:
            rir = []
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
    # Sector/buyer metadata may prompt review, but cannot prove a product match.
    if not companies:
        context = opportunity.sector + ' ' + opportunity.buyer
        ambiguous = ambiguous or bool(match_keywords_in_text(context, rir_terms) or match_keywords_in_text(context, rs_terms))
    opportunity.raw_payload['qualification'] = screen(opportunity, strong=bool(companies), ambiguous=ambiguous)
    opportunity.raw_payload['budget_rules_pending'] = list(dict.fromkeys(budget_pending)) if rs else []
    if rs and budget_pending:
        quality = opportunity.raw_payload['qualification']
        if quality['bucket'] == 'relevant':
            quality['bucket'] = 'review'
        quality['reason'] += '; Presupuesto no publicado: falta comprobar el monto mínimo de la palabra clave'
    return opportunity


def should_alert(opportunity: Opportunity) -> bool:
    quality = opportunity.raw_payload.get('qualification')
    if quality is None:
        classify_opportunity(opportunity)
        quality = opportunity.raw_payload['qualification']
    return bool(opportunity.matched_company) and effective_bucket(quality) == 'relevant'
