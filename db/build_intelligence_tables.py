from __future__ import annotations

"""Construye la capa analitica normalizada para Inteligencia de Proveedores.

La base operacional de Panama Compra se conserva intacta. Este proceso genera
tablas derivadas, optimizadas para consultas por periodo, ficha y proveedor.
Puede escribir una base SQLite local y, opcionalmente, publicar las mismas
tablas en PostgreSQL/Supabase.
"""

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_SOURCE_DB = REPO_ROOT / "data" / "db" / "panamacompra.db"
DEFAULT_OUTPUT_DB = REPO_ROOT / "data" / "db" / "inteligencia_proveedores.db"
DEFAULT_APP_ROOT = Path.home() / "GEAPP"
DEFAULT_METADATA_XLSX = DEFAULT_APP_ROOT / "fichas_ctni_con_enlace.xlsx"
DEFAULT_CATALOG_XLSX = DEFAULT_APP_ROOT / "oferentes_catalogos.xlsx"
DEFAULT_ALIAS_JSON = REPO_ROOT / "data" / "fichas" / "ficha_aliases.json"
DEFAULT_CLASSIFICATION_XLSX = REPO_ROOT / "data" / "fichas" / "todas_las_fichas.xlsx"
DEFAULT_RISK_CLASS_XLSX = REPO_ROOT / "minsa_scraper" / "outputs" / "fichas_ctni.xlsx"

ANALYTICS_SCHEMA_VERSION = "3.4.0"
SOURCE_CHUNK_SIZE = 5_000
WRITE_CHUNK_SIZE = 5_000
SQLITE_MAX_BOUND_PARAMETERS = 30_000
FICHA_TOKEN_RE = re.compile(r"(?<!\d)(\d{3,8})\*?(?!\d)")
DATE_TOKEN_RE = re.compile(r"(?<!\d)(\d{1,2}[\-/]\d{1,2}[\-/]\d{4}|\d{4}[\-/]\d{1,2}[\-/]\d{1,2})(?!\d)")

FACT_COLUMNS = [
    "acto_key",
    "source_id",
    "ficha",
    "is_unique_ficha",
    "detected_ficha_count",
    "detection_score",
    "detection_method",
    "detection_field",
    "detection_evidence",
    "detector_version",
    "catalog_version",
    "enlace",
    "titulo",
    "entidad",
    "unidad_solicitante",
    "estado",
    "publication_date",
    "celebration_date",
    "celebration_end_date",
    "award_date",
    "update_date",
    "source_line_count",
    "attributed_line_count",
    "reference_amount",
    "reference_amount_context",
    "reference_amount_attributed",
    "reference_amount_attribution_source",
    "reference_amount_reliable",
    "award_amount",
    "award_amount_context",
    "award_amount_attributed",
    "award_amount_attribution_source",
    "award_amount_reliable",
    "award_amount_source",
    "winner",
    "winner_short",
    "participant_count",
    "search_text_norm",
]

PROPONENT_COLUMNS = [
    "acto_key",
    "source_id",
    "ordinal",
    "proveedor",
    "proveedor_norm",
    "offered_amount",
    "is_winner",
]

METADATA_COLUMNS = [
    "ficha",
    "nombre_ficha",
    "descripcion",
    "area",
    "tipo_producto",
    "especialidad",
    "clase_riesgo",
    "tiene_ct",
    "registro_sanitario",
    "enlace_minsa",
    "metadata_source",
    "search_text_norm",
]

CATALOG_COLUMNS = [
    "ficha",
    "oferente",
    "contacto",
    "telefono",
    "correo",
    "catalogo",
    "producto",
    "fabricante",
    "marca",
    "modelo_web",
    "estado_catalogo",
]


def _log(label: str, message: str, started: float) -> None:
    elapsed = time.perf_counter() - started
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} | {label:<10} | +{elapsed:,.1f}s | {message}", flush=True)


def _sqlite_multi_chunksize(column_count: int) -> int:
    """Evita superar el lÃ­mite de variables enlazadas de SQLite."""

    return max(1, min(1_000, SQLITE_MAX_BOUND_PARAMETERS // max(1, column_count)))


def clean_text(value: object) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>", "n/a"}:
        return ""
    return re.sub(r"\s+", " ", text)


def normalize_text(value: object) -> str:
    text = clean_text(value).lower()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_header(value: object) -> str:
    return normalize_text(value).replace(" ", "_")


def parse_number(value: object) -> float:
    """Convierte moneda local/US a float sin confundir miles y decimales."""
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        number = float(value)
        return number if math.isfinite(number) else 0.0
    text = clean_text(value)
    if not text:
        return 0.0
    text = re.sub(r"(?i)(B/\.?|USD|US\$|PAB|\$)", "", text).replace(" ", "")
    negative = text.startswith("(") and text.endswith(")")
    text = text.strip("()")
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        tail = text.rsplit(",", 1)[-1]
        text = text.replace(",", "." if len(tail) <= 2 else "")
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", "-", ".", "-."}:
        return 0.0
    try:
        number = float(text)
        if negative:
            number = -abs(number)
        return number if math.isfinite(number) else 0.0
    except ValueError:
        return 0.0


def parse_int(value: object) -> int:
    try:
        return max(0, int(round(parse_number(value))))
    except Exception:
        return 0


def parse_date_tokens(value: object) -> list[str]:
    text = clean_text(value)
    if not text:
        return []
    tokens = DATE_TOKEN_RE.findall(text)
    if not tokens:
        tokens = [text]
    result: list[str] = []
    for token in tokens:
        # Los valores ISO deben interpretarse aÃ±o-mes-dÃ­a. Aplicar dayfirst=True
        # indiscriminadamente genera advertencias y puede intercambiar mes/dÃ­a.
        iso_first = bool(re.match(r"^\d{4}[\-/]", token.strip()))
        parsed = pd.to_datetime(token, errors="coerce", dayfirst=not iso_first)
        if pd.isna(parsed):
            parsed = pd.to_datetime(token, errors="coerce")
        if pd.isna(parsed):
            continue
        iso = parsed.date().isoformat()
        if iso not in result:
            result.append(iso)
    return result


def parse_date(value: object, *, position: str = "first") -> str:
    dates = parse_date_tokens(value)
    if not dates:
        return ""
    return dates[-1] if position == "last" else dates[0]


def stable_act_key(row: Mapping[str, Any]) -> str:
    link = clean_text(row.get("enlace"))
    source_id = clean_text(row.get("id"))
    raw = link.lower() if link else f"source:{source_id}"
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def _normalize_ficha(value: object) -> str:
    match = FICHA_TOKEN_RE.search(clean_text(value))
    return match.group(1).lstrip("0") or "0" if match else ""


def extract_ficha_evidence(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Obtiene fichas del JSON V3 y usa la columna legada solo como respaldo."""
    candidates: dict[str, dict[str, Any]] = {}
    raw_json = clean_text(row.get("fichas_detectadas_json"))
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = []
        if isinstance(parsed, dict):
            parsed = parsed.get("fichas") or parsed.get("detecciones") or [parsed]
        if isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, str):
                    item = {"code": item}
                if not isinstance(item, Mapping):
                    continue
                code = _normalize_ficha(item.get("code") or item.get("ficha") or item.get("numero"))
                if not code:
                    continue
                score = parse_number(item.get("score"))
                record = {
                    "ficha": code,
                    "score": max(0.0, min(100.0, score)),
                    "method": clean_text(item.get("method") or item.get("metodo")) or "json",
                    "field": clean_text(item.get("field") or item.get("campo")),
                    "evidence": clean_text(item.get("evidence") or item.get("evidencia")),
                }
                previous = candidates.get(code)
                if previous is None or float(record["score"]) > float(previous["score"]):
                    candidates[code] = record

    legacy = clean_text(row.get("ficha_detectada"))
    for token in FICHA_TOKEN_RE.findall(legacy):
        code = token.lstrip("0") or "0"
        if code not in candidates:
            candidates[code] = {
                "ficha": code,
                "score": 70.0,
                "method": "columna_legacy",
                "field": "ficha_detectada",
                "evidence": legacy,
            }
    return sorted(candidates.values(), key=lambda item: (int(item["ficha"]), -float(item["score"])))


def provider_matches_winner(provider: object, winner_names: Sequence[object]) -> bool:
    provider_norm = normalize_text(provider)
    if not provider_norm:
        return False
    for raw_winner in winner_names:
        winner_norm = normalize_text(raw_winner)
        if not winner_norm:
            continue
        if provider_norm == winner_norm:
            return True
        shorter, longer = sorted((provider_norm, winner_norm), key=len)
        if len(shorter) >= 7 and re.search(rf"(?:^| ){re.escape(shorter)}(?:$| )", longer):
            return True
    return False


def extract_proponents(row: Mapping[str, Any], acto_key: str) -> list[dict[str, Any]]:
    winner_names = [row.get("razon_social"), row.get("nombre_comercial")]
    out: list[dict[str, Any]] = []
    source_id = clean_text(row.get("id"))
    for index in range(1, 15):
        provider = clean_text(row.get(f"Proponente {index}"))
        amount = parse_number(row.get(f"Precio Proponente {index}"))
        if not provider and amount == 0:
            continue
        out.append(
            {
                "acto_key": acto_key,
                "source_id": source_id,
                "ordinal": index,
                "proveedor": provider,
                "proveedor_norm": normalize_text(provider),
                "offered_amount": amount,
                "is_winner": int(provider_matches_winner(provider, winner_names)),
            }
        )
    return out


def resolve_award_amount(row: Mapping[str, Any], proponents: Sequence[Mapping[str, Any]]) -> tuple[float, str]:
    for prop in proponents:
        if int(prop.get("is_winner") or 0) and parse_number(prop.get("offered_amount")) > 0:
            return parse_number(prop.get("offered_amount")), "proponente_ganador"

    total_items = parse_number(row.get("total_items_ofertados"))
    if total_items > 0 and "adjud" in normalize_text(row.get("estado")):
        return total_items, "total_items_ofertados"

    valid_amounts = [parse_number(prop.get("offered_amount")) for prop in proponents]
    valid_amounts = [amount for amount in valid_amounts if amount > 0]
    if len(valid_amounts) == 1 and "adjud" in normalize_text(row.get("estado")):
        return valid_amounts[0], "unico_proponente"
    return 0.0, "sin_monto_confirmado"


def parse_item_details(raw_value: object) -> list[dict[str, Any]]:
    """Normaliza ``items_json`` legado y enriquecido sin inventar importes."""

    if isinstance(raw_value, list):
        payload = raw_value
    else:
        try:
            payload = json.loads(clean_text(raw_value) or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = []
    if not isinstance(payload, list):
        return []

    details: list[dict[str, Any]] = []
    for position, item in enumerate(payload, start=1):
        if isinstance(item, Mapping):
            description = clean_text(
                item.get("descripcion")
                or item.get("description")
                or item.get("detalle")
                or item.get("nombre")
            )
            line_number = clean_text(
                item.get("numero_renglon")
                or item.get("numRenglon")
                or item.get("renglon")
                or position
            )
            quantity = parse_number(
                item.get("cantidad")
                or item.get("quantity")
                or item.get("cantidadSolicitada")
            )
            reference_unit = parse_number(
                item.get("precio_referencia_unitario")
                or item.get("precioReferenciaUnitario")
                or item.get("precioUnitarioReferencia")
            )
            reference_total = parse_number(
                item.get("precio_referencia_total")
                or item.get("precioReferenciaTotal")
                or item.get("montoReferencia")
                or item.get("precioTotalReferencia")
                or item.get("precioReferencia")
            )
            if reference_total <= 0 and reference_unit > 0 and quantity > 0:
                reference_total = reference_unit * quantity
        else:
            description = clean_text(item)
            line_number = str(position)
            quantity = 0.0
            reference_unit = 0.0
            reference_total = 0.0
        if not description:
            continue
        details.append(
            {
                "position": position,
                "line_number": line_number,
                "description": description,
                "quantity": quantity,
                "reference_unit": reference_unit,
                "reference_total": reference_total,
            }
        )
    return details


def _all_detection_entries(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = clean_text(row.get("fichas_detectadas_json"))
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if isinstance(payload, Mapping):
        payload = payload.get("fichas") or payload.get("detecciones") or [payload]
    return [dict(item) for item in payload if isinstance(item, Mapping)] if isinstance(payload, list) else []


def _detected_item_positions(row: Mapping[str, Any], ficha: str) -> set[int]:
    positions: set[int] = set()
    for item in _all_detection_entries(row):
        code = _normalize_ficha(item.get("code") or item.get("ficha") or item.get("numero"))
        if code != ficha:
            continue
        field = clean_text(item.get("field") or item.get("campo"))
        match = re.search(r"(?:item|renglon|linea)[ _-]*(\d+)", field, re.IGNORECASE)
        if match:
            positions.add(int(match.group(1)))
    return positions


def _amount_attribution(
    row: Mapping[str, Any],
    *,
    ficha: str,
    distinct_fichas: int,
    reference_context: float,
    award_context: float,
    winner_names: Sequence[object] = (),
    confirmed_line_amounts: Mapping[tuple[str, str], object] | None = None,
) -> dict[str, Any]:
    """Atribuye dinero a una ficha sin repartir arbitrariamente el total del acto.

    Jerarquía:
    1. suma confirmada por el estudio de renglones;
    2. importes de renglones API donde el detector ubicó esa ficha;
    3. total del acto únicamente si consta un solo renglón y una sola ficha.
    Los actos mixtos o sin detalle conservan el monto global como contexto,
    pero aportan cero al monto financiero atribuible.
    """

    link = clean_text(row.get("enlace"))
    items = parse_item_details(row.get("items_json"))
    source_line_count = len(items)
    confirmed = (confirmed_line_amounts or {}).get((link, ficha))
    confirmed_reference = 0.0
    confirmed_lines = 0
    confirmed_awards: Mapping[str, object] = {}
    if isinstance(confirmed, Mapping):
        confirmed_reference = parse_number(confirmed.get("reference_amount"))
        confirmed_lines = parse_int(confirmed.get("line_count"))
        raw_awards = confirmed.get("award_by_provider")
        if isinstance(raw_awards, Mapping):
            confirmed_awards = raw_awards
    elif isinstance(confirmed, Sequence) and not isinstance(confirmed, (str, bytes)):
        # Compatibilidad con la primera versiÃ³n local: (monto_referencia, renglones).
        confirmed_reference = parse_number(confirmed[0]) if confirmed else 0.0
        confirmed_lines = parse_int(confirmed[1]) if len(confirmed) > 1 else 0

    if confirmed_reference > 0:
        reference_attributed = confirmed_reference
        attributed_line_count = max(1, confirmed_lines)
        reference_source = "estudio_renglon_confirmado"
    else:
        positions = _detected_item_positions(row, ficha)
        matched_items = [item for item in items if int(item["position"]) in positions]
        amount_items = [item for item in matched_items if parse_number(item["reference_total"]) > 0]
        if amount_items:
            reference_attributed = sum(parse_number(item["reference_total"]) for item in amount_items)
            attributed_line_count = len(amount_items)
            reference_source = "api_renglon_detectado"
        elif source_line_count == 1 and distinct_fichas == 1 and reference_context > 0:
            reference_attributed = reference_context
            attributed_line_count = 1
            reference_source = "acto_un_renglon_ficha_unica"
        else:
            reference_attributed = 0.0
            attributed_line_count = 0
            reference_source = (
                "sin_detalle_renglones"
                if source_line_count == 0
                else "acto_mixto_sin_monto_atribuible"
            )

    matched_awards = [
        parse_number(amount)
        for provider, amount in confirmed_awards.items()
        if parse_number(amount) > 0 and provider_matches_winner(provider, winner_names)
    ]
    if matched_awards:
        # Si el mismo ganador aparece con variantes de nombre, no se suman
        # entre sÃ­: se conserva el total confirmado mÃ¡s completo.
        award_attributed = max(matched_awards)
        award_source = "estudio_renglon_ganador_confirmado"
    elif source_line_count == 1 and distinct_fichas == 1 and award_context > 0:
        award_attributed = award_context
        award_source = "acto_un_renglon_ficha_unica"
    else:
        award_attributed = 0.0
        award_source = (
            "sin_detalle_renglones"
            if source_line_count == 0
            else "sin_adjudicacion_por_renglon_confirmada"
        )

    return {
        "source_line_count": source_line_count,
        "attributed_line_count": attributed_line_count,
        "reference_amount_context": round(reference_context, 6),
        "reference_amount_attributed": round(reference_attributed, 6),
        "reference_amount_attribution_source": reference_source,
        "reference_amount_reliable": int(reference_attributed > 0),
        "award_amount_context": round(award_context, 6),
        "award_amount_attributed": round(award_attributed, 6),
        "award_amount_attribution_source": award_source,
        "award_amount_reliable": int(award_attributed > 0),
    }


def source_rows(connection: sqlite3.Connection, chunk_size: int = SOURCE_CHUNK_SIZE) -> Iterator[pd.DataFrame]:
    columns = {row[1] for row in connection.execute("PRAGMA table_info(actos_publicos)").fetchall()}
    required = {
        "id", "enlace", "titulo", "entidad", "unidad_solic", "estado", "publicacion", "fecha",
        "fecha_adjudicacion", "fecha_actualizacion", "precio_referencia", "total_items_ofertados",
        "num_participantes", "razon_social", "nombre_comercial", "ficha_detectada",
        "fichas_detectadas_json", "ficha_detector_version", "ficha_catalogo_version", "items_json",
    }
    for index in range(1, 15):
        required.add(f"Proponente {index}")
        required.add(f"Precio Proponente {index}")
    selected = [column for column in required if column in columns]
    if "id" not in selected:
        raise RuntimeError("La tabla actos_publicos no contiene la columna id.")
    quoted = ", ".join('"' + column.replace('"', '""') + '"' for column in selected)
    yield from pd.read_sql_query(f"SELECT {quoted} FROM actos_publicos", connection, chunksize=chunk_size)


def row_to_records(
    row: Mapping[str, Any],
    *,
    confirmed_line_amounts: Mapping[tuple[str, str], object] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    evidences = extract_ficha_evidence(row)
    if not evidences:
        return [], []
    acto_key = stable_act_key(row)
    proponents = extract_proponents(row, acto_key)
    award_amount, award_source = resolve_award_amount(row, proponents)
    celebration_dates = parse_date_tokens(row.get("fecha"))
    distinct_count = len(evidences)
    reference_context = parse_number(row.get("precio_referencia"))
    base = {
        "acto_key": acto_key,
        "source_id": clean_text(row.get("id")),
        "detected_ficha_count": distinct_count,
        "is_unique_ficha": int(distinct_count == 1),
        "detector_version": clean_text(row.get("ficha_detector_version")),
        "catalog_version": clean_text(row.get("ficha_catalogo_version")),
        "enlace": clean_text(row.get("enlace")),
        "titulo": clean_text(row.get("titulo")),
        "entidad": clean_text(row.get("entidad")),
        "unidad_solicitante": clean_text(row.get("unidad_solic")),
        "estado": clean_text(row.get("estado")),
        "publication_date": parse_date(row.get("publicacion")),
        "celebration_date": celebration_dates[0] if celebration_dates else "",
        "celebration_end_date": celebration_dates[-1] if celebration_dates else "",
        "award_date": parse_date(row.get("fecha_adjudicacion")),
        "update_date": parse_date(row.get("fecha_actualizacion")),
        # Columnas legadas: se mantienen como totales globales del acto para
        # compatibilidad y auditoría. Las métricas financieras usan *_attributed.
        "reference_amount": reference_context,
        "award_amount": award_amount,
        "award_amount_source": award_source,
        "winner": clean_text(row.get("razon_social")),
        "winner_short": clean_text(row.get("nombre_comercial")),
        "participant_count": parse_int(row.get("num_participantes")) or len([p for p in proponents if p["proveedor"]]),
        # Campo desacentuado para bÃºsquedas rÃ¡pidas y consistentes en SQLite y PostgreSQL.
        "search_text_norm": normalize_text(
            " ".join(
                [
                    clean_text(row.get("titulo")),
                    clean_text(row.get("entidad")),
                    clean_text(row.get("unidad_solic")),
                    clean_text(row.get("estado")),
                ]
            )
        ),
    }
    facts: list[dict[str, Any]] = []
    for evidence in evidences:
        fact = dict(base)
        attribution = _amount_attribution(
            row,
            ficha=evidence["ficha"],
            distinct_fichas=distinct_count,
            reference_context=reference_context,
            award_context=award_amount,
            winner_names=(row.get("razon_social"), row.get("nombre_comercial")),
            confirmed_line_amounts=confirmed_line_amounts,
        )
        fact.update(attribution)
        fact.update(
            {
                "ficha": evidence["ficha"],
                "detection_score": float(evidence["score"]),
                "detection_method": evidence["method"],
                "detection_field": evidence["field"],
                "detection_evidence": evidence["evidence"],
            }
        )
        facts.append(fact)
    return facts, proponents


def _find_column(frame: pd.DataFrame, aliases: Sequence[str]) -> str:
    normalized = {normalize_header(column): str(column) for column in frame.columns}
    for alias in aliases:
        found = normalized.get(normalize_header(alias))
        if found:
            return found
    return ""


def _yes_no(value: object) -> str:
    normalized = normalize_text(value)
    if normalized in {"si", "s", "yes", "true", "1", "x"}:
        return "Si"
    if normalized in {"no", "n", "false", "0"}:
        return "No"
    if "si" in normalized.split():
        return "Si"
    return "No" if normalized else ""


def _load_aliases(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    result: dict[str, list[str]] = defaultdict(list)
    if isinstance(raw, Mapping):
        iterable = raw.items()
    elif isinstance(raw, list):
        iterable = []
        for item in raw:
            if isinstance(item, Mapping):
                iterable.append((item.get("ficha") or item.get("code"), item))
    else:
        iterable = []
    for raw_code, value in iterable:
        code = _normalize_ficha(raw_code)
        if not code:
            continue
        aliases: list[object] = []
        if isinstance(value, str):
            aliases = [value]
        elif isinstance(value, list):
            aliases = value
        elif isinstance(value, Mapping):
            for key in ("aliases", "nombres", "alias", "nombre", "nombre_generico"):
                candidate = value.get(key)
                if isinstance(candidate, list):
                    aliases.extend(candidate)
                elif candidate:
                    aliases.append(candidate)
        for alias in aliases:
            text = clean_text(alias)
            if text and text not in result[code]:
                result[code].append(text)
    return dict(result)


def _normalize_risk_class(value: object) -> str:
    normalized = clean_text(value).upper()
    return normalized if normalized in {"A", "B", "C"} else ""


def _load_risk_classes(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    raw = pd.read_excel(path, dtype=object)
    if raw.empty:
        return {}
    ficha_column = _find_column(raw, ["Número Ficha", "Numero Ficha", "ficha", "numero_ficha"])
    class_column = _find_column(raw, ["Clase de Riesgo", "Clase Riesgo", "clase_riesgo"])
    if not ficha_column or not class_column:
        return {}
    result: dict[str, str] = {}
    for _, row in raw.iterrows():
        code = _normalize_ficha(row.get(ficha_column))
        risk_class = _normalize_risk_class(row.get(class_column))
        if code and risk_class:
            result[code] = risk_class
    return result


def _load_primary_metadata(
    path: Path,
    aliases_path: Path = DEFAULT_ALIAS_JSON,
    risk_class_path: Path = DEFAULT_RISK_CLASS_XLSX,
) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=METADATA_COLUMNS)
    raw = pd.read_excel(path, dtype=object)
    if raw.empty:
        return pd.DataFrame(columns=METADATA_COLUMNS)
    columns = {
        "ficha": _find_column(raw, ["Número Ficha", "Numero Ficha", "ficha", "numero_ficha"]),
        "nombre": _find_column(raw, ["Nombre Genérico", "Nombre Generico", "nombre_ficha"]),
        "descripcion": _find_column(raw, ["Descripción", "Descripcion"]),
        "area": _find_column(raw, ["Área", "Area"]),
        "tipo": _find_column(raw, ["Tipo Producto", "Tipo de Producto"]),
        "especialidad": _find_column(raw, ["Especialidad"]),
        "clase": _find_column(raw, ["Clase de Riesgo", "Clase Riesgo", "clase_riesgo"]),
        "ct": _find_column(raw, ["Criterio", "Tiene criterio tecnico", "Tiene CT"]),
        "rs": _find_column(raw, ["Registro Sanitario", "Tiene registro sanitario"]),
        "url": _find_column(raw, ["enlace_ficha_tecnica", "Enlace ficha MINSA", "Enlace"]),
    }
    if not columns["ficha"]:
        return pd.DataFrame(columns=METADATA_COLUMNS)
    aliases = _load_aliases(aliases_path)
    risk_classes = _load_risk_classes(risk_class_path)
    records: dict[str, dict[str, Any]] = {}
    for _, row in raw.iterrows():
        code = _normalize_ficha(row.get(columns["ficha"]))
        if not code:
            continue
        name = clean_text(row.get(columns["nombre"])) if columns["nombre"] else ""
        description = clean_text(row.get(columns["descripcion"])) if columns["descripcion"] else ""
        alias_candidates = aliases.get(code, [])
        if not name or name.endswith("..."):
            complete = [item for item in alias_candidates if not item.endswith("...")]
            if complete:
                name = max(complete, key=len)
        record = {
            "ficha": code,
            "nombre_ficha": name,
            "descripcion": description,
            "area": clean_text(row.get(columns["area"])) if columns["area"] else "",
            "tipo_producto": clean_text(row.get(columns["tipo"])) if columns["tipo"] else "",
            "especialidad": clean_text(row.get(columns["especialidad"])) if columns["especialidad"] else "",
            "clase_riesgo": (
                _normalize_risk_class(row.get(columns["clase"])) or risk_classes.get(code, "")
                if columns["clase"]
                else risk_classes.get(code, "")
            ),
            "tiene_ct": _yes_no(row.get(columns["ct"])) if columns["ct"] else "",
            "registro_sanitario": _yes_no(row.get(columns["rs"])) if columns["rs"] else "",
            "enlace_minsa": clean_text(row.get(columns["url"])) if columns["url"] else "",
            "metadata_source": path.name,
            "search_text_norm": normalize_text(
                " ".join(
                    [
                        code,
                        name,
                        description,
                        clean_text(row.get(columns["area"])) if columns["area"] else "",
                        clean_text(row.get(columns["tipo"])) if columns["tipo"] else "",
                        clean_text(row.get(columns["especialidad"])) if columns["especialidad"] else "",
                    ]
                )
            ),
        }
        previous = records.get(code)
        if previous is None or sum(bool(v) for v in record.values()) > sum(bool(v) for v in previous.values()):
            records[code] = record
    return pd.DataFrame(records.values(), columns=METADATA_COLUMNS)


def _classification_flag(value: object, *, registro_sanitario: bool = False) -> str:
    """Normaliza las clasificaciones usadas por los scrapers.

    En ``todas_las_fichas.xlsx`` el registro sanitario puede venir como
    ``SI RS LCRSP``, ``SI RS DNFD`` u otra variante. Todas significan que la
    ficha requiere registro; únicamente ``NO`` significa que no lo requiere.
    """
    normalized = normalize_text(value)
    if not normalized:
        return ""
    if registro_sanitario:
        if normalized == "no" or normalized.startswith("no "):
            return "No"
        if normalized == "si" or normalized.startswith("si "):
            return "Si"
    return _yes_no(value)


def load_classification_metadata(
    path: Path,
    aliases_path: Path = DEFAULT_ALIAS_JSON,
) -> pd.DataFrame:
    """Carga el inventario CT/RS utilizado por clv, clrir y rir1.

    Este archivo histórico no tiene encabezados: ficha, criterio técnico y
    registro sanitario. Sirve como cobertura complementaria cuando el catálogo
    enriquecido de MINSA no contiene una ficha que sí fue detectada en actos.
    """
    if not path.exists():
        return pd.DataFrame(columns=METADATA_COLUMNS)
    raw = pd.read_excel(path, header=None, dtype=object)
    if raw.empty or raw.shape[1] < 3:
        return pd.DataFrame(columns=METADATA_COLUMNS)

    aliases = _load_aliases(aliases_path)
    records: dict[str, dict[str, Any]] = {}
    for _, row in raw.iterrows():
        code = _normalize_ficha(row.iloc[0])
        if not code:
            continue
        alias_candidates = [item for item in aliases.get(code, []) if not item.endswith("...")]
        name = max(alias_candidates, key=len) if alias_candidates else ""
        record = {
            "ficha": code,
            "nombre_ficha": name,
            "descripcion": "",
            "area": "",
            "tipo_producto": "",
            "especialidad": "",
            "clase_riesgo": "",
            "tiene_ct": _classification_flag(row.iloc[1]),
            "registro_sanitario": _classification_flag(row.iloc[2], registro_sanitario=True),
            "enlace_minsa": "",
            "metadata_source": path.name,
            "search_text_norm": normalize_text(f"{code} {name}"),
        }
        previous = records.get(code)
        if previous is None or sum(bool(v) for v in record.values()) > sum(bool(v) for v in previous.values()):
            records[code] = record
    return pd.DataFrame(records.values(), columns=METADATA_COLUMNS)


def _metadata_search_text(record: Mapping[str, Any]) -> str:
    return normalize_text(
        " ".join(
            clean_text(record.get(field))
            for field in (
                "ficha",
                "nombre_ficha",
                "descripcion",
                "area",
                "tipo_producto",
                "especialidad",
                "clase_riesgo",
            )
        )
    )


def load_metadata(
    path: Path,
    aliases_path: Path = DEFAULT_ALIAS_JSON,
    classification_path: Path = DEFAULT_CLASSIFICATION_XLSX,
    risk_class_path: Path = DEFAULT_RISK_CLASS_XLSX,
) -> pd.DataFrame:
    """Combina metadata enriquecida con la clasificación oficial de scrapers.

    El catálogo enriquecido conserva nombres, descripciones y enlaces. La
    clasificación CT/RS de los scrapers completa fichas ausentes y es la fuente
    autoritativa para esos dos indicadores, evitando excluir fichas válidas por
    falta de metadata (por ejemplo, 100523).
    """
    primary = _load_primary_metadata(path, aliases_path, risk_class_path)
    classification = load_classification_metadata(classification_path, aliases_path)
    records = {clean_text(row["ficha"]): row.to_dict() for _, row in primary.iterrows()}
    for _, fallback_row in classification.iterrows():
        fallback = fallback_row.to_dict()
        code = clean_text(fallback.get("ficha"))
        if not code:
            continue
        current = records.get(code)
        if current is None:
            records[code] = fallback
            continue

        used_fallback = False
        for field in ("tiene_ct", "registro_sanitario"):
            value = clean_text(fallback.get(field))
            if value and clean_text(current.get(field)) != value:
                current[field] = value
                used_fallback = True
        if not clean_text(current.get("nombre_ficha")) and clean_text(fallback.get("nombre_ficha")):
            current["nombre_ficha"] = fallback["nombre_ficha"]
            used_fallback = True
        if used_fallback:
            sources = [clean_text(current.get("metadata_source")), clean_text(fallback.get("metadata_source"))]
            current["metadata_source"] = " + ".join(dict.fromkeys(item for item in sources if item))
            current["search_text_norm"] = _metadata_search_text(current)

    risk_classes = _load_risk_classes(risk_class_path)
    for code, risk_class in risk_classes.items():
        current = records.get(code)
        if current is None:
            current = {column: "" for column in METADATA_COLUMNS}
            current["ficha"] = code
            current["metadata_source"] = risk_class_path.name
            records[code] = current
        if not clean_text(current.get("clase_riesgo")):
            current["clase_riesgo"] = risk_class
            sources = [clean_text(current.get("metadata_source")), risk_class_path.name]
            current["metadata_source"] = " + ".join(dict.fromkeys(item for item in sources if item))
        current["search_text_norm"] = _metadata_search_text(current)

    return pd.DataFrame(records.values(), columns=METADATA_COLUMNS)


def load_catalog(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=CATALOG_COLUMNS)
    raw = pd.read_excel(path, dtype=object)
    if raw.empty:
        return pd.DataFrame(columns=CATALOG_COLUMNS)
    aliases = {
        "ficha": ["N° Ficha CTNI", "No Ficha CTNI", "Ficha CTNI", "ficha"],
        "oferente": ["Oferente", "Proveedor"],
        "contacto": ["Contacto"],
        "telefono": ["Teléfono", "Telefono"],
        "correo": ["Correo", "Email"],
        "catalogo": ["N° Catálogo", "No Catalogo", "Catalogo"],
        "producto": ["Nombre del Producto", "Producto"],
        "fabricante": ["Casa Productora / Laboratorio", "Fabricante", "Laboratorio"],
        "marca": ["Marca"],
        "modelo_web": ["Modelo / Sitio Web", "Modelo", "Sitio Web"],
        "estado_catalogo": ["Estado"],
    }
    cols = {key: _find_column(raw, values) for key, values in aliases.items()}
    if not cols["ficha"]:
        return pd.DataFrame(columns=CATALOG_COLUMNS)
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for _, row in raw.iterrows():
        codes = []
        for token in FICHA_TOKEN_RE.findall(clean_text(row.get(cols["ficha"]))):
            code = token.lstrip("0") or "0"
            if code not in codes:
                codes.append(code)
        for code in codes:
            record = {"ficha": code}
            for key in CATALOG_COLUMNS[1:]:
                column = cols.get(key, "")
                record[key] = clean_text(row.get(column)) if column else ""
            identity = tuple(str(record.get(column, "")) for column in CATALOG_COLUMNS)
            if identity in seen:
                continue
            seen.add(identity)
            records.append(record)
    return pd.DataFrame(records, columns=CATALOG_COLUMNS)


def _create_local_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;

        DROP TABLE IF EXISTS intel_actos_fichas;
        DROP TABLE IF EXISTS intel_acto_proponentes;
        DROP TABLE IF EXISTS intel_metricas_ficha_mes;
        DROP TABLE IF EXISTS intel_ficha_metadata;
        DROP TABLE IF EXISTS intel_ficha_catalogo;
        DROP TABLE IF EXISTS intel_build_metadata;

        CREATE TABLE intel_actos_fichas (
            acto_key TEXT NOT NULL,
            source_id TEXT,
            ficha TEXT NOT NULL,
            is_unique_ficha INTEGER NOT NULL DEFAULT 0,
            detected_ficha_count INTEGER NOT NULL DEFAULT 0,
            detection_score REAL NOT NULL DEFAULT 0,
            detection_method TEXT,
            detection_field TEXT,
            detection_evidence TEXT,
            detector_version TEXT,
            catalog_version TEXT,
            enlace TEXT,
            titulo TEXT,
            entidad TEXT,
            unidad_solicitante TEXT,
            estado TEXT,
            publication_date TEXT,
            celebration_date TEXT,
            celebration_end_date TEXT,
            award_date TEXT,
            update_date TEXT,
            source_line_count INTEGER NOT NULL DEFAULT 0,
            attributed_line_count INTEGER NOT NULL DEFAULT 0,
            reference_amount REAL NOT NULL DEFAULT 0,
            reference_amount_context REAL NOT NULL DEFAULT 0,
            reference_amount_attributed REAL NOT NULL DEFAULT 0,
            reference_amount_attribution_source TEXT,
            reference_amount_reliable INTEGER NOT NULL DEFAULT 0,
            award_amount REAL NOT NULL DEFAULT 0,
            award_amount_context REAL NOT NULL DEFAULT 0,
            award_amount_attributed REAL NOT NULL DEFAULT 0,
            award_amount_attribution_source TEXT,
            award_amount_reliable INTEGER NOT NULL DEFAULT 0,
            award_amount_source TEXT,
            winner TEXT,
            winner_short TEXT,
            participant_count INTEGER NOT NULL DEFAULT 0,
            search_text_norm TEXT,
            PRIMARY KEY (acto_key, ficha)
        );

        CREATE TABLE intel_acto_proponentes (
            acto_key TEXT NOT NULL,
            source_id TEXT,
            ordinal INTEGER NOT NULL,
            proveedor TEXT,
            proveedor_norm TEXT,
            offered_amount REAL NOT NULL DEFAULT 0,
            is_winner INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (acto_key, ordinal)
        );

        CREATE TABLE intel_metricas_ficha_mes (
            date_basis TEXT NOT NULL,
            period_month TEXT NOT NULL,
            detection_profile TEXT NOT NULL,
            ficha TEXT NOT NULL,
            actos INTEGER NOT NULL DEFAULT 0,
            actos_ficha_unica INTEGER NOT NULL DEFAULT 0,
            entidades INTEGER NOT NULL DEFAULT 0,
            monto_referencia REAL NOT NULL DEFAULT 0,
            monto_adjudicado REAL NOT NULL DEFAULT 0,
            monto_referencia_contexto REAL NOT NULL DEFAULT 0,
            monto_adjudicado_contexto REAL NOT NULL DEFAULT 0,
            actos_monto_referencia INTEGER NOT NULL DEFAULT 0,
            actos_monto_adjudicado INTEGER NOT NULL DEFAULT 0,
            actos_con_ganador INTEGER NOT NULL DEFAULT 0,
            participantes_promedio REAL NOT NULL DEFAULT 0,
            confianza_deteccion REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (date_basis, period_month, detection_profile, ficha)
        );

        CREATE TABLE intel_ficha_metadata (
            ficha TEXT PRIMARY KEY,
            nombre_ficha TEXT,
            descripcion TEXT,
            area TEXT,
            tipo_producto TEXT,
            especialidad TEXT,
            clase_riesgo TEXT,
            tiene_ct TEXT,
            registro_sanitario TEXT,
            enlace_minsa TEXT,
            metadata_source TEXT,
            search_text_norm TEXT
        );

        CREATE TABLE intel_ficha_catalogo (
            ficha TEXT NOT NULL,
            oferente TEXT,
            contacto TEXT,
            telefono TEXT,
            correo TEXT,
            catalogo TEXT,
            producto TEXT,
            fabricante TEXT,
            marca TEXT,
            modelo_web TEXT,
            estado_catalogo TEXT
        );

        CREATE TABLE intel_build_metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )


def _create_local_indexes(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_iaf_ficha ON intel_actos_fichas(ficha);
        CREATE INDEX IF NOT EXISTS idx_iaf_publication ON intel_actos_fichas(publication_date);
        CREATE INDEX IF NOT EXISTS idx_iaf_celebration ON intel_actos_fichas(celebration_date);
        CREATE INDEX IF NOT EXISTS idx_iaf_award ON intel_actos_fichas(award_date);
        CREATE INDEX IF NOT EXISTS idx_iaf_update ON intel_actos_fichas(update_date);
        CREATE INDEX IF NOT EXISTS idx_iaf_score ON intel_actos_fichas(detection_score);
        CREATE INDEX IF NOT EXISTS idx_iaf_estado ON intel_actos_fichas(estado);
        CREATE INDEX IF NOT EXISTS idx_iaf_entidad ON intel_actos_fichas(entidad);
        CREATE INDEX IF NOT EXISTS idx_iaf_search ON intel_actos_fichas(search_text_norm);
        CREATE INDEX IF NOT EXISTS idx_iap_acto ON intel_acto_proponentes(acto_key);
        CREATE INDEX IF NOT EXISTS idx_iap_provider ON intel_acto_proponentes(proveedor_norm);
        CREATE INDEX IF NOT EXISTS idx_iap_winner ON intel_acto_proponentes(is_winner);
        CREATE INDEX IF NOT EXISTS idx_ifm_month_profile ON intel_metricas_ficha_mes(period_month, detection_profile);
        CREATE INDEX IF NOT EXISTS idx_ifm_ficha ON intel_metricas_ficha_mes(ficha);
        CREATE INDEX IF NOT EXISTS idx_ifc_ficha ON intel_ficha_catalogo(ficha);
        CREATE INDEX IF NOT EXISTS idx_ifc_oferente ON intel_ficha_catalogo(oferente);
        CREATE INDEX IF NOT EXISTS idx_ifm_search ON intel_ficha_metadata(search_text_norm);
        ANALYZE;
        """
    )


def load_confirmed_line_amounts(
    source: sqlite3.Connection,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Carga montos referenciales confirmados por estudios de renglón.

    La tabla es opcional: una base que todavía no haya ejecutado estudios
    profundos sigue construyéndose con la regla conservadora de un solo renglón.
    """

    table_exists = source.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='intel_ficha_line_amounts'"
    ).fetchone()
    if not table_exists:
        return {}
    rows = source.execute(
        """
        SELECT ficha, acto_url,
               COALESCE(NULLIF(renglon_id, ''), NULLIF(renglon_numero, ''), line_key) AS line_identity,
               MAX(COALESCE(reference_total, 0)) AS reference_total,
               COALESCE(provider, '') AS provider,
               MAX(COALESCE(participation_total, 0)) AS participation_total
        FROM intel_ficha_line_amounts
        WHERE COALESCE(requires_review, 1) = 0
          AND COALESCE(ficha, '') <> ''
          AND COALESCE(acto_url, '') <> ''
        GROUP BY ficha, acto_url,
                 COALESCE(NULLIF(renglon_id, ''), NULLIF(renglon_numero, ''), line_key),
                 COALESCE(provider, '')
        """
    ).fetchall()
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for ficha, acto_url, line_identity, reference_total, provider, participation_total in rows:
        key = (clean_text(acto_url), _normalize_ficha(ficha))
        line_key = clean_text(line_identity)
        if not key[0] or not key[1] or not line_key:
            continue
        record = grouped.setdefault(
            key,
            {
                "reference_by_line": {},
                "award_lines_by_provider": defaultdict(dict),
            },
        )
        record["reference_by_line"][line_key] = max(
            parse_number(record["reference_by_line"].get(line_key)),
            parse_number(reference_total),
        )
        provider_name = clean_text(provider)
        if provider_name and parse_number(participation_total) > 0:
            provider_lines = record["award_lines_by_provider"][provider_name]
            provider_lines[line_key] = max(
                parse_number(provider_lines.get(line_key)),
                parse_number(participation_total),
            )

    output: dict[tuple[str, str], dict[str, Any]] = {}
    for key, record in grouped.items():
        reference_by_line = record["reference_by_line"]
        award_lines_by_provider = record["award_lines_by_provider"]
        output[key] = {
            "reference_amount": sum(parse_number(value) for value in reference_by_line.values()),
            "line_count": len(reference_by_line),
            "award_by_provider": {
                provider: sum(parse_number(value) for value in line_values.values())
                for provider, line_values in award_lines_by_provider.items()
            },
        }
    return output


def build_local_analytics(
    source_db: Path,
    output_db: Path,
    metadata_xlsx: Path,
    catalog_xlsx: Path,
    aliases_json: Path,
    *,
    classification_xlsx: Path = DEFAULT_CLASSIFICATION_XLSX,
    risk_class_xlsx: Path = DEFAULT_RISK_CLASS_XLSX,
    limit: int = 0,
) -> dict[str, Any]:
    started = time.perf_counter()
    if not source_db.exists():
        raise FileNotFoundError(f"No existe la base fuente: {source_db}")
    output_db.parent.mkdir(parents=True, exist_ok=True)
    # Un nombre por proceso evita que un cierre forzado deje bloqueada la siguiente corrida.
    temp_db = output_db.with_name(output_db.name + f".building.{os.getpid()}")
    if temp_db.exists():
        temp_db.unlink()
    source = sqlite3.connect(f"file:{source_db.as_posix()}?mode=ro", uri=True, timeout=120)
    target = sqlite3.connect(temp_db, timeout=120)
    _create_local_schema(target)
    _log("START", f"fuente={source_db} salida={output_db}", started)

    source_count = int(source.execute("SELECT COUNT(*) FROM actos_publicos").fetchone()[0])
    confirmed_line_amounts = load_confirmed_line_amounts(source)
    if confirmed_line_amounts:
        _log(
            "LINES",
            f"{len(confirmed_line_amounts):,} relaciones ficha-acto con monto por renglón confirmado",
            started,
        )
    processed = 0
    fact_count = 0
    proponent_count = 0
    try:
        for chunk in source_rows(source):
            if limit and processed >= limit:
                break
            if limit and processed + len(chunk) > limit:
                chunk = chunk.head(limit - processed)
            facts: list[dict[str, Any]] = []
            proponents: list[dict[str, Any]] = []
            for row in chunk.to_dict(orient="records"):
                row_facts, row_proponents = row_to_records(
                    row,
                    confirmed_line_amounts=confirmed_line_amounts,
                )
                facts.extend(row_facts)
                proponents.extend(row_proponents)
            if facts:
                pd.DataFrame(facts, columns=FACT_COLUMNS).to_sql(
                    "intel_actos_fichas",
                    target,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=_sqlite_multi_chunksize(len(FACT_COLUMNS)),
                )
            if proponents:
                pd.DataFrame(proponents, columns=PROPONENT_COLUMNS).drop_duplicates(
                    subset=["acto_key", "ordinal"], keep="last"
                ).to_sql(
                    "intel_acto_proponentes",
                    target,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=_sqlite_multi_chunksize(len(PROPONENT_COLUMNS)),
                )
            processed += len(chunk)
            fact_count += len(facts)
            proponent_count += len(proponents)
            if processed % 25_000 < len(chunk):
                _log("BUILD", f"{processed:,}/{min(source_count, limit or source_count):,} actos", started)

        # Materializa tendencias mensuales para los cuatro ejes temporales y
        # los tres perfiles. La tabla maestra sigue consultando hechos cuando
        # hay filtros arbitrarios; las tendencias comunes quedan listas sin
        # volver a recorrer toda la base operacional.
        target.executescript(
            """
            WITH dated AS (
                SELECT ficha, acto_key, is_unique_ficha, entidad,
                       reference_amount_attributed, award_amount_attributed,
                       reference_amount_context, award_amount_context,
                       reference_amount_reliable, award_amount_reliable,
                       winner, participant_count, detection_score, 'publicacion' date_basis,
                       substr(publication_date, 1, 7) period_month
                FROM intel_actos_fichas WHERE publication_date IS NOT NULL AND length(publication_date) >= 7
                UNION ALL
                SELECT ficha, acto_key, is_unique_ficha, entidad,
                       reference_amount_attributed, award_amount_attributed,
                       reference_amount_context, award_amount_context,
                       reference_amount_reliable, award_amount_reliable,
                       winner, participant_count, detection_score, 'celebracion', substr(celebration_date, 1, 7)
                FROM intel_actos_fichas WHERE celebration_date IS NOT NULL AND length(celebration_date) >= 7
                UNION ALL
                SELECT ficha, acto_key, is_unique_ficha, entidad,
                       reference_amount_attributed, award_amount_attributed,
                       reference_amount_context, award_amount_context,
                       reference_amount_reliable, award_amount_reliable,
                       winner, participant_count, detection_score, 'adjudicacion', substr(award_date, 1, 7)
                FROM intel_actos_fichas WHERE award_date IS NOT NULL AND length(award_date) >= 7
                UNION ALL
                SELECT ficha, acto_key, is_unique_ficha, entidad,
                       reference_amount_attributed, award_amount_attributed,
                       reference_amount_context, award_amount_context,
                       reference_amount_reliable, award_amount_reliable,
                       winner, participant_count, detection_score, 'actualizacion', substr(update_date, 1, 7)
                FROM intel_actos_fichas WHERE update_date IS NOT NULL AND length(update_date) >= 7
            ), profiles(detection_profile, threshold) AS (
                VALUES ('flexible', 55.0), ('moderado', 70.0), ('estricto', 92.0)
            )
            INSERT INTO intel_metricas_ficha_mes (
                date_basis, period_month, detection_profile, ficha, actos, actos_ficha_unica,
                entidades, monto_referencia, monto_adjudicado,
                monto_referencia_contexto, monto_adjudicado_contexto,
                actos_monto_referencia, actos_monto_adjudicado, actos_con_ganador,
                participantes_promedio, confianza_deteccion
            )
            SELECT d.date_basis, d.period_month, p.detection_profile, d.ficha,
                   COUNT(DISTINCT d.acto_key),
                   COUNT(DISTINCT CASE WHEN d.is_unique_ficha = 1 THEN d.acto_key END),
                   COUNT(DISTINCT NULLIF(trim(d.entidad), '')),
                   SUM(d.reference_amount_attributed), SUM(d.award_amount_attributed),
                   SUM(d.reference_amount_context), SUM(d.award_amount_context),
                   COUNT(DISTINCT CASE WHEN d.reference_amount_reliable = 1 THEN d.acto_key END),
                   COUNT(DISTINCT CASE WHEN d.award_amount_reliable = 1 THEN d.acto_key END),
                   COUNT(DISTINCT CASE WHEN trim(COALESCE(d.winner, '')) <> '' THEN d.acto_key END),
                   AVG(d.participant_count), AVG(d.detection_score)
            FROM dated d
            CROSS JOIN profiles p
            WHERE d.detection_score >= p.threshold
              AND d.period_month GLOB '[12][0-9][0-9][0-9]-[01][0-9]'
            GROUP BY d.date_basis, d.period_month, p.detection_profile, d.ficha;
            """
        )
        monthly_count = int(target.execute("SELECT COUNT(*) FROM intel_metricas_ficha_mes").fetchone()[0])

        metadata = load_metadata(
            metadata_xlsx,
            aliases_json,
            classification_xlsx,
            risk_class_xlsx,
        )
        catalog = load_catalog(catalog_xlsx)
        if not metadata.empty:
            metadata.to_sql(
                "intel_ficha_metadata",
                target,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=_sqlite_multi_chunksize(len(METADATA_COLUMNS)),
            )
        if not catalog.empty:
            catalog.to_sql(
                "intel_ficha_catalogo",
                target,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=_sqlite_multi_chunksize(len(CATALOG_COLUMNS)),
            )

        build_values = {
            "schema_version": ANALYTICS_SCHEMA_VERSION,
            "built_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_db": str(source_db.resolve()),
            "source_rows": str(processed),
            "fact_rows": str(fact_count),
            "proponent_rows": str(proponent_count),
            "monthly_rows": str(monthly_count),
            "metadata_rows": str(len(metadata)),
            "classification_xlsx": str(classification_xlsx.resolve()) if classification_xlsx.exists() else "",
            "catalog_rows": str(len(catalog)),
            "confirmed_line_amount_relations": str(len(confirmed_line_amounts)),
        }
        target.executemany(
            "INSERT OR REPLACE INTO intel_build_metadata(key, value) VALUES (?, ?)", build_values.items()
        )
        _create_local_indexes(target)
        target.commit()
        integrity = target.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"La base analitica no supero integrity_check: {integrity}")
        # Consolida cualquier WAL antes del reemplazo atomico del archivo principal.
        target.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        target.execute("PRAGMA journal_mode=DELETE")
    finally:
        source.close()
        target.close()

    os.replace(temp_db, output_db)
    _log(
        "DONE",
        f"actos={processed:,} hechos={fact_count:,} proponentes={proponent_count:,} "
        f"metadata={len(metadata):,} catalogo={len(catalog):,}",
        started,
    )
    return {
        "source_rows": processed,
        "fact_rows": fact_count,
        "proponent_rows": proponent_count,
        "monthly_rows": monthly_count,
        "metadata_rows": len(metadata),
        "catalog_rows": len(catalog),
        "output_db": str(output_db),
    }


def _postgres_url(explicit: str = "") -> str:
    return clean_text(explicit or os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"))


def publish_postgres(local_db: Path, database_url: str, *, batch_size: int = WRITE_CHUNK_SIZE) -> dict[str, int]:
    """Publica con tablas staging y un swap transaccional para no dejar datos parciales."""
    if not database_url:
        raise RuntimeError("No se definio SUPABASE_DB_URL/DATABASE_URL.")
    from sqlalchemy import create_engine, text

    engine = create_engine(database_url, pool_pre_ping=True, pool_recycle=240, connect_args={"connect_timeout": 20})
    source = sqlite3.connect(f"file:{local_db.as_posix()}?mode=ro", uri=True)
    table_names = [
        "intel_actos_fichas",
        "intel_acto_proponentes",
        "intel_metricas_ficha_mes",
        "intel_ficha_metadata",
        "intel_ficha_catalogo",
        "intel_build_metadata",
    ]
    uploaded: dict[str, int] = {}
    try:
        with engine.begin() as conn:
            for table in table_names:
                conn.execute(text(f'DROP TABLE IF EXISTS "{table}__new"'))
        for table in table_names:
            first = True
            count = 0
            for frame in pd.read_sql_query(f'SELECT * FROM "{table}"', source, chunksize=batch_size):
                frame.to_sql(
                    f"{table}__new",
                    engine,
                    if_exists="replace" if first else "append",
                    index=False,
                    method="multi",
                    chunksize=1_000,
                )
                first = False
                count += len(frame)
            if first:
                empty = pd.read_sql_query(f'SELECT * FROM "{table}" LIMIT 0', source)
                empty.to_sql(f"{table}__new", engine, if_exists="replace", index=False)
            uploaded[table] = count

        with engine.begin() as conn:
            for table in table_names:
                conn.execute(text(f'DROP TABLE IF EXISTS "{table}__old"'))
                conn.execute(text(f'ALTER TABLE IF EXISTS "{table}" RENAME TO "{table}__old"'))
                conn.execute(text(f'ALTER TABLE "{table}__new" RENAME TO "{table}"'))
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_intel_actos_fichas ON intel_actos_fichas(acto_key, ficha)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_iaf_ficha ON intel_actos_fichas(ficha)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_iaf_publication ON intel_actos_fichas(publication_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_iaf_celebration ON intel_actos_fichas(celebration_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_iaf_award ON intel_actos_fichas(award_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_iaf_update ON intel_actos_fichas(update_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_iaf_score ON intel_actos_fichas(detection_score)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_iaf_search ON intel_actos_fichas(search_text_norm)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_iap_acto ON intel_acto_proponentes(acto_key)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_iap_provider ON intel_acto_proponentes(proveedor_norm)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_ifm_month_profile ON intel_metricas_ficha_mes(period_month, detection_profile)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_ifm_ficha ON intel_metricas_ficha_mes(ficha)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_ifc_ficha ON intel_ficha_catalogo(ficha)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_intel_ifm_search ON intel_ficha_metadata(search_text_norm)"))
            for table in table_names:
                conn.execute(text(f'DROP TABLE IF EXISTS "{table}__old"'))
        return uploaded
    finally:
        source.close()
        engine.dispose()


def verify_analytics(local_db: Path) -> dict[str, Any]:
    connection = sqlite3.connect(local_db)
    try:
        counts = {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for table in (
                "intel_actos_fichas",
                "intel_acto_proponentes",
                "intel_metricas_ficha_mes",
                "intel_ficha_metadata",
                "intel_ficha_catalogo",
            )
        }
        duplicates = int(
            connection.execute(
                "SELECT COUNT(*) FROM (SELECT acto_key, ficha, COUNT(*) c FROM intel_actos_fichas "
                "GROUP BY acto_key, ficha HAVING c > 1)"
            ).fetchone()[0]
        )
        invalid_unique = int(
            connection.execute(
                "SELECT COUNT(*) FROM intel_actos_fichas WHERE is_unique_ficha = 1 AND detected_ficha_count <> 1"
            ).fetchone()[0]
        )
        invalid_scores = int(
            connection.execute(
                "SELECT COUNT(*) FROM intel_actos_fichas WHERE detection_score < 0 OR detection_score > 100"
            ).fetchone()[0]
        )
        invalid_amounts = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM intel_actos_fichas
                WHERE reference_amount_attributed < 0
                   OR award_amount_attributed < 0
                   OR (reference_amount_reliable = 0 AND reference_amount_attributed <> 0)
                   OR (award_amount_reliable = 0 AND award_amount_attributed <> 0)
                """
            ).fetchone()[0]
        )
        result = {
            **counts,
            "duplicates": duplicates,
            "invalid_unique": invalid_unique,
            "invalid_scores": invalid_scores,
            "invalid_amounts": invalid_amounts,
        }
        if duplicates or invalid_unique or invalid_scores or invalid_amounts:
            raise RuntimeError(f"Validacion analitica fallida: {result}")
        return result
    finally:
        connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Construye/publica la capa analitica de Inteligencia de Proveedores")
    parser.add_argument("--source-db", type=Path, default=DEFAULT_SOURCE_DB)
    parser.add_argument("--output-db", type=Path, default=DEFAULT_OUTPUT_DB)
    parser.add_argument("--metadata-xlsx", type=Path, default=DEFAULT_METADATA_XLSX)
    parser.add_argument("--catalog-xlsx", type=Path, default=DEFAULT_CATALOG_XLSX)
    parser.add_argument("--aliases-json", type=Path, default=DEFAULT_ALIAS_JSON)
    parser.add_argument("--classification-xlsx", type=Path, default=DEFAULT_CLASSIFICATION_XLSX)
    parser.add_argument("--risk-class-xlsx", type=Path, default=DEFAULT_RISK_CLASS_XLSX)
    parser.add_argument("--postgres-url", default="")
    parser.add_argument("--publish-postgres", action="store_true")
    parser.add_argument("--require-postgres", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="Solo para pruebas")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if not args.skip_build:
            build_local_analytics(
                args.source_db,
                args.output_db,
                args.metadata_xlsx,
                args.catalog_xlsx,
                args.aliases_json,
                classification_xlsx=args.classification_xlsx,
                risk_class_xlsx=args.risk_class_xlsx,
                limit=max(0, args.limit),
            )
        verification = verify_analytics(args.output_db)
        print("VERIFY_JSON=" + json.dumps(verification, ensure_ascii=False), flush=True)
        if args.publish_postgres or args.require_postgres:
            url = _postgres_url(args.postgres_url)
            if not url:
                if args.require_postgres:
                    raise RuntimeError("Se solicito publicar, pero falta SUPABASE_DB_URL/DATABASE_URL.")
            else:
                uploaded = publish_postgres(args.output_db, url)
                print("POSTGRES_JSON=" + json.dumps(uploaded, ensure_ascii=False), flush=True)
        return 0
    except Exception as exc:
        print(f"[ERROR] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
