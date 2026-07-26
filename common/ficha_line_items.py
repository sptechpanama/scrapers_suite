"""Atribución auditable de fichas técnicas a renglones de Panamá Compra.

Este módulo es deliberadamente independiente del cálculo analítico vigente.
Extrae renglones, normaliza especificaciones y relaciona ofertas por renglón,
pero no reemplaza ni modifica los montos históricos a nivel de acto.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from html import unescape
from io import StringIO
from typing import Iterable, Sequence

import pandas as pd

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - el parser tabular sigue disponible
    BeautifulSoup = None  # type: ignore[assignment]


FICHA_CONTEXT_RE = re.compile(
    r"(?:ficha(?:\s+t[eé]cnica)?|c[oó]digo\s+de\s+ficha|n[úu]mero\s+de\s+ficha)"
    r"\s*(?:n[°ºo.]*)?\s*[:#-]?\s*(\d{3,8})\*?",
    re.IGNORECASE,
)
FICHA_VALUE_RE = re.compile(r"^\s*(\d{3,8})\*?\s*$")

STOP_WORDS = {
    "a",
    "al",
    "con",
    "de",
    "del",
    "el",
    "en",
    "la",
    "las",
    "los",
    "o",
    "para",
    "por",
    "un",
    "una",
    "y",
}

FRACTION_CHARS = {
    "½": "1/2",
    "¼": "1/4",
    "¾": "3/4",
    "⅛": "1/8",
    "⅜": "3/8",
    "⅝": "5/8",
    "⅞": "7/8",
}


@dataclass(frozen=True)
class FichaProfile:
    code: str
    name: str
    specification_text: str
    normalized_text: str
    keywords: frozenset[str]
    dimensions_mm: frozenset[float]


@dataclass(frozen=True)
class LineItem:
    line_id: str
    line_number: str
    description: str
    ficha_codes: tuple[str, ...]
    quantity: float
    unit: str
    reference_unit_price: float
    reference_total: float
    source_table: int
    source_row: int


@dataclass(frozen=True)
class LineMatch:
    line: LineItem
    score: float
    method: str
    evidence: str
    requires_review: bool


@dataclass(frozen=True)
class OfferLine:
    provider: str
    line_number: str
    description: str
    quantity: float
    unit: str
    unit_price: float
    total: float
    source_table: int
    source_row: int


@dataclass(frozen=True)
class BoundLineOffer:
    match: LineMatch
    offer: OfferLine | None
    binding_method: str
    binding_score: float


def _repair_mojibake(value: str) -> str:
    """Repara UTF-8 interpretado como latin-1 sin tocar texto ya correcto."""

    if not any(marker in value for marker in ("Ã", "Â", "â€", "â€™")):
        return value
    try:
        repaired = value.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value
    return repaired if repaired.count("Ã") + repaired.count("Â") < value.count("Ã") + value.count("Â") else value


def clean_text(value: object) -> str:
    text = _repair_mojibake(str(value or "")).strip()
    return "" if text.lower() in {"nan", "none", "null", "<na>", "n/a"} else text


def normalize_text(value: object) -> str:
    text = clean_text(value).lower()
    for source, target in FRACTION_CHARS.items():
        text = text.replace(source, f" {target} ")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.replace("″", '"').replace("”", '"').replace("“", '"')
    text = text.replace("pulgadas", "pulgada").replace("inches", "inch")
    text = re.sub(r"[^a-z0-9./\"'-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_number(value: object) -> float:
    text = clean_text(value)
    text = (
        text.replace("B/.", "")
        .replace("B/", "")
        .replace("$", "")
        .replace("USD", "")
        .replace(" ", "")
    )
    if "," in text and "." in text:
        text = (
            text.replace(".", "").replace(",", ".")
            if text.rfind(",") > text.rfind(".")
            else text.replace(",", "")
        )
    elif "," in text:
        text = text.replace(",", ".")
    text = re.sub(r"[^0-9.\-]", "", text)
    if text in {"", "-", ".", "-."}:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def normalize_ficha(value: object) -> str:
    match = re.search(r"\d{3,8}", clean_text(value))
    return match.group(0) if match else ""


def _fraction_value(integer: str, numerator: str, denominator: str) -> float:
    try:
        denominator_value = float(denominator)
        if denominator_value == 0:
            return 0.0
        return float(integer or 0) + float(numerator) / denominator_value
    except (TypeError, ValueError):
        return 0.0


def extract_dimensions_mm(value: object) -> frozenset[float]:
    """Normaliza pulgadas, centímetros y milímetros a milímetros.

    Solo considera números acompañados por una unidad para evitar confundir
    cantidades, precios o números de ficha con medidas.
    """

    text = normalize_text(value)
    if not text:
        return frozenset()
    dimensions: set[float] = set()

    inch_boundary = r"(?=\s|$|[,;:)xX])"
    mixed_inch = re.compile(
        rf"(?<![\d/])(?:(\d+)\s+)?(\d+)\s*/\s*(\d+)\s*(?:\"|inch|pulgada){inch_boundary}"
    )
    consumed: list[tuple[int, int]] = []
    for match in mixed_inch.finditer(text):
        value_inch = _fraction_value(match.group(1) or "0", match.group(2), match.group(3))
        if value_inch > 0:
            dimensions.add(round(value_inch * 25.4, 3))
            consumed.append(match.span())

    scalar = re.compile(
        rf"(?<![\d/])(\d+(?:\.\d+)?)\s*(mm|milimetro|cm|centimetro|\"|inch|pulgada){inch_boundary}"
    )
    for match in scalar.finditer(text):
        if any(start <= match.start() < end for start, end in consumed):
            continue
        number = float(match.group(1))
        unit = match.group(2)
        if number <= 0:
            continue
        if unit in {'"', "inch", "pulgada"}:
            number *= 25.4
        elif unit in {"cm", "centimetro"}:
            number *= 10.0
        dimensions.add(round(number, 3))
    return frozenset(dimensions)


def meaningful_keywords(value: object) -> frozenset[str]:
    tokens = {
        token
        for token in normalize_text(value).split()
        if len(token) >= 3
        and token not in STOP_WORDS
        and not token.isdigit()
        and not re.fullmatch(r"\d+(?:\.\d+)?", token)
    }
    return frozenset(tokens)


def build_ficha_profile(
    code: object,
    name: object,
    specification_parts: Iterable[object] = (),
) -> FichaProfile:
    normalized_code = normalize_ficha(code)
    texts = [clean_text(name)] + [clean_text(value) for value in specification_parts]
    combined = " | ".join(value for value in texts if value)
    return FichaProfile(
        code=normalized_code,
        name=clean_text(name),
        specification_text=combined,
        normalized_text=normalize_text(combined),
        keywords=meaningful_keywords(combined),
        dimensions_mm=extract_dimensions_mm(combined),
    )


def _flatten_column(column: object) -> str:
    if isinstance(column, tuple):
        return " ".join(clean_text(part) for part in column if clean_text(part)).strip()
    return clean_text(column)


def _find_column(columns: Sequence[str], aliases: Sequence[str]) -> str:
    normalized = {normalize_text(column): column for column in columns}
    for alias in aliases:
        alias_normalized = normalize_text(alias)
        exact = normalized.get(alias_normalized)
        if exact:
            return exact
    for column_normalized, original in normalized.items():
        if any(normalize_text(alias) in column_normalized for alias in aliases):
            return original
    return ""


def _extract_ficha_codes(dedicated_value: object, description: object) -> tuple[str, ...]:
    codes: list[str] = []
    for raw in re.split(r"[,;|\s]+", clean_text(dedicated_value)):
        match = FICHA_VALUE_RE.match(raw)
        if match and match.group(1) not in codes:
            codes.append(match.group(1))
    for match in FICHA_CONTEXT_RE.finditer(clean_text(description)):
        if match.group(1) not in codes:
            codes.append(match.group(1))
    return tuple(codes)


def extract_line_items_from_html(html: str) -> list[LineItem]:
    """Extrae todos los renglones estructurados sin escoger uno arbitrariamente."""

    if not clean_text(html):
        return []
    try:
        tables = pd.read_html(StringIO(html))
    except (ValueError, ImportError):
        return []

    results: list[LineItem] = []
    seen: set[tuple[object, ...]] = set()
    for table_index, raw_table in enumerate(tables):
        if raw_table.empty:
            continue
        table = raw_table.copy()
        table.columns = [_flatten_column(column) for column in table.columns]
        columns = list(table.columns)
        description_columns = [
            column
            for column in columns
            if any(
                token in normalize_text(column)
                for token in (
                    "descripcion",
                    "especificacion",
                    "nombre generico",
                    "detalle del bien",
                    "bien servicio",
                )
            )
        ]
        ficha_column = _find_column(
            columns,
            ("ficha tecnica", "numero ficha", "n ficha", "ficha"),
        )
        if not description_columns and not ficha_column:
            continue
        line_column = _find_column(
            columns,
            ("renglon", "numero renglon", "item", "numero item", "linea"),
        )
        quantity_column = _find_column(columns, ("cantidad solicitada", "cantidad propuesta", "cantidad"))
        unit_column = _find_column(columns, ("unidad de medida", "unidad", "u m"))
        reference_unit_column = _find_column(
            columns,
            ("precio de referencia unitario", "precio referencia unitario", "precio unitario referencia"),
        )
        reference_column = _find_column(columns, ("precio de referencia", "precio referencia"))
        total_column = _find_column(
            columns,
            ("monto de referencia", "precio total", "monto total", "subtotal"),
        )

        for row_position, (_, row) in enumerate(table.fillna("").iterrows(), start=1):
            description = " | ".join(
                dict.fromkeys(
                    clean_text(row.get(column, ""))
                    for column in description_columns
                    if clean_text(row.get(column, ""))
                )
            )
            dedicated_ficha = row.get(ficha_column, "") if ficha_column else ""
            ficha_codes = _extract_ficha_codes(dedicated_ficha, description)
            if not description and not ficha_codes:
                continue
            line_number = clean_text(row.get(line_column, "")) if line_column else str(row_position)
            quantity = parse_number(row.get(quantity_column, "")) if quantity_column else 0.0
            reference_unit = (
                parse_number(row.get(reference_unit_column, ""))
                if reference_unit_column
                else 0.0
            )
            reference_total = parse_number(row.get(total_column, "")) if total_column else 0.0
            if reference_column and not reference_unit_column and not total_column:
                reference_total = parse_number(row.get(reference_column, ""))
            if reference_total <= 0 and reference_unit > 0 and quantity > 0:
                reference_total = reference_unit * quantity
            signature = (
                line_number,
                normalize_text(description),
                ficha_codes,
                round(quantity, 6),
                round(reference_unit, 6),
                round(reference_total, 6),
            )
            if signature in seen:
                continue
            seen.add(signature)
            results.append(
                LineItem(
                    line_id=f"t{table_index + 1}-r{row_position}",
                    line_number=line_number,
                    description=description,
                    ficha_codes=ficha_codes,
                    quantity=quantity,
                    unit=clean_text(row.get(unit_column, "")) if unit_column else "",
                    reference_unit_price=reference_unit,
                    reference_total=reference_total,
                    source_table=table_index + 1,
                    source_row=row_position,
                )
            )
    return results


def _dimensions_conflict(profile: FichaProfile, line: LineItem) -> bool:
    line_dimensions = extract_dimensions_mm(line.description)
    if not profile.dimensions_mm or not line_dimensions:
        return False
    return profile.dimensions_mm.isdisjoint(line_dimensions)


def match_ficha_to_lines(
    profile: FichaProfile,
    lines: Sequence[LineItem],
    *,
    minimum_score: float = 0.72,
) -> list[LineMatch]:
    """Devuelve todas las coincidencias fiables; nunca escoge el primer renglón."""

    matches: list[LineMatch] = []
    for line in lines:
        if profile.code and profile.code in line.ficha_codes:
            matches.append(
                LineMatch(
                    line=line,
                    score=1.0,
                    method="ficha_exacta_renglon",
                    evidence=f"Ficha {profile.code} declarada en el renglón",
                    requires_review=False,
                )
            )
            continue

        if not profile.keywords:
            continue
        line_keywords = meaningful_keywords(line.description)
        if not line_keywords:
            continue
        common = profile.keywords.intersection(line_keywords)
        coverage = len(common) / max(len(profile.keywords), 1)
        precision = len(common) / max(len(line_keywords), 1)
        sequence = SequenceMatcher(
            None,
            profile.normalized_text,
            normalize_text(line.description),
        ).ratio()
        conflict = _dimensions_conflict(profile, line)
        if conflict:
            continue
        line_dimensions = extract_dimensions_mm(line.description)
        dimension_match = bool(
            profile.dimensions_mm
            and line_dimensions
            and profile.dimensions_mm.intersection(line_dimensions)
        )
        score = min(
            0.96,
            0.52 * coverage
            + 0.18 * precision
            + 0.20 * sequence
            + (0.10 if dimension_match else 0.0),
        )
        if len(common) < 2 or coverage < 0.50 or score < minimum_score:
            continue
        matches.append(
            LineMatch(
                line=line,
                score=round(score, 4),
                method="especificacion_normalizada",
                evidence=(
                    f"Términos comunes: {', '.join(sorted(common))}"
                    + (
                        f"; medidas compatibles: {sorted(profile.dimensions_mm)} mm"
                        if dimension_match
                        else ""
                    )
                ),
                requires_review=score < 0.86,
            )
        )
    return sorted(
        matches,
        key=lambda match: (
            _sortable_line_number(match.line.line_number),
            -match.score,
            match.line.line_id,
        ),
    )


def _sortable_line_number(value: object) -> tuple[int, str]:
    match = re.search(r"\d+", clean_text(value))
    return (int(match.group(0)) if match else 10**9, clean_text(value))


def _offer_lines_from_table(table: object, table_index: int) -> list[OfferLine]:
    headers = [
        normalize_text(cell.get_text(" ", strip=True))
        for cell in table.select("thead th")
    ]
    if not headers:
        first_row = table.select_one("tr")
        if first_row:
            headers = [
                normalize_text(cell.get_text(" ", strip=True))
                for cell in first_row.select("th,td")
            ]
    price_index = next(
        (index for index, header in enumerate(headers) if "precio unitario" in header),
        -1,
    )
    if price_index < 0:
        return []
    line_index = next(
        (
            index
            for index, header in enumerate(headers)
            if "renglon" in header or header in {"item", "linea", "numero item"}
        ),
        -1,
    )
    description_index = next(
        (
            index
            for index, header in enumerate(headers)
            if "descripcion" in header or "especificacion" in header
        ),
        -1,
    )
    quantity_index = next(
        (index for index, header in enumerate(headers) if "cantidad" in header),
        -1,
    )
    unit_index = next(
        (index for index, header in enumerate(headers) if "unidad de medida" in header),
        -1,
    )
    total_index = next(
        (
            index
            for index, header in enumerate(headers)
            if "precio total" in header or "monto total" in header or header == "total"
        ),
        -1,
    )
    caption = table.select_one("caption")
    provider = clean_text(caption.get_text(" ", strip=True) if caption else "")
    if provider:
        provider = re.sub(
            r"(?i)\b(cuadro|propuesta|proponente|oferta)\b\s*[:#-]?\s*",
            "",
            provider,
        ).strip()
    rows: list[OfferLine] = []
    body_rows = table.select("tbody tr") or table.select("tr")[1:]
    for row_position, row in enumerate(body_rows, start=1):
        cells = row.select("th,td")
        if not cells:
            continue
        values = [clean_text(cell.get_text(" ", strip=True)) for cell in cells]
        unit_price = parse_number(values[price_index]) if price_index < len(values) else 0.0
        if unit_price <= 0:
            continue
        quantity = (
            parse_number(values[quantity_index])
            if quantity_index >= 0 and quantity_index < len(values)
            else 0.0
        )
        total = (
            parse_number(values[total_index])
            if total_index >= 0 and total_index < len(values)
            else 0.0
        )
        if total <= 0 and quantity > 0:
            total = unit_price * quantity
        rows.append(
            OfferLine(
                provider=provider,
                line_number=(
                    values[line_index]
                    if line_index >= 0 and line_index < len(values)
                    else str(row_position)
                ),
                description=(
                    values[description_index]
                    if description_index >= 0 and description_index < len(values)
                    else ""
                ),
                quantity=quantity,
                unit=(
                    values[unit_index]
                    if unit_index >= 0 and unit_index < len(values)
                    else ""
                ),
                unit_price=unit_price,
                total=total,
                source_table=table_index,
                source_row=row_position,
            )
        )
    return rows


def extract_offer_lines_from_html(html: str) -> list[OfferLine]:
    if not clean_text(html):
        return []
    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        results: list[OfferLine] = []
        for table_index, table in enumerate(soup.select("table"), start=1):
            results.extend(_offer_lines_from_table(table, table_index))
        if results:
            return results

    # ``beautifulsoup4`` no es una dependencia obligatoria del orquestador.
    # El respaldo tabular conserva todos los renglones y evita que la ausencia
    # del parser HTML convierta silenciosamente las ofertas en una lista vacía.
    results: list[OfferLine] = []
    table_blocks = re.findall(r"(?is)<table\b[^>]*>.*?</table>", html)
    sources = table_blocks or [html]
    table_index = 0
    for source in sources:
        caption_match = re.search(r"(?is)<caption\b[^>]*>(.*?)</caption>", source)
        provider_from_caption = ""
        if caption_match:
            provider_from_caption = clean_text(
                unescape(re.sub(r"(?is)<[^>]+>", " ", caption_match.group(1)))
            )
            provider_from_caption = re.sub(
                r"(?i)\b(cuadro|propuesta|proponente|oferta)\b\s*[:#-]?\s*",
                "",
                provider_from_caption,
            ).strip()
        try:
            tables = pd.read_html(StringIO(source))
        except (ValueError, ImportError):
            continue
        for raw_table in tables:
            table_index += 1
            if raw_table.empty:
                continue
            table = raw_table.copy()
            table.columns = [_flatten_column(column) for column in table.columns]
            columns = list(table.columns)
            price_column = _find_column(
                columns,
                ("precio unitario", "precio de oferta unitario", "precio propuesto unitario"),
            )
            if not price_column:
                continue
            line_column = _find_column(
                columns,
                ("renglon", "numero renglon", "item", "numero item", "linea"),
            )
            description_column = _find_column(
                columns,
                ("descripcion del bien", "descripcion", "especificacion"),
            )
            quantity_column = _find_column(
                columns,
                ("cantidad propuesta", "cantidad ofertada", "cantidad"),
            )
            unit_column = _find_column(columns, ("unidad de medida", "unidad", "u m"))
            total_column = _find_column(
                columns,
                ("precio total", "monto total", "subtotal", "total"),
            )
            provider_column = _find_column(
                columns,
                ("proponente", "proveedor", "razon social", "empresa"),
            )
            for row_position, (_, row) in enumerate(table.iterrows(), start=1):
                unit_price = parse_number(row.get(price_column))
                if unit_price <= 0:
                    continue
                quantity = parse_number(row.get(quantity_column)) if quantity_column else 0.0
                total = parse_number(row.get(total_column)) if total_column else 0.0
                if total <= 0 and quantity > 0:
                    total = unit_price * quantity
                results.append(
                    OfferLine(
                        provider=(
                            clean_text(row.get(provider_column))
                            if provider_column
                            else provider_from_caption
                        ),
                        line_number=(
                            clean_text(row.get(line_column))
                            if line_column
                            else str(row_position)
                        ),
                        description=(
                            clean_text(row.get(description_column))
                            if description_column
                            else ""
                        ),
                        quantity=quantity,
                        unit=clean_text(row.get(unit_column)) if unit_column else "",
                        unit_price=unit_price,
                        total=total,
                        source_table=table_index,
                        source_row=row_position,
                    )
                )
    return results


def _line_similarity(line: LineItem, offer: OfferLine) -> float:
    if (
        clean_text(line.line_number)
        and clean_text(offer.line_number)
        and normalize_text(line.line_number) == normalize_text(offer.line_number)
    ):
        return 1.0
    left = normalize_text(line.description)
    right = normalize_text(offer.description)
    if not left or not right:
        return 0.0
    left_dimensions = extract_dimensions_mm(left)
    right_dimensions = extract_dimensions_mm(right)
    if left_dimensions and right_dimensions and left_dimensions.isdisjoint(right_dimensions):
        return 0.0
    left_words = meaningful_keywords(left)
    right_words = meaningful_keywords(right)
    overlap = len(left_words.intersection(right_words)) / max(len(left_words), 1)
    sequence = SequenceMatcher(None, left, right).ratio()
    return min(0.99, 0.65 * overlap + 0.35 * sequence)


def bind_offers_to_matches(
    matches: Sequence[LineMatch],
    offers: Sequence[OfferLine],
    *,
    minimum_score: float = 0.68,
) -> list[BoundLineOffer]:
    """Relaciona cada oferta con el renglón aplicable sin usar el total del acto."""

    output: list[BoundLineOffer] = []
    for match in matches:
        compatible: list[tuple[float, OfferLine]] = []
        for offer in offers:
            score = _line_similarity(match.line, offer)
            if score >= minimum_score:
                compatible.append((score, offer))
        if not compatible:
            output.append(
                BoundLineOffer(
                    match=match,
                    offer=None,
                    binding_method="sin_precio_renglon_confirmado",
                    binding_score=0.0,
                )
            )
            continue
        compatible.sort(
            key=lambda item: (
                normalize_text(item[1].provider),
                -item[0],
                item[1].source_table,
                item[1].source_row,
            )
        )
        for score, offer in compatible:
            output.append(
                BoundLineOffer(
                    match=match,
                    offer=offer,
                    binding_method=(
                        "numero_renglon"
                        if score == 1.0
                        else "descripcion_especificacion"
                    ),
                    binding_score=round(score, 4),
                )
            )
    return output


def matched_reference_amount(matches: Sequence[LineMatch]) -> tuple[float, int, int]:
    """Monto referencial limpio de los renglones coincidentes.

    Retorna ``(monto, renglones_con_monto, renglones_totales)`` para que la
    cobertura sea explícita. No utiliza el monto global del acto.
    """

    total = 0.0
    covered = 0
    for match in matches:
        line_total = float(match.line.reference_total or 0.0)
        if line_total <= 0 and match.line.reference_unit_price > 0 and match.line.quantity > 0:
            line_total = match.line.reference_unit_price * match.line.quantity
        if line_total > 0:
            total += line_total
            covered += 1
    return round(total, 6), covered, len(matches)
