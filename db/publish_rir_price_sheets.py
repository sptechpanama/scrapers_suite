# -*- coding: utf-8 -*-
"""Publica referencias de precio RIR y prepara la hoja de investigación.

La tabla histórica se reemplaza de forma completa porque es un producto
derivado de la capa analítica. La hoja de investigación nunca se limpia: solo
se crea y completa su encabezado para conservar el trabajo de ChatGPT Pro.
"""

from __future__ import annotations

import argparse
import math
import os
import random
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Sequence

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ANALYTICS_DB = REPO_ROOT / "data" / "db" / "inteligencia_proveedores.db"
DEFAULT_CREDENTIALS = REPO_ROOT / "credentials" / "service-account.json"
DEFAULT_SPREADSHEET_ID = "17hOfP-vMdJ4D7xym1cUp7vAcd8XJPErpY3V-9Ui2tCo"
HISTORICAL_SHEET = "RIR_PRECIOS_HISTORICOS"
RESEARCH_SHEET = "RIR_INVESTIGACION_PROVEEDORES"

HISTORICAL_HEADERS = [
    "ficha",
    "nombre_ficha",
    "unidad_comparable",
    "precio_referencia_tipico",
    "precio_participacion_tipico",
    "precio_competitivo_historico",
    "actos_con_muestra",
    "muestras_referencia",
    "muestras_participacion",
    "muestras_ganadoras",
    "unidad_dominante_pct",
    "mapeo_explicito_pct",
    "ultima_muestra",
    "nivel_confianza",
    "confianza_precio",
    "actualizado_en",
]

RESEARCH_HEADERS = [
    "id_estable",
    "fecha_investigacion",
    "estado_investigacion",
    "tipo_convocatoria",
    "numero_acto",
    "enlace_acto",
    "ficha",
    "nombre_ficha",
    "renglon",
    "descripcion_renglon",
    "cantidad",
    "unidad",
    "termino_entrega",
    "precio_referencia_tipico",
    "precio_participacion_tipico",
    "precio_competitivo_historico",
    "cantidad_muestras",
    "confianza_precio",
    "medio_recomendado",
    "proveedor_con_precio",
    "pais",
    "contacto_proveedor",
    "precio_proveedor",
    "moneda",
    "incoterm",
    "proveedor_potencial",
    "pais_potencial",
    "contacto_potencial",
    "observaciones",
    "fuentes",
    "actualizado_en",
]


def _column_letter(index: int) -> str:
    letters: list[str] = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def _call_with_backoff(action: Callable[[], Any], *, attempts: int = 5) -> Any:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return action()
        except HttpError as exc:
            last_error = exc
            status = int(getattr(exc.resp, "status", 0) or 0)
            if status not in {429, 500, 502, 503, 504} or attempt == attempts - 1:
                raise
        except (TimeoutError, ConnectionError, OSError) as exc:
            last_error = exc
            if attempt == attempts - 1:
                raise
        time.sleep(min(20.0, 1.5 * (2**attempt)) + random.uniform(0, 0.4))
    raise RuntimeError(f"Google Sheets agotó reintentos: {last_error}")


def load_historical_rows(database: Path) -> list[list[Any]]:
    if not database.exists():
        raise FileNotFoundError(f"No existe la capa analítica: {database}")
    connection = sqlite3.connect(
        f"file:{database.as_posix()}?mode=ro", uri=True, timeout=60
    )
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='intel_ficha_price_benchmarks'"
        ).fetchone()
        if not exists:
            raise RuntimeError(
                "La capa analítica no contiene intel_ficha_price_benchmarks"
            )
        frame = pd.read_sql_query(
            """
            SELECT ficha, nombre_ficha, unidad_comparable,
                   precio_referencia_tipico, precio_participacion_tipico,
                   precio_competitivo_historico, actos_con_muestra,
                   muestras_referencia, muestras_participacion,
                   muestras_ganadoras, unidad_dominante_pct,
                   mapeo_explicito_pct, ultima_muestra, nivel_confianza,
                   confianza_precio, updated_at AS actualizado_en
            FROM intel_ficha_price_benchmarks
            ORDER BY CAST(ficha AS INTEGER), ficha
            """,
            connection,
        )
    finally:
        connection.close()
    rows: list[list[Any]] = []
    for values in frame.itertuples(index=False, name=None):
        rows.append(
            [
                "" if value is None or (isinstance(value, float) and math.isnan(value)) else value
                for value in values
            ]
        )
    return rows


def merge_research_headers(existing: Sequence[object]) -> list[str]:
    """Conserva columnas externas y agrega al final las que falten."""

    merged = [str(value or "").strip() for value in existing if str(value or "").strip()]
    lowered = {value.casefold() for value in merged}
    for header in RESEARCH_HEADERS:
        if header.casefold() not in lowered:
            merged.append(header)
            lowered.add(header.casefold())
    return merged


def _sheet_titles(service: Any, spreadsheet_id: str) -> set[str]:
    metadata = _call_with_backoff(
        lambda: service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title")
        .execute()
    )
    return {
        str(item.get("properties", {}).get("title") or "")
        for item in metadata.get("sheets", [])
    }


def _ensure_sheets(service: Any, spreadsheet_id: str) -> None:
    titles = _sheet_titles(service, spreadsheet_id)
    missing = [
        title for title in (HISTORICAL_SHEET, RESEARCH_SHEET) if title not in titles
    ]
    if not missing:
        return
    _call_with_backoff(
        lambda: service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {"addSheet": {"properties": {"title": title}}}
                    for title in missing
                ]
            },
        )
        .execute()
    )


def publish(
    *,
    database: Path,
    credentials_file: Path,
    spreadsheet_id: str,
) -> dict[str, int]:
    rows = load_historical_rows(database)
    credentials = Credentials.from_service_account_file(
        str(credentials_file),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    _ensure_sheets(service, spreadsheet_id)

    _call_with_backoff(
        lambda: service.spreadsheets()
        .values()
        .clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{HISTORICAL_SHEET}'!A:Z",
        )
        .execute()
    )
    historical_values = [HISTORICAL_HEADERS, *rows]
    _call_with_backoff(
        lambda: service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=(
                f"'{HISTORICAL_SHEET}'!A1:"
                f"{_column_letter(len(HISTORICAL_HEADERS))}{len(historical_values)}"
            ),
            valueInputOption="RAW",
            body={"values": historical_values},
        )
        .execute()
    )

    header_response = _call_with_backoff(
        lambda: service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=f"'{RESEARCH_SHEET}'!1:1",
        )
        .execute()
    )
    existing = (header_response.get("values") or [[]])[0]
    research_headers = merge_research_headers(existing)
    _call_with_backoff(
        lambda: service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=(
                f"'{RESEARCH_SHEET}'!A1:"
                f"{_column_letter(len(research_headers))}1"
            ),
            valueInputOption="RAW",
            body={"values": [research_headers]},
        )
        .execute()
    )
    return {
        "historical_rows": len(rows),
        "research_columns": len(research_headers),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analytics-db", type=Path, default=DEFAULT_ANALYTICS_DB)
    parser.add_argument("--credentials", type=Path, default=DEFAULT_CREDENTIALS)
    parser.add_argument(
        "--spreadsheet-id",
        default=(
            os.getenv("ORQUESTADOR_PANAMACOMPRA_SPREADSHEET_ID")
            or DEFAULT_SPREADSHEET_ID
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = publish(
        database=args.analytics_db,
        credentials_file=args.credentials,
        spreadsheet_id=str(args.spreadsheet_id).strip(),
    )
    print(
        f"[OK] {HISTORICAL_SHEET}: {result['historical_rows']:,} fichas | "
        f"{RESEARCH_SHEET}: {result['research_columns']} columnas",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
