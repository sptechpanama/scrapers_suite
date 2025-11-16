# -*- coding: utf-8 -*-
"""Scraper de portales pÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºblicos del MINSA usando Selenium."""







from __future__ import annotations







import argparse

import contextlib



import json



import mimetypes



import os



import re



import shutil



import sys



import time



import unicodedata
import builtins



from dataclasses import dataclass



from io import StringIO



from pathlib import Path



from typing import Any, Callable, Iterable, Sequence, Optional







import pandas as pd



from google.oauth2.service_account import Credentials



from googleapiclient.discovery import build



from googleapiclient.errors import HttpError



from googleapiclient.http import MediaFileUpload



from selenium import webdriver



from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
    StaleElementReferenceException,
)



from selenium.webdriver import Chrome



from selenium.webdriver.chrome.options import Options



from selenium.webdriver.chrome.service import Service



from selenium.webdriver.common.by import By



from selenium.webdriver.remote.webelement import WebElement



from selenium.webdriver.support import expected_conditions as EC



from selenium.webdriver.support.ui import WebDriverWait



from webdriver_manager.chrome import ChromeDriverManager

try:
    from PyPDF2 import PdfReader  # type: ignore
except ImportError:  # pragma: no cover
    PdfReader = None

try:
    import pdfplumber  # type: ignore
except ImportError:  # pragma: no cover
    pdfplumber = None

try:
    import fitz  # type: ignore
except ImportError:  # pragma: no cover
    fitz = None

try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text  # type: ignore
except ImportError:  # pragma: no cover
    pdfminer_extract_text = None


BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent



CTNI_URL = "https://ctni.minsa.gob.pa/Home/ConsultarFichas"



CRITERIOS_URL = "https://dndmcriterios.minsa.gob.pa/ct/Consultas/frmCRITERIOS_Criterios.aspx"



OFERENTES_URL = "https://appwebs.minsa.gob.pa/woferentes/Oferentes.aspx"



CATALOGO_PUBLICO_URL = "https://appwebs.minsa.gob.pa/woferentes/Public/Catalogos.aspx"



DRIVE_SCOPES = ("https://www.googleapis.com/auth/drive",)



DEFAULT_DRIVE_FOLDER_ID = "0AMdsC0UugWLkUk9PVA"



DEFAULT_OUTPUT_DIR = BASE_DIR / "outputs"
PDF_DOWNLOAD_DIR = BASE_DIR / "tmp_catalog_pdf"



_DEFAULT_CREDENTIALS_PATH = REPO_ROOT / "credentials" / "service-account.json"



COLUMN_FIXES = {
    "Cat\u00E1logo": "Cat\u00E1logo",
    "Cat\u00F3logo": "Cat\u00E1logo",
    "Cat?laogo": "Cat\u00E1logo",
    "Cat\u00E1logo:": "",
    "Oferente:": "",
    "Pa?s": "Pa\u00EDs",
    "Pa\u00EDs": "Pa\u00EDs",
    "Tel?fono": "Tel\u00E9fono",
    "Tel\u00E9fono": "Tel\u00E9fono",
    "Comit?": "Comit\u00E9",
    "Comit\u00E9": "Comit\u00E9",
    "N?": "N\u00B0",
    "N\u00B0": "N\u00B0",
    "N? Ficha": "N\u00FAmero Ficha",
    "N\u00B0 Ficha": "N\u00FAmero Ficha",
    "N\u00BA Ficha": "N\u00FAmero Ficha",
    "N\u00FAmero Ficha": "N\u00FAmero Ficha",
    "T?cnica": "T\u00E9cnica",
    "T\u00E9cnica": "T\u00E9cnica",
    "T?cnico": "T\u00E9cnico",
    "T\u00E9cnico": "T\u00E9cnico",
    "T?c.": "T\u00E9c.",
    "T\u00E9c.": "T\u00E9c.",
    "Gen?rico": "Gen\u00E9rico",
    "Gen\u00E9rico": "Gen\u00E9rico",
    "Importaci?n": "Importaci\u00F3n",
    "Importaci\u00F3n": "Importaci\u00F3n",
    "Permiso Especial Importaci?n": "Permiso Especial Importaci\u00F3n",
    "Permiso Especial Importaci\u00F3n": "Permiso Especial Importaci\u00F3n",
    "No. de Oferente": "N\u00FAmero de Oferente",
    "No. de Cat\u00E1logo": "N\u00B0 Cat\u00E1logo",
    "N\u00FAmero de Oferente": "N\u00FAmero de Oferente",
    "Nombre de Producto": "Nombre del Producto",
    "Sub Comit\u00E9": "Subcomit\u00E9",
    "Pais Origen": "Pa\u00EDs Origen",
    "Pais Procedencia": "Pa\u00EDs Procedencia",
    "Casa Productora, Laboratorio, Fabricante": "Casa Productora / Laboratorio",
    "N\u00B0 de Cat\u00E1laogo o Modelo, Sitio Web": "Modelo / Sitio Web",
    "N\u00B0 de Cat\u00E1logo o Modelo, Sitio Web": "Modelo / Sitio Web",
    "N\uFFFD de Cat\u00E1logo": "N\u00B0 Cat\u00E1logo",
    "N\u00FAmero Gen\u00E9rico": "Nombre Gen\u00E9rico",
    "Tipo de Producto": "Tipo de Producto",
    "N\u00B0 Registro Sanitario": "N\u00B0 Reg. Sanitario",
    "N\uFFFD Ficha": "N\u00FAmero Ficha",
    "N\uFFFD Ficha CTNI": "N\u00B0 Ficha CTNI",
    "Fecha Vencimiento": "Fecha Venc. Reg. Sanitario",
    "Permiso Especial Importaci\u00F3n": "P. E. I.",
    "Fecha Permiso Importaci\u00F3n": "Fecha P. E. I.",
    "FechaCaptura": "Fecha Captura",
}


def _normalize_console_text(value: str) -> str:
    if not isinstance(value, str):
        return value
    original = value
    for _ in range(3):
        try:
            repaired = value.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            break
        if not repaired or repaired == value:
            break
        value = repaired
    normalized = unicodedata.normalize("NFKD", value)
    cleaned = normalized.encode("ascii", "ignore").decode("ascii", "ignore")
    return cleaned or original


_ORIGINAL_PRINT = builtins.print


def _patched_print(*args, **kwargs):
    if args:
        args = tuple(_normalize_console_text(arg) if isinstance(arg, str) else arg for arg in args)
    _ORIGINAL_PRINT(*args, **kwargs)


builtins.print = _patched_print


DEDUP_CONFIG: dict[str, tuple[str, ...]] = {
    "fichas_ctni": ("N\u00FAmero Ficha",),
    "criterios_tecnicos": ("Certificado",),
}

DEFAULT_DRIVE_CREDENTIALS = _DEFAULT_CREDENTIALS_PATH if _DEFAULT_CREDENTIALS_PATH.exists() else None



CATALOGO_PUBLIC_TABLE_XPATH = "//table[contains(@id,'gvCat\u00E1logo')]"



CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")



WHITESPACE_RE = re.compile(r"\s+")



@dataclass



class DuplicateLogEntry:



    dataset: str



    row_number: int



    original_row_number: int



    row_data: dict[str, Any]



    original_row_data: dict[str, Any]







@dataclass



class CatalogRunCounters:



    pages: int = 0



    unique_rows: int = 0



    duplicate_rows: int = 0







DUPLICATE_LOGS: list[DuplicateLogEntry] = []



CATALOG_COUNTERS = CatalogRunCounters()



SKIPPED_OFERENTES: list[dict[str, str]] = []



SKIP_OFERENTE_NAMES: set[str] = set()



CATALOG_SCRAPE_MODE = "legacy"



CSV_OUTPUT_DATASETS: set[str] = set()



PDF_EXTRACTION_ISSUES: list[dict[str, Any]] = []



OFERENTES_COLUMN_RENAMES = {
    "Oferente:No. de Oferente": "N\u00FAmero de Oferente",
    "No. de Oferente": "N\u00FAmero de Oferente",
    "Oferente:Oferente": "Oferente",
    "Oferente:Vencimiento Cert.": "Vencimiento Cert.",
    "Oferente:Representante Legal": "Representante Legal",
    "Oferente:Contacto": "Contacto",
    "Oferente:Tel\u00E9fono": "Tel\u00E9fono",
    "Oferente:Correo": "Correo",
    "Oferente:Certificado": "Certificado",
    "Cat\u00E1logo:N\u00B0 Cat\u00E1logo": "N\u00B0 Cat\u00E1logo",
    "N\u00B0 de Cat\u00E1logo": "N\u00B0 Cat\u00E1logo",
    "Cat\u00E1logo:Nombre del Producto": "Nombre del Producto",
    "Cat\u00E1logo:Subcomit\u00E9": "Subcomit\u00E9",
    "Cat\u00E1logo:Pa\u00EDs Origen": "Pa\u00EDs Origen",
    "Cat\u00E1logo:Pa\u00EDs Procedencia": "Pa\u00EDs Procedencia",
    "Cat\u00E1logo:Casa Productora / Laboratorio": "Casa Productora / Laboratorio",
    "Cat\u00E1logo:Marca": "Marca",
    "Cat\u00E1logo:Modelo / Sitio Web": "Modelo / Sitio Web",
    "Cat\u00E1logo:N\u00B0 Ficha CTNI": "N\u00B0 Ficha CTNI",
    "N\u00FAmero Ficha CTNI": "N\u00B0 Ficha CTNI",
    "N\u00B0 Ficha CTNI": "N\u00B0 Ficha CTNI",
    "N\u00BA Ficha CTNI": "N\u00B0 Ficha CTNI",
    "Cat\u00E1logo:Nombre Gen\u00E9rico CTNI": "Nombre Gen\u00E9rico CTNI",
    "Cat\u00E1logo:Tipo de Producto": "Tipo de Producto",
    "Cat\u00E1logo:N\u00B0 Reg. Sanitario": "N\u00B0 Reg. Sanitario",
    "Cat\u00E1logo:Fecha Venc. Reg. Sanitario": "Fecha Venc. Reg. Sanitario",
    "Cat\u00E1logo:Habilitado por FyD": "Habilitado por FyD",
    "Cat\u00E1logo:Criterio T\u00E9cnico": "Criterio T\u00E9cnico",
    "Cat\u00E1logo:Fecha Vencimiento Criterio T\u00E9cnico": "Fecha Vencimiento Criterio T\u00E9cnico",
    "Fecha Venc. Reg. Sanitario Criterio T\u00E9cnico": "Fecha Vencimiento Criterio T\u00E9cnico",
    "Cat\u00E1logo:P. E. I.": "P. E. I.",
    "Cat\u00E1logo:Fecha P. E. I.": "Fecha P. E. I.",
    "Cat\u00E1logo:Fecha Captura": "Fecha Captura",
    "Cat\u00E1logo:Estado": "Estado",
}



OFERENTE_BASE_COLUMN_ORDER = [
    "N\u00FAmero de Oferente",
    "Oferente",
    "Vencimiento Cert.",
    "Representante Legal",
    "Contacto",
    "Tel\u00E9fono",
    "Correo",
    "Certificado",
]



CATALOGO_COLUMN_ORDER = [
    "N\u00B0 Cat\u00E1logo",
    "Nombre del Producto",
    "Subcomit\u00E9",
    "Pa\u00EDs Origen",
    "Pa\u00EDs Procedencia",
    "Casa Productora / Laboratorio",
    "Marca",
    "Modelo / Sitio Web",
    "N\u00B0 Ficha CTNI",
    "Nombre Gen\u00E9rico CTNI",
    "Tipo de Producto",
    "N\u00B0 Reg. Sanitario",
    "Fecha Venc. Reg. Sanitario",
    "Habilitado por FyD",
    "Criterio T\u00E9cnico",
    "Fecha Vencimiento Criterio T\u00E9cnico",
    "P. E. I.",
    "Fecha P. E. I.",
    "Fecha Captura",
    "Estado",
]

CRITERIO_TECNICO_PATTERN = re.compile(r"^[A-Za-z]{2,10}-[A-Za-z]{2}-\d{2}-\d{2}$")
CT_REGEX = re.compile(
    r"(?:[A-Z.]+[-\s]?){1,4}[A-Z]{2,4}-\d{2,4}-\d{2}-\d{2}(?:/[A-Z])?",
    re.IGNORECASE,
)

CATALOG_FIELD_KEYS = tuple(f"Cat\u00E1logo:{column}" for column in CATALOGO_COLUMN_ORDER)
CATALOG_REQUIRED_KEYS = (
    "Cat\u00E1logo:Nombre del Producto",
    "Cat\u00E1logo:Nombre Gen\u00E9rico CTNI",
    "Cat\u00E1logo:Tipo de Producto",
    "Cat\u00E1logo:Marca",
)



@dataclass



class ScrapeResult:



    name: str



    dataframe: pd.DataFrame







def normalize_column_label(label: str) -> str:
    text = str(label).strip()
    if not text:
        return text
    try:
        text = text.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    for wrong, right in COLUMN_FIXES.items():
        text = text.replace(wrong, right)
    if "::" in text:
        text = text.split("::", 1)[1].strip()
    return text


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:

    if df is None or df.empty:

        return df

    df = df.copy()

    columns = df.columns.astype(str)

    valid_mask = ~columns.str.startswith("Unnamed", na=False)

    df = df.loc[:, valid_mask]

    df.columns = [normalize_column_label(col) for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    return df


def _sanitize_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    text = str(value)
    text = CONTROL_CHAR_PATTERN.sub(" ", text)
    text = text.replace("\r", " ").replace("\n", " ")
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def sanitize_dataframe_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].map(_sanitize_text)
    return df


def standardize_oferentes_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.rename(columns=OFERENTES_COLUMN_RENAMES)
    if df.columns.duplicated().any():
        duplicate_names = df.columns[df.columns.duplicated(keep=False)].unique()
        for name in duplicate_names:
            cols = [col for col in df.columns if col == name]
            combined = df[cols].bfill(axis=1).iloc[:, 0]
            df[name] = combined
        df = df.loc[:, ~df.columns.duplicated()]
    return df


def reorder_oferentes_catalogs(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    column_order = OFERENTE_BASE_COLUMN_ORDER + CATALOGO_COLUMN_ORDER
    new_df = pd.DataFrame(columns=column_order)
    for column in column_order:
        if column in df.columns:
            new_df[column] = df[column]
        else:
            new_df[column] = ""
    new_df = new_df.drop(columns=["Certificado", "N\u00B0 Cat\u00E1logo"], errors="ignore")
    return new_df


def log_pdf_issue(oferente: str, expected: int | None, extracted: int, reason: str) -> None:
    PDF_EXTRACTION_ISSUES.append(
        {
            "oferente": oferente,
            "esperados": expected,
            "extraidos": extracted,
            "razon": reason,
        }
    )


def build_full_row_signature(row: pd.Series, columns: Sequence[str]) -> tuple[str, ...]:
    return tuple(_sanitize_text(row.get(column, "")) for column in columns)


def log_duplicate(
    dataset: str,
    row_number: int,
    original_row_number: int,
    *,
    row_data: dict[str, Any],
    original_row_data: dict[str, Any],
) -> None:
    entry = DuplicateLogEntry(
        dataset=dataset,
        row_number=row_number,
        original_row_number=original_row_number,
        row_data=row_data,
        original_row_data=original_row_data,
    )
    DUPLICATE_LOGS.append(entry)
    print(f"[CAT][DUP] fila #{row_number} repetida (original #{original_row_number})")
    print(f"    DUP: {row_data}")
    print(f"    ORG: {original_row_data}")


def _column_series(df: pd.DataFrame, column: str) -> pd.Series:

    if column in df.columns:

        return df[column].astype(str).fillna("").str.strip()

    return pd.Series([""] * len(df), index=df.index)


def build_key_series(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:

    if not columns:

        raise ValueError("Se requieren columnas para construir la clave de deduplicaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n.")

    if df is None or df.empty:

        return pd.Series(dtype="object")

    key = _column_series(df, columns[0])

    for column in columns[1:]:

        key = key + "||" + _column_series(df, column)

    key = key.str.strip("|").str.strip()

    key = key.replace("", pd.NA)

    return key











@dataclass



class DriveUploader:



    """PequeÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±o helper para subir archivos a Google Drive."""







    service: Any



    folder_id: str



    drive_id: str | None







    @classmethod



    def from_service_account(cls, credentials_path: Path, folder_id: str) -> "DriveUploader":



        credentials = Credentials.from_service_account_file(



            str(credentials_path),



            scopes=DRIVE_SCOPES,



        )



        service = build("drive", "v3", credentials=credentials, cache_discovery=False)



        drive_id = None



        try:



            folder_metadata = (



                service.files()



                .get(fileId=folder_id, fields="id,name,driveId", supportsAllDrives=True)



                .execute()



            )



            drive_id = folder_metadata.get("driveId")



        except HttpError as exc:



            status = getattr(exc, "resp", None)



            status_code = getattr(status, "status", None)



            if status_code == 404:



                try:



                    drive_metadata = (



                        service.drives()



                        .get(driveId=folder_id, fields="id,name")



                        .execute()



                    )



                    drive_id = drive_metadata["id"]



                except HttpError as drive_exc:



                    raise RuntimeError(



                        f"No se pudo acceder a la carpeta o drive ({folder_id}): {drive_exc}"



                    ) from drive_exc



            else:



                raise RuntimeError(f"No se pudo acceder a la carpeta de Drive ({folder_id}): {exc}") from exc



        return cls(service=service, folder_id=folder_id, drive_id=drive_id)







    def _list_kwargs(self) -> dict[str, Any]:



        kwargs = {



            "supportsAllDrives": True,



            "includeItemsFromAllDrives": True,



            "spaces": "drive",



        }



        if self.drive_id:



            kwargs["driveId"] = self.drive_id



            kwargs["corpora"] = "drive"



        else:



            kwargs["corpora"] = "user"



        return kwargs







    def _write_kwargs(self) -> dict[str, Any]:



        return {"supportsAllDrives": True} if self.drive_id else {}







    def upload_many(self, files: Sequence[Path]) -> None:



        for file_path in files:



            self.upload(file_path)







    def upload(self, file_path: Path) -> None:



        if not file_path.exists():



            raise FileNotFoundError(f"No se encontrÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ el archivo a subir: {file_path}")







        file_name = file_path.name



        query_name = file_name.replace("'", "\\'")



        query = f"name = '{query_name}' and '{self.folder_id}' in parents and trashed = false"



        media = MediaFileUpload(



            str(file_path),



            mimetype=mimetypes.guess_type(file_name)[0] or "application/octet-stream",



            resumable=False,



        )







        list_kwargs = self._list_kwargs()



        write_kwargs = self._write_kwargs()







        try:



            response = (



                self.service.files()



                .list(q=query, fields="files(id,name)", pageSize=1, **list_kwargs)



                .execute()



            )



            files = response.get("files", [])



            if files:



                file_id = files[0]["id"]



                self.service.files().update(



                    fileId=file_id,



                    media_body=media,



                    **write_kwargs,



                ).execute()



                print(f"[Drive] Actualizado {file_name} ({file_id})")



                return







            metadata = {"name": file_name, "parents": [self.folder_id]}



            created = (



                self.service.files()



                .create(



                    body=metadata,



                    media_body=media,



                    fields="id",



                    **write_kwargs,



                )



                .execute()



            )



            print(f"[Drive] Creado {file_name} ({created['id']})")



        except HttpError as exc:



            raise RuntimeError(f"FallÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ la subida a Drive para {file_name}: {exc}") from exc











def create_driver(headless: bool = False) -> Chrome:
    """Crea una instancia de Chrome lista para automatizaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n."""

    PDF_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    for existing in PDF_DOWNLOAD_DIR.glob("*"):
        with contextlib.suppress(Exception):
            if existing.is_file():
                existing.unlink()

    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    prefs = {
        "download.default_directory": str(PDF_DOWNLOAD_DIR),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    if headless:
        chrome_options.add_argument("--headless=new")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    try:
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": str(PDF_DOWNLOAD_DIR)})
    except Exception:
        pass
    return driver


def _reset_pdf_download_dir() -> None:
    """Limpia el directorio temporal utilizado para los catÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡logos descargados."""
    PDF_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    for item in PDF_DOWNLOAD_DIR.glob("*"):
        with contextlib.suppress(Exception):
            if item.is_file():
                item.unlink()


def _wait_for_downloaded_pdf(timeout: int = 90) -> Path | None:
    """Espera a que Chrome complete la descarga de un PDF."""
    end_time = time.time() + timeout
    while time.time() < end_time:
        partials = list(PDF_DOWNLOAD_DIR.glob("*.crdownload"))
        if partials:
            time.sleep(0.5)
            continue
        pdfs = list(PDF_DOWNLOAD_DIR.glob("*.pdf"))
        if pdfs:
            latest = max(pdfs, key=lambda p: p.stat().st_mtime)
            if latest.stat().st_size > 0:
                return latest
        time.sleep(0.5)
    return None


def _extract_text_with_pdfplumber(pdf_path: Path) -> tuple[list[str] | None, str | None]:
    if pdfplumber is None:
        return None, "pdfplumber no estÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ disponible"
    try:
        with pdfplumber.open(str(pdf_path)) as pdf:
            return [page.extract_text() or "" for page in pdf.pages], None
    except Exception as exc:  # pragma: no cover
        return None, f"pdfplumber fallÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ ({exc})"


def _extract_text_with_pypdf2(pdf_path: Path) -> tuple[list[str] | None, str | None]:
    if PdfReader is None:
        return None, "PyPDF2 no estÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ disponible"
    try:
        reader = PdfReader(str(pdf_path))
    except Exception as exc:  # pragma: no cover
        return None, f"no se pudo leer el PDF ({exc})"
    pages: list[str] = []
    for page in reader.pages:
        pages.append(page.extract_text() or "")
    return pages, None



CATALOG_PRODUCT_PATTERN = re.compile(
    r"(?:^|\s)(\d+)\)\s*Nombre de Producto\s*[:=]\s*(.+)",
    re.IGNORECASE,
)

CATALOG_ALIAS_SOURCE = {
    "SubComit\u00E9": "Subcomit\u00E9",
    "SubComite": "Subcomit\u00E9",
    "Pa\u00EDs Origen": "Pa\u00EDs Origen",
    "Pais Origen": "Pa\u00EDs Origen",
    "Pa\u00EDs Procedencia": "Pa\u00EDs Procedencia",
    "Pais Procedencia": "Pa\u00EDs Procedencia",
    "Casa Productora, Laboratorio o Fabricante": "Casa Productora / Laboratorio",
    "Casa Productora, Laboratorio o Marca": "Casa Productora / Laboratorio",
    "Casa Productora, Laboratorio, Fabricante": "Casa Productora / Laboratorio",
    "Marca": "Marca",
    "No. de Cat\u00E1logo o Modelo, Sitio Web": "Modelo / Sitio Web",
    "No. Cat\u00E1logo o Modelo, Sitio Web": "Modelo / Sitio Web",
    "N\u00FAmero de Cat\u00E1logo o Modelo, Sitio Web": "Modelo / Sitio Web",
    "No. Ficha T\u00E9cnica CTNI": "N\u00B0 Ficha CTNI",
    "N\u00B0 Ficha T\u00E9cnica CTNI": "N\u00B0 Ficha CTNI",
    "Nombre Gen\u00E9rico Ficha T\u00E9cnica CTNI": "Nombre Gen\u00E9rico CTNI",
    "Nombre Gen\u00E9rico de la Ficha T\u00E9cnica del CTNI": "Nombre Gen\u00E9rico CTNI",
    "Tipo de Producto": "Tipo de Producto",
    "No. Reg. Sanitario": "N\u00B0 Reg. Sanitario",
    "N\u00B0 Reg. Sanitario": "N\u00B0 Reg. Sanitario",
    "Fecha Vencimiento Reg. Sanitario": "Fecha Venc. Reg. Sanitario",
    "Fecha Vencimiento": "Fecha Venc. Reg. Sanitario",
    "Habilitado por FyD": "Habilitado por FyD",
    "Criterio Tecnico": "Criterio T\u00E9cnico",
    "Criterio T\u00E9cnico": "Criterio T\u00E9cnico",
    "Fecha Vencimiento Criterio Tecnico": "Fecha Vencimiento Criterio T\u00E9cnico",
    "Fecha Vencimiento Criterio T\u00E9cnico": "Fecha Vencimiento Criterio T\u00E9cnico",
    "P. E. I.": "P. E. I.",
    "Permiso Especial Importaci\u00F3n": "P. E. I.",
    "Fecha P. E. I.": "Fecha P. E. I.",
    "Fecha Permiso Importaci\u00F3n": "Fecha P. E. I.",
    "Fecha Captura": "Fecha Captura",
    "Estado": "Estado",
}


def _normalize_catalog_label(label: str) -> str:
    text = unicodedata.normalize("NFKD", label or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace(".", " ").replace(",", " ").replace("\u00BA", " ")
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def _extract_ct_from_text(text: str) -> str:
    if not text:
        return ""
    match = CT_REGEX.search(text.upper())
    if not match:
        return ""
    return match.group(0).strip()


def _build_catalog_alias_map() -> dict[str, str]:
    alias_map: dict[str, str] = {}
    for alias, canonical in CATALOG_ALIAS_SOURCE.items():
        alias_map[_normalize_catalog_label(alias)] = canonical
    return alias_map


def _extract_catalog_expected_total(text_pages: Sequence[str] | None) -> int | None:
    if not text_pages:
        return None
    full_text = "\n".join(text_pages)
    match_total = re.search(r"Cantidad\s+de\s+Cat[a\u00E1]logos\s*:?\s*(\d+)", full_text, re.IGNORECASE)
    if match_total:
        try:
            return int(match_total.group(1))
        except ValueError:
            return None
    return None


def _extract_cell_key_value(cell_text: str, alias_map: dict[str, str]) -> tuple[str, str] | None:
    lines = [ln.strip() for ln in cell_text.splitlines() if ln.strip()]
    if not lines:
        return None
    max_take = min(len(lines), 3)
    for take in range(1, max_take + 1):
        header_candidate = " ".join(lines[:take])
        normalized = _normalize_catalog_label(header_candidate)
        canonical = alias_map.get(normalized)
        if canonical:
            value = " ".join(lines[take:]).strip()
            if canonical == "Criterio T\u00E9cnico" and not value:
                value = _extract_ct_from_text(cell_text)
            return canonical, value
    return None


def _consume_catalog_row(
    row: Sequence[str | None], current: dict[str, Any], alias_map: dict[str, str]
) -> None:
    if not row:
        return
    for cell in row:
        if not cell:
            continue
        extracted = _extract_cell_key_value(cell, alias_map)
        if not extracted:
            continue
        canonical, value = extracted
        current[f"Cat\u00E1logo:{canonical}"] = value


def _parse_catalog_table(
    table: Sequence[Sequence[str | None]], alias_map: dict[str, str]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for raw_row in table:
        first_cell = (raw_row[0] or "").strip() if raw_row else ""
        match = CATALOG_PRODUCT_PATTERN.search(first_cell)
        if match:
            if current:
                rows.append(current)
            current = {
                "Cat\u00E1logo:N\u00B0 Cat\u00E1logo": match.group(1).strip(),
                "Cat\u00E1logo:Nombre del Producto": match.group(2).strip(),
            }
            continue
        if current is None:
            continue
        _consume_catalog_row(raw_row, current, alias_map)
    if current:
        rows.append(current)
    return rows


def _cleanup_catalog_rows(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        row.pop("Cat\u00E1logo:P\u00E1gina", None)
        row.pop("Cat\u00E1logo:Pagina", None)
        row.pop("Cat\u00E1logo:Impresi\u00F3n", None)



def _extract_text_with_pymupdf(pdf_path: Path) -> tuple[list[str] | None, str | None]:
    if fitz is None:
        return None, "PyMuPDF no est\u00E1 disponible"
    doc = None
    try:
        doc = fitz.open(pdf_path)
        pages = [page.get_text("text") or "" for page in doc]
        return pages, None
    except Exception as exc:  # pragma: no cover
        return None, f"PyMuPDF fall\u00F3 ({exc})"
    finally:
        with contextlib.suppress(Exception):
            if doc is not None:
                doc.close()


def _extract_text_with_pdfminer(pdf_path: Path) -> tuple[list[str] | None, str | None]:
    if pdfminer_extract_text is None:
        return None, "pdfminer.six no est\u00E1 disponible"
    try:
        text = pdfminer_extract_text(str(pdf_path))
    except Exception as exc:  # pragma: no cover
        return None, f"pdfminer.six fall\u00F3 ({exc})"
    if not text:
        return [], None
    return [text], None


def _parse_catalog_text_pages(text_pages: Sequence[str] | None) -> list[dict[str, Any]]:
    if not text_pages:
        return []
    alias_map = _build_catalog_alias_map()
    rows: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    pending_canonical: str | None = None
    pending_value: list[str] = []

    def flush_pending() -> None:
        nonlocal pending_canonical, pending_value
        if current and pending_canonical and pending_value:
            value = " ".join(pending_value).strip()
            if value:
                current[f"Cat\u00E1logo:{pending_canonical}"] = value
        pending_canonical = None
        pending_value = []

    for page_text in text_pages:
        for raw_line in (page_text or "").splitlines():
            line = raw_line.strip()
            if not line:
                flush_pending()
                continue
            match = CATALOG_PRODUCT_PATTERN.match(line)
            if match:
                flush_pending()
                if current:
                    rows.append(current)
                current = {
                    "Cat\u00E1logo:N\u00B0 Cat\u00E1logo": match.group(1).strip(),
                    "Cat\u00E1logo:Nombre del Producto": match.group(2).strip(),
                }
                continue
            header_canonical, inline_value = _match_catalog_header(alias_map, line)
            if header_canonical:
                flush_pending()
                pending_canonical = header_canonical
                if inline_value:
                    pending_value = [inline_value]
                    flush_pending()
                else:
                    pending_value = []
                continue
            if pending_canonical:
                pending_value.append(line)
    flush_pending()
    if current:
        rows.append(current)
    return rows


def _match_catalog_header(alias_map: dict[str, str], line: str) -> tuple[str | None, str]:
    for sep in (":", "="):
        if sep in line:
            header, value = line.split(sep, 1)
            normalized = _normalize_catalog_label(header)
            canonical = alias_map.get(normalized)
            if canonical:
                return canonical, value.strip()
    normalized = _normalize_catalog_label(line)
    return alias_map.get(normalized), ""


def _catalog_row_key(row: dict[str, Any], fallback_index: int) -> str:
    number = _sanitize_text(row.get("Cat\u00E1logo:N\u00B0 Cat\u00E1logo", "")) if row else ""
    name = _sanitize_text(row.get("Cat\u00E1logo:Nombre del Producto", "")).lower() if row else ""
    if number or name:
        return f"{number}||{name}"
    return f"__idx__{fallback_index}"


def _index_catalog_rows(rows: Sequence[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[int, dict[str, Any]]]:
    keyed: dict[str, dict[str, Any]] = {}
    indexed: dict[int, dict[str, Any]] = {}
    for idx, row in enumerate(rows):
        key = _catalog_row_key(row, idx)
        keyed.setdefault(key, row)
        indexed[idx] = row
    return keyed, indexed


def _merge_catalog_rows(primary_rows: list[dict[str, Any]], fallback_sets: Sequence[list[dict[str, Any]]]) -> None:
    for fallback in fallback_sets:
        if not fallback:
            continue
        keyed, indexed = _index_catalog_rows(fallback)
        for idx, row in enumerate(primary_rows):
            if not _catalog_row_needs_enrichment(row):
                continue
            key = _catalog_row_key(row, idx)
            candidate = keyed.get(key) or indexed.get(idx)
            if candidate:
                _merge_catalog_row_values(row, candidate)


def _catalog_row_needs_enrichment(row: dict[str, Any]) -> bool:
    for column in CATALOG_FIELD_KEYS:
        if not _sanitize_text(row.get(column, "")):
            return True
    return False


def _merge_catalog_row_values(target: dict[str, Any], source: dict[str, Any]) -> None:
    for column, value in source.items():
        if not column.startswith("Cat\u00E1logo:"):
            continue
        if _sanitize_text(target.get(column, "")):
            continue
        if not _sanitize_text(value):
            continue
        if column == "Cat\u00E1logo:Criterio T\u00E9cnico" and not CRITERIO_TECNICO_PATTERN.match(value.strip()):
            value = _extract_ct_from_text(value)
            if not value:
                continue
        target[column] = value


def _find_catalog_missing_required_fields(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        missing = [field for field in CATALOG_REQUIRED_KEYS if not _sanitize_text(row.get(field, ""))]
        if missing:
            issues.append(
                {
                    "row": idx,
                    "producto": _sanitize_text(row.get("Cat\u00E1logo:Nombre del Producto", "")),
                    "campos": missing,
                }
            )
    return issues

def _parse_catalog_pdf(pdf_path: Path) -> tuple[list[dict[str, Any]], int | None, str | None]:
    """Convierte el PDF descargado en registros tabulares."""
    if pdfplumber is None:
        return [], None, "pdfplumber no est\u00E1 disponible"

    alias_map = _build_catalog_alias_map()
    rows: list[dict[str, Any]] = []
    text_pages: list[str] = []
    try:
        with pdfplumber.open(str(pdf_path)) as pdf_obj:
            for page in pdf_obj.pages:
                page_text = page.extract_text() or ""
                text_pages.append(page_text)
                tables = page.extract_tables() or []
                for table in tables:
                    if not table:
                        continue
                    rows.extend(_parse_catalog_table(table, alias_map))
    except Exception as exc:  # pragma: no cover
        return [], None, f"no se pudo leer el PDF ({exc})"

    expected_total = _extract_catalog_expected_total(text_pages)
    plumber_text_rows = _parse_catalog_text_pages(text_pages)
    fallback_sets: list[list[dict[str, Any]]] = []
    if not rows and plumber_text_rows:
        rows = [row.copy() for row in plumber_text_rows]
    elif plumber_text_rows:
        fallback_sets.append(plumber_text_rows)

    pymupdf_pages, _ = _extract_text_with_pymupdf(pdf_path)
    pymupdf_rows = _parse_catalog_text_pages(pymupdf_pages)
    if not rows and pymupdf_rows:
        rows = [row.copy() for row in pymupdf_rows]
    elif pymupdf_rows:
        fallback_sets.append(pymupdf_rows)

    pdfminer_pages, _ = _extract_text_with_pdfminer(pdf_path)
    pdfminer_rows = _parse_catalog_text_pages(pdfminer_pages)
    if not rows and pdfminer_rows:
        rows = [row.copy() for row in pdfminer_rows]
    elif pdfminer_rows:
        fallback_sets.append(pdfminer_rows)

    if not rows:
        return [], expected_total, "el PDF no conten\u00EDa filas reconocibles"

    if fallback_sets:
        _merge_catalog_rows(rows, fallback_sets)

    _cleanup_catalog_rows(rows)
    return rows, expected_total, None
def table_to_dataframe(table_element) -> pd.DataFrame:



    """Convierte un elemento <table> en DataFrame."""



    html = table_element.get_attribute("outerHTML")



    dfs = pd.read_html(StringIO(html))



    if not dfs:



        return pd.DataFrame()



    df = dfs[0]



    df.columns = [str(col).strip() for col in df.columns]



    return df











def wait_for_nonempty_table(



    driver: Chrome,



    table_xpath: str,



    *,



    timeout: int = 30,



    min_rows: int = 1,



) -> WebElement:



    """Espera a que la tabla tenga registros reales y la devuelve."""







    def _table_if_ready(_driver: Chrome) -> WebElement | bool:



        try:



            table = _driver.find_element(By.XPATH, table_xpath)



            rows = table.find_elements(By.XPATH, "./tbody/tr")



        except (NoSuchElementException, StaleElementReferenceException):



            return False



        valid = 0



        try:



            for row in rows:



                cells = row.find_elements(By.TAG_NAME, "td")



                if not cells:



                    continue



                text = row.text.strip().lower()



                if not text:



                    continue



                if "no se encontraron" in text or "no matching records" in text:



                    continue



                valid += 1



        except StaleElementReferenceException:



            return False



        return table if valid >= min_rows else False







    return WebDriverWait(driver, timeout).until(_table_if_ready)



def wait_prm_idle(driver: Chrome, timeout: int = 15) -> bool:
    """Espera a que no haya postbacks as?ncronos activos (UpdatePanel/jQuery)."""
    end_time = time.time() + timeout
    last_ok = False
    while time.time() < end_time:
        ok_ready = True
        ok_jq = True
        ok_prm = True

        try:
            ok_ready = driver.execute_script("return document.readyState") == "complete"
        except Exception:
            ok_ready = True

        try:
            jq_active = driver.execute_script(
                "return (window.jQuery && jQuery.active) ? jQuery.active : 0"
            )
            ok_jq = jq_active == 0
        except Exception:
            ok_jq = True

        try:
            prm_busy = driver.execute_script(
                "return (window.Sys && Sys.WebForms && Sys.WebForms.PageRequestManager) ? "
                "Sys.WebForms.PageRequestManager.getInstance().get_isInAsyncPostBack() : false;"
            )
            ok_prm = prm_busy is False
        except Exception:
            ok_prm = True

        last_ok = ok_ready and ok_jq and ok_prm
        if last_ok:
            return True
        time.sleep(0.2)
    if not last_ok:
        print("[WAIT][WARN] wait_prm_idle agot? timeout; continuando.")
    return False


def _table_signature(table: WebElement, sample_rows: int = 5) -> str:
    """Genera una huella ligera de la tabla para detectar si cambi?."""
    signature_parts: list[str] = []
    try:
        rows = table.find_elements(By.XPATH, "./tbody/tr")
        for idx, row in enumerate(rows):
            if idx >= sample_rows:
                break
            cells = row.find_elements(By.TAG_NAME, "td")
            signature_parts.append("|".join(cell.text.strip() for cell in cells))
    except Exception:
        return ""
    return "||".join(signature_parts)


def _ensure_oferentes_table(driver: Chrome, wait: WebDriverWait, target_page: int) -> WebElement:
    """Garantiza que estemos en la tabla principal de oferentes (p?gina deseada)."""
    for attempt in range(3):
        try:
            if "Oferentes.aspx" not in driver.current_url:
                driver.get(OFERENTES_URL)
                wait_prm_idle(driver, timeout=20)
            table = wait.until(EC.presence_of_element_located((By.ID, "MainContent_gvOferentes")))
            if target_page > 1:
                driver.execute_script(
                    "__doPostBack(arguments[0], arguments[1]);",
                    "ctl00$MainContent$gvOferentes",
                    f"Page${target_page}",
                )
                wait_prm_idle(driver, timeout=20)
                table = wait.until(EC.presence_of_element_located((By.ID, "MainContent_gvOferentes")))
            return table
        except TimeoutException:
            continue
    raise TimeoutException("No se pudo recuperar la tabla principal de oferentes.")


def _scrape_catalog_via_print_button(
    driver: Chrome,
    wait: WebDriverWait,
    base_entry: dict[str, Any],
    oferente_label: str,
) -> tuple[list[dict[str, Any]], str | None]:
    wait_prm_idle(driver, timeout=30)
    with contextlib.suppress(TimeoutException):
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[contains(@id,'btnVolver') or contains(@value,'Volver')]")
            )
        )

    _reset_pdf_download_dir()
    button_xpath = (
        "//input[contains(@value,'Cat\u00E1logo Completo') or contains(@value,'Cat\u00E1logo Completo') "
        "or contains(@id,'btnImprimir') or contains(@id,'btnPrintCat')]"
        "|//button[contains(@value,'Cat\u00E1logo Completo') or contains(@value,'Cat\u00E1logo Completo') "
        "or contains(@id,'btnImprimir') or contains(@id,'btnPrintCat')]"
        "|//a[contains(normalize-space(.),'Cat\u00E1logo Completo') or contains(normalize-space(.),'Cat\u00E1logo Completo')]"
    )

    try:
        print_button = wait.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    button_xpath,
                )
            )
        )
    except TimeoutException:
        log_pdf_issue(oferente_label, None, 0, "boton 'Imprimir Cat\u00E1logo Completo' no disponible")
        return [], "boton 'Imprimir Cat\u00E1logo Completo' no disponible"

    print(f"[PDF] '{oferente_label}': se pulsa 'Imprimir Cat\u00E1logo Completo'")
    main_handle = driver.current_window_handle
    handles_before = set(driver.window_handles)

    try:
        print_button.click()
    except WebDriverException as exc:
        log_pdf_issue(oferente_label, None, 0, f"no se pudo hacer clic en Imprimir: {exc}")
        return [], f"no se pudo hacer clic en Imprimir: {exc}"

    print(f"[PDF] '{oferente_label}': esperando archivo PDF...")
    pdf_path = _wait_for_downloaded_pdf()
    if not pdf_path:
        log_pdf_issue(oferente_label, None, 0, "no se detecto el PDF del Cat\u00E1logo")
        return [], "no se detecto el PDF del Cat\u00E1logo"

    handles_after = set(driver.window_handles)
    new_handles = handles_after - handles_before
    for handle in new_handles:
        with contextlib.suppress(Exception):
            driver.switch_to.window(handle)
            driver.close()
    with contextlib.suppress(Exception):
        driver.switch_to.window(main_handle)

    print(f"[PDF] '{oferente_label}': descarga completada ({pdf_path.name})")
    rows_raw, expected_total, error = _parse_catalog_pdf(pdf_path)

    keep_env = os.environ.get("CATALOG_KEEP_PDF")
    keep_path: Path | None = None
    if keep_env:
        if keep_env.strip() in {"1", "true", "TRUE"}:
            keep_path = BASE_DIR / "debug_catalog.pdf"
        else:
            keep_path = Path(keep_env).expanduser()
    if keep_path:
        with contextlib.suppress(Exception):
            keep_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(pdf_path, keep_path)
            print(f"[PDF] '{oferente_label}': copia debug -> {keep_path}")

    removed = False
    if not keep_env:
        with contextlib.suppress(Exception):
            pdf_path.unlink()
            removed = True
    if removed:
        print(f"[PDF] '{oferente_label}': PDF temporal eliminado")

    if error:
        log_pdf_issue(oferente_label, expected_total, 0, error)
        print(f"[PDF][WARN] '{oferente_label}': {error}")
        return [], error

    mismatch_reason = ""
    if expected_total is not None:
        print(f"[PDF] '{oferente_label}': extraidos={len(rows_raw)} vs esperados={expected_total}")
        if len(rows_raw) != expected_total:
            mismatch_reason = (
                f"PDF incompleto (esperados={expected_total}, extraidos={len(rows_raw)})"
            )
            print(f"[PDF][WARN] '{oferente_label}': {mismatch_reason}")
            log_pdf_issue(oferente_label, expected_total, len(rows_raw), mismatch_reason)
    else:
        print(f"[PDF] '{oferente_label}': extraidos={len(rows_raw)} (sin total declarado)")

    missing_required = _find_catalog_missing_required_fields(rows_raw)
    if missing_required:
        missing_msg = f"{len(missing_required)} cat\u00E1logos con campos clave incompletos"
        print(f"[PDF][WARN] '{oferente_label}': {missing_msg}")
        log_pdf_issue(oferente_label, expected_total, len(rows_raw), missing_msg)
        for issue in missing_required[:5]:
            campos = ", ".join(field.replace("Cat\u00E1logo:", "") for field in issue["campos"])
            producto = issue["producto"] or "Sin nombre"
            print(f"[PDF][WARN]   - #{issue['row']} '{producto}' faltan: {campos}")

    rows: list[dict[str, Any]] = []
    for catalog_row in rows_raw:
        combined = base_entry.copy()
        for col_name, value in catalog_row.items():
            final_name = col_name.replace("Cat\u00E1logo:", "", 1)
            combined[final_name] = value
        rows.append(combined)
    print(f"[PDF] '{oferente_label}': {len(rows)} filas extraidas del PDF")
    return rows, mismatch_reason or None
def clean_catalog_dataframe(df: pd.DataFrame) -> pd.DataFrame:



    """Elimina filas del catÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡logo que corresponden a paginadores o filas vacÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­as."""



    if df is None or df.empty:



        return pd.DataFrame()



    df = df.loc[:, ~df.columns.str.contains("^Unnamed", case=False)]



    if df.empty:



        return df







    pagination_keywords = {"...", "?", "anterior", "siguiente", "ultimo", "primero", "pagina"}



    pagination_pattern = re.compile(r"^[\d\s.,;:<>?????-]+$")







    def _strip_accents(text: str) -> str:



        return "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))







    def _is_pagination_row(row: pd.Series) -> bool:



        tokens = []



        for value in row.tolist():



            if pd.isna(value):



                continue



            text = str(value).strip()



            if not text:



                continue



            tokens.append(text)



        if not tokens:



            return True



        normalized = [_strip_accents(token).lower() for token in tokens]



        return all(



            token in pagination_keywords or pagination_pattern.fullmatch(token)



            for token in normalized



        )







    mask = ~df.apply(_is_pagination_row, axis=1)



    return df[mask].reset_index(drop=True)













def drop_numeric_pagination_rows(df: pd.DataFrame) -> pd.DataFrame:

    """Quita filas que solo contienen numeraciones o puntos usados como paginadores."""

    if df is None or df.empty:

        return pd.DataFrame()



    def _is_pagination_row(row: pd.Series) -> bool:

        has_value = False

        for value in row:

            if pd.isna(value):

                continue

            text = str(value).strip()

            if not text:

                continue

            has_value = True

            normalized = text.replace('.', '').replace('ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦', '').replace(' ', '')

            if not normalized:

                continue

            if not normalized.isdigit():

                return False

        return has_value



    mask = df.apply(_is_pagination_row, axis=1)

    return df[~mask].reset_index(drop=True)





def _find_next_postback(



    driver: Chrome,



    current_page: int,



    *,



    table: WebElement | None = None,



):



    search_context = table if table is not None else driver



    anchors = search_context.find_elements(



        By.XPATH, ".//a[contains(@href,'Page$')]"



    )



    expected = f"Page${current_page + 1}"



    for anchor in anchors:



        args = parse_postback_arguments(anchor)



        if not args:



            continue



        text = anchor.text.strip()



        href = anchor.get_attribute("href") or ""



        if text.isdigit() and int(text) == current_page + 1:



            return args



        if expected in href:



            return args



    return None











def click_if_present(driver: Chrome, xpath: str, timeout: int = 5) -> bool:



    """Intenta hacer clic en un elemento si existe."""



    try:



        btn = WebDriverWait(driver, timeout).until(



            EC.element_to_be_clickable((By.XPATH, xpath))



        )



        btn.click()



        return True



    except TimeoutException:



        return False











def scrape_ctni_fichas(driver: Chrome, max_pages: int = 0) -> pd.DataFrame:



    """Extrae las fichas tÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â©cnicas publicadas."""



    driver.get(CTNI_URL)
    print("[LOG] CTNI: portal cargado, esperando tablas...")



    wait = WebDriverWait(driver, 30)



    dataframes: list[pd.DataFrame] = []



    page = 1



    while True:



        table_xpath = "//table[@id='FichasTable']"



        table = wait.until(EC.presence_of_element_located((By.XPATH, table_xpath)))



        wait_for_nonempty_table(driver, table_xpath)



        df = table_to_dataframe(table)



        df["PÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡gina"] = page



        dataframes.append(df)
        print(f"[LOG] CTNI: pagina {page} procesada ({len(df)} filas)")



        if max_pages > 0 and page >= max_pages:



            break



        next_clicked = click_if_present(



            driver,



            "//li[@id='FichasTable_next' and not(contains(@class,'disabled'))]/a",



        )



        if not next_clicked:



            break



        time.sleep(1.5)



        page += 1



    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()











def scrape_criterios(driver: Chrome, max_pages: int = 0) -> pd.DataFrame:



    """Extrae criterios t?cnicos desde el sitio ASP.NET."""



    driver.get(CRITERIOS_URL)
    print("[LOG] Criterios: portal cargado, preparando grid...")



    wait = WebDriverWait(driver, 30)







    # Algunas veces el grid se llena solo tras pulsar "Mostrar Todos".



    try:



        mostrar_todos = wait.until(



            EC.element_to_be_clickable((By.ID, "ctl00_ContentPlaceHolder1_cmdMostrarTodos"))



        )



        mostrar_todos.click()



        time.sleep(2)



    except TimeoutException:



        pass







    dataframes: list[pd.DataFrame] = []



    page = 1



    while True:



        table = wait.until(



            EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_Grddata"))



        )



        df = drop_numeric_pagination_rows(table_to_dataframe(table))

        df["PÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡gina"] = page



        dataframes.append(df)
        print(f"[LOG] Criterios: pagina {page} procesada ({len(df)} filas utiles)")



        if max_pages > 0 and page >= max_pages:



            break







        next_postback = _find_next_postback(driver, page, table=table)



        if not next_postback:



            break







        try:



            driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_postback)



        except WebDriverException:



            break



        time.sleep(2)



        page += 1



    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()











def parse_postback_arguments(link) -> tuple[str, str] | None:



    """Extrae target y argumento de un enlace javascript:__doPostBack."""



    href = link.get_attribute("href")



    if not href:



        return None



    match = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", href)



    if not match:



        return None



    return match.group(1), match.group(2)


def _find_catalog_postback(source_row: WebElement) -> tuple[str, str] | None:
    """Ubica el enlace de 'Catalogo' tolerando variaciones en el href o texto."""
    xpath_variants = [
        ".//a[contains(@href,'Catalogo$')]",
        ".//a[contains(@href,'Catálogo$')]",
        ".//a[contains(@href,'CatalogoCompleto')]",
        ".//a[contains(@href,'CatálogoCompleto')]",
    ]
    for variant in xpath_variants:
        with contextlib.suppress(NoSuchElementException):
            anchor = source_row.find_element(By.XPATH, variant)
            postback = parse_postback_arguments(anchor)
            if postback:
                return postback
    anchors = source_row.find_elements(By.TAG_NAME, "a")
    for anchor in anchors:
        href = anchor.get_attribute("href") or ""
        text = anchor.text or ""
        normalized_href = href.lower()
        normalized_text = text.lower()
        if any(keyword in normalized_href for keyword in ("catalog", "catálogo")) or any(
            keyword in normalized_text for keyword in ("catalog", "catálogo")
        ):
            postback = parse_postback_arguments(anchor)
            if postback:
                return postback
    return None











def scrape_catalogo_public(driver: Chrome, max_pages: int = 0) -> pd.DataFrame:
    global CATALOG_COUNTERS
    driver.get(CATALOGO_PUBLICO_URL)
    print("[LOG] Cat\u00E1logo p?blico: portal cargado, esperando tabla...")
    records: list[dict[str, Any]] = []
    signature_map: dict[tuple[str, ...], tuple[int, dict[str, Any]]] = {}
    page = 1
    row_sequence = 0

    stagnant_retries = 0
    signature_retries = 0
    last_signature: str | None = None
    while True:
        try:
            table = wait_for_nonempty_table(
                driver,
                CATALOGO_PUBLIC_TABLE_XPATH,
                timeout=40,
                min_rows=2,
            )
        except TimeoutException:
            print(f"[CAT][WARN] No se encontr? tabla ?til en la p?gina {page}.")
            break

        df_page = table_to_dataframe(table)
        df_page = drop_numeric_pagination_rows(df_page)
        base_columns = list(df_page.columns)
        rows_total = len(df_page)
        added = 0
        current_signature = _table_signature(table)
        if last_signature is not None and current_signature == last_signature:
            signature_retries += 1
            if signature_retries < 3:
                print(
                    f"[CAT][WARN] P?gina {page} no cambi? (huella repetida). "
                    f"Reintentando ({signature_retries}/3)..."
                )
                wait_prm_idle(driver, timeout=15)
                time.sleep(1.5)
                continue
            print(
                "[CAT][ERR] La tabla no cambi? tras 3 intentos; se saltar? a la p?gina siguiente."
            )
            next_postback = _find_next_postback(driver, page, table=table)
            if not next_postback:
                print("[CAT][ERR] No se pudo localizar enlace a la siguiente p?gina; se detiene.")
                break
            try:
                driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_postback)
            except WebDriverException as exc:
                print(f"[NAV][ERR] No se pudo saltar a la p?gina siguiente ({exc}); se detiene.")
                break
            try:
                WebDriverWait(driver, 25).until(EC.staleness_of(table))
            except TimeoutException:
                print("[CAT][ERR] La tabla no se actualiz? tras intentar saltar; se detiene.")
                break
            wait_prm_idle(driver, timeout=20)
            time.sleep(1.0)
            page += 1
            last_signature = None
            signature_retries = 0
            stagnant_retries = 0
            continue
        else:
            signature_retries = 0
        last_signature = current_signature

        for _, row in df_page.iterrows():
            row_sequence += 1
            row_dict = {str(col): _sanitize_text(row.get(col, "")) for col in df_page.columns}
            has_payload = any(row_dict.get(col, "") for col in base_columns)
            if not has_payload:
                continue
            signature = tuple(row_dict.get(col, "") for col in base_columns)
            if signature in signature_map:
                original_row_number, original_row = signature_map[signature]
                CATALOG_COUNTERS.duplicate_rows += 1
                log_duplicate(
                    "oferentes_Cat\u00E1logos",
                    row_sequence,
                    original_row_number,
                    row_data=row_dict,
                    original_row_data=original_row,
                )
                continue
            signature_map[signature] = (row_sequence, row_dict.copy())
            records.append(row_dict)
            CATALOG_COUNTERS.unique_rows += 1
            added += 1

        CATALOG_COUNTERS.pages += 1
        print(
            f"[CAT] P?gina {page}: {added} filas nuevas (total p?gina={rows_total}, acumulado={len(records)})"
        )

        if max_pages > 0 and page >= max_pages:
            break

        next_postback = _find_next_postback(driver, page, table=table)
        if not next_postback:
            break
        try:
            driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_postback)
        except WebDriverException as exc:
            print(f"[NAV][WARN] No se pudo avanzar a la p?gina {page + 1}: {exc}")
            break
        try:
            WebDriverWait(driver, 25).until(EC.staleness_of(table))
        except TimeoutException:
            stagnant_retries += 1
            print(
                f"[CAT][WARN] La tabla no cambi? tras solicitar la p?gina {page + 1} "
                f"(reintento {stagnant_retries}/3)."
            )
            if stagnant_retries >= 3:
                print("[CAT][ERR] Se alcanz? el m?ximo de reintentos; se detiene el scraping de Cat\u00E1logos.")
                break
        else:
            stagnant_retries = 0
            signature_retries = 0
        wait_prm_idle(driver, timeout=20)
        time.sleep(1.0)
        page += 1

    return pd.DataFrame(records)


def scrape_oferentes(driver: Chrome, max_pages: int = 0) -> pd.DataFrame:



    """Descarga los oferentes y sus catÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡logos completos."""



    driver.get(OFERENTES_URL)
    print("[LOG] Oferentes: portal cargado, esperando listado...")



    wait = WebDriverWait(driver, 30)



    time.sleep(3)



    records: list[dict] = []



    page = 1



    table = _ensure_oferentes_table(driver, wait, page)



    while True:



        header_cells = table.find_elements(By.XPATH, ".//thead//th")



        headers = [cell.text.strip() or f"Columna {idx+1}" for idx, cell in enumerate(header_cells)]



        row_selector = ".//tbody/tr[not(contains(@style,'display:none'))]"



        row_idx = 0
        page_oferentes = 0

        while True:



            rows = table.find_elements(By.XPATH, row_selector)



            if row_idx >= len(rows):



                break



            row = rows[row_idx]



            cells = row.find_elements(By.XPATH, "./td")



            text_vals = [cell.text.strip() for cell in cells]



            base_entry = {
                "Oferente:No. de Oferente": text_vals[0] if len(text_vals) > 0 else "",
                "Oferente:Oferente": text_vals[1] if len(text_vals) > 1 else "",
                "Oferente:Vencimiento Cert.": text_vals[2] if len(text_vals) > 2 else "",
                "Oferente:Representante Legal": text_vals[3] if len(text_vals) > 3 else "",
                "Oferente:Contacto": text_vals[4] if len(text_vals) > 4 else "",
                "Oferente:Tel\u00E9fono": text_vals[5] if len(text_vals) > 5 else "",
                "Oferente:Correo": text_vals[6] if len(text_vals) > 6 else "",
            }




            if len(cells) > 9:



                base_entry["Oferente:Certificado"] = cells[9].text.strip()



            catalog_link = _find_catalog_postback(row)







            oferente_label = (

                base_entry.get("Oferente:Oferente")

                or base_entry.get("Oferente:No. de Oferente")

                or f"Fila {page_oferentes + 1}"

            )

            page_oferentes += 1

            print(

                f"[LOG] Oferentes: procesando '{oferente_label}' (p?gina {page}, fila {page_oferentes})"

            )



            catalog_rows_added = False
            catalog_rows_total = 0



            normalized_label = (oferente_label or "").upper()
            if any(skip_name.upper() in normalized_label for skip_name in SKIP_OFERENTE_NAMES):
                reason = "omitido por configuraci?n (lista de exclusi?n)"
                print(f"[WARN] Oferentes: se omite '{oferente_label}' -> {reason}")
                SKIPPED_OFERENTES.append(
                    {
                        "oferente": oferente_label,
                        "pagina_listado": str(page),
                        "razon": reason,
                    }
                )
                row_idx += 1
                continue




            if catalog_link:

                target, argument = catalog_link

                driver.execute_script("__doPostBack(arguments[0], arguments[1]);", target, argument)

                catalog_page = 1
                catalog_stalled_reason: str | None = None

                if CATALOG_SCRAPE_MODE == "download":

                    rows, reason = _scrape_catalog_via_print_button(
                        driver,
                        wait,
                        base_entry,
                        oferente_label,
                    )

                    if rows:
                        for combined in rows:
                            records.append(combined)
                        catalog_rows_total = len(rows)
                        catalog_rows_added = True
                    else:
                        catalog_stalled_reason = reason or "falla al descargar el Cat\u00E1logo completo"

                else:

                    catalog_table = wait.until(
                        EC.presence_of_element_located((By.XPATH, "//table[contains(@id,'gvCat\u00E1logo')]"))
                    )

                    last_catalog_signature: str | None = None
                    signature_retries = 0
                    empty_retries = 0

                    while True:

                        catalog_df = clean_catalog_dataframe(table_to_dataframe(catalog_table))

                        print(
                            f"[LOG] CatÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡logo pÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡gina {catalog_page} para '{oferente_label}': {len(catalog_df)} filas"
                        )

                        signature = _table_signature(catalog_table)
                        if last_catalog_signature is not None and signature == last_catalog_signature:
                            signature_retries += 1
                        else:
                            signature_retries = 0
                            last_catalog_signature = signature

                        if signature_retries >= 3:
                            catalog_stalled_reason = f"tabla sin cambios (pagina {catalog_page})"
                            break

                        if catalog_df.empty:
                            empty_retries += 1
                        else:
                            empty_retries = 0

                        if empty_retries >= 3:
                            catalog_stalled_reason = f"Cat\u00E1logo vacio o repetido (pagina {catalog_page})"
                            break

                        if not catalog_df.empty:
                            catalog_rows_added = True

                        for _, catalog_row in catalog_df.iterrows():

                            combined = base_entry.copy()

                            for col_name, value in catalog_row.items():
                                combined[f"CatÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡logo:{col_name}"] = value

                            records.append(combined)
                            catalog_rows_total += 1

                        next_postback = _find_next_postback(
                            driver,
                            catalog_page,
                            table=catalog_table,
                        )

                        if not next_postback:
                            break

                        previous_table = catalog_table

                        driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_postback)

                        try:
                            WebDriverWait(driver, 15).until(EC.staleness_of(previous_table))
                            catalog_table = WebDriverWait(driver, 15).until(
                                EC.presence_of_element_located((By.XPATH, "//table[contains(@id,'gvCat\u00E1logo')]"))
                            )
                        except TimeoutException:
                            catalog_stalled_reason = f"timeout cambiando a la pagina {catalog_page + 1}"
                            break

                        catalog_page += 1

                if catalog_rows_total:

                    print(
                        f"[LOG] Oferentes: '{oferente_label}' acumulÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ {catalog_rows_total} filas de catÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡logos"
                    )

                else:

                    print(
                        f"[LOG] Oferentes: '{oferente_label}' no mostrÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ catÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡logos"
                    )

                if catalog_stalled_reason:

                    print(
                        f"[WARN] Oferentes: '{oferente_label}' se saltÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ el catÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡logo -> {catalog_stalled_reason}"
                    )
                    SKIPPED_OFERENTES.append(
                        {
                            "oferente": oferente_label,
                            "pagina_listado": str(page),
                            "razon": catalog_stalled_reason,
                        }
                    )

                clicked_back = click_if_present(

                    driver,

                    "//input[@id='MainContent_btnVolver']",

                    timeout=5,

                )

                if not clicked_back:

                    click_if_present(

                        driver,

                        "//input[contains(@id,'btnCerrar') or contains(@value,'Cerrar') or contains(@value,'Regresar')]",

                        timeout=5,

                    )

                try:

                    wait.until(EC.presence_of_element_located((By.ID, "MainContent_gvOferentes")))

                    table = driver.find_element(By.ID, "MainContent_gvOferentes")

                except TimeoutException:

                    print("[Oferentes][WARN] No se pudo volver al listado; se recarga la pÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡gina.")

                    table = _ensure_oferentes_table(driver, wait, page)


            if not catalog_rows_added:



                row_idx += 1



                continue







            row_idx += 1



        if max_pages > 0 and page >= max_pages:



            break







        next_page_postback = _find_next_postback(driver, page, table=table)



        if not next_page_postback:



            break



        try:



            driver.execute_script("__doPostBack(arguments[0], arguments[1]);", *next_page_postback)



        except WebDriverException:



            break



        try:



            wait.until(EC.presence_of_element_located((By.ID, "MainContent_gvOferentes")))



            table = driver.find_element(By.ID, "MainContent_gvOferentes")



        except TimeoutException:



            print("[Oferentes][WARN] No se pudo cargar la siguiente p?gina; se recarga el listado.")



            target_page = page + 1



            table = _ensure_oferentes_table(driver, wait, target_page)



            page = target_page



            continue



        time.sleep(1)



        print(
            f"[LOG] Oferentes: p?gina {page} completada ({page_oferentes} oferentes revisados)"
        )



        page += 1







    return pd.DataFrame(records)











def _deduplicate_dataframe(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if df is not None else [])
    keys = build_key_series(df, columns)
    keep_mask = ~(keys.duplicated(keep="first") & keys.notna())
    return df[keep_mask].reset_index(drop=True)


def export_results(
    results: Iterable[ScrapeResult],
    output_dir: Path,
    *,
    drive_uploader: DriveUploader | None = None,
    ignore_existing: bool = True,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    saved_paths: list[Path] = []
    for result in results:
        export_format = "csv" if result.name in CSV_OUTPUT_DATASETS else "xlsx"
        suffix = ".csv" if export_format == "csv" else ".xlsx"
        output_path = output_dir / f"{result.name}{suffix}"
        df = normalize_dataframe_columns(result.dataframe)
        if result.name == "oferentes_Cat\u00E1logos":
            df = standardize_oferentes_columns(df)
        dedup_columns = DEDUP_CONFIG.get(result.name)
        new_rows_count = len(df)
        if dedup_columns:
            existing_len = 0
            if not ignore_existing and output_path.exists():
                if export_format == "csv":
                    existing_df = normalize_dataframe_columns(pd.read_csv(output_path))
                else:
                    existing_df = normalize_dataframe_columns(pd.read_excel(output_path))
                existing_len = len(existing_df)
                df = pd.concat([existing_df, df], ignore_index=True)
            df = _deduplicate_dataframe(df, dedup_columns)
            if not ignore_existing:
                new_rows_count = max(0, len(df) - existing_len)
            else:
                new_rows_count = len(df)
        if result.name == "oferentes_Cat\u00E1logos":
            df = reorder_oferentes_catalogs(df)
        df = sanitize_dataframe_for_excel(df)
        if export_format == "csv":
            df.to_csv(output_path, index=False)
        else:
            df.to_excel(output_path, index=False)
        if dedup_columns:
            print(f"[LOG] {result.name}: aÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±adidos {new_rows_count} nuevos (total {len(df)})")
        else:
            print(f"[LOG] {result.name}: exportadas {len(df)} filas (sin deduplicaciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n)")
        print(f"[OK] {result.name} -> {output_path}")
        saved_paths.append(output_path)
        if drive_uploader is not None:
            drive_uploader.upload(output_path)
    return saved_paths



def export_duplicate_report(
    output_dir: Path,
    *,
    drive_uploader: DriveUploader | None = None,
) -> Path | None:
    if not DUPLICATE_LOGS:
        print("[DUP] No se detectaron filas duplicadas durante la corrida.")
        return None
    rows: list[dict[str, Any]] = []
    for entry in DUPLICATE_LOGS:
        rows.append(
            {
                "dataset": entry.dataset,
                "fila_duplicada": entry.row_number,
                "fila_original": entry.original_row_number,
                "datos_duplicado": json.dumps(entry.row_data, ensure_ascii=False),
                "datos_original": json.dumps(entry.original_row_data, ensure_ascii=False),
            }
        )
    df = pd.DataFrame(rows)
    df = sanitize_dataframe_for_excel(df)
    path = output_dir / "duplicates_oferentes_Cat\u00E1logos.xlsx"
    df.to_excel(path, index=False)
    print(f"[DUP] Reporte de duplicados -> {path}")
    if drive_uploader is not None:
        drive_uploader.upload(path)
    return path



def print_catalog_summary() -> None:
    if (
        CATALOG_COUNTERS.pages == 0
        and CATALOG_COUNTERS.unique_rows == 0
        and CATALOG_COUNTERS.duplicate_rows == 0
    ) and not SKIPPED_OFERENTES:
        return
    print(
        f"[REPORT] Cat\u00E1logos ?nicos(run)={CATALOG_COUNTERS.unique_rows} | duplicados(run)={CATALOG_COUNTERS.duplicate_rows}"
    )
    if SKIPPED_OFERENTES:
        print("[REPORT] Oferentes saltados por Cat\u00E1logo:")
        for entry in SKIPPED_OFERENTES:
            print(
                "  - {oferente} (p?gina listado {page}): {reason}".format(
                    oferente=entry.get("oferente", "?"),
                    page=entry.get("pagina_listado", "?"),
                    reason=entry.get("razon", "motivo desconocido"),
                )
            )
    else:
        print("[REPORT] Oferentes saltados: ninguno.")



def print_pdf_issue_summary() -> None:
    if not PDF_EXTRACTION_ISSUES:
        print("[PDF] Todos los catÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡logos PDF coincidieron con el total declarado.")
        return
    print("[PDF][WARN] CatÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡logos con discrepancias en PDF:")
    for entry in PDF_EXTRACTION_ISSUES:
        print(
            f"  - {entry['oferente']}: extraÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­dos={entry['extraidos']} vs esperados={entry['esperados']} -> {entry['razon']}"
        )


def parse_args(argv: list[str]) -> argparse.Namespace:



    parser = argparse.ArgumentParser(



        description="Scraper de fichas, criterios y oferentes del MINSA."



    )



    parser.add_argument(



        "--headless",



        action="store_true",



        help="Ejecuta Chrome en modo headless.",



    )



    parser.add_argument(



        "--max-pages",



        type=int,



        default=0,



        help="NÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âºmero mÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ximo de pÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ginas por portal (0 recorre todas).",



    )



    parser.add_argument(



        "--skip",



        choices=["fichas", "criterios", "oferentes"],



        nargs="*",



        default=[],



        help="Portales que quieres omitir en esta ejecuciÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³n.",



    )
    parser.add_argument(



        "--mode",



        choices=["all", "fichas", "criterios", "Cat\u00E1logo"],



        default="all",



        help="Define qu? portal ejecutar (all=fichas+criterios+Cat\u00E1logo).",



    )



    parser.add_argument(



        "--oferentes-source",



        choices=["catalog", "legacy"],



        default="legacy",



        help=(



            "Origen del dataset de oferentes: 'catalog' usa el Cat\u00E1logo p?blico, 'legacy' el flujo antiguo."



        ),



    )



    parser.add_argument(



        "--catalog-mode",



        choices=["legacy", "download"],



        default="download",



        help=(



            "Modo de extracci?n de Cat\u00E1logos por oferente: 'legacy' pagina la tabla, "



            "'download' usa el bot?n 'Imprimir Cat\u00E1logo Completo'."



        ),



    )







    parser.add_argument(



        "--respect-existing-output",



        action="store_true",



        help="Mantiene los datos previos de los Excel y suma nuevos registros en vez de recalcular desde cero.",



    )




    parser.add_argument(



        "--upload-to-drive",



        dest="upload_to_drive",



        action="store_true",



        default=True,



        help="Sube los archivos generados a Google Drive (por defecto activado).",



    )



    parser.add_argument(



        "--no-upload",



        dest="upload_to_drive",



        action="store_false",



        help="Desactiva la subida autom?tica a Drive para esta ejecuci?n.",



    )



    parser.add_argument(



        "--drive-folder-id",



        default=DEFAULT_DRIVE_FOLDER_ID,



        help="ID de la carpeta de Drive destino.",



    )



    parser.add_argument(



        "--drive-credentials",



        type=Path,



        default=DEFAULT_DRIVE_CREDENTIALS,



        help="Ruta del JSON del service account con permisos de Drive.",



    )



    return parser.parse_args(argv)











def resolve_drive_uploader(args: argparse.Namespace) -> DriveUploader | None:



    if not args.upload_to_drive:



        return None



    if not args.drive_folder_id:



        raise SystemExit("Debes indicar --drive-folder-id para poder subir a Drive.")



    credentials_path = args.drive_credentials



    if credentials_path is None:



        raise SystemExit("Debes indicar --drive-credentials con el JSON del service account.")



    credentials_path = Path(credentials_path)



    if not credentials_path.exists():



        raise SystemExit(f"No se encontrÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢?ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â³ el archivo de credenciales: {credentials_path}")



    print(f"[Drive] Carpeta destino: {args.drive_folder_id}")



    return DriveUploader.from_service_account(credentials_path, args.drive_folder_id)











def run_with_driver(



    scraper: Callable[[Chrome, int], pd.DataFrame],



    *,



    headless: bool,



    max_pages: int,



) -> pd.DataFrame:



    driver = create_driver(headless=headless)



    try:



        return scraper(driver, max_pages)



    finally:



        driver.quit()











def main(argv: list[str] | None = None) -> int:

    global SKIPPED_OFERENTES, CATALOG_SCRAPE_MODE, PDF_EXTRACTION_ISSUES

    SKIPPED_OFERENTES = []
    PDF_EXTRACTION_ISSUES = []

    args = parse_args(argv or sys.argv[1:])
    if args.catalog_mode != "download":
        print("[WARN] Se fuerza --catalog-mode=download; el modo legacy estÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ deshabilitado.")
    args.catalog_mode = "download"
    drive_uploader = resolve_drive_uploader(args)
    CATALOG_SCRAPE_MODE = args.catalog_mode

    def _mode_allows(target: str) -> bool:
        if args.mode == "all":
            return True
        if target == "oferentes":
            return args.mode == "Cat\u00E1logo"
        return args.mode == target

    exported_any = False
    ran_catalog = False

    if _mode_allows("fichas") and "fichas" not in args.skip:
        print("[LOG] Iniciando scraping de fichas CTNI...")
        df_fichas = run_with_driver(
            scrape_ctni_fichas,
            headless=args.headless,
            max_pages=args.max_pages,
        )
        export_results(
            [ScrapeResult("fichas_ctni", df_fichas)],
            DEFAULT_OUTPUT_DIR,
            drive_uploader=drive_uploader,
            ignore_existing=not args.respect_existing_output,
        )
        print(f"[LOG] Fichas CTNI: recuperadas {len(df_fichas)} filas.")
        exported_any = True

    if _mode_allows("criterios") and "criterios" not in args.skip:
        print("[LOG] Iniciando scraping de criterios t?cnicos...")
        df_criterios = run_with_driver(
            scrape_criterios,
            headless=args.headless,
            max_pages=args.max_pages,
        )
        export_results(
            [ScrapeResult("criterios_tecnicos", df_criterios)],
            DEFAULT_OUTPUT_DIR,
            drive_uploader=drive_uploader,
            ignore_existing=not args.respect_existing_output,
        )
        print(f"[LOG] Criterios t?cnicos: recuperadas {len(df_criterios)} filas.")
        exported_any = True

    if _mode_allows("oferentes") and "oferentes" not in args.skip:
        print(f"[LOG] Iniciando scraping de oferentes ({args.oferentes_source})...")
        scraper_fn = (
            scrape_catalogo_public if args.oferentes_source == "catalog" else scrape_oferentes
        )
        df_oferentes = run_with_driver(
            scraper_fn,
            headless=args.headless,
            max_pages=args.max_pages,
        )
        export_results(
            [ScrapeResult("oferentes_Cat\u00E1logos", df_oferentes)],
            DEFAULT_OUTPUT_DIR,
            drive_uploader=drive_uploader,
            ignore_existing=not args.respect_existing_output,
        )
        print(f"[LOG] Oferentes: recuperadas {len(df_oferentes)} filas.")
        ran_catalog = True
        exported_any = True

    if ran_catalog:
        export_duplicate_report(DEFAULT_OUTPUT_DIR, drive_uploader=drive_uploader)
        print_catalog_summary()
        print_pdf_issue_summary()

    if not exported_any:
        print("[WARN] No se ejecut? ning?n portal (revisa --mode y --skip).")

    return 0


if __name__ == "__main__":



    raise SystemExit(main())

















