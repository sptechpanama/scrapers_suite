"""Utilities for ficha detection shared between scraping scripts.

This module centralizes the logic used to load ficha definitions and to
locate them inside arbitrary text blocks. Patterns are cached so importing
modules do not incur repeated Excel reads or regex compilation.
"""

from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import pandas as pd

FICHAS_DEFAULT_PATH = Path(r"C:\Users\rodri\fichas\fichas-y-nombre.xlsx")


def _normalize_name(value: str | None) -> str:
    text = "" if value is None else str(value)
    text = "".join(
        ch for ch in unicodedata.normalize("NFD", text.lower())
        if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _path_key(path: Path | str | None) -> str:
    base = Path(path) if path else FICHAS_DEFAULT_PATH
    return str(base.expanduser().resolve())


@lru_cache(maxsize=4)
def _load_fichas(path_key: str) -> Tuple[Dict[str, str], Set[str], Set[str]]:
    path = Path(path_key)
    if not path.exists():
        return {}, set(), set()

    try:
        df = pd.read_excel(path)
    except Exception:
        return {}, set(), set()

    fichas_dict: Dict[str, str] = {}
    fichas_set: Set[str] = set()
    nombres_truncados: Set[str] = set()
    for _, row in df.iterrows():
        raw_ficha = row.iloc[0]
        if pd.isna(raw_ficha):
            continue
        ficha_match = re.search(r"(?<!\d)(\d{1,6})(?!\d)", str(raw_ficha))
        if not ficha_match:
            continue
        ficha = ficha_match.group(1).lstrip("0") or "0"
        fichas_set.add(ficha)
        if len(row) > 1:
            raw_nombre = row.iloc[1]
            if pd.isna(raw_nombre):
                continue
            raw_nombre_str = str(raw_nombre)
            nombre = _normalize_name(raw_nombre_str)
            if nombre:
                fichas_dict[nombre] = ficha
                if "..." in raw_nombre_str or "…" in raw_nombre_str:
                    nombres_truncados.add(nombre)
    return fichas_dict, fichas_set, nombres_truncados


@lru_cache(maxsize=4)
def _compile_patterns(path_key: str) -> Tuple[List[Tuple[re.Pattern[str], str]], Dict[str, Tuple[re.Pattern[str], str]]]:
    nombres, codigos, nombres_truncados = _load_fichas(path_key)
    patrones_numericos: List[Tuple[re.Pattern[str], str]] = []
    for ficha in sorted(codigos):
        # Acepta codigos con o sin '*' pegado (ej: 43358 o 43358*).
        patrones_numericos.append(
            (
                re.compile(rf"(?<![0-9A-Za-z]){re.escape(ficha)}(?:\s*\*)?(?![0-9A-Za-z])"),
                ficha,
            )
        )

    patrones_nombres: Dict[str, Tuple[re.Pattern[str], str]] = {}
    for nombre, ficha in nombres.items():
        suffix = r"(?:[a-z0-9]*)" if nombre in nombres_truncados else ""
        patrones_nombres[nombre] = (
            re.compile(rf"(?<![a-z0-9]){re.escape(nombre)}{suffix}(?![a-z0-9])"),
            ficha,
        )
    return patrones_numericos, patrones_nombres


def load_valid_fichas_con_nombres(path: Path | str | None = None) -> Tuple[Dict[str, str], Set[str]]:
    """Return (name->code map, valid numeric codes)."""
    nombres, codigos, _ = _load_fichas(_path_key(path))
    return nombres, codigos


def get_fichas_codigos(path: Path | str | None = None) -> Set[str]:
    return load_valid_fichas_con_nombres(path)[1]


def get_fichas_nombres(path: Path | str | None = None) -> Dict[str, str]:
    return load_valid_fichas_con_nombres(path)[0]


def detectar_ficha(texto: str | None, path: Path | str | None = None) -> str | None:
    if not texto:
        return None
    patrones, _ = _compile_patterns(_path_key(path))
    for patron, ficha in patrones:
        if patron.search(texto):
            return ficha
    return None


def detectar_fichas_multiples(texto: str | None, path: Path | str | None = None) -> str:
    if not texto:
        return ""
    patrones, _ = _compile_patterns(_path_key(path))
    encontrados = [ficha for patron, ficha in patrones if patron.search(texto)]
    unicos = sorted(set(encontrados))
    return ", ".join(unicos)


def detectar_fichas_tokens(
    texto: str | None,
    path: Path | str | None = None,
    include_prefixed: bool = True,
) -> List[str]:
    if not texto:
        return []
    path_key = _path_key(path)
    patrones_num, patrones_nom = _compile_patterns(path_key)
    texto_safe = _normalize_name(texto)

    tokens: List[str] = []
    bases_detectadas: Set[str] = set()
    prefijadas_agregadas: Set[str] = set()

    for patron, ficha in patrones_num:
        if patron.search(texto):
            if ficha not in bases_detectadas:
                tokens.append(ficha)
                bases_detectadas.add(ficha)

    if include_prefixed:
        for nombre, (patron, ficha) in patrones_nom.items():
            if patron.search(texto_safe):
                base = ficha.strip()
                if base in bases_detectadas:
                    continue
                if base not in prefijadas_agregadas:
                    tokens.append(f"* {base}")
                    prefijadas_agregadas.add(base)
                    bases_detectadas.add(base)

    return tokens


def detectar_fichas_y_nombres(texto: str | None, path: Path | str | None = None) -> str:
    tokens = detectar_fichas_tokens(texto, path=path, include_prefixed=True)
    return ", ".join(tokens)


def fichas_base_desde_tokens(tokens: Iterable[str]) -> Set[str]:
    codigos: Set[str] = set()
    for token in tokens:
        if not token:
            continue
        codigos.add(token.replace("*", "").strip())
    return {codigo for codigo in codigos if codigo}


# Backwards-compatible module-level exports for existing callers
FICHAS_NOMBRES, FICHAS_VALIDAS = load_valid_fichas_con_nombres()
PATRONES_NUMERICOS, PATRONES_NOMBRES = _compile_patterns(_path_key(None))
