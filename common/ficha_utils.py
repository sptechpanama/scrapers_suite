"""Utilities for ficha detection shared between scraping scripts.

This module centralizes the logic used to load ficha definitions and to
locate them inside arbitrary text blocks. Patterns are cached so importing
modules do not incur repeated Excel reads or regex compilation.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import pandas as pd

FICHAS_DEFAULT_PATH = Path(r"C:\Users\rodri\fichas\fichas-y-nombre.xlsx")


def _path_key(path: Path | str | None) -> str:
    base = Path(path) if path else FICHAS_DEFAULT_PATH
    return str(base.expanduser().resolve())


@lru_cache(maxsize=4)
def _load_fichas(path_key: str) -> Tuple[Dict[str, str], Set[str]]:
    path = Path(path_key)
    if not path.exists():
        return {}, set()

    try:
        df = pd.read_excel(path)
    except Exception:
        return {}, set()

    fichas_dict: Dict[str, str] = {}
    fichas_set: Set[str] = set()
    for _, row in df.iterrows():
        ficha = str(row.iloc[0]).strip()
        if not ficha or len(ficha) > 6:
            continue
        fichas_set.add(ficha)
        if len(row) > 1:
            nombre = str(row.iloc[1]).strip().lower()
            if nombre:
                fichas_dict[nombre] = ficha
    return fichas_dict, fichas_set


@lru_cache(maxsize=4)
def _compile_patterns(path_key: str) -> Tuple[List[Tuple[re.Pattern[str], str]], Dict[str, Tuple[re.Pattern[str], str]]]:
    nombres, codigos = _load_fichas(path_key)
    patrones_numericos: List[Tuple[re.Pattern[str], str]] = []
    for ficha in sorted(codigos):
        patrones_numericos.append((re.compile(rf"(?<![\w\d]){re.escape(ficha)}(?![\w\d])"), ficha))

    patrones_nombres: Dict[str, Tuple[re.Pattern[str], str]] = {}
    for nombre, ficha in nombres.items():
        patrones_nombres[nombre] = (
            re.compile(rf"(?<![\w\d]){re.escape(nombre)}(?![\w\d])"),
            ficha,
        )
    return patrones_numericos, patrones_nombres


def load_valid_fichas_con_nombres(path: Path | str | None = None) -> Tuple[Dict[str, str], Set[str]]:
    """Return (name->code map, valid numeric codes)."""
    return _load_fichas(_path_key(path))


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
    texto_safe = texto.lower()

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
