from __future__ import annotations

from functools import lru_cache
import logging
import os
from pathlib import Path
import re

import pandas as pd


def _name(value: object) -> str:
    text = " ".join(str(value or "").split())
    return "" if text.casefold() in {"", "nan", "none", "null", "si", "sí", "no"} else text


def _truncated(value: str) -> bool:
    return "..." in value or "…" in value


@lru_cache(maxsize=4)
def _load_names(signature: tuple[tuple[str, int, int], ...]) -> dict[str, str]:
    names = {}
    for filename, modified, size in signature:
        try:
            path = Path(filename)
            frame = pd.read_csv(path, header=None, dtype=str, keep_default_na=False) if path.suffix.lower() == ".csv" else pd.read_excel(path, header=None, dtype=str, keep_default_na=False)
            if frame.shape[1] < 2:
                continue
            for code_raw, name_raw in frame.iloc[:, :2].itertuples(index=False, name=None):
                match = re.fullmatch(r"\s*(\d{1,6})(?:\.0+)?\s*", str(code_raw))
                name = _name(name_raw)
                if not match or not name:
                    continue
                code = match[1].lstrip("0") or "0"
                if code not in names or (_truncated(names[code]) and not _truncated(name)):
                    names[code] = name
        except Exception as exc:
            logging.warning("No se pudo leer nombres de fichas en %s: %s", filename, exc)
    return names


def load_ficha_names(root: Path) -> dict[str, str]:
    configured = [Path(value.strip()).expanduser() for value in os.environ.get("FICHAS_CATALOG_PATHS", "").split(os.pathsep) if value.strip()]
    paths = [
        *configured,
        root / "data/fichas/fichas-y-nombre.xlsx",
        Path.home() / "fichas/fichas-y-nombre.xlsx",
        Path.home() / "GEAPP/fichas_ctni_con_enlace.xlsx",
    ]
    signature = []
    seen = set()
    for path in paths:
        try:
            path = path.resolve()
            if str(path) in seen or not path.is_file():
                continue
            seen.add(str(path))
            stat = path.stat()
            signature.append((str(path), stat.st_mtime_ns, stat.st_size))
        except OSError as exc:
            logging.warning("No se pudo comprobar el catálogo %s: %s", path, exc)
    return _load_names(tuple(signature))


def format_ficha_names(label: object, names: dict[str, str], *, tracked_codes: set[str] | None = None) -> str:
    codes = list(dict.fromkeys(match[1].lstrip("0") or "0" for match in re.finditer(r"(?<!\d)(\d{1,6})(?:\.0+)?(?!\d)", str(label or ""))))
    missing = "Nombre no disponible en el catálogo"
    if not codes:
        return missing
    if tracked_codes is not None:
        codes = [code for code in codes if code in tracked_codes]
        if not codes:
            return "Ficha RIR no identificada en la lista actual"
    if len(codes) == 1:
        return names.get(codes[0]) or missing
    return "; ".join(f"{code}: {names.get(code) or missing}" for code in codes)
