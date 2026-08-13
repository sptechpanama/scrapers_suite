"""Deteccion unificada, rapida y auditable de fichas tecnicas.

Este modulo es la unica fuente de deteccion para la base historica y para
CLV/CLRIR/RIR1. Conserva la interfaz anterior (``detectar_fichas_tokens``),
pero internamente trabaja por campos, indexa el catalogo y devuelve evidencia.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import unicodedata
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence, Set, Tuple

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DETECTOR_VERSION = "3.2.0"
FICHAS_DEFAULT_PATH = Path(r"C:\Users\rodri\fichas\fichas-y-nombre.xlsx")
ALIASES_DEFAULT_PATH = REPO_ROOT / "data" / "fichas" / "ficha_aliases.json"

_STOPWORDS = {
    "a", "al", "con", "de", "del", "el", "en", "la", "las", "lo",
    "los", "o", "para", "por", "su", "sus", "un", "una", "y",
}
_NUMERIC_CONTEXT_RE = re.compile(
    r"(?:\bficha(?:\s+tecnica)?\b|\bcodigo\s+de\s+ficha\b|"
    r"\bc\s*[.\-]?\s*t\s*[.\-]?\s*n\s*[.\-]?\s*i\b|"
    r"\bf\s*(?:[./\-]\s*)?(?:tec(?:nica)?|t)\b)",
    re.IGNORECASE,
)


def _normalize_name(value: str | None) -> str:
    text = "" if value is None else str(value)
    text = "".join(
        ch for ch in unicodedata.normalize("NFD", text.lower())
        if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _numeric_ficha(value: object) -> str:
    text = str(value or "").strip()
    match = re.fullmatch(r"(\d{1,6})(?:\.0+)?", text)
    if not match:
        return ""
    return match.group(1).lstrip("0") or "0"


def _has_ellipsis(value: object) -> bool:
    text = str(value or "")
    return "..." in text or "\u2026" in text or "â€¦" in text


def _significant(tokens: Sequence[str]) -> tuple[str, ...]:
    return tuple(token for token in tokens if token not in _STOPWORDS)


def _token_variants(token: str) -> frozenset[str]:
    """Devuelve variantes singular/plural conservadoras de un token.

    Solo se usan dentro de nombres largos y especificos de ficha. Esto cubre
    diferencias editoriales como ``PACIENTE``/``PACIENTES`` sin convertir una
    palabra generica aislada en evidencia suficiente.
    """
    value = str(token or "").strip()
    variants = {value} if value else set()
    if len(value) >= 5 and value.endswith("s"):
        variants.add(value[:-1])
    if len(value) >= 6 and value.endswith("es"):
        variants.add(value[:-2])
    if len(value) >= 6 and value.endswith("ces"):
        variants.add(value[:-3] + "z")
    return frozenset(item for item in variants if len(item) >= 3)


def _tokens_equivalent(left: str, right: str, *, allow_inflection: bool) -> bool:
    if left == right:
        return True
    if not allow_inflection:
        return False
    return bool(_token_variants(left).intersection(_token_variants(right)))


def _catalog_candidates(explicit_path: Path | str | None = None) -> tuple[Path, ...]:
    if explicit_path:
        return (Path(explicit_path).expanduser().resolve(),)

    env_paths = [
        value.strip() for value in os.environ.get("FICHAS_CATALOG_PATHS", "").split(os.pathsep)
        if value.strip()
    ]
    candidates = [
        *(Path(value).expanduser() for value in env_paths),
        REPO_ROOT / "data" / "fichas" / "fichas-y-nombre.xlsx",
        FICHAS_DEFAULT_PATH,
        Path.home() / "GEAPP" / "fichas_ctni_con_enlace.xlsx",
    ]
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        key = str(resolved).lower()
        if key not in seen and resolved.exists():
            seen.add(key)
            unique.append(resolved)
    return tuple(unique)


def _aliases_path() -> Path:
    configured = os.environ.get("FICHAS_ALIASES_PATH", "").strip()
    return Path(configured).expanduser().resolve() if configured else ALIASES_DEFAULT_PATH


def _catalog_key(explicit_path: Path | str | None = None) -> tuple[str, ...]:
    paths = list(_catalog_candidates(explicit_path))
    aliases = _aliases_path()
    if aliases.exists():
        paths.append(aliases)
    return tuple(str(path) for path in paths)


def catalog_version(path: Path | str | None = None) -> str:
    digest = hashlib.sha256(DETECTOR_VERSION.encode("utf-8"))
    for raw_path in _catalog_key(path):
        candidate = Path(raw_path)
        try:
            stat = candidate.stat()
            digest.update(str(candidate).lower().encode("utf-8"))
            digest.update(str(stat.st_size).encode("ascii"))
            digest.update(str(stat.st_mtime_ns).encode("ascii"))
        except OSError:
            continue
    return digest.hexdigest()[:16]


@dataclass(frozen=True)
class AliasEntry:
    code: str
    raw: str
    tokens: tuple[str, ...]
    significant_tokens: tuple[str, ...]
    truncated: bool
    source: str


@dataclass(frozen=True)
class FichaMatch:
    code: str
    method: str
    field: str
    evidence: str
    score: int

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class Catalog:
    valid_codes: frozenset[str]
    names: Mapping[str, str]
    aliases_by_anchor: Mapping[str, tuple[AliasEntry, ...]]
    version: str


def _read_catalog_rows(path: Path) -> list[tuple[str, str, bool, str]]:
    if path.suffix.lower() not in {".xlsx", ".xls", ".csv"}:
        return []
    try:
        frame = (
            pd.read_csv(path, header=None, dtype=str, keep_default_na=False)
            if path.suffix.lower() == ".csv"
            else pd.read_excel(path, header=None, dtype=str, keep_default_na=False)
        )
    except Exception:
        return []
    if frame.shape[1] < 1:
        return []
    rows: list[tuple[str, str, bool, str]] = []
    for _, row in frame.iterrows():
        code = _numeric_ficha(row.iloc[0])
        if not code:
            continue
        raw_name = str(row.iloc[1]).strip() if frame.shape[1] > 1 else ""
        rows.append((code, raw_name, _has_ellipsis(raw_name), path.name))
    return rows


def _read_manual_aliases(path: Path) -> list[tuple[str, str, bool, str]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    aliases = payload.get("aliases", payload) if isinstance(payload, dict) else {}
    rows: list[tuple[str, str, bool, str]] = []
    if not isinstance(aliases, dict):
        return rows
    for raw_code, values in aliases.items():
        code = _numeric_ficha(raw_code)
        if not code:
            continue
        if isinstance(values, str):
            values = [values]
        if not isinstance(values, list):
            continue
        for value in values:
            alias = str(value or "").strip()
            if alias:
                rows.append((code, alias, _has_ellipsis(alias), f"manual:{path.name}"))
    return rows


def _contains_sequence(
    alias: Sequence[str],
    text_tokens: Sequence[str],
    *,
    truncated: bool,
    allow_inflection: bool = False,
) -> tuple[str, ...] | None:
    if not alias or len(text_tokens) < len(alias):
        return None
    width = len(alias)
    for start, token in enumerate(text_tokens):
        if (
            not _tokens_equivalent(token, alias[0], allow_inflection=allow_inflection)
            or start + width > len(text_tokens)
        ):
            continue
        window = tuple(text_tokens[start : start + width])
        if width > 1 and not all(
            _tokens_equivalent(current, expected, allow_inflection=allow_inflection)
            for current, expected in zip(window[:-1], alias[:-1])
        ):
            continue
        tail_matches = (
            window[-1].startswith(alias[-1])
            if truncated
            else _tokens_equivalent(
                window[-1], alias[-1], allow_inflection=allow_inflection
            )
        )
        if tail_matches:
            return window
    return None


def _alias_matches(
    entry: AliasEntry,
    text_tokens: Sequence[str],
    token_set: set[str],
    compact_text: Sequence[str],
    compact_token_set: set[str],
) -> tuple[str, str] | None:
    alias = entry.tokens
    if not alias or not text_tokens:
        return None

    allow_inflection = (
        len(entry.significant_tokens) >= 4
        and sum(map(len, entry.significant_tokens)) >= 18
    )

    # Coincidencia contigua. En nombres truncados, el ultimo token es prefijo.
    required_tokens = tuple(alias[:-1] if entry.truncated else alias)
    required = set(required_tokens)
    required_present = required.issubset(token_set)
    if allow_inflection and not required_present:
        required_present = all(
            any(_tokens_equivalent(expected, actual, allow_inflection=True) for actual in token_set)
            for expected in required_tokens
        )
    truncated_tail_present = (
        not entry.truncated or any(token.startswith(alias[-1]) for token in token_set)
    )
    if required_present and truncated_tail_present:
        window = _contains_sequence(
            alias,
            text_tokens,
            truncated=entry.truncated,
            allow_inflection=allow_inflection,
        )
        if window:
            return "nombre_truncado" if entry.truncated else "nombre_exacto", " ".join(window)

    # Variante compacta: permite omitir conectores, pero conserva orden y todos
    # los terminos informativos. Se exige una frase suficientemente especifica.
    compact_alias = entry.significant_tokens
    if len(compact_alias) < 4 or sum(map(len, compact_alias)) < 18:
        return None
    compact_required_tokens = tuple(
        compact_alias[:-1] if entry.truncated else compact_alias
    )
    compact_required = set(compact_required_tokens)
    compact_required_present = compact_required.issubset(compact_token_set)
    if not compact_required_present:
        compact_required_present = all(
            any(_tokens_equivalent(expected, actual, allow_inflection=True) for actual in compact_token_set)
            for expected in compact_required_tokens
        )
    compact_tail_present = (
        not entry.truncated or any(token.startswith(compact_alias[-1]) for token in compact_token_set)
    )
    if compact_required_present and compact_tail_present:
        window = _contains_sequence(
            compact_alias,
            compact_text,
            truncated=entry.truncated,
            allow_inflection=True,
        )
        if window:
            return "nombre_compacto", " ".join(window)
    return None


@lru_cache(maxsize=8)
def _load_catalog_cached(key: tuple[str, ...]) -> Catalog:
    rows: list[tuple[str, str, bool, str]] = []
    for raw_path in key:
        path = Path(raw_path)
        if path.suffix.lower() == ".json":
            rows.extend(_read_manual_aliases(path))
        else:
            rows.extend(_read_catalog_rows(path))

    valid: set[str] = set()
    names: dict[str, str] = {}
    aliases: dict[tuple[str, str], AliasEntry] = {}
    for code, raw_name, truncated, source in rows:
        valid.add(code)
        normalized = _normalize_name(raw_name)
        if not normalized:
            continue
        tokens = tuple(normalized.split())
        entry = AliasEntry(
            code=code,
            raw=raw_name,
            tokens=tokens,
            significant_tokens=_significant(tokens),
            truncated=truncated,
            source=source,
        )
        aliases.setdefault((code, normalized), entry)
        names.setdefault(normalized, code)

    codes_by_alias: dict[str, set[str]] = {}
    for (code, normalized), entry in aliases.items():
        if not entry.source.startswith("manual:"):
            codes_by_alias.setdefault(normalized, set()).add(code)

    by_anchor: dict[str, list[AliasEntry]] = {}
    for entry in aliases.values():
        normalized = _normalize_name(entry.raw)
        # Un mismo nombre para varias fichas no es evidencia suficiente. Los
        # aliases manuales si se aceptan porque fueron validados expresamente.
        if (
            not entry.source.startswith("manual:")
            and len(codes_by_alias.get(normalized, set())) > 1
        ):
            continue
        exact_anchors = [
            token for index, token in enumerate(entry.significant_tokens)
            if not (entry.truncated and index == len(entry.significant_tokens) - 1)
        ]
        if not exact_anchors:
            exact_anchors = list(entry.significant_tokens or entry.tokens)
        if not exact_anchors:
            continue
        anchor = max(exact_anchors, key=lambda token: (len(token), token))
        by_anchor.setdefault(anchor, []).append(entry)

    frozen_index = {
        token: tuple(sorted(entries, key=lambda item: (item.code, item.raw)))
        for token, entries in by_anchor.items()
    }
    version = catalog_version_from_key(key)
    return Catalog(frozenset(valid), names, frozen_index, version)


def catalog_version_from_key(key: tuple[str, ...]) -> str:
    digest = hashlib.sha256(DETECTOR_VERSION.encode("utf-8"))
    for raw_path in key:
        path = Path(raw_path)
        try:
            stat = path.stat()
            digest.update(str(path).lower().encode("utf-8"))
            digest.update(f"{stat.st_size}:{stat.st_mtime_ns}".encode("ascii"))
        except OSError:
            continue
    return digest.hexdigest()[:16]


@lru_cache(maxsize=1)
def _default_catalog() -> Catalog:
    return _load_catalog_cached(_catalog_key(None))


def get_catalog(path: Path | str | None = None) -> Catalog:
    catalog = _load_catalog_cached(_catalog_key(path)) if path else _default_catalog()
    if not catalog.valid_codes:
        locations = ", ".join(_catalog_key(path)) or "ninguna ruta disponible"
        raise FileNotFoundError(f"No se pudo cargar el catalogo de fichas: {locations}")
    return catalog


def detectar_fichas_detalladas(
    fields: Mapping[str, object] | str | None,
    path: Path | str | None = None,
) -> list[FichaMatch]:
    if not fields:
        return []
    field_map = {"texto": fields} if isinstance(fields, str) else dict(fields)
    catalog = get_catalog(path)
    best: dict[str, FichaMatch] = {}

    for field, raw_value in field_map.items():
        raw_text = str(raw_value or "").strip()
        if not raw_text:
            continue
        normalized = _normalize_name(raw_text)
        tokens = normalized.split()
        token_set = set(tokens)
        compact_text = _significant(tokens)
        compact_token_set = set(compact_text)

        for number_match in re.finditer(
            r"(?<![0-9A-Za-z])(\d{1,6})(?![0-9A-Za-z])",
            normalized,
        ):
            number = number_match.group(1)
            code = number.lstrip("0") or "0"
            if code not in catalog.valid_codes:
                continue
            match_at = number_match.start(1)
            context = normalized[max(0, match_at - 35) : match_at + len(code) + 20]
            field_is_ficha = "ficha" in _normalize_name(str(field))
            explicitly_starred = bool(
                re.search(rf"(?<!\d)0*{re.escape(code)}\s*\*(?!\d)", raw_text)
            )
            contextual = bool(
                _NUMERIC_CONTEXT_RE.search(context)
                or field_is_ficha
                or explicitly_starred
                or normalized == number
            )
            if not contextual:
                # Series, placas, registros y montos pueden coincidir por azar
                # con una ficha valida. Se conservan fuera de la clasificacion.
                continue
            candidate = FichaMatch(
                code=code,
                method="codigo_contextual",
                field=str(field),
                evidence=context or code,
                score=100,
            )
            current = best.get(code)
            if current is None or candidate.score > current.score:
                best[code] = candidate

        candidate_entries: dict[tuple[str, str], AliasEntry] = {}
        for token in token_set:
            for anchor in _token_variants(token):
                for entry in catalog.aliases_by_anchor.get(anchor, ()):
                    candidate_entries[(entry.code, entry.raw)] = entry
        for entry in candidate_entries.values():
            found = _alias_matches(
                entry,
                tokens,
                token_set,
                compact_text,
                compact_token_set,
            )
            if not found:
                continue
            method, evidence = found
            score = {"nombre_exacto": 96, "nombre_truncado": 94, "nombre_compacto": 90}[method]
            candidate = FichaMatch(entry.code, method, str(field), evidence, score)
            current = best.get(entry.code)
            if current is None or candidate.score > current.score:
                best[entry.code] = candidate

    return sorted(best.values(), key=lambda item: (-item.score, int(item.code)))


def legacy_tokens(matches: Iterable[FichaMatch], include_prefixed: bool = True) -> list[str]:
    tokens: list[str] = []
    for match in sorted(matches, key=lambda item: int(item.code)):
        if match.method.startswith("codigo_") or not include_prefixed:
            tokens.append(match.code)
        else:
            tokens.append(f"* {match.code}")
    return tokens


def detection_json(matches: Iterable[FichaMatch]) -> str:
    return json.dumps([match.as_dict() for match in matches], ensure_ascii=False, separators=(",", ":"))


def load_valid_fichas_con_nombres(path: Path | str | None = None) -> Tuple[Dict[str, str], Set[str]]:
    catalog = get_catalog(path)
    return dict(catalog.names), set(catalog.valid_codes)


def get_fichas_codigos(path: Path | str | None = None) -> Set[str]:
    return load_valid_fichas_con_nombres(path)[1]


def get_fichas_nombres(path: Path | str | None = None) -> Dict[str, str]:
    return load_valid_fichas_con_nombres(path)[0]


def detectar_ficha(texto: str | None, path: Path | str | None = None) -> str | None:
    matches = detectar_fichas_detalladas(texto, path=path)
    return matches[0].code if matches else None


def detectar_fichas_multiples(texto: str | None, path: Path | str | None = None) -> str:
    matches = detectar_fichas_detalladas(texto, path=path)
    return ", ".join(sorted({match.code for match in matches}, key=int))


def detectar_fichas_tokens(
    texto: str | None,
    path: Path | str | None = None,
    include_prefixed: bool = True,
) -> List[str]:
    return legacy_tokens(
        detectar_fichas_detalladas(texto, path=path),
        include_prefixed=include_prefixed,
    )


def detectar_fichas_y_nombres(texto: str | None, path: Path | str | None = None) -> str:
    return ", ".join(detectar_fichas_tokens(texto, path=path, include_prefixed=True))


def fichas_base_desde_tokens(tokens: Iterable[str]) -> Set[str]:
    codigos: Set[str] = set()
    for token in tokens:
        match = re.search(r"(?<!\d)(\d{1,6})(?!\d)", str(token or ""))
        if match:
            codigos.add(match.group(1).lstrip("0") or "0")
    return codigos
