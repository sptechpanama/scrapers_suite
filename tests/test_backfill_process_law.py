from __future__ import annotations

from tools.backfill_process_law import (
    PROCESS_LAW_COLUMN,
    _align_row,
    _merged_target_header,
    migration_candidates,
)


def test_migration_selects_only_confirmed_419_without_ficha() -> None:
    rows = [
        ["enlace", "ficha_detectada", PROCESS_LAW_COLUMN, "titulo"],
        ["https://example/1", "No Detectada", "Ley 419", "A"],
        ["https://example/2", "* 43358", "Ley 419", "B"],
        ["https://example/3", "No Detectada", "No identificado", "C"],
        ["https://example/4", "No Detectada", "Ley 22", "D"],
    ]
    assert migration_candidates(rows) == [(2, "https://example/1")]


def test_migration_is_idempotent_when_row_is_no_longer_in_source() -> None:
    header = ["enlace", "ficha_detectada", PROCESS_LAW_COLUMN]
    assert migration_candidates([header]) == []


def test_row_alignment_uses_headers_not_column_positions() -> None:
    source_header = ["titulo", "enlace", PROCESS_LAW_COLUMN]
    source_row = ["Compra", "https://example/1", "Ley 419"]
    target_header = ["enlace", PROCESS_LAW_COLUMN, "titulo", "extra"]
    assert _align_row(source_header, source_row, target_header) == [
        "https://example/1",
        "Ley 419",
        "Compra",
        "",
    ]


def test_target_header_keeps_existing_order_and_adds_new_source_columns() -> None:
    assert _merged_target_header(
        ["enlace", "titulo", "Ley del proceso", "Contacto"],
        ["titulo", "enlace", "Ley del proceso"],
    ) == ["titulo", "enlace", "Ley del proceso", "Contacto"]
