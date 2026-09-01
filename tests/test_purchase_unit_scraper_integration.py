from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRAPERS = (ROOT / "clv" / "clv.py", ROOT / "clrir" / "clrir.py", ROOT / "rir1" / "rir1.py")
REQUIRED_COLUMNS = (
    "DEPENDENCY_COLUMN",
    "PURCHASE_UNIT_COLUMN",
    "PROVINCE_COLUMN",
    "CONTACT_NAME_COLUMN",
    "CONTACT_ROLE_COLUMN",
    "CONTACT_PHONE_COLUMN",
    "CONTACT_EMAIL_COLUMN",
)


def test_all_official_scrapers_use_shared_purchase_unit_extractor() -> None:
    for path in SCRAPERS:
        source = path.read_text(encoding="utf-8")
        assert "extract_purchase_unit_details(page.d)" in source, path
        for column in REQUIRED_COLUMNS:
            assert column in source, f"{path}: falta {column}"


def test_rir1_resume_path_updates_all_new_columns() -> None:
    source = (ROOT / "rir1" / "rir1.py").read_text(encoding="utf-8")
    for column in REQUIRED_COLUMNS:
        expected = f'_set_row_value_by_header(hmap, out, {column},'
        assert expected in source
