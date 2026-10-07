import os

import pandas as pd
import pytest

from common import ficha_email as names


@pytest.fixture(autouse=True)
def isolated_catalogs(monkeypatch, tmp_path):
    monkeypatch.delenv("FICHAS_CATALOG_PATHS", raising=False)
    monkeypatch.setattr(names.Path, "home", lambda: tmp_path / "home")
    names._load_names.cache_clear()


@pytest.mark.parametrize("label", ["107861", "* 107861", "107861*", "107861.0", "* 107861, 107861"])
def test_prefixes_and_duplicate_codes_do_not_change_the_name(label):
    assert names.format_ficha_names(label, {"107861": "HUMIDIFICADOR DE ALTO FLUJO"}) == "HUMIDIFICADOR DE ALTO FLUJO"


def test_multiple_fichas_are_explicitly_paired_with_their_own_names():
    result = names.format_ficha_names("* 22241, * 101414", {"22241": "TUBO ENDOTRAQUEAL CON BALÓN", "101414": "TUBO ENDOTRAQUEAL CÓNICO"})
    assert result == "22241: TUBO ENDOTRAQUEAL CON BALÓN; 101414: TUBO ENDOTRAQUEAL CÓNICO"


def test_only_fichas_in_the_rir_watchlist_are_named_as_the_trigger():
    result = names.format_ficha_names("* 22241, * 101414", {"22241": "TUBO CON BALÓN", "101414": "OTRO TUBO"}, tracked_codes={"22241"})
    assert result == "TUBO CON BALÓN"


def test_removed_watchlist_ficha_is_not_presented_as_a_current_rir_trigger():
    result = names.format_ficha_names("22241", {"22241": "TUBO CON BALÓN"}, tracked_codes={"43358"})
    assert result == "Ficha RIR no identificada en la lista actual"


@pytest.mark.parametrize("label", [None, "", "No Detectada", "987654", "12345678"])
def test_unavailable_names_are_not_inferred_from_a_title_or_another_code(label):
    assert names.format_ficha_names(label, {"107861": "HUMIDIFICADOR"}) == "Nombre no disponible en el catálogo"


def test_missing_one_name_does_not_hide_the_other_ficha():
    result = names.format_ficha_names("22241, 101414", {"22241": "TUBO ENDOTRAQUEAL"})
    assert result == "22241: TUBO ENDOTRAQUEAL; 101414: Nombre no disponible en el catálogo"


def catalog(root, rows):
    path = root / "data/fichas/fichas-y-nombre.xlsx"
    path.parent.mkdir(parents=True)
    pd.DataFrame(rows).to_excel(path, index=False, header=False)
    return path


def test_official_catalog_preserves_first_row_and_accents_and_ignores_requirements(tmp_path):
    catalog(tmp_path, [[104747, "RESPIRADOR FACIAL"], [107861, "HUMIDIFICADOR DE ALTO FLUJO"], [22241, "TUBO CON BALÓN"], [101414, "SI"], [999999, "NO"]])
    assert names.load_ficha_names(tmp_path) == {"104747": "RESPIRADOR FACIAL", "107861": "HUMIDIFICADOR DE ALTO FLUJO", "22241": "TUBO CON BALÓN"}


def test_catalog_cache_is_reused_and_refreshes_when_the_file_changes(tmp_path, monkeypatch):
    path = catalog(tmp_path, [[107861, "NOMBRE ANTERIOR"]])
    original = names.pd.read_excel
    reads = []
    def tracked(*args, **kwargs):
        reads.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(names.pd, "read_excel", tracked)
    assert names.load_ficha_names(tmp_path)["107861"] == "NOMBRE ANTERIOR"
    names.load_ficha_names(tmp_path)
    assert len(reads) == 1
    prior_stat = path.stat()
    pd.DataFrame([[107861, "NOMBRE ACTUALIZADO"]]).to_excel(path, header=False, index=False)
    os.utime(path, ns=(prior_stat.st_atime_ns, prior_stat.st_mtime_ns + 1000000))
    assert names.load_ficha_names(tmp_path)["107861"] == "NOMBRE ACTUALIZADO"
    assert len(reads) == 2


def test_complete_name_is_preferred_to_a_truncated_name_without_inventing_it(tmp_path, monkeypatch):
    catalog(tmp_path, [[104747, "RESPIRADOR FACIAL..."]])
    additional = tmp_path / "completo.csv"
    additional.write_text("104747,RESPIRADOR FACIAL CONTRA PARTÍCULAS\n", encoding="utf-8")
    monkeypatch.setenv("FICHAS_CATALOG_PATHS", str(additional))
    assert names.load_ficha_names(tmp_path)["104747"] == "RESPIRADOR FACIAL CONTRA PARTÍCULAS"


def test_missing_or_corrupt_catalog_does_not_block_email(tmp_path):
    assert names.load_ficha_names(tmp_path) == {}
    path = tmp_path / "data/fichas/fichas-y-nombre.xlsx"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"invalid excel")
    assert names.load_ficha_names(tmp_path) == {}
