from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pandas as pd
import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "minsa_scraper" / "scrape_minsa.py"
SPEC = importlib.util.spec_from_file_location("scrape_minsa_resilience", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
minsa = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = minsa
SPEC.loader.exec_module(minsa)


class _Body:
    def __init__(self, text: str) -> None:
        self.text = text


class _Driver:
    def __init__(self, body: str) -> None:
        self.title = "MINSA-SITIO EN MANTENIMIENTO"
        self.current_url = minsa.OFERENTES_URL
        self.page_source = f"<html><body>{body}</body></html>"
        self._body = body

    def find_element(self, *_args):
        return _Body(self._body)


class _WaitMustNotRun:
    def until(self, *_args, **_kwargs):
        raise AssertionError("La espera Selenium no debe comenzar durante mantenimiento")


def test_detecta_mantenimiento_sin_agotar_timeout() -> None:
    driver = _Driver("El Sistema se encuentra en mantenimiento. Intente mas tarde.")

    assert minsa._portal_unavailable_reason(driver) == "sitio en mantenimiento"
    with pytest.raises(minsa.MinsaPortalUnavailable):
        minsa._ensure_oferentes_table(driver, _WaitMustNotRun(), 1)


def test_snapshot_valido_se_conserva(tmp_path: Path) -> None:
    snapshot = tmp_path / "oferentes_Cat\u00E1logos.xlsx"
    pd.DataFrame(
        {
            "Oferente": ["Proveedor de prueba"],
            "Producto": ["Insumo medico"],
        }
    ).to_excel(snapshot, index=False)

    found = minsa._find_usable_oferentes_snapshot(tmp_path)
    preserved = minsa._preserve_oferentes_snapshot_or_raise(
        minsa.MinsaPortalUnavailable("mantenimiento"),
        tmp_path,
    )

    assert found == snapshot
    assert preserved == snapshot
    assert pd.read_excel(snapshot).iloc[0]["Oferente"] == "Proveedor de prueba"


def test_snapshot_corrupto_no_oculta_el_error(tmp_path: Path) -> None:
    (tmp_path / "oferentes_Cat\u00E1logos.xlsx").write_bytes(b"archivo corrupto")
    failure = minsa.MinsaPortalUnavailable("mantenimiento")

    assert minsa._find_usable_oferentes_snapshot(tmp_path) is None
    with pytest.raises(minsa.MinsaPortalUnavailable, match="mantenimiento"):
        minsa._preserve_oferentes_snapshot_or_raise(failure, tmp_path)
