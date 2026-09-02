from __future__ import annotations

"""Completa metadata auditable en las tres hojas sin requisitos.

La utilidad solo consulta los listados oficiales de PanamáCompra y actualiza
columnas existentes de Google Sheets. No crea actos, no borra filas y no envía
correos. Puede ejecutarse repetidamente sin producir duplicados.
"""

import argparse
import importlib.util
import os
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.no_requirements import (  # noqa: E402
    ADJUDICATION_TYPE_COLUMN,
    ADJUDICATION_UNKNOWN,
    NO_REQUIREMENTS_FICHAS_COLUMN,
    NO_REQUIREMENTS_SCOPE_COLUMN,
    REQUIREMENTS_FICHAS_COLUMN,
    UNCLASSIFIED_FICHAS_COLUMN,
    normalize_adjudication_type,
)


SOURCES = {
    "clv": {
        "path": ROOT / "clv" / "clv.py",
        "sheet": "cl_abiertas_rir_sin_requisitos",
        "credentials_env": "CLV_SERVICE_ACCOUNT_FILE",
        "radio": "btnradio1",
    },
    "clrir": {
        "path": ROOT / "clrir" / "clrir.py",
        "sheet": "cl_prog_sin_requisitos",
        "credentials_env": "CLRIR_SERVICE_ACCOUNT_FILE",
        "radio": "btnradio2",
    },
    "rir1": {
        "path": ROOT / "rir1" / "rir1.py",
        "sheet": "ap_sin_requisitos",
        "credentials_env": "RIR1_SERVICE_ACCOUNT_FILE",
        "advanced": True,
    },
}


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"backfill_{name}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sheet_target_links(module: ModuleType, sheet: str) -> dict[str, str]:
    rows = module.gs_get(f"{sheet}!A1:ZZ")
    if not rows:
        return {}
    link_idx = module.find_idx(rows[0], "enlace")
    if link_idx is None:
        return {}
    links: dict[str, str] = {}
    for row in rows[1:]:
        if link_idx >= len(row):
            continue
        raw = str(row[link_idx] or "").strip()
        key = module.normalize_url(raw)
        if raw and key:
            links[key] = raw
    return links


def _open_listing(module: ModuleType, source: Mapping[str, object], driver, page_tools) -> None:
    By = module.By
    EC = module.EC
    WebDriverWait = module.WebDriverWait
    Select = module.Select

    driver.get(module.CFG["url_list"])
    time.sleep(1.5)
    page_tools.close_popup()
    driver.execute_script("window.scrollBy(0,400)")

    radio = source.get("radio")
    if radio:
        button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, f"//label[@for='{radio}']"))
        )
        driver.execute_script("arguments[0].click();", button)
        select_xpath = "//select[contains(@class,'form-select')]"
    else:
        state = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "//select[@id='estado']"))
        )
        Select(state).select_by_visible_text("Vigente")
        search = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//form[@name='busquedaA2']//button[@type='submit' and contains(.,'Buscar')]",
                )
            )
        )
        driver.execute_script("arguments[0].click();", search)
        select_xpath = (
            "//tabla-busqueda-avanzada-v3//div[contains(@class,'d-inline-flex')]"
            "//select[contains(@class,'form-select-sm')]"
        )

    page_tools.close_popup()
    selector = WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, select_xpath))
    )
    Select(selector).select_by_visible_text("50")
    WebDriverWait(driver, 30).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, module.CFG["css_links"]))
    )


def _capture_official_modes(
    module: ModuleType,
    source: Mapping[str, object],
    target_links: Mapping[str, str],
) -> dict[str, str]:
    driver = module.start_browser()
    page_tools = module.PageTools(driver)
    unresolved = set(target_links)
    visited_signatures: set[tuple[object, ...]] = set()
    try:
        _open_listing(module, source, driver, page_tools)
        for _ in range(1000):
            urls = [url for url in page_tools.collect_links() if url]
            # Ver el enlace no basta: Angular puede repintar la fila antes de
            # leer su modalidad. Solo se resuelve cuando el valor es valido.
            unresolved = {
                key
                for key in unresolved
                if normalize_adjudication_type(
                    page_tools.adjudication_by_url.get(key, "")
                )
                == ADJUDICATION_UNKNOWN
            }
            if not unresolved:
                break

            current, total = page_tools.page_xy()
            signature = (current, total, *(module.normalize_url(url) for url in urls[:2]))
            if signature in visited_signatures:
                break
            visited_signatures.add(signature)
            if current and total and current >= total:
                break

            old_body = page_tools.tbody_ref()
            if not page_tools.click_next():
                break
            try:
                if old_body is not None:
                    module.WebDriverWait(driver, 15).until(module.EC.staleness_of(old_body))
                else:
                    time.sleep(1)
            except Exception:
                time.sleep(1)

        # Respaldo acotado para actos que ya salieron del listado vigente.
        for key in sorted(unresolved):
            raw_link = target_links[key]
            try:
                driver.get(raw_link)
                # ``presence_of_element_located(body)`` ocurre antes de que
                # Angular pinte los datos del acto. Esperamos expresamente la
                # modalidad; así no guardamos un falso ``No identificado``.
                def _loaded_mode(active_driver):
                    try:
                        body = active_driver.find_element(module.By.TAG_NAME, "body").text
                    except Exception:
                        return False
                    mode = module.resolve_adjudication_type("", body)
                    return mode if mode != ADJUDICATION_UNKNOWN else False

                mode = module.WebDriverWait(driver, 25).until(_loaded_mode)
                if mode != ADJUDICATION_UNKNOWN:
                    page_tools.adjudication_by_url[key] = mode
            except Exception:
                continue
    finally:
        try:
            driver.quit()
        except Exception:
            pass
    return dict(page_tools.adjudication_by_url)


def _verify_sheet(module: ModuleType, sheet: str) -> dict[str, int]:
    if module.__name__ == "backfill_clrir":
        rows = module.gs_get(f"{sheet}!A1:ZZ", use_cache=False)
    else:
        rows = module.gs_get(f"{sheet}!A1:ZZ")
    if not rows:
        return {"filas": 0, "celdas_vacias": 0, "modalidad_no_identificada": 0}
    columns = (
        NO_REQUIREMENTS_FICHAS_COLUMN,
        REQUIREMENTS_FICHAS_COLUMN,
        UNCLASSIFIED_FICHAS_COLUMN,
        ADJUDICATION_TYPE_COLUMN,
        NO_REQUIREMENTS_SCOPE_COLUMN,
    )
    indexes = [module.find_idx(rows[0], column) for column in columns]
    missing = sum(index is None for index in indexes)
    empty = 0
    unknown = 0
    for row in rows[1:]:
        for index in indexes:
            if index is None or index >= len(row) or not str(row[index]).strip():
                empty += 1
        mode_idx = indexes[3]
        if (
            mode_idx is None
            or mode_idx >= len(row)
            or str(row[mode_idx]).strip() == ADJUDICATION_UNKNOWN
        ):
            unknown += 1
    return {
        "filas": max(len(rows) - 1, 0),
        "columnas_faltantes": missing,
        "celdas_vacias": empty,
        "modalidad_no_identificada": unknown,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["all", *SOURCES], default="all")
    parser.add_argument("--credentials", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    selected = list(SOURCES) if args.source == "all" else [args.source]
    for name in selected:
        source = SOURCES[name]
        if args.credentials:
            os.environ[str(source["credentials_env"])] = str(args.credentials.resolve())
        module = _load_module(name, Path(source["path"]))
        sheet = str(source["sheet"])
        targets = _sheet_target_links(module, sheet)
        print(f"[{name}] {sheet}: {len(targets)} actos por completar", flush=True)
        official_modes = _capture_official_modes(module, source, targets)
        recognized = sum(
            normalize_adjudication_type(official_modes.get(key, ""))
            != ADJUDICATION_UNKNOWN
            for key in targets
        )
        print(
            f"[{name}] modalidades oficiales resueltas: {recognized}/{len(targets)}",
            flush=True,
        )
        if not args.dry_run:
            module.ensure_no_requirements_scope(sheet, official_modes)
            print(f"[{name}] verificación: {_verify_sheet(module, sheet)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
