from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ctni_monitor.monitor import DEFAULT_DB_PATH, CtniHttpClient, run_monitor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor diario del portal público CTNI/MINSA")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--no-sheets", action="store_true", help="No sincroniza Google Sheets (solo pruebas)")
    parser.add_argument("--probe", action="store_true", help="Comprueba endpoints con una muestra sin persistir")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if args.probe:
        client = CtniHttpClient()
        requests_rows = client._datatable(  # pylint: disable=protected-access
            "/Formularios/CargarFormulariosEstado",
            {"filtro": ""},
            page_size=3,
            max_pages=1,
            allow_partial=True,
        )
        ficha_rows = client._datatable(  # pylint: disable=protected-access
            "/Home/LoadFichasTrabajadas",
            {"idSubComite": "0", "idFiltro": "0", "filtro": ""},
            page_size=3,
            max_pages=1,
            allow_partial=True,
        )
        detail_ok = False
        if requests_rows:
            official_id = requests_rows[0].get("id") or requests_rows[0].get("Id")
            detail_ok = bool(client.fetch_request_detail(str(official_id)))
        published_check = False
        if ficha_rows:
            ficha_number = ficha_rows[0].get("numFicha") or ficha_rows[0].get("numeroFicha")
            published_check = client.confirm_published_ficha(str(ficha_number))
        homepage = client.fetch_homepage()
        print(
            "CTNI_PROBE_JSON="
            + json.dumps(
                {
                    "solicitudes": len(requests_rows),
                    "detalle_solicitud": detail_ok,
                    "fichas": len(ficha_rows),
                    "confirmacion_publicada": published_check,
                    "homepage_bytes": len(homepage.encode("utf-8")),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 0

    result = run_monitor(db_path=args.db_path, sync_sheets=not args.no_sheets)
    summary = result.summary()
    print("CTNI_SUMMARY_JSON=" + json.dumps(summary, ensure_ascii=False, separators=(",", ":")), flush=True)
    successful_sources = sum(1 for status in result.source_status.values() if status == "success")
    if successful_sources == 0:
        logging.error("Todas las fuentes CTNI fallaron; se conservaron los datos históricos")
        return 1
    if result.errors:
        logging.warning("Corrida CTNI parcial: %s", result.errors)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
