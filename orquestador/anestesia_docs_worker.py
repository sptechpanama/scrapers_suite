"""Manual worker for Anestesia-Docs, sharing pc_manual and existing Google credentials.

No emails, purchases, scheduled self-invocation, or automatic tender submissions.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys


def main():
    root = Path(__file__).resolve().parents[1]
    local_config = root / "orquestador/anestesia_docs.local.json"
    settings = json.loads(local_config.read_text(encoding="utf-8")) if local_config.is_file() else {}
    app = Path(os.environ.get("ANESTESIA_GEAPP_PATH", settings.get("geapp_path", str(root.parent / "GEAPP")))).resolve()
    if not (app / "services/anestesia_worker.py").is_file():
        raise RuntimeError("Falta services/anestesia_worker.py en GEAPP. Actualiza GEAPP o configura ANESTESIA_GEAPP_PATH.")
    sys.path.insert(0, str(app))
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    from services.anestesia_storage import AnestesiaStorage, DRIVE_PARENT, SHEET_ID
    from services.anestesia_worker import run_request

    raw = os.environ.get("ORQUESTADOR_MANUAL_PAYLOAD", "")
    if not raw:
        raise ValueError("Este worker requiere una solicitud manual desde Anestesia-Docs.")
    payload = json.loads(raw)
    credentials_file = Path(os.environ.get("ORQUESTADOR_GOOGLE_SERVICE_ACCOUNT", root / "credentials/service-account.json"))
    credentials = Credentials.from_service_account_file(str(credentials_file), scopes=[
        "https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"])
    storage = AnestesiaStorage(build("drive", "v3", credentials=credentials, cache_discovery=False),
        build("sheets", "v4", credentials=credentials, cache_discovery=False),
        sheet_id=payload.get("sheet_id", SHEET_ID), parent_id=payload.get("parent_id", DRIVE_PARENT))
    storage.ensure_tables()
    execution = os.environ.get("ORQUESTADOR_MANUAL_ID", "")
    if not execution:
        raise ValueError("Falta el identificador de la solicitud manual.")
    result = run_request(storage, payload, execution_id=execution, root=app)
    manual_row = os.environ.get("ORQUESTADOR_MANUAL_ROW")
    if manual_row:
        from sheets_bridge import update_manual_request_result
        update_manual_request_result(int(manual_row), {
            "result_file_id": result.get("folder_id", ""),
            "result_file_url": result.get("final_url") or result.get("draft_url") or result.get("folder_url", ""),
            "result_file_name": f"Anestesia-Docs · {result.get('number', '')} · {result.get('state', '')}",
            "result_error": ""})
    print(json.dumps({"expediente": result["id"], "estado": result["state"], "detalle": result.get("detail", "")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
