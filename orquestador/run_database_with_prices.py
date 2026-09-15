"""Keep the configured database pipeline, then sync RIR price references to Sheets."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def main(argv=None, *, pipeline=None, runner=None) -> int:
    if pipeline is None:
        # The existing server pipeline remains authoritative for DB and Supabase.
        from database_pipeline import main as pipeline
    result = pipeline(argv)
    if result:
        return int(result)
    runner = runner or subprocess.run
    print("[PIPELINE] Sincronizando precios RIR con Google Sheets...", flush=True)
    completed = runner(
        [sys.executable, str(REPO_ROOT / "db" / "publish_rir_price_sheets.py")],
        cwd=str(REPO_ROOT), check=False,
    )
    if completed.returncode:
        print("[ERROR] La base quedó actualizada, pero falló la publicación de precios RIR en Sheets.", file=sys.stderr, flush=True)
    return int(completed.returncode)
