from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from otras_fuentes import run_monitor


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor diario de siete fuentes externas")
    parser.add_argument("--require-postgres", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    result = run_monitor(require_postgres=args.require_postgres)
    summary = result.summary()
    print("OTRAS_FUENTES_SUMMARY_JSON=" + json.dumps(summary, ensure_ascii=False, default=str))
    return 1 if result.status == "error" else 0


if __name__ == "__main__":
    raise SystemExit(main())

