from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from orquestador.intel_priority_worker import _processable, _summary  # noqa: E402


class PriorityWorkerTests(unittest.TestCase):
    def test_processable_skips_completed_and_exhausted_and_orders_best_rank(self) -> None:
        rows = [
            {
                "ficha": "3",
                "estado": "pendiente",
                "intentos": "0",
                "rank_score": "30",
                "rank_monto_ficha_unica": "2",
                "rank_actos_ficha_unica": "",
            },
            {
                "ficha": "1",
                "estado": "completado",
                "intentos": "1",
                "rank_score": "1",
            },
            {
                "ficha": "2",
                "estado": "error",
                "intentos": "1",
                "rank_score": "3",
            },
            {
                "ficha": "4",
                "estado": "error",
                "intentos": "3",
                "rank_score": "1",
            },
        ]

        selected = _processable(rows, max_attempts=3)

        self.assertEqual([row["ficha"] for row in selected], ["3", "2"])
        self.assertEqual(_summary(rows)["error"], 2)
        self.assertEqual(_summary(rows)["completado"], 1)


if __name__ == "__main__":
    unittest.main()
