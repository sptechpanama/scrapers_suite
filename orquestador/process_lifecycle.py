"""Protect publication jobs and terminate their complete process tree."""
from __future__ import annotations

import os
import subprocess


PROTECTED_PUBLICATION_JOBS = frozenset({
    "database_daily", "database_weekly_full", "sunday_db_minsa",
})


def can_preempt_job(name: str) -> bool:
    return name != "cotizacion_panama" and name not in PROTECTED_PUBLICATION_JOBS


def terminate_process_tree(process, *, timeout: float = 15) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        # terminate()/kill() on the venv launcher leave grandchildren alive on Windows.
        result = subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True, timeout=timeout, check=False,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        if result.returncode and process.poll() is None:
            raise RuntimeError(f"No se pudo cerrar el árbol de procesos PID {process.pid}")
    else:
        # Jobs are launched in their own session by run_job_interruptible.
        import signal
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout=timeout)
