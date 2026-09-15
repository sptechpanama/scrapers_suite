"""Pipeline robusto de actualización de PanamáCompra y su capa analítica.

El proceso publica estados estructurados en stdout. El orquestador los captura
y los registra por separado como:

- ``db_local``
- ``supabase_operational``
- ``analytics`` (proveedores médicos + mercado no médico Inteligencia PC)

De esta manera un fallo posterior no oculta qué componentes sí quedaron
actualizados.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence
from collections import deque


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON_EXE = REPO_ROOT / ".venv" / "Scripts" / "python.exe"
DB_UPDATER = REPO_ROOT / "db" / "db_api_updater.py"
ANALYTICS_BUILDER = REPO_ROOT / "db" / "build_intelligence_tables.py"
OPERATIONAL_DB = REPO_ROOT / "data" / "db" / "panamacompra.db"
ANALYTICS_DB = REPO_ROOT / "data" / "db" / "inteligencia_proveedores.db"
GEAPP_ROOT = Path(os.environ.get("GEAPP_ROOT", str(Path.home() / "GEAPP"))).resolve()
PC_ANALYTICS_BUILDER = GEAPP_ROOT / "scripts" / "build_inteligencia_pc.py"
PC_ANALYTICS_DB = REPO_ROOT / "data" / "db" / "inteligencia_pc.db"
COMPONENT_PREFIX = "ORQUESTADOR_COMPONENT_STATE="
DEFAULT_FULL_RECONCILE_FROM = "2024-01-01"
LAST_COMMAND_ERROR = ""


def _now() -> datetime:
    return datetime.now()


def emit_component(
    job_name: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    detail: str = "",
) -> None:
    payload = {
        "job_name": job_name,
        "status": status,
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "detail": str(detail or "")[:2000],
    }
    print(
        COMPONENT_PREFIX + json.dumps(payload, ensure_ascii=False, sort_keys=True),
        flush=True,
    )


def read_metadata(database_path: Path) -> dict[str, str]:
    if not database_path.exists() or database_path.stat().st_size == 0:
        return {}
    connection = sqlite3.connect(
        f"file:{database_path.as_posix()}?mode=ro",
        uri=True,
        timeout=30,
    )
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='db_metadata'"
        ).fetchone()
        if not table:
            return {}
        return {
            str(key): "" if value is None else str(value)
            for key, value in connection.execute(
                "SELECT key, value FROM db_metadata"
            ).fetchall()
        }
    finally:
        connection.close()


def read_analytics_metadata(database_path: Path) -> dict[str, str]:
    if not database_path.exists() or database_path.stat().st_size == 0:
        return {}
    connection = sqlite3.connect(
        f"file:{database_path.as_posix()}?mode=ro",
        uri=True,
        timeout=30,
    )
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='intel_build_metadata'"
        ).fetchone()
        if not table:
            return {}
        return {
            str(key): "" if value is None else str(value)
            for key, value in connection.execute(
                "SELECT key, value FROM intel_build_metadata"
            ).fetchall()
        }
    finally:
        connection.close()


def read_pc_analytics_metadata(database_path: Path) -> dict[str, str]:
    if not database_path.exists() or database_path.stat().st_size == 0:
        return {}
    connection = sqlite3.connect(
        f"file:{database_path.as_posix()}?mode=ro",
        uri=True,
        timeout=30,
    )
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='pc_build_metadata'"
        ).fetchone()
        if not table:
            return {}
        return {
            str(key): "" if value is None else str(value)
            for key, value in connection.execute(
                "SELECT key, value FROM pc_build_metadata"
            ).fetchall()
        }
    finally:
        connection.close()


def _run(command: Sequence[str]) -> int:
    global LAST_COMMAND_ERROR
    LAST_COMMAND_ERROR = ""
    print("[PIPELINE] " + subprocess.list2cmdline(list(command)), flush=True)
    log_dir = REPO_ROOT / "data" / "pipeline_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{_now():%Y%m%d_%H%M%S}_{Path(command[1]).stem}.log"
    tail = deque(maxlen=25)
    with log_path.open("w", encoding="utf-8") as logfile:
        with subprocess.Popen(list(command), cwd=str(REPO_ROOT),
                              env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, encoding="utf-8", errors="replace") as process:
            for line in process.stdout:
                logfile.write(line)
                logfile.flush()
                print(line, end="", flush=True)
                tail.append(line.strip())
            result = process.wait()
    if result:
        LAST_COMMAND_ERROR = " | ".join(line for line in tail if line)[-1500:]
    return int(result)


def updater_command(mode: str) -> list[str]:
    command = [
        str(PYTHON_EXE),
        str(DB_UPDATER),
        "--require-postgres",
    ]
    if mode == "full":
        command.extend(
            [
                "--force-reclassify",
                "--postgres-full",
                "--from-date",
                os.environ.get(
                    "DB_FULL_RECONCILE_FROM",
                    DEFAULT_FULL_RECONCILE_FROM,
                ),
            ]
        )
    else:
        # New/changed acts are classified during ingestion. Reclassifying the
        # entire archive belongs to the weekly job and must not delay daily
        # publication whenever a catalog/detector version changes.
        command.append("--skip-reclassify")
    return command


def analytics_command() -> list[str]:
    return [
        str(PYTHON_EXE),
        str(ANALYTICS_BUILDER),
        "--publish-postgres",
        "--require-postgres",
    ]


def pc_analytics_command() -> list[str]:
    return [
        str(PYTHON_EXE),
        str(PC_ANALYTICS_BUILDER),
        "--source",
        str(OPERATIONAL_DB),
        "--output",
        str(PC_ANALYTICS_DB),
        "--publish-postgres",
        "--require-postgres",
    ]


def _local_detail(metadata: dict[str, str], mode: str) -> str:
    fields = [
        f"modo={mode}",
        f"filas={metadata.get('last_total_rows', '?')}",
        f"fecha_fuente={metadata.get('last_data_source_max_date', '?')}",
        f"nuevos={metadata.get('last_new_links_count', '?')}",
        f"reclasificadas={metadata.get('last_reclassified_rows', '?')}",
    ]
    error = metadata.get("last_local_update_error", "").strip()
    if error:
        fields.append(f"error={error}")
    return " | ".join(fields)


def _postgres_detail(metadata: dict[str, str], mode: str) -> str:
    fields = [
        f"modo={mode}",
        "publicación obligatoria",
        f"fecha_fuente={metadata.get('last_data_source_max_date', '?')}",
    ]
    error = metadata.get("last_postgres_sync_error", "").strip()
    if error:
        fields.append(f"error={error}")
    return " | ".join(fields)


def _analytics_detail(
    metadata: dict[str, str],
    pc_metadata: dict[str, str],
    mode: str,
) -> str:
    return " | ".join(
        [
            f"modo={mode}",
            f"construida={metadata.get('built_at_utc', '?')}",
            f"relaciones_ficha_acto={metadata.get('fact_rows', '?')}",
            f"proponentes={metadata.get('proponent_rows', '?')}",
            f"filas_fuente={metadata.get('source_rows', '?')}",
            f"pc_construida={pc_metadata.get('built_at_utc', '?')}",
            f"pc_actos={pc_metadata.get('act_rows', '?')}",
            f"pc_propuestas={pc_metadata.get('proposal_rows', '?')}",
        ]
    )


def run_pipeline(mode: str) -> int:
    if mode not in {"incremental", "full"}:
        raise ValueError(f"Modo no soportado: {mode}")
    if not (os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")):
        now = _now()
        detail = (
            "Falta SUPABASE_DB_URL/DATABASE_URL. La ejecución se detuvo antes "
            "de modificar la base para evitar una actualización solo local."
        )
        emit_component("db_local", "error", now, now, detail)
        emit_component("supabase_operational", "error", now, now, detail)
        emit_component("analytics", "blocked", now, now, detail)
        print(f"[ERROR] {detail}", file=sys.stderr, flush=True)
        return 2

    update_started = _now()
    try:
        updater_returncode = _run(updater_command(mode))
        update_finished = _now()
        operational_metadata = read_metadata(OPERATIONAL_DB)
    except Exception as exc:  # pylint: disable=broad-except
        update_finished = _now()
        detail = f"No se pudo ejecutar o verificar la actualización: {type(exc).__name__}: {exc}"
        emit_component("db_local", "error", update_started, update_finished, detail)
        emit_component(
            "supabase_operational",
            "error",
            update_started,
            update_finished,
            detail,
        )
        emit_component(
            "analytics",
            "blocked",
            update_finished,
            update_finished,
            "No se reconstruyó porque falló la etapa operacional.",
        )
        print(f"[ERROR] {detail}", file=sys.stderr, flush=True)
        return 1

    # A lock/preflight error may happen before the updater writes any metadata.
    # Never report yesterday's success or counters as this execution's result.
    try:
        metadata_started = datetime.fromisoformat(operational_metadata.get("last_run_started_at", ""))
        current_run = metadata_started >= update_started.replace(microsecond=0)
    except ValueError:
        current_run = False
    if not current_run:
        detail = LAST_COMMAND_ERROR or f"Actualizador terminó sin registrar esta ejecución (salida {updater_returncode})."
        for component in ("db_local", "supabase_operational", "analytics"):
            emit_component(component, "blocked", update_started, update_finished, detail)
        return updater_returncode or 1

    local_status = operational_metadata.get("last_local_update_status", "")
    if local_status not in {"success", "error"}:
        local_status = "success" if updater_returncode == 0 else "error"
    postgres_status = operational_metadata.get("last_postgres_sync_status", "")
    if postgres_status != "success":
        postgres_status = "error"

    emit_component(
        "db_local",
        local_status,
        update_started,
        update_finished,
        _local_detail(operational_metadata, mode),
    )
    emit_component(
        "supabase_operational",
        postgres_status,
        update_started,
        update_finished,
        _postgres_detail(operational_metadata, mode),
    )

    if updater_returncode != 0 or local_status != "success" or postgres_status != "success":
        emit_component(
            "analytics",
            "blocked",
            update_finished,
            update_finished,
            "No se reconstruyó porque la base local o Supabase operacional no "
            "terminaron correctamente.",
        )
        return updater_returncode or 1

    analytics_started = _now()
    try:
        analytics_returncode = _run(analytics_command())
        pc_analytics_returncode = _run(pc_analytics_command())
        analytics_finished = _now()
        analytics_metadata = read_analytics_metadata(ANALYTICS_DB)
        pc_analytics_metadata = read_pc_analytics_metadata(PC_ANALYTICS_DB)
    except Exception as exc:  # pylint: disable=broad-except
        analytics_finished = _now()
        emit_component(
            "analytics",
            "error",
            analytics_started,
            analytics_finished,
            f"No se pudo construir o verificar la analítica: {type(exc).__name__}: {exc}",
        )
        return 1
    analytics_status = (
        "success"
        if analytics_returncode == 0 and pc_analytics_returncode == 0
        else "error"
    )
    emit_component(
        "analytics",
        analytics_status,
        analytics_started,
        analytics_finished,
        _analytics_detail(analytics_metadata, pc_analytics_metadata, mode),
    )
    return analytics_returncode or pc_analytics_returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Actualiza base local, Supabase y analítica en orden seguro"
    )
    parser.add_argument(
        "--mode",
        choices=("incremental", "full"),
        default="incremental",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run_pipeline(args.mode)
    except Exception as exc:  # pylint: disable=broad-except
        now = _now()
        detail = f"Excepción no controlada del pipeline: {type(exc).__name__}: {exc}"
        for component in ("db_local", "supabase_operational", "analytics"):
            emit_component(component, "error", now, now, detail)
        print(
            f"[ERROR] {type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
