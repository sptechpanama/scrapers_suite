from __future__ import annotations

import json
import itertools
import logging
import os
import queue
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_JOB_SUBMITTED,
)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel, validator
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from sheets_bridge import (
    CONFIG_SHEET_NAME,
    SPREADSHEET_ID,
    STATE_SHEET_NAME,
    fetch_manual_requests,
    fetch_jobs_from_sheet,
    push_jobs_to_sheet,
    push_state_to_sheet,
    update_manual_request_status,
    update_manual_request_result,
    SERVICE_ACCOUNT_FILE,
)

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
STATE_PATH = BASE_DIR / "state.json"
DEFAULT_ORQUESTADOR_CREDENTIALS = BASE_DIR / "pure-beach-474203-p1-fdc9557f33d0.json"

# Ajusta esta lista para controlar manualmente los horarios sin depender todavía de una interfaz externa.
DEFAULT_TIMEZONE = "America/Panama"

try:
    CONFIG_REFRESH_INTERVAL_MINUTES = int(
        os.environ.get("ORQUESTADOR_CONFIG_REFRESH_MINUTES", "2")
    )
except ValueError:
    CONFIG_REFRESH_INTERVAL_MINUTES = 2

CONFIG_WATCH_JOB_ID = "sheets-config-refresh"
MANUAL_WATCH_JOB_ID = "sheets-manual-poll"

try:
    MANUAL_POLL_INTERVAL_SECONDS = int(
        os.environ.get("ORQUESTADOR_MANUAL_POLL_SECONDS", "30")
    )
except ValueError:
    MANUAL_POLL_INTERVAL_SECONDS = 30

DEFAULT_JOB_DATA = [
    {
        "name": "clv",
        "python": "C:/Users/rodri/clv/venv/Scripts/python.exe",
        "script": "C:/Users/rodri/clv/clv.py",
        "days_of_week": ["mon", "tue", "wed", "thu", "fri"],
        "times": ["08:00", "14:00", "22:04"],
    },
    {
        "name": "clrir",
        "python": "C:/Users/rodri/clrir/venv/Scripts/python.exe",
        "script": "C:/Users/rodri/clrir/clrir.py",
        "days_of_week": ["mon", "tue", "wed", "thu", "fri"],
        "times": ["08:30", "15:00"],
    },
    {
        "name": "rir1",
        "python": "C:/Users/rodri/rir1/venv/Scripts/python.exe",
        "script": "C:/Users/rodri/rir1/rir1.py",
        "days_of_week": ["mon", "wed", "fri"],
        "times": ["09:00"],
    },
]

JOB_NAME_LABELS = {
    "clv": "Cotizaciones Abiertas",
    "clrir": "Cotizaciones Programadas",
    "rir1": "Licitaciones",
}

JOB_LABEL_TITLES = {
    CONFIG_WATCH_JOB_ID: "monitor de configuracion",
    MANUAL_WATCH_JOB_ID: "monitor de solicitudes manuales",
}

class JobConfig(BaseModel):
    name: str
    python: str
    script: str
    days_of_week: List[str]
    times: List[str]

    @validator("times", each_item=True)
    def _check_time_format(cls, value: str) -> str:
        parts = value.split(":")
        if len(parts) != 2:
            raise ValueError("time must be in HH:MM format")
        hour, minute = parts
        if not hour.isdigit() or not minute.isdigit():
            raise ValueError("hour and minute must be numeric")
        h, m = int(hour), int(minute)
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError("hour must be 0-23 and minute 0-59")
        return f"{h:02d}:{m:02d}"

    @validator("days_of_week", each_item=True)
    def _normalize_days(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}:
            raise ValueError("days_of_week must use three-letter lowercase abbreviations")
        return normalized


class OrchestratorConfig(BaseModel):
    jobs: List[JobConfig] = []


@dataclass
class ExecutionRequest:
    job: JobConfig
    source: str
    manual_row: Optional[int] = None
    manual_id: Optional[str] = None
    manual_requested_by: Optional[str] = None
    manual_requested_at: Optional[str] = None
    manual_notes: Optional[str] = None
    manual_payload: Optional[str] = None


def format_execution_label(execution: ExecutionRequest) -> str:
    base_label = JOB_NAME_LABELS.get(execution.job.name, execution.job.name)
    if execution.source == "manual":
        manual_tag = execution.manual_id
        if not manual_tag and execution.manual_row is not None:
            manual_tag = f"fila {execution.manual_row + 1}"
        manual_tag = manual_tag or "sin-id"
        return f"{base_label} (manual {manual_tag})"
    return f"{base_label} ({execution.source})"


def describe_manual_request(manual_request: Dict[str, str]) -> tuple[str, str]:
    request_id = (
        manual_request.get("id")
        or manual_request.get("row")
        or manual_request.get("requested_at")
        or "sin-id"
    )
    requested_by = manual_request.get("requested_by") or "desconocido"
    return request_id, requested_by


def manual_log(message: str, *, level: str = "info") -> None:
    """Envía el mensaje tanto al logger como a stdout para facilitar el monitoreo."""
    if level == "error":
        logging.error(message)
    elif level == "warning":
        logging.warning(message)
    else:
        logging.info(message)
    print(message, flush=True)


def configure_service_account() -> None:
    """Asegura que el orquestador use las credenciales de Finance sin afectar otros scrapers."""
    manual_path = os.environ.get("ORQUESTADOR_SERVICE_ACCOUNT_FILE")
    candidate = None
    if manual_path:
        candidate = Path(manual_path).expanduser()
    elif DEFAULT_ORQUESTADOR_CREDENTIALS.exists():
        candidate = DEFAULT_ORQUESTADOR_CREDENTIALS

    if candidate and candidate.exists():
        os.environ["FINAPP_SERVICE_ACCOUNT_FILE"] = str(candidate)
        logging.info("Cuenta de servicio para orquestador: %s", candidate)
    else:
        logging.warning(
            "No se pudo establecer FINAPP_SERVICE_ACCOUNT_FILE específico del orquestador; "
            "se usará la configuración existente."
        )


def build_config_signature(config: OrchestratorConfig) -> tuple:
    signature: list[tuple] = []
    for job in sorted(config.jobs, key=lambda item: item.name):
        signature.append(
            (
                job.name,
                job.python,
                job.script,
                tuple(sorted(job.days_of_week)),
                tuple(sorted(job.times)),
            )
        )
    return tuple(signature)


def load_config() -> OrchestratorConfig:
    sheet_jobs: List[JobConfig] = []
    sheet_had_rows = False
    try:
        sheet_job_dicts, sheet_had_rows = fetch_jobs_from_sheet()
        if sheet_job_dicts:
            sheet_jobs = [JobConfig(**job_dict) for job_dict in sheet_job_dicts]
            logging.info(
                "Configuracion leida de Google Sheets (spreadsheet %s | pestana %s)",
                SPREADSHEET_ID,
                CONFIG_SHEET_NAME,
            )
        elif sheet_had_rows:
            logging.warning(
                "La hoja %s/%s no tiene filas validas; usare la configuracion local",
                SPREADSHEET_ID,
                CONFIG_SHEET_NAME,
            )
    except Exception:  # pylint: disable=broad-except
        logging.exception("Error al obtener la configuracion desde Google Sheets")

    if sheet_jobs:
        return OrchestratorConfig(jobs=sheet_jobs)

    fallback_config: OrchestratorConfig
    fallback_job_dicts: List[dict]
    fallback_source: str

    if CONFIG_PATH.exists():
        with CONFIG_PATH.open("r", encoding="utf-8") as fp:
            data = json.load(fp)
        fallback_config = OrchestratorConfig(**data)
        fallback_job_dicts = [job.dict() for job in fallback_config.jobs]
        fallback_source = f"archivo {CONFIG_PATH}"
    else:
        fallback_jobs = [JobConfig(**job) for job in DEFAULT_JOB_DATA]
        fallback_config = OrchestratorConfig(jobs=fallback_jobs)
        fallback_job_dicts = [job.dict() for job in fallback_jobs]
        fallback_source = "valores por defecto definidos en el código"

    logging.info("Configuracion tomada desde %s", fallback_source)

    # if not sheet_had_rows and fallback_job_dicts:
    #     try:
    #         push_jobs_to_sheet(fallback_job_dicts)
    #         logging.info(
    #             "Config base replicada en Google Sheets (spreadsheet %s, pestaña %s)",
    #             SPREADSHEET_ID,
    #             CONFIG_SHEET_NAME,
    #         )
    #     except Exception:  # pylint: disable=broad-except
    #         logging.exception("No se pudo sembrar la configuración inicial en Google Sheets")

    return fallback_config


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {"last_run": {}}
    with STATE_PATH.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with STATE_PATH.open("w", encoding="utf-8") as fp:
        json.dump(state, fp, indent=2, ensure_ascii=False)


DEFAULT_COT_DRIVE_FOLDER_ID = os.environ.get(
    "ORQUESTADOR_COTIZACIONES_FOLDER_ID",
    "0AOB-QlptrUHYUk9PVA",
)


def _get_drive_client():
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_FILE), scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _upload_excel_to_drive(
    local_path: str,
    filename: str,
    folder_id: str | None = None,
    existing_file_id: str | None = None,
) -> dict:
    media = MediaFileUpload(
        local_path,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=False,
    )
    drive = _get_drive_client()
    if existing_file_id:
        return drive.files().update(
            fileId=existing_file_id,
            media_body=media,
            fields="id,name",
            supportsAllDrives=True,
        ).execute()

    metadata: Dict[str, object] = {"name": filename}
    if folder_id:
        metadata["parents"] = [folder_id]
    return drive.files().create(
        body=metadata,
        media_body=media,
        fields="id,name",
        supportsAllDrives=True,
    ).execute()


def _extract_result_json(stdout: str) -> Optional[Dict[str, str]]:
    if not stdout:
        return None
    for line in stdout.splitlines()[::-1]:
        line = line.strip()
        if not line.startswith("RESULT_JSON="):
            continue
        raw = line.split("RESULT_JSON=", 1)[-1].strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if isinstance(payload, dict):
            return payload
        return None
    return None


def update_last_run(
    job_name: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    detail: str | None = None,
) -> None:
    state = load_state()
    duration_seconds = (finished_at - started_at).total_seconds()
    duration_display = format_duration(duration_seconds)
    state.setdefault("last_run", {})[job_name] = {
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "duration_seconds": round(duration_seconds, 3),
        "duration_display": duration_display,
        "status": status,
        "detail": detail or "",
    }
    save_state(state)
    try:
        push_state_to_sheet(state)
    except Exception:  # pylint: disable=broad-except
        logging.exception("No se pudo sincronizar el estado con Google Sheets")


def run_job(job: JobConfig, execution: Optional[ExecutionRequest] = None) -> tuple[str, str]:
    logging.info("Iniciando ejecucion del job %s", job.name)
    cmd = [job.python, job.script]
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    if execution and execution.source == "manual":
        if execution.manual_row is not None:
            env["ORQUESTADOR_MANUAL_ROW"] = str(execution.manual_row)
        if execution.manual_id:
            env["ORQUESTADOR_MANUAL_ID"] = str(execution.manual_id)
        if execution.manual_requested_by:
            env["ORQUESTADOR_MANUAL_REQUESTED_BY"] = str(execution.manual_requested_by)
        if execution.manual_requested_at:
            env["ORQUESTADOR_MANUAL_REQUESTED_AT"] = str(execution.manual_requested_at)
        if execution.manual_payload:
            env["ORQUESTADOR_MANUAL_PAYLOAD"] = str(execution.manual_payload)
    start_time = datetime.now()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
        end_time = datetime.now()
        if result.returncode == 0:
            logging.info("Job %s finalizo correctamente", job.name)
            if result.stdout:
                logging.debug("%s stdout:\n%s", job.name, result.stdout.strip())
            if execution and execution.manual_row is not None:
                result_json = _extract_result_json(result.stdout)
                if result_json:
                    local_path = str(result_json.get("local_path") or "")
                    file_name = str(result_json.get("file_name") or "")
                    if local_path and os.path.exists(local_path):
                        if not file_name:
                            file_name = os.path.basename(local_path)
                        try:
                            upload = _upload_excel_to_drive(
                                local_path,
                                file_name,
                                folder_id=DEFAULT_COT_DRIVE_FOLDER_ID,
                            )
                            file_id = upload.get("id", "")
                            file_url = (
                                f"https://drive.google.com/file/d/{file_id}/view"
                                if file_id
                                else ""
                            )
                            update_manual_request_result(
                                execution.manual_row,
                                {
                                    "result_file_id": file_id,
                                    "result_file_url": file_url,
                                    "result_file_name": upload.get("name", file_name),
                                    "result_error": "",
                                },
                            )
                        except Exception as exc:  # pylint: disable=broad-except
                            update_manual_request_result(
                                execution.manual_row,
                                {"result_error": str(exc)},
                            )
            update_last_run(job.name, "success", started_at=start_time, finished_at=end_time)
            return "success", ""
        else:
            logging.error("Job %s fallo (return code %s)", job.name, result.returncode)
            if result.stdout:
                logging.error("%s stdout:\n%s", job.name, result.stdout.strip())
            if result.stderr:
                logging.error("%s stderr:\n%s", job.name, result.stderr.strip())
            detail = result.stderr.strip() if result.stderr else "return code != 0"
            update_last_run(
                job.name,
                "error",
                started_at=start_time,
                finished_at=end_time,
                detail=detail,
            )
            return "error", detail
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("Excepcion no controlada al ejecutar el job %s", job.name)
        end_time = datetime.now()
        update_last_run(
            job.name,
            "exception",
            started_at=start_time,
            finished_at=end_time,
            detail=str(exc),
        )
        return "exception", str(exc)


def run_job_interruptible(
    job: JobConfig,
    execution: Optional[ExecutionRequest] = None,
    *,
    interrupt_event: Optional[threading.Event] = None,
    on_process_start=None,
    on_process_end=None,
) -> tuple[str, str]:
    logging.info("Iniciando ejecucion del job %s", job.name)
    cmd = [job.python, job.script]
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    if execution and execution.source == "manual":
        if execution.manual_row is not None:
            env["ORQUESTADOR_MANUAL_ROW"] = str(execution.manual_row)
        if execution.manual_id:
            env["ORQUESTADOR_MANUAL_ID"] = str(execution.manual_id)
        if execution.manual_requested_by:
            env["ORQUESTADOR_MANUAL_REQUESTED_BY"] = str(execution.manual_requested_by)
        if execution.manual_requested_at:
            env["ORQUESTADOR_MANUAL_REQUESTED_AT"] = str(execution.manual_requested_at)
        if execution.manual_payload:
            env["ORQUESTADOR_MANUAL_PAYLOAD"] = str(execution.manual_payload)

    start_time = datetime.now()
    process: Optional[subprocess.Popen] = None
    interrupted = False
    interrupt_detail = "Interrumpido por prioridad de cotizacion_panama"
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        if on_process_start:
            on_process_start(execution, process)

        while True:
            if (
                interrupt_event is not None
                and interrupt_event.is_set()
                and execution is not None
                and execution.job.name != "cotizacion_panama"
                and process.poll() is None
            ):
                interrupted = True
                try:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                except Exception:  # pylint: disable=broad-except
                    logging.exception("No se pudo interrumpir el proceso del job %s", job.name)
                break
            if process.poll() is not None:
                break
            time.sleep(0.5)

        stdout, stderr = process.communicate()
        end_time = datetime.now()

        if interrupted:
            update_last_run(
                job.name,
                "interrupted",
                started_at=start_time,
                finished_at=end_time,
                detail=interrupt_detail,
            )
            return "interrupted_preempt", interrupt_detail

        if process.returncode == 0:
            logging.info("Job %s finalizo correctamente", job.name)
            if stdout:
                logging.debug("%s stdout:\n%s", job.name, stdout.strip())
            if execution and execution.manual_row is not None:
                result_json = _extract_result_json(stdout)
                if result_json:
                    local_path = str(result_json.get("local_path") or "")
                    file_name = str(result_json.get("file_name") or "")
                    if local_path and os.path.exists(local_path):
                        if not file_name:
                            file_name = os.path.basename(local_path)
                        try:
                            upload = _upload_excel_to_drive(
                                local_path,
                                file_name,
                                folder_id=DEFAULT_COT_DRIVE_FOLDER_ID,
                            )
                            file_id = upload.get("id", "")
                            file_url = (
                                f"https://drive.google.com/file/d/{file_id}/view"
                                if file_id
                                else ""
                            )
                            update_manual_request_result(
                                execution.manual_row,
                                {
                                    "result_file_id": file_id,
                                    "result_file_url": file_url,
                                    "result_file_name": upload.get("name", file_name),
                                    "result_error": "",
                                },
                            )
                        except Exception as exc:  # pylint: disable=broad-except
                            update_manual_request_result(
                                execution.manual_row,
                                {"result_error": str(exc)},
                            )
            update_last_run(job.name, "success", started_at=start_time, finished_at=end_time)
            return "success", ""

        logging.error("Job %s fallo (return code %s)", job.name, process.returncode)
        if stdout:
            logging.error("%s stdout:\n%s", job.name, stdout.strip())
        if stderr:
            logging.error("%s stderr:\n%s", job.name, stderr.strip())
        detail = stderr.strip() if stderr else "return code != 0"
        update_last_run(
            job.name,
            "error",
            started_at=start_time,
            finished_at=end_time,
            detail=detail,
        )
        return "error", detail
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("Excepcion no controlada al ejecutar el job %s", job.name)
        end_time = datetime.now()
        update_last_run(
            job.name,
            "exception",
            started_at=start_time,
            finished_at=end_time,
            detail=str(exc),
        )
        return "exception", str(exc)
    finally:
        if on_process_end:
            on_process_end()
        if interrupt_event is not None:
            interrupt_event.clear()


def format_duration(total_seconds: float) -> str:
    total_seconds_int = int(round(total_seconds))
    hours, remainder = divmod(total_seconds_int, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts: list[str] = []
    if hours:
        parts.append(f"{hours} h" if hours > 1 else "1 h")
    if minutes:
        parts.append(f"{minutes} min")
    if seconds or not parts:
        parts.append(f"{seconds} s")
    return " ".join(parts)


def compose_note(existing: Optional[str], addition: str) -> str:
    existing_normalized = (existing or "").strip()
    addition_normalized = addition.strip()
    if not existing_normalized:
        return addition_normalized
    if not addition_normalized:
        return existing_normalized
    return f"{existing_normalized} | {addition_normalized}"


def schedule_jobs(
    scheduler: BackgroundScheduler,
    config: OrchestratorConfig,
    enqueue_func,
) -> None:
    for job in config.jobs:
        if not job.days_of_week or not job.times:
            logging.info("Job %s configurado como manual-only; no se agenda.", job.name)
            continue
        days = ",".join(job.days_of_week)
        for time_str in job.times:
            hour, minute = map(int, time_str.split(":"))
            trigger = CronTrigger(day_of_week=days, hour=hour, minute=minute, timezone=DEFAULT_TIMEZONE)
            job_id = f"{job.name}-{hour:02d}{minute:02d}"
            scheduler.add_job(
                enqueue_func,
                trigger=trigger,
                args=[job, "cron"],
                id=job_id,
                name=job.name,
                replace_existing=True,
            )
            logging.info(
                "Job %s programado a las %s (dias: %s)",
                job.name,
                time_str,
                days,
            )


def apply_config_to_scheduler(
    scheduler: BackgroundScheduler,
    config: OrchestratorConfig,
    protected_job_ids: set[str],
    enqueue_func,
) -> None:
    for scheduled_job in scheduler.get_jobs():
        if scheduled_job.id in protected_job_ids:
            continue
        scheduler.remove_job(scheduled_job.id)
    schedule_jobs(scheduler, config, enqueue_func)
    logging.info(
        "Programador actualizado: %d jobs activos",
        len([job for job in scheduler.get_jobs() if job.id not in protected_job_ids]),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.getLogger("apscheduler").setLevel(logging.WARNING)

    configure_service_account()

    try:
        config = load_config()
    except Exception as exc:  # pylint: disable=broad-except
        logging.exception("No se pudo cargar la configuracion del orquestador")
        raise SystemExit(1) from exc

    current_config = config
    current_signature = build_config_signature(current_config)
    job_lookup: Dict[str, JobConfig] = {job.name: job for job in current_config.jobs}

    try:
        current_state = load_state()
        push_state_to_sheet(current_state)
        logging.info(
            "Estado local enviado a Google Sheets (spreadsheet %s | pestana %s)",
            SPREADSHEET_ID,
            STATE_SHEET_NAME,
        )
    except Exception:  # pylint: disable=broad-except
        logging.exception("No se pudo sincronizar el estado inicial con Google Sheets")

    job_queue: "queue.PriorityQueue[tuple[int, int, ExecutionRequest]]" = queue.PriorityQueue()
    queue_counter = itertools.count()
    running_lock = threading.Lock()
    running_execution: Optional[ExecutionRequest] = None
    running_process: Optional[subprocess.Popen] = None
    interrupt_running_event = threading.Event()
    stop_event = threading.Event()
    protected_job_ids: set[str] = set()

    def to_row_index(raw_value: Optional[str]) -> Optional[int]:
        try:
            if raw_value is None:
                return None
            stripped = str(raw_value).strip()
            if not stripped:
                return None
            return int(stripped)
        except (TypeError, ValueError):
            return None

    scheduler = BackgroundScheduler(timezone=DEFAULT_TIMEZONE)

    def describe_job(job_id: str) -> str:
        if job_id in JOB_LABEL_TITLES:
            return JOB_LABEL_TITLES[job_id]
        job = scheduler.get_job(job_id)
        if job is None:
            return job_id
        job_name = job.name or job_id
        if job.id and job.id != job_name:
            return f"{job_name} ({job.id})"
        return job_name

    def job_event_listener(event) -> None:
        label = describe_job(event.job_id)
        if event.code == EVENT_JOB_SUBMITTED:
            logging.info("%s: inicio de ejecucion", label)
            return
        if event.code == EVENT_JOB_EXECUTED:
            logging.info("%s: ejecucion exitosa", label)
            return
        if event.code == EVENT_JOB_ERROR:
            logging.error("%s: ejecucion con error: %s", label, event.exception)
            return
        if event.code == EVENT_JOB_MISSED:
            logging.warning(
                "%s: ejecucion saltada; estaba programada para %s",
                label,
                event.scheduled_run_time.isoformat() if event.scheduled_run_time else "hora desconocida",
            )

    scheduler.add_listener(
        job_event_listener,
        EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
    )

    def enqueue_execution(
        job: JobConfig,
        source: str,
        manual_request: Optional[Dict[str, str]] = None,
        existing_execution: Optional[ExecutionRequest] = None,
        priority_override: Optional[int] = None,
    ) -> None:
        priority = 50
        if source == "manual":
            # Prioridad alta para cotización Panamá Compra:
            # termina el scraper en curso y luego se ejecuta antes que el resto de cola.
            priority = 0 if job.name == "cotizacion_panama" else 10
        if source == "resumed":
            priority = 5
        if priority_override is not None:
            priority = int(priority_override)

        manual_row: Optional[int] = None
        manual_id: Optional[str] = None
        manual_requested_by: Optional[str] = None
        manual_requested_at: Optional[str] = None
        manual_notes: Optional[str] = None
        manual_payload: Optional[str] = None

        if existing_execution is not None:
            execution = existing_execution
            execution.source = source
        elif manual_request:
            manual_row = to_row_index(manual_request.get("row"))
            manual_id = manual_request.get("id")
            manual_requested_by = manual_request.get("requested_by")
            manual_requested_at = manual_request.get("requested_at")
            manual_notes = manual_request.get("notes")
            manual_payload = manual_request.get("payload")
            if manual_row is not None:
                enqueue_note = compose_note(
                    manual_notes,
                    f"Encolado {datetime.now().isoformat(timespec='seconds')}",
                )
                try:
                    update_manual_request_status(manual_row, "enqueued", enqueue_note)
                    manual_notes = enqueue_note
                    logging.info(
                        "Solicitud manual %s marcada como enqueued",
                        manual_id or f"fila {manual_row}",
                    )
                except Exception:  # pylint: disable=broad-except
                    logging.exception(
                        "No se pudo marcar la solicitud manual fila %s como encolada",
                        manual_row,
                    )

            execution = ExecutionRequest(
                job=job,
                source=source,
                manual_row=manual_row,
                manual_id=manual_id,
                manual_requested_by=manual_requested_by,
                manual_requested_at=manual_requested_at,
                manual_notes=manual_notes,
                manual_payload=manual_payload,
            )
        job_queue.put((priority, next(queue_counter), execution))
        logging.info(
            "Job %s agregado a la cola (origen: %s, prioridad: %s)",
            job.name,
            source,
            priority,
        )
        if manual_request:
            manual_tag, requester = describe_manual_request(manual_request)
            manual_log(
                f"Solicitud manual {manual_tag} ({requester}) encolada para {JOB_NAME_LABELS.get(job.name, job.name)}"
            )
        if source == "manual" and job.name == "cotizacion_panama":
            with running_lock:
                should_preempt = (
                    running_execution is not None
                    and running_process is not None
                    and running_process.poll() is None
                    and running_execution.job.name != "cotizacion_panama"
                )
                running_name = running_execution.job.name if running_execution else ""
            if should_preempt:
                manual_log(
                    f"Solicitud prioritaria de cotización detectada: interrumpiendo {running_name} para ejecutar cotizacion_panama.",
                    level="warning",
                )
                interrupt_running_event.set()

    def _register_running(execution: Optional[ExecutionRequest], process: subprocess.Popen) -> None:
        nonlocal running_execution, running_process
        with running_lock:
            running_execution = execution
            running_process = process

    def _clear_running() -> None:
        nonlocal running_execution, running_process
        with running_lock:
            running_execution = None
            running_process = None

    def worker_loop() -> None:
        while True:
            if stop_event.is_set() and job_queue.empty():
                break
            try:
                _, _, execution = job_queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                label = format_execution_label(execution)
                if execution.source == "manual":
                    manual_log(f"{label}: inicio de ejecucion")

                manual_row = execution.manual_row
                if manual_row is not None:
                    running_note = compose_note(
                        execution.manual_notes,
                        f"Ejecutando {datetime.now().isoformat(timespec='seconds')}",
                    )
                    try:
                        update_manual_request_status(manual_row, "running", running_note)
                        execution.manual_notes = running_note
                        manual_log(
                            f"{label}: estado actualizado a running (fila {manual_row + 1})"
                        )
                    except Exception:  # pylint: disable=broad-except
                        logging.exception(
                            "No se pudo marcar la solicitud manual fila %s como en ejecución",
                            manual_row,
                        )

                status, detail = run_job_interruptible(
                    execution.job,
                    execution,
                    interrupt_event=interrupt_running_event,
                    on_process_start=_register_running,
                    on_process_end=_clear_running,
                )

                if status == "interrupted_preempt":
                    resume_note = compose_note(
                        execution.manual_notes,
                        "Interrumpido por prioridad de cotizacion_panama; reencolado automáticamente.",
                    )
                    execution.manual_notes = resume_note
                    if manual_row is not None:
                        try:
                            update_manual_request_status(manual_row, "enqueued", resume_note)
                        except Exception:  # pylint: disable=broad-except
                            logging.exception(
                                "No se pudo reencolar la solicitud manual fila %s tras interrupción",
                                manual_row,
                            )
                    resume_execution = ExecutionRequest(
                        job=execution.job,
                        source=execution.source,
                        manual_row=execution.manual_row,
                        manual_id=execution.manual_id,
                        manual_requested_by=execution.manual_requested_by,
                        manual_requested_at=execution.manual_requested_at,
                        manual_notes=execution.manual_notes,
                        manual_payload=execution.manual_payload,
                    )
                    enqueue_execution(
                        resume_execution.job,
                        "resumed",
                        existing_execution=resume_execution,
                    )
                    manual_log(
                        f"{label}: interrumpido y reencolado para continuar después de cotizacion_panama.",
                        level="warning",
                    )
                    continue

                if execution.source == "manual":
                    if status == "success":
                        manual_log(f"{label}: ejecucion exitosa")
                    else:
                        manual_log(
                            f"{label}: ejecucion con error: {detail or status}",
                            level="error",
                        )
                else:
                    logging.info("%s: ejecucion completada con estado %s", label, status)

                if manual_row is not None:
                    final_status = "done" if status == "success" else "error"
                    final_note = compose_note(
                        execution.manual_notes,
                        f"Finalizado {datetime.now().isoformat(timespec='seconds')}",
                    )
                    if final_status != "done" and detail:
                        shortened = detail if len(detail) <= 240 else f"{detail[:237]}..."
                        final_note = compose_note(final_note, shortened)
                    try:
                        update_manual_request_status(manual_row, final_status, final_note)
                        manual_log(
                            f"{label}: estado actualizado a {final_status} (fila {manual_row + 1})"
                        )
                    except Exception:  # pylint: disable=broad-except
                        logging.exception(
                            "No se pudo actualizar el resultado de la solicitud manual fila %s",
                            manual_row,
                        )
            finally:
                job_queue.task_done()

    def poll_manual_requests() -> None:
        try:
            pending_requests = fetch_manual_requests()
        except Exception:  # pylint: disable=broad-except
            logging.exception("No se pudo obtener solicitudes manuales desde Google Sheets")
            return

        if pending_requests:
            manual_log(f"Monitor manual: {len(pending_requests)} solicitudes detectadas")
        else:
            manual_log("Monitor manual: sin solicitudes pendientes")
        for manual_request in pending_requests:
            job_name = (manual_request.get("job") or "").strip()
            row_index = to_row_index(manual_request.get("row"))
            if not job_name:
                note = compose_note(manual_request.get("notes"), "Fila sin nombre de job")
                if row_index is not None:
                    try:
                        update_manual_request_status(row_index, "error", note)
                    except Exception:  # pylint: disable=broad-except
                        logging.exception(
                            "No se pudo marcar la fila manual %s sin job como error",
                            row_index,
                        )
                continue

            job = job_lookup.get(job_name)
            if job is None:
                refresh_config_from_sheet()
                job = job_lookup.get(job_name)
            if job is None:
                note = compose_note(
                    manual_request.get("notes"),
                    f"Job '{job_name}' no existe en la configuración",
                )
                if row_index is not None:
                    try:
                        update_manual_request_status(row_index, "error", note)
                    except Exception:  # pylint: disable=broad-except
                        logging.exception(
                            "No se pudo marcar la solicitud manual fila %s como error",
                            row_index,
                        )
                continue

            manual_tag, requester = describe_manual_request(manual_request)
            manual_log(
                f"Solicitud manual {manual_tag} ({requester}) detectada para {JOB_NAME_LABELS.get(job.name, job.name)} (estado pending)"
            )

            manual_log(
                f"Solicitud manual {manual_tag} ({requester}) en proceso de encolado"
            )

            enqueue_execution(job, "manual", manual_request=manual_request)

    schedule_jobs(scheduler, current_config, enqueue_execution)

    def refresh_config_from_sheet() -> None:
        nonlocal current_config, current_signature, job_lookup
        try:
            new_config = load_config()
        except Exception:  # pylint: disable=broad-except
            logging.exception("No se pudo refrescar la configuración desde Google Sheets")
            return

        new_signature = build_config_signature(new_config)
        if new_signature == current_signature:
            logging.debug("Configuracion en Google Sheets sin cambios; no se reprograman jobs")
            return

        logging.info("Se detectaron cambios en Google Sheets; reprogramare los jobs")
        current_config = new_config
        current_signature = new_signature
        job_lookup = {job.name: job for job in current_config.jobs}
        apply_config_to_scheduler(
            scheduler,
            current_config,
            protected_job_ids,
            enqueue_execution,
        )

    if CONFIG_REFRESH_INTERVAL_MINUTES > 0:
        scheduler.add_job(
            refresh_config_from_sheet,
            "interval",
            minutes=CONFIG_REFRESH_INTERVAL_MINUTES,
            id=CONFIG_WATCH_JOB_ID,
            replace_existing=True,
        )
        protected_job_ids.add(CONFIG_WATCH_JOB_ID)
        logging.info(
            "Monitor de configuracion agendado (cada %s minutos)",
            CONFIG_REFRESH_INTERVAL_MINUTES,
        )

    if MANUAL_POLL_INTERVAL_SECONDS > 0:
        scheduler.add_job(
            poll_manual_requests,
            "interval",
            seconds=MANUAL_POLL_INTERVAL_SECONDS,
            id=MANUAL_WATCH_JOB_ID,
            replace_existing=True,
        )
        protected_job_ids.add(MANUAL_WATCH_JOB_ID)
        logging.info(
            "Monitor de solicitudes manuales agendado (cada %s segundos)",
            MANUAL_POLL_INTERVAL_SECONDS,
        )

    worker_thread = threading.Thread(target=worker_loop, name="job-runner", daemon=True)
    worker_thread.start()

    scheduler.start()
    active_jobs = [job for job in scheduler.get_jobs() if job.id not in protected_job_ids]
    logging.info("Scheduler iniciado con %d jobs activos", len(active_jobs))

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Interrupcion solicitada; cerrando orquestador...")
    finally:
        scheduler.shutdown(wait=False)
        pending_in_queue = job_queue.qsize()
        if pending_in_queue > 0:
            logging.info(
                "Esperando a que finalicen %d ejecuciones pendientes",
                pending_in_queue,
            )
        job_queue.join()
        stop_event.set()
        worker_thread.join(timeout=10)
        logging.info("Scheduler detenido")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logging.exception("Orquestador detenido por excepcion no controlada: %s", exc)
        while True:
            time.sleep(60)
