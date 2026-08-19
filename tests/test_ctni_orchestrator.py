from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path


ORCHESTRATOR_DIR = Path(__file__).resolve().parents[1] / "orquestador"
if str(ORCHESTRATOR_DIR) not in sys.path:
    sys.path.insert(0, str(ORCHESTRATOR_DIR))

import main as orchestrator  # noqa: E402


class FakeSmtp:
    sent_messages = []

    def __init__(self, *_args, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def login(self, *_args):
        return None

    def send_message(self, message):
        self.sent_messages.append(message)


def _stdout_with_one_event() -> str:
    payload = {
        "events": [
            {
                "event_id": "evt-1",
                "categoria": "solicitudes",
                "tipo_evento": "Solicitud nueva",
                "producto": "Reactivo de prueba",
                "numero_formulario": "100",
                "subcomite": "Laboratorio",
                "fecha": "2026-08-18",
                "estado": "Recibido",
                "enlace": "https://ctni.minsa.gob.pa/Formularios/FormularioInfo?Id=1",
            }
        ]
    }
    return "log previo\nCTNI_SUMMARY_JSON=" + json.dumps(payload)


def test_ctni_notification_queue_and_email_are_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(orchestrator, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(
        orchestrator,
        "_ctni_email_config",
        lambda: ("sender@example.com", "app-password", ["recipient@example.com"]),
    )
    FakeSmtp.sent_messages = []
    monkeypatch.setattr(orchestrator.smtplib, "SMTP_SSL", FakeSmtp)

    stdout = _stdout_with_one_event()
    assert orchestrator._queue_ctni_notifications("ctni", stdout, datetime(2026, 8, 18, 8, 0)) == 1
    ok, detail, sent = orchestrator._send_pending_ctni_email()
    assert (ok, detail, sent) == (True, "", 1)
    assert len(FakeSmtp.sent_messages) == 1

    assert orchestrator._queue_ctni_notifications("ctni", stdout, datetime(2026, 8, 18, 9, 0)) == 0
    ok, detail, sent = orchestrator._send_pending_ctni_email()
    assert (ok, detail, sent) == (True, "", 0)
    assert len(FakeSmtp.sent_messages) == 1


def test_ctni_is_injected_as_required_daily_fallback():
    primary = [
        orchestrator.JobConfig(
            name="clv",
            python="python.exe",
            script="clv.py",
            days_of_week=["mon"],
            times=["08:00"],
        )
    ]
    fallback = [
        orchestrator.JobConfig(
            name="ctni",
            python="python.exe",
            script="scrape_ctni.py",
            days_of_week=["mon", "tue", "wed", "thu", "fri", "sat", "sun"],
            times=["05:15"],
        )
    ]
    merged, injected = orchestrator._merge_required_jobs(primary, fallback)
    assert [job.name for job in merged] == ["clv", "ctni"]
    assert injected == ["ctni"]
