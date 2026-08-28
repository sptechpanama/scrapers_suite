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


def test_otras_fuentes_summary_and_email_are_idempotent(tmp_path, monkeypatch):
    payload = {
        "events": [
            {
                "id": "evt-1", "source": "acp", "event_type": "new",
                "title": "Sistema fotovoltaico", "matched_company": "RS/SP",
                "priority": "Alta", "fit_score": 88, "deadline": "2026-09-01",
                "source_url": "https://example.test/1",
            }
        ]
    }
    stdout = "log\nOTRAS_FUENTES_SUMMARY_JSON=" + json.dumps(payload)
    assert orchestrator._extract_otras_fuentes_summary(stdout) == payload
    monkeypatch.setattr(orchestrator, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(orchestrator, "_ctni_email_config", lambda: ("a@b.test", "pw", ["c@d.test"]))
    FakeSmtp.sent_messages = []
    monkeypatch.setattr(orchestrator.smtplib, "SMTP_SSL", FakeSmtp)
    orchestrator._process_otras_fuentes_notifications("otras_fuentes", stdout, datetime(2026, 8, 28, 8, 0))
    orchestrator._process_otras_fuentes_notifications("otras_fuentes", stdout, datetime(2026, 8, 28, 9, 0))
    assert len(FakeSmtp.sent_messages) == 1


def test_otras_fuentes_is_a_required_fallback_job():
    assert "otras_fuentes" in orchestrator.REQUIRED_FALLBACK_JOB_NAMES
    config = orchestrator.load_config()
    job = next(item for item in config.jobs if item.name == "otras_fuentes")
    assert job.times == ["06:20", "12:20", "18:20"]

