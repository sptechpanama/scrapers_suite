from __future__ import annotations

import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
ORCHESTRATOR_DIR = REPO_ROOT / "orquestador"
sys.path.insert(0, str(ORCHESTRATOR_DIR))
SPEC = importlib.util.spec_from_file_location(
    "orchestrator_ct_rir_under_test",
    ORCHESTRATOR_DIR / "main.py",
)
assert SPEC and SPEC.loader
orchestrator = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = orchestrator
SPEC.loader.exec_module(orchestrator)


class _Request:
    def __init__(self, outcomes):
        self.outcomes = outcomes
        self.calls = 0

    def execute(self):
        outcome = self.outcomes[self.calls]
        self.calls += 1
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _Values:
    def __init__(self, request):
        self.request = request

    def get(self, **_kwargs):
        return self.request


class _Spreadsheets:
    def __init__(self, request):
        self.request = request

    def values(self):
        return _Values(self.request)


class _Service:
    def __init__(self, request):
        self.request = request

    def spreadsheets(self):
        return _Spreadsheets(self.request)


def _configure_isolated_state(monkeypatch, tmp_path, state):
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    monkeypatch.setattr(orchestrator, "STATE_PATH", state_path)
    return state_path


def test_sheet_reader_retries_transient_failures(monkeypatch):
    request = _Request(
        [
            TimeoutError("temporal 1"),
            ConnectionError("temporal 2"),
            {"values": [["ok"]]},
        ]
    )
    monkeypatch.setattr(orchestrator, "_get_panamacompra_service", lambda: _Service(request))
    monkeypatch.setattr(orchestrator, "PANAMACOMPRA_READ_RETRY_DELAYS_SECONDS", (0, 0, 0))

    assert orchestrator._read_panamacompra_sheet("prueba") == [["ok"]]
    assert request.calls == 3


def test_semantic_prefilter_accepts_plural_variant_for_43358():
    anchors = orchestrator._ct_rir_semantic_anchor_tokens({"43358"})
    fields = {
        "titulo": "KIT DE CIRCUITO DE PACIENTES PEDIATRICOS PARA MAQUINA DE ANESTESIA",
    }

    assert orchestrator._row_may_contain_tracked_ficha(fields, {"43358"}, anchors)


def test_direct_scan_reads_only_dedicated_ct_rir_sheets(monkeypatch):
    requested = []
    monkeypatch.setattr(orchestrator, "PANAMACOMPRA_CT_RIR_DIRECT_SHEETS", {"directa"})
    monkeypatch.setattr(orchestrator, "PANAMACOMPRA_CT_RIR_SCAN_SHEETS", ["general", "directa"])
    monkeypatch.setattr(orchestrator, "_load_ct_rir_fichas_for_notifications", lambda: {"43358"})

    def fake_read(sheet):
        requested.append(sheet)
        return [
            ["titulo", "ficha_detectada", "enlace"],
            ["Circuito", "* 43358", "https://example.test/acto"],
        ]

    monkeypatch.setattr(orchestrator, "_read_panamacompra_sheet", fake_read)

    rows = orchestrator._scan_ct_rir_candidates(direct_only=True)

    assert requested == ["directa"]
    assert len(rows) == 1
    assert rows[0]["ficha_detectada"] == "* 43358"


def test_direct_sheet_does_not_alert_removed_ficha(monkeypatch):
    monkeypatch.setattr(orchestrator, "PANAMACOMPRA_CT_RIR_DIRECT_SHEETS", {"directa"})
    monkeypatch.setattr(orchestrator, "_load_ct_rir_fichas_for_notifications", lambda: {"43358"})
    monkeypatch.setattr(
        orchestrator,
        "_read_panamacompra_sheet",
        lambda _sheet: [
            ["titulo", "ficha_detectada", "enlace"],
            ["Otra ficha", "* 99999", "https://example.test/otra"],
        ],
    )

    assert orchestrator._scan_ct_rir_candidates(direct_only=True) == []


def test_summary_does_not_duplicate_act_already_seen_by_semantic_scan(monkeypatch, tmp_path):
    act_url = "https://example.test/acto-semantico"
    act_key = orchestrator._build_module_act_key({"enlace": act_url})
    state_path = _configure_isolated_state(
        monkeypatch,
        tmp_path,
        {
            "last_run": {},
            "ct_rir_module_seen_keys": {act_key: "2026-09-08T08:00:00"},
            "ct_rir_email_pending": [],
            "ct_rir_email_sent_keys": {},
        },
    )
    payload = {
        "sheet": "cl_abiertas_ct_rir",
        "rows": [
            {
                "ficha_detectada": "43358",
                "titulo": "Circuito",
                "enlace": act_url,
            }
        ],
    }

    queued = orchestrator._queue_ct_rir_notifications(
        "clv",
        "CT_RIR_SUMMARY_JSON=" + json.dumps(payload),
        datetime(2026, 9, 8, 9, 0, 0),
    )
    saved = json.loads(state_path.read_text(encoding="utf-8"))

    assert queued == 0
    assert saved["ct_rir_email_pending"] == []


def test_failed_email_keeps_pending_and_records_error(monkeypatch, tmp_path):
    pending = {
        "unique_key": "hoja|acto",
        "hoja_origen": "cl_abiertas_ct_rir",
        "ficha_detectada": "43358",
        "titulo": "Prueba",
        "enlace": "https://example.test/acto",
    }
    state_path = _configure_isolated_state(
        monkeypatch,
        tmp_path,
        {"last_run": {}, "ct_rir_email_pending": [pending]},
    )
    monkeypatch.setattr(
        orchestrator,
        "_ct_rir_email_config",
        lambda: ("sender@example.test", "secret", ["to@example.test"]),
    )
    monkeypatch.setattr(orchestrator, "CT_RIR_EMAIL_RETRY_DELAYS_SECONDS", (0, 0, 0))
    attempts = {"count": 0}

    class FailingSMTP:
        def __init__(self, *_args, **_kwargs):
            attempts["count"] += 1
            raise TimeoutError("smtp temporal")

    monkeypatch.setattr(orchestrator.smtplib, "SMTP_SSL", FailingSMTP)

    ok, detail, sent = orchestrator._send_pending_ct_rir_email()
    saved = json.loads(state_path.read_text(encoding="utf-8"))

    assert not ok
    assert sent == 0
    assert "3 intentos" in detail
    assert attempts["count"] == 3
    assert saved["ct_rir_email_pending"] == [pending]
    assert saved["ct_rir_email_last_error"]["pending_count"] == 1


def test_successful_email_marks_only_delivered_entries(monkeypatch, tmp_path):
    pending = {
        "unique_key": "hoja|acto",
        "hoja_origen": "cl_abiertas_ct_rir",
        "ficha_detectada": "43358",
        "titulo": "Prueba",
        "enlace": "https://example.test/acto",
    }
    state_path = _configure_isolated_state(
        monkeypatch,
        tmp_path,
        {"last_run": {}, "ct_rir_email_pending": [pending]},
    )
    monkeypatch.setattr(
        orchestrator,
        "_ct_rir_email_config",
        lambda: ("sender@example.test", "secret", ["to@example.test"]),
    )
    monkeypatch.setattr(orchestrator, "CT_RIR_EMAIL_RETRY_DELAYS_SECONDS", (0,))

    class SuccessfulSMTP:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def login(self, *_args):
            return None

        def send_message(self, _message):
            return None

    monkeypatch.setattr(orchestrator.smtplib, "SMTP_SSL", SuccessfulSMTP)

    ok, detail, sent = orchestrator._send_pending_ct_rir_email()
    saved = json.loads(state_path.read_text(encoding="utf-8"))

    assert ok
    assert detail == ""
    assert sent == 1
    assert saved["ct_rir_email_pending"] == []
    assert "hoja|acto" in saved["ct_rir_email_sent_keys"]


def test_watchdog_retries_pending_even_when_scan_queues_nothing(monkeypatch, tmp_path):
    _configure_isolated_state(
        monkeypatch,
        tmp_path,
        {
            "last_run": {},
            "ct_rir_watchdog_last_full_scan_at": datetime.now().isoformat(timespec="seconds"),
        },
    )
    calls = {"scan": 0, "send": 0}

    def fake_scan(*_args, **kwargs):
        calls["scan"] += 1
        assert kwargs["direct_only"] is True
        return 0

    def fake_send():
        calls["send"] += 1
        return True, "", 1

    monkeypatch.setattr(orchestrator, "_queue_scan_based_notifications", fake_scan)
    monkeypatch.setattr(orchestrator, "_send_pending_ct_rir_email", fake_send)

    result = orchestrator._run_ct_rir_notification_watchdog()

    assert calls == {"scan": 1, "send": 1}
    assert result["status"] == "success"
    assert result["sent"] == 1


@pytest.mark.parametrize(
    "relative_path",
    ("clv/clv.py", "clrir/clrir.py", "rir1/rir1.py"),
)
def test_ct_rir_watchlist_is_evaluated_before_general_discard(relative_path):
    """Una ficha vigilada no puede perderse por precio, RS o medicamento."""
    source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
    marker = 'FICHAS_CT_RIR_DYNAMIC.intersection(fichas_base)'
    marker_index = source.index(marker, source.index("def main():"))
    discard_index = source.index("desc, mot = want_descartar(info)", marker_index)

    assert marker_index < discard_index
