from __future__ import annotations

from pathlib import Path

import requests

from ctni_monitor.monitor import (
    CtniRepository,
    CtniHttpClient,
    _ficha_event_decider,
    deduplicate_payloads,
    ficha_record_key,
    homologation_record_key,
    parse_homepage_homologations,
    request_record_key,
    run_monitor,
)


HOME_ONE = """
<h5>ALERTA HOMOLOGACIONES VIRTUALES</h5>
<table><tbody><tr><td>
Fecha: 20 agosto 2026 Hora: 9:00 AM Área: Subcomité Médico Quirúrgico
Titulo: FICHA TÉCNICA 100724 MONITOR MATERNO FETAL (FORMULARIO 10764).
<a href="/Documentos/homvirt/primera.docx">Documento</a>
</td></tr></tbody></table>
<table id="avisos"><tr><th>Nombre</th><th>Subcomité</th><th>Observación</th><th>Adjunto</th></tr></table>
"""

HOME_TWO = HOME_ONE.replace(
    "</table>\n<table id=\"avisos\">",
    """</table>
<h5>AVISOS</h5><table id="avisos">""",
).replace(
    "<tr><th>Nombre</th><th>Subcomité</th><th>Observación</th><th>Adjunto</th></tr></table>",
    """
<tr><th>Nombre</th><th>Subcomité</th><th>Observación</th><th>Adjunto</th></tr>
<tr><td>AVISO IMPORTANTE</td><td>Laboratorio</td>
<td>Se suspende la homologación del formulario 24501 el 21 de agosto de 2026 a las 11:00 AM.</td>
<td><a href="/Documentos/avisos/suspension.docx">Documento</a></td></tr></table>
""",
)


class FakeCtniClient:
    def __init__(self) -> None:
        self.version = 1

    def fetch_requests(self):
        rows = [
            {
                "id": 1,
                "numFormulario": "100",
                "tipoFormulario": "Elaboración",
                "numFicha": "",
                "subComite": "Laboratorio",
                "institucion": "CSS",
                "unidadEjecutora": "Hospital",
                "nombreGenerico": "Reactivo de prueba",
                "fecha": "18-08-2026",
            }
        ]
        if self.version >= 2:
            rows.append(
                {
                    "id": 2,
                    "numFormulario": "101",
                    "tipoFormulario": "Actualización",
                    "numFicha": "43358",
                    "subComite": "Médico Quirúrgico",
                    "institucion": "MINSA",
                    "unidadEjecutora": "Hospital 2",
                    "nombreGenerico": "Circuito de paciente",
                    "fecha": "18-08-2026",
                }
            )
        return rows

    def fetch_request_detail(self, official_id):
        if str(official_id) == "1" and self.version >= 2:
            return {"fechaRecibida": "18-08-2026", "observacionRecibida": "Recibido"}
        return {}

    def fetch_worked_fichas(self):
        rows = [
            {
                "id": 10,
                "numFicha": "100724",
                "accion": "Actualización",
                "numacta": "1",
                "titulo": "Monitor materno fetal",
                "subcomite": "Médico Quirúrgico",
                "fecha": "18-Agosto-2026",
            }
        ]
        if self.version >= 2:
            rows.append(
                {
                    "id": 11,
                    "numFicha": "110999",
                    "accion": "Nueva Ficha",
                    "numacta": "2",
                    "titulo": "Producto nuevo",
                    "subcomite": "Laboratorio",
                    "fecha": "18-Agosto-2026",
                }
            )
        return rows

    def confirm_published_ficha(self, ficha_number):
        return str(ficha_number) == "110999"

    def fetch_homepage(self):
        return HOME_TWO if self.version >= 2 else HOME_ONE


class FakeSheetStore:
    def __init__(self) -> None:
        self.rows = {}

    def upsert_rows(self, title, headers, rows, *, key_field):
        bucket = self.rows.setdefault(title, {})
        for row in rows:
            bucket[str(row[key_field])] = dict(row)
        return len(rows)


class TerminalRequestClient:
    def __init__(self) -> None:
        self.detail_calls = 0

    def fetch_requests(self):
        return [
            {
                "id": 99,
                "numFormulario": "500",
                "tipoFormulario": "Elaboración",
                "numFicha": "43358",
                "subComite": "Médico Quirúrgico",
                "institucion": "CSS",
                "unidadEjecutora": "Hospital",
                "nombreGenerico": "Circuito de paciente",
                "fecha": "01-01-2020",
            }
        ]

    def fetch_request_detail(self, _official_id):
        self.detail_calls += 1
        return {
            "fechaFinal": "02-01-2020",
            "observacionFinal": "Trámite finalizado con resolución oficial",
        }

    def fetch_worked_fichas(self):
        return []

    def confirm_published_ficha(self, _ficha_number):
        return False

    def fetch_homepage(self):
        return "<html><body>Sin avisos</body></html>"


class AlternatingFichaPagesClient(TerminalRequestClient):
    def __init__(self) -> None:
        super().__init__()
        self.ficha_calls = 0

    def fetch_requests(self):
        return []

    def fetch_worked_fichas(self):
        self.ficha_calls += 1
        number = "100" if self.ficha_calls % 2 else "200"
        return [
            {
                "id": number,
                "numFicha": number,
                "accion": "Actualizacion",
                "numacta": "1",
                "titulo": f"Ficha {number}",
                "subcomite": "Laboratorio",
                "fecha": "18-Agosto-2026",
            }
        ]


class FailingSheetStore:
    def upsert_rows(self, *_args, **_kwargs):
        raise RuntimeError("Google temporalmente no disponible")


def test_official_keys_follow_requested_deduplication_rules():
    assert request_record_key({"id": 43422, "numFormulario": "24489"}) == "solicitud:id:43422"
    assert ficha_record_key(
        {"numFicha": "110827", "accion": "Corrección", "numacta": "77", "fecha": "18-Agosto-2026"}
    ).startswith("ficha:110827|corregida|77|2026-08-18")
    assert homologation_record_key({"enlace_adjunto": "https://ctni.minsa.gob.pa/a.docx"}).startswith(
        "homologacion:url:"
    )


def test_duplicate_official_keys_are_collapsed_deterministically():
    older = {
        "record_key": "ficha:43358|actualizada|10|2026-08-18",
        "id_oficial": "100",
        "producto": "Circuito incompleto",
    }
    newer = {
        "record_key": "ficha:43358|actualizada|10|2026-08-18",
        "id_oficial": "200",
        "producto": "Circuito de paciente completo",
        "subcomite": "Médico Quirúrgico",
    }

    forward = deduplicate_payloads([older, newer])
    reverse = deduplicate_payloads([newer, older])

    assert forward == reverse == [newer]


def test_ficha_watermark_suppresses_late_historical_discoveries():
    historical = {"accion": "Elaborada", "id_oficial": "99"}
    genuinely_new = {"accion": "Elaborada", "id_oficial": "101"}

    assert _ficha_event_decider(None, historical, True, minimum_new_id=100) is None
    assert _ficha_event_decider(None, genuinely_new, True, minimum_new_id=100) == (
        "Ficha nueva publicada",
        True,
    )


def test_non_notifiable_change_does_not_create_generic_event(tmp_path: Path):
    repository = CtniRepository(tmp_path / "events.db")
    try:
        repository.apply_records(
            source="fichas",
            category="fichas",
            records=[{"record_key": "ficha:1", "producto": "Nombre A"}],
            event_decider=lambda *_args: None,
        )
        changed = repository.apply_records(
            source="fichas",
            category="fichas",
            records=[{"record_key": "ficha:1", "producto": "Nombre B"}],
            event_decider=lambda *_args: None,
        )
        event_count = repository.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]

        assert len(changed.changed_records) == 1
        assert changed.events == []
        assert event_count == 0
    finally:
        repository.close()


def test_homepage_parser_extracts_schedule_and_status_changes():
    rows = parse_homepage_homologations(HOME_TWO)
    assert len(rows) == 2
    scheduled = next(row for row in rows if row["tipo_evento"] == "Programada")
    suspended = next(row for row in rows if row["tipo_evento"] == "Suspendida")
    assert scheduled["numero_formulario"] == "10764"
    assert scheduled["numero_ficha"] == "100724"
    assert suspended["numero_formulario"] == "24501"
    assert suspended["enlace_adjunto"].endswith("suspension.docx")


def test_first_run_is_baseline_and_repeated_runs_are_idempotent(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("CTNI_REQUEST_DETAIL_LIMIT", "20")
    client = FakeCtniClient()
    sheets = FakeSheetStore()
    db_path = tmp_path / "ctni.db"

    first = run_monitor(db_path=db_path, client=client, sheet_store=sheets)
    assert set(first.baseline_sources) == {"solicitudes", "fichas", "homologaciones"}
    assert first.summary()["events"] == []

    second = run_monitor(db_path=db_path, client=client, sheet_store=sheets)
    assert second.events == []
    assert second.summary()["events"] == []

    client.version = 2
    changed = run_monitor(db_path=db_path, client=client, sheet_store=sheets)
    notifications = changed.summary()["events"]
    event_types = {event["tipo_evento"] for event in notifications}
    assert "Solicitud nueva" in event_types
    assert any(event_type.startswith("Estado:") for event_type in event_types)
    assert "Ficha nueva publicada" in event_types
    assert "Homologación suspendida" in event_types
    assert len(notifications) == 4

    repeated = run_monitor(db_path=db_path, client=client, sheet_store=sheets)
    assert repeated.events == []
    assert repeated.summary()["events"] == []


def test_terminal_request_keeps_its_detail_when_refresh_is_skipped(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("CTNI_REQUEST_DETAIL_LOOKBACK_DAYS", "4000")
    monkeypatch.setenv("CTNI_TERMINAL_REFRESH_DAYS", "1")
    client = TerminalRequestClient()
    sheets = FakeSheetStore()
    db_path = tmp_path / "terminal.db"

    first = run_monitor(db_path=db_path, client=client, sheet_store=sheets)
    assert first.summary()["events"] == []
    assert client.detail_calls == 1

    second = run_monitor(db_path=db_path, client=client, sheet_store=sheets)
    repository = CtniRepository(db_path)
    try:
        saved = repository.get_records("solicitudes")["solicitud:id:99"]
        event_count = repository.connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    finally:
        repository.close()

    assert second.events == []
    assert client.detail_calls == 1
    assert saved["estado"] == "Finalizado"
    assert saved["observacion_estado"] == "Trámite finalizado con resolución oficial"
    assert saved["detalle_disponible"] is True
    assert event_count == 0


def test_monitor_unions_two_unstable_ficha_passes(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("CTNI_FICHA_PASSES", "2")
    client = AlternatingFichaPagesClient()
    db_path = tmp_path / "two-passes.db"

    result = run_monitor(db_path=db_path, client=client, sync_sheets=False)
    repository = CtniRepository(db_path)
    try:
        fichas = repository.get_records("fichas")
    finally:
        repository.close()

    assert client.ficha_calls == 2
    assert result.counts["fichas"] == 2
    assert result.counts["fichas_filas_fuente"] == 2
    assert len(fichas) == 2


def test_failed_sheet_sync_is_rebuilt_from_sqlite_on_next_identical_run(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("CTNI_REQUEST_DETAIL_LIMIT", "20")
    client = FakeCtniClient()
    db_path = tmp_path / "ctni.db"

    failed = run_monitor(db_path=db_path, client=client, sheet_store=FailingSheetStore())
    assert failed.sheets_synced is False
    assert "google_sheets" in failed.errors

    recovered_sheets = FakeSheetStore()
    recovered = run_monitor(db_path=db_path, client=client, sheet_store=recovered_sheets)
    assert recovered.sheets_synced is True
    assert len(recovered_sheets.rows["ctni_solicitudes"]) == 1
    assert len(recovered_sheets.rows["ctni_fichas"]) == 1
    assert len(recovered_sheets.rows["ctni_homologaciones"]) == 1


def test_source_error_never_deletes_historical_records(tmp_path: Path):
    repository = CtniRepository(tmp_path / "history.db")
    try:
        repository.apply_records(
            source="solicitudes",
            category="solicitudes",
            records=[{"record_key": "solicitud:id:1", "producto": "Histórico"}],
            event_decider=lambda *_args: None,
        )
        repository.record_health(
            "solicitudes", status="error", count=0, duration_seconds=1, error="HTTP 502"
        )
        assert repository.get_records("solicitudes")["solicitud:id:1"]["producto"] == "Histórico"
    finally:
        repository.close()


class FakeResponse:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def json(self):
        return self._body


class RetrySession:
    def __init__(self):
        self.headers = {}
        self.calls = 0

    def request(self, *_args, **_kwargs):
        self.calls += 1
        return FakeResponse(502 if self.calls <= 3 else 200)


def test_http_client_performs_initial_attempt_plus_three_retries():
    session = RetrySession()
    waits = []
    client = CtniHttpClient(session_factory=lambda: session, sleeper=waits.append)
    response = client.request("GET", "/health")
    assert response.status_code == 200
    assert session.calls == 4
    assert waits == [1.0, 2.0, 4.0]


class PublishedFichaSession:
    def __init__(self):
        self.headers = {}
        self.last_kwargs = {}

    def request(self, *_args, **kwargs):
        self.last_kwargs = kwargs
        return FakeResponse(200, {"data": [{"numFicha": "110827"}]})


def test_published_ficha_confirmation_uses_official_number_filter():
    session = PublishedFichaSession()
    client = CtniHttpClient(session_factory=lambda: session)
    assert client.confirm_published_ficha("110827") is True
    assert session.last_kwargs["data"]["IdCriterio"] == "1"
    assert session.last_kwargs["data"]["Filtro"] == "110827"
    assert session.last_kwargs["data"]["All"] == "0"
