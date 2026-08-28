from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from otras_fuentes.adapters import (
    AcpAdapter,
    CiudadSaberAdapter,
    CruzRojaAdapter,
    EnaAdapter,
    EnsaAdapter,
    IdaanAdapter,
    UngmAdapter,
)
from otras_fuentes.classifier import classify_opportunity
from otras_fuentes.models import Opportunity, SourceDocument, SourceFetchResult
from otras_fuentes.storage import OpportunityStore


class FakeClient:
    def __init__(self, *, text: str = "", payload=None):
        self.response = SimpleNamespace(text=text, json=lambda: payload or {})

    def get(self, *_args, **_kwargs):
        return SimpleNamespace(response=self.response)

    def post(self, *_args, **_kwargs):
        return SimpleNamespace(response=self.response)


@pytest.mark.parametrize(
    ("adapter", "html", "expected_id"),
    [
        (
            AcpAdapter,
            '<a href="https://pancanal.com/wp-content/uploads/2026/08/rfi-solar.pdf">Sistema fotovoltaico</a>',
            "rfi-solar.pdf",
        ),
        (
            EnsaAdapter,
            "<rss><channel><item><title>Chiller</title><link>https://ensa.com.pa/licitaciones/l-1</link>"
            "<pubDate>Mon, 24 Aug 2026 10:00:00 -0500</pubDate><description>Agua helada</description></item></channel></rss>",
            "l-1",
        ),
        (
            IdaanAdapter,
            "<table><tr><th>Número</th><th>Descripción</th><th>Estado</th></tr>"
            "<tr><td>2026-2-ABC-10</td><td>Suministro de bombas Monto: B/. 20,000.00</td><td>Activa</td></tr></table>",
            "2026-2-ABC-10",
        ),
        (
            EnaAdapter,
            '<div>Solicitud de cotización sistema eléctrico <a href="https://ena.com.pa/wp-content/uploads/2026/08/SC-12_26.pdf">Solicitud de cotización</a></div>',
            "12_26",
        ),
        (
            CruzRojaAdapter,
            "<table><tr><th>Código</th><th>Título</th><th>Fecha de cierre</th><th>Estado</th></tr>"
            '<tr><td>CR-1</td><td>Equipo médico</td><td>28/08/2026</td><td>Abierta</td><td><a href="/docs/cr1.pdf">Aplicar</a></td></tr></table>',
            "CR-1",
        ),
    ],
)
def test_html_adapters_extract_one_record(adapter, html, expected_id):
    result = adapter(client=FakeClient(text=html)).fetch()
    assert result.status == "success"
    assert len(result.opportunities) == 1
    assert result.opportunities[0].external_id == expected_id


def test_ungm_and_ciudad_saber_public_payloads_are_parsed():
    ungm_html = """
    <div class="dataRow" data-noticeid="987">
      <div class="resultTitle"><a href="/Public/Notice/987"><span class="ungm-title">Equipo hospitalario</span></a></div>
      <div class="tableCell">a</div><div class="tableCell">b</div><div class="tableCell">c</div>
      <div class="tableCell">24-Aug-2026</div><div class="tableCell">OPS</div>
      <div class="tableCell">RFQ</div><div class="tableCell">PA-987</div><div class="tableCell">Panama</div>
      <div data-description="Deadline"><span>30-Aug-2026</span></div>
    </div>"""
    ungm = UngmAdapter(client=FakeClient(text=ungm_html))
    assert ungm.fetch().opportunities[0].external_id == "987"

    payload = {
        "data": [
            {
                "id": 7,
                "attributes": {
                    "title": "Convocatoria de energía solar",
                    "slug": "energia-solar",
                    "publishedAt": "2026-08-20",
                    "generalInfo": {"deadline": "2026-09-01"},
                },
            }
        ]
    }
    cds = CiudadSaberAdapter(client=FakeClient(payload=payload))
    assert cds.fetch().opportunities[0].external_id == "7"


def _relevant(title: str, *, external_id: str = "A-1") -> Opportunity:
    return classify_opportunity(
        Opportunity(
            source="acp",
            external_id=external_id,
            title=title,
            source_url=f"https://example.test/{external_id}",
        ).normalize()
    )


def test_baseline_is_silent_and_second_run_is_idempotent(tmp_path):
    store = OpportunityStore.sqlite(tmp_path / "monitor.db")
    first = SourceFetchResult(source="acp", opportunities=[_relevant("Chiller agua helada")])
    stats = store.ingest_source("run-1", "2026-08-28T08:00:00+00:00", "2026-08-28T08:01:00+00:00", first)
    assert (stats.new, stats.events, stats.baseline_created) == (1, 0, True)

    stats = store.ingest_source("run-2", "2026-08-28T09:00:00+00:00", "2026-08-28T09:01:00+00:00", first)
    assert (stats.new, stats.changed, stats.events) == (0, 0, 0)
    assert store.events_for_run("run-2") == []

    changed = SourceFetchResult(source="acp", opportunities=[_relevant("Chiller agua helada fotovoltaico")])
    stats = store.ingest_source("run-3", "2026-08-28T10:00:00+00:00", "2026-08-28T10:01:00+00:00", changed)
    assert (stats.changed, stats.events) == (1, 1)
    assert len(store.events_for_run("run-3")) == 1

    repeated = store.ingest_source("run-4", "2026-08-28T11:00:00+00:00", "2026-08-28T11:01:00+00:00", changed)
    assert (repeated.changed, repeated.events) == (0, 0)
    store.close()


def test_source_failure_never_deletes_history(tmp_path):
    store = OpportunityStore.sqlite(tmp_path / "monitor.db")
    success = SourceFetchResult(source="acp", opportunities=[_relevant("Chiller")])
    store.ingest_source("ok", "2026-08-28T08:00:00+00:00", "2026-08-28T08:01:00+00:00", success)
    failure = SourceFetchResult(source="acp", opportunities=[], status="error", error="502")
    store.ingest_source("fail", "2026-08-28T09:00:00+00:00", "2026-08-28T09:01:00+00:00", failure)
    count = store.connection.execute("SELECT COUNT(*) FROM external_opportunities").fetchone()[0]
    assert count == 1
    store.close()


def test_cross_source_key_groups_same_business_notice():
    left = Opportunity(source="acp", external_id="1", title="Sistema solar", buyer="Entidad", deadline="2026-09-01", source_url="https://a.test/1")
    right = Opportunity(source="ensa", external_id="2", title="Sistema solar", buyer="Entidad", deadline="2026-09-01", source_url="https://b.test/2")
    assert left.cross_source_key == right.cross_source_key


def test_content_hash_ignores_dynamic_payload_and_document_order():
    first = Opportunity(
        source="ungm",
        external_id="987",
        title="Sistema solar",
        source_url="https://example.test/987",
        raw_payload={"countdown": "17 minutes"},
        documents=[
            SourceDocument(title="Anexo B", url="https://example.test/b.pdf"),
            SourceDocument(title="Anexo A", url="https://example.test/a.pdf"),
        ],
    )
    second = Opportunity(
        source="ungm",
        external_id="987",
        title="Sistema solar",
        source_url="https://example.test/987",
        raw_payload={"countdown": "16 minutes", "render_id": "dynamic"},
        documents=[
            SourceDocument(title="Anexo A", url="https://example.test/a.pdf"),
            SourceDocument(title="Anexo B", url="https://example.test/b.pdf"),
        ],
    )
    assert first.content_hash == second.content_hash


def test_silent_reclassification_updates_without_creating_alert(tmp_path):
    store = OpportunityStore.sqlite(tmp_path / "monitor.db")
    baseline = SourceFetchResult(source="acp", opportunities=[_relevant("Chiller agua helada")])
    store.ingest_source(
        "run-1", "2026-08-28T08:00:00+00:00", "2026-08-28T08:01:00+00:00", baseline
    )
    changed = SourceFetchResult(
        source="acp", opportunities=[_relevant("Chiller agua helada fotovoltaico")]
    )
    stats = store.ingest_source(
        "run-2",
        "2026-08-28T09:00:00+00:00",
        "2026-08-28T09:01:00+00:00",
        changed,
        emit_events=False,
    )
    assert (stats.changed, stats.events) == (1, 0)
    assert store.events_for_run("run-2") == []
    store.close()
