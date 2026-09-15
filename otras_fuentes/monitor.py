from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Iterable, Type

from .adapters import (
    AcpAdapter,
    AcpSliAdapter,
    IfrcAdapter,
    CiudadSaberAdapter,
    CruzRojaAdapter,
    EnaAdapter,
    EnsaAdapter,
    IdaanAdapter,
    IdbAdapter,
    UngmAdapter,
    UngmInternationalAdapter,
    UnicefAdapter,
    WorldBankAdapter,
    NaturgyAdapter,
    AesAdapter,
)
from .adapters.base import SourceAdapter
from .classifier import classify_opportunity
from .enrichment import DetailEnricher
from .models import MonitorResult, SourceFetchResult, stable_hash, utc_now_iso
from .storage import OpportunityStore, default_sqlite_path, postgres_dsn


LOGGER = logging.getLogger("otras_fuentes")
DEFAULT_ADAPTERS: tuple[Type[SourceAdapter], ...] = (
    AcpSliAdapter,
    AcpAdapter,
    EnsaAdapter,
    IdaanAdapter,
    EnaAdapter,
    UngmAdapter,
    UngmInternationalAdapter,
    IdbAdapter,
    WorldBankAdapter,
    UnicefAdapter,
    CruzRojaAdapter,
    CiudadSaberAdapter,
    IfrcAdapter,
    NaturgyAdapter,
    AesAdapter,
)


def run_monitor(
    adapter_classes: Iterable[Type[SourceAdapter]] = DEFAULT_ADAPTERS,
    *,
    require_postgres: bool = False,
) -> MonitorResult:
    adapter_classes = tuple(adapter_classes)
    started_at = utc_now_iso()
    run_id = stable_hash(started_at, "otras_fuentes", length=24)
    local = OpportunityStore.sqlite(default_sqlite_path())
    remote: OpportunityStore | None = None
    errors: dict[str, str] = {}
    postgres_error = ""
    dsn = postgres_dsn()
    if dsn:
        try:
            remote = OpportunityStore.postgres(dsn)
        except Exception as exc:
            postgres_error = f"{type(exc).__name__}: {exc}"
            errors["postgres"] = postgres_error
            if require_postgres:
                local.close()
                raise RuntimeError(f"Supabase obligatorio no disponible: {postgres_error}") from exc
    elif require_postgres:
        local.close()
        raise RuntimeError("SUPABASE_DB_URL/DATABASE_URL no está configurado")

    stores = [local, *([remote] if remote is not None else [])]
    for store in stores:
        store.begin_run(run_id, started_at, len(adapter_classes))

    source_summaries: list[dict[str, object]] = []
    totals = {"records": 0, "new": 0, "changed": 0, "events": 0, "success": 0, "error": 0, "partial": 0, "access_required": 0}
    emit_events = os.environ.get("OTRAS_FUENTES_SILENT_RUN", "").strip().lower() not in {
        "1", "true", "yes", "si", "sí",
    }
    from .profiles import load_profiles
    profiles = load_profiles()
    enricher = DetailEnricher(budget=int(os.getenv('OTRAS_FUENTES_DETAIL_BUDGET', '180')))
    remaining_budget = enricher.budget
    for index, adapter_class in enumerate(adapter_classes):
        source_started = utc_now_iso()
        LOGGER.info("Fuente %s: inicio", adapter_class.source)
        result = adapter_class().fetch()
        for opportunity in result.opportunities:
            classify_opportunity(opportunity, profiles)
        # Reserve a share for later sources: ACP/ENA used to exhaust the entire
        # document budget before UNGM. Unused reads remain available downstream.
        allowance = remaining_budget // (len(adapter_classes) - index)
        enricher.budget = allowance
        try:
            enricher.enrich(result.opportunities)
        except Exception:
            LOGGER.exception("Detalle de %s incompleto; se conservan los avisos", result.source)
        remaining_budget -= allowance - enricher.budget
        analyzed = [o.raw_payload.get('document_analysis') or {} for o in result.opportunities]
        details_read = sum(a.get('status') == 'ok' and not a.get('using_previous') for a in analyzed)
        documents_total = sum(a.get('attachments_total', 0) for a in analyzed)
        documents_read = sum(a.get('attachments_read', 0) for a in analyzed)
        if result.opportunities:
            result.coverage += (f'. Detalles leídos: {details_read}/{len(result.opportunities)}; '
                f'adjuntos interpretados: {documents_read}/{documents_total} identificados. '
                'El detalle y los adjuntos pendientes se revisan en siguientes corridas.')
        for opportunity in result.opportunities:
            classify_opportunity(opportunity, profiles)
        source_finished = utc_now_iso()
        if result.status == "error":
            errors[result.source] = result.error
            totals["error"] += 1
        elif result.status == "partial":
            errors[result.source] = result.error
            totals["partial"] += 1
        elif result.status == 'access_required':
            totals['access_required'] += 1
        else:
            totals["success"] += 1

        local_stats = local.ingest_source(
            run_id, source_started, source_finished, result, emit_events=emit_events
        )
        if remote is not None:
            try:
                remote.ingest_source(
                    run_id, source_started, source_finished, result, emit_events=emit_events
                )
            except Exception as exc:
                remote.connection.rollback()
                postgres_error = f"{type(exc).__name__}: {exc}"
                errors["postgres"] = postgres_error
                LOGGER.exception("No se pudo sincronizar %s con Supabase", result.source)
        totals["records"] += local_stats.records
        totals["new"] += local_stats.new
        totals["changed"] += local_stats.changed
        totals["events"] += local_stats.events
        source_summaries.append(
            {
                "source": result.source,
                "status": result.status,
                "records": local_stats.records,
                "new": local_stats.new,
                "changed": local_stats.changed,
                "events": local_stats.events,
                "baseline_created": local_stats.baseline_created,
                "coverage": result.coverage,
                "response_ms": result.response_ms,
                "pages_fetched": result.pages_fetched,
                "error": result.error,
            }
        )
        LOGGER.info(
            "Fuente %s: %s | registros=%s nuevos=%s cambios=%s eventos=%s",
            result.source, result.status, local_stats.records, local_stats.new,
            local_stats.changed, local_stats.events,
        )

    finished_at = utc_now_iso()
    status = "success" if totals["error"] == 0 and totals["partial"] == 0 and totals['access_required'] == 0 and not postgres_error else "partial"
    if totals["success"] == 0 and totals["partial"] == 0 and totals['access_required'] == 0:
        status = "error"
    postgres_synced = remote is not None and not postgres_error
    events = local.events_for_run(run_id)
    finish_kwargs = dict(
        finished_at=finished_at,
        status=status,
        success_count=totals["success"],
        error_count=totals["error"],
        total_records=totals["records"],
        new_records=totals["new"],
        changed_records=totals["changed"],
        event_count=len(events),
        postgres_synced=postgres_synced,
        errors=errors,
    )
    local.finish_run(run_id, **finish_kwargs)
    if remote is not None:
        try:
            remote.finish_run(run_id, **finish_kwargs)
        except Exception:
            LOGGER.exception("No se pudo cerrar la corrida en Supabase")
            postgres_synced = False
            errors['postgres_finish'] = 'No se pudo confirmar el cierre de la corrida en Supabase'
            status = 'partial' if totals['success'] else 'error'
            local.finish_run(run_id, **{**finish_kwargs, 'status': status, 'postgres_synced': False, 'errors': errors})
    local.close()
    enricher.close()
    if remote is not None:
        remote.close()

    return MonitorResult(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        status=status,
        source_results=source_summaries,
        counts=totals,
        events=events,
        postgres_synced=postgres_synced,
        errors=errors,
    )
