"""CL alert state: scheduled and open are separate, idempotent events.

No network or file access. Legacy observations become a silent baseline;
they are never represented as delivered mail.
"""
from __future__ import annotations

import re


def cl_stage(entry: dict) -> str:
    sheet = str(entry.get("hoja_origen") or entry.get("sheet") or "").lower()
    if sheet.startswith("cl_abiertas"):
        return "abierta"
    if sheet.startswith("cl_prog"):
        return "programada"
    return ""


def process_number(value: object) -> str:
    match = re.search(r"\b\d{4}-(?:\d+-){4}CL-\d+\b", str(value or ""), re.I)
    return match.group(0).upper() if match else ""


def queue_cl_events(state: dict, module: str, entries: list[dict], stamp: str,
                    *, scan: bool = False) -> int:
    """Entries carry canonical identity, stage-specific key and revision.

    One baseline per legacy act prevents a deployment from replaying old mail.
    Opening after an observed scheduled stage is always a new event, even when
    URL, dates and amount do not change. A stale scheduled row cannot reverse it.
    """
    records = state.setdefault(f"{module}_cl_notification_stages", {})
    pending = state.setdefault(f"{module}_email_pending", [])
    sent = state.setdefault(f"{module}_email_sent_keys", {})
    seen = state.get(f"{module}_module_seen_keys", {})
    legacy_keys = [*seen, *sent, *(str(row.get("unique_key", "")) for row in pending)]
    legacy_codes = {process_number(key) for key in legacy_keys} - {""}
    first_scan = scan and not records and not legacy_keys
    pending_keys = {row.get("unique_key") for row in pending}
    queued = 0
    # If both snapshots exist on first discovery, announce the actionable stage.
    for raw in sorted(entries, key=lambda row: cl_stage(row) != "abierta"):
        entry = dict(raw)
        identity = entry.pop("_cl_identity")
        revision = entry.pop("_cl_revision")
        silent_rule_baseline = entry.pop("_cl_silent_baseline", False)
        stage = cl_stage(entry)
        stages = records.get(identity)
        legacy_known = identity in legacy_codes or identity in seen
        migration = stages is None and (legacy_known or first_scan or silent_rule_baseline)
        if stages is None:
            stages = records[identity] = {}
        previous = stages.get(stage)
        stale_scheduled = stage == "programada" and "abierta" in stages
        changed = bool(previous and module == "rs_sp" and
                       previous.get("revision") != revision)
        observation = {"revision": revision, "observed_at": stamp,
                       "fecha": entry.get("fecha", ""),
                       "source": "baseline" if migration else "observed"}
        if migration or stale_scheduled or (previous and not changed):
            if not previous:
                stages[stage] = observation
            continue
        stages[stage] = observation
        unique_key = entry["unique_key"]
        if unique_key in sent or unique_key in pending_keys:
            continue
        entry["tipo_evento"] = "Actualizado" if changed else f"CL {stage}"
        if changed:
            entry["fecha_anterior"] = previous.get("fecha", "")
        entry["queued_at"] = stamp
        pending.append(entry)
        pending_keys.add(unique_key)
        stages[stage]["source"] = "queued"
        queued += 1
    if queued:
        state[f"{module}_email_last_queue"] = {
            "job": entries[0].get("job", ""), "count": queued,
            "queued_at": stamp, "mode": "cl_stage",
        }
    return queued
