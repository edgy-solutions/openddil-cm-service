"""
AssetCM — the Restate Virtual Object that owns per-asset Configuration
Management state.

ADR-0014: one Virtual Object instance per `asset_id`. State is durable
across restarts. Replaces the three-Faust-agent design from the original
Phase 3 recipe with explicit handlers on a single object plus
Restate-scheduled re-evaluation timers.

Handlers:
  observe(silver_event_dict)        — telemetry arrival from raw-sensor-stream
  apply_cm_event(cm_event_dict)     — explicit CM action from cm-events
  recheck_compliance(_)             — scheduled callback to catch overdue mods
  mark_stale(_)                     — scheduled callback for staleness

Inputs arrive as plain dicts via Kafka subscriptions on the Restate side.
The handler is responsible for projecting that dict into the proto shape the
analyzer wants, calling the analyzer, persisting, and emitting outputs.

Durable state keys on the object (one per asset_id):
  am_state          — dict form of AsMaintainedRecord
  next_recheck_ns   — int, unix ns of the next scheduled recheck (debounce)
"""
from __future__ import annotations

import dataclasses
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import restate
# cloudevents 1.x exposes these directly under `cloudevents.http` and
# `cloudevents.conversion`; cloudevents 2.x moved them to `.v1.*`. Our
# pyproject pins <2.0.0, so we use the 1.x layout and let pip's resolver
# pick the latest 1.x release.
from cloudevents.conversion import to_json as cloud_event_to_json
from cloudevents.http import CloudEvent
from google.protobuf.json_format import MessageToDict

from openddil.configuration.v1 import (
    as_maintained_pb2 as am,
    cm_events_pb2 as cme,
    configuration_baseline_pb2 as cb,
    discrepancy_pb2 as disc,
)
from openddil.telemetry.v1 import telemetry_pb2 as tel

from as_maintained.persistence_model import (
    AsMaintainedRecord,
    DiscrepancyRecord,
    InstalledCiRecord,
    ModComplianceRecord,
)
from as_maintained.store import proto_to_record, record_to_proto
from baselines.loader import BaselineRegistry
from discrepancy.analyzer import (
    compute_discrepancies,
    initialize_from_baseline,
    overall_status,
)

logger = logging.getLogger("cm_service.asset_cm")

# Staleness window — if no telemetry arrives within this many seconds the
# asset transitions to LIFECYCLE_STALE.
STALENESS_WINDOW_S = int(os.getenv("CM_STALENESS_WINDOW_S", str(15 * 60)))

# Floor for scheduled rechecks. Don't schedule sooner than this even if a
# due date is immediately past; protects against tight reschedule loops.
RECHECK_MIN_DELAY_S = int(os.getenv("CM_RECHECK_MIN_DELAY_S", "30"))

# Restate state keys (constants to avoid stringly-typed typos).
_KEY_AM_STATE = "am_state"
_KEY_NEXT_RECHECK = "next_recheck_ns"

# Module-level baseline registry — initialized by main.py on app startup and
# referenced from handlers. Tests inject their own via set_baseline_registry().
_baseline_registry: BaselineRegistry | None = None


def set_baseline_registry(reg: BaselineRegistry) -> None:
    global _baseline_registry
    _baseline_registry = reg


def _baselines() -> BaselineRegistry:
    if _baseline_registry is None:
        raise RuntimeError(
            "Baseline registry not initialized — call set_baseline_registry() "
            "before serving the AssetCM object"
        )
    return _baseline_registry


# ---------------------------------------------------------------------------
# Virtual Object declaration
# ---------------------------------------------------------------------------
asset_cm = restate.VirtualObject("AssetCM")


@asset_cm.handler(
    "observe",
    accept="*/*",
    input_serde=restate.serde.BytesSerde(),
)
async def observe(ctx: restate.ObjectContext, event: bytes) -> None:
    """Process a Silver telemetry event for this asset.

    `BytesSerde` + `accept="*/*"` lets us accept either binary protobuf
    bytes (from raw-sensor-stream via the Kafka subscription) or JSON
    bytes (from direct Restate ingress for testing). `_decode_silver_event`
    handles both shapes — protobuf-binary parses cleanly; JSON falls back
    to dict-shape.

    The Default Serde tries to UTF-8 decode the body before passing to
    the handler, which silently fails on protobuf binary with
    `[500] Unable to parse an input argument. 'utf-8' codec can't decode...`.
    BytesSerde is a pass-through.
    """
    asset_id = ctx.key()
    now_ns = _now_ns(ctx)
    event = _decode_silver_event(event)

    record = await _load_or_init(ctx, asset_id, event, now_ns)
    if record is None:
        # _load_or_init logged and produced an UNREGISTERED record; nothing
        # more to do.
        return

    record.last_observed_at_ns = now_ns
    if record.lifecycle == am.LIFECYCLE_STALE:
        record.lifecycle = am.LIFECYCLE_ACTIVE  # recovered from stale

    # Re-run analyzer against current baseline (in case baseline rev changed
    # via SIGHUP since first init).
    record = _reanalyze(record, now_ns=now_ns)

    await _persist_and_emit_transitions(ctx, record, now_ns)
    await _schedule_next_recheck(ctx, record, now_ns)


@asset_cm.handler(
    "apply_cm_event",
    accept="*/*",
    input_serde=restate.serde.BytesSerde(),
)
async def apply_cm_event(ctx: restate.ObjectContext, event: bytes) -> None:
    """Apply a discrete CM action recorded externally.

    The Kafka subscription on `cm-events` delivers protobuf-binary `CmEvent`
    payloads (the cli/submit_cm_event.py helper and any future producer
    SHOULD encode as protobuf). We decode locally to a dict so the downstream
    `_apply_event_to_record` logic stays JSON-shape friendly.
    """
    asset_id = ctx.key()
    now_ns = _now_ns(ctx)
    event = _decode_cm_event(event)

    stored = await ctx.get(_KEY_AM_STATE, type_hint=dict)
    if stored is None:
        # CM events for unknown assets are not actionable until first
        # telemetry arrives. Log and drop; the cm-events topic retains them
        # for 30 days so an operator can replay.
        logger.warning(
            "Dropping CM event for unknown asset %s: %s",
            asset_id, event.get("eventId") or event.get("event_id"),
        )
        return

    record = _dict_to_record(stored)
    _apply_event_to_record(record, event, now_ns)
    record = _reanalyze(record, now_ns=now_ns)

    await _persist_and_emit_transitions(ctx, record, now_ns)
    await _schedule_next_recheck(ctx, record, now_ns)


@asset_cm.handler("recheck_compliance")
async def recheck_compliance(ctx: restate.ObjectContext, _: dict | None = None) -> None:
    """Scheduled callback. Re-runs the analyzer without any new input —
    used to catch mods that became overdue while no telemetry arrived."""
    asset_id = ctx.key()
    now_ns = _now_ns(ctx)

    stored = await ctx.get(_KEY_AM_STATE, type_hint=dict)
    if stored is None:
        return  # asset was never registered or was decommissioned

    record = _dict_to_record(stored)

    # If staleness threshold exceeded, transition lifecycle.
    if record.last_observed_at_ns:
        stale_after = record.last_observed_at_ns + STALENESS_WINDOW_S * 1_000_000_000
        if now_ns >= stale_after and record.lifecycle == am.LIFECYCLE_ACTIVE:
            record.lifecycle = am.LIFECYCLE_STALE
            logger.info("Asset %s transitioned to LIFECYCLE_STALE", asset_id)

    record = _reanalyze(record, now_ns=now_ns)

    await _persist_and_emit_transitions(ctx, record, now_ns)
    await _schedule_next_recheck(ctx, record, now_ns)


@asset_cm.handler("decommission")
async def decommission(ctx: restate.ObjectContext, reason: dict) -> None:
    """Explicit retirement. Sets lifecycle to DECOMMISSIONED and stops
    scheduling rechecks. State is preserved for audit; not cleared."""
    asset_id = ctx.key()
    now_ns = _now_ns(ctx)
    stored = await ctx.get(_KEY_AM_STATE, type_hint=dict)
    if stored is None:
        logger.info("Decommission requested for unknown asset %s", asset_id)
        return
    record = _dict_to_record(stored)
    record.lifecycle = am.LIFECYCLE_DECOMMISSIONED
    ctx.set(_KEY_AM_STATE, _record_to_dict(record))
    await _emit_asset_cm_state(ctx, record)
    logger.info("Asset %s decommissioned (reason=%s)", asset_id, reason)


# ---------------------------------------------------------------------------
# Initialization helpers
# ---------------------------------------------------------------------------

async def _load_or_init(
    ctx: restate.ObjectContext,
    asset_id: str,
    silver_event: dict,
    now_ns: int,
) -> AsMaintainedRecord | None:
    """Load existing record or initialize from baseline on first-seen.

    Returns None if no baseline is available (asset registered but UNKNOWN).
    """
    stored = await ctx.get(_KEY_AM_STATE, type_hint=dict)
    if stored is not None:
        return _dict_to_record(stored)

    # First-seen path. Look up baseline by platform_variant from the event.
    asset = (silver_event or {}).get("asset", {}) or {}
    variant = asset.get("platformVariant") or asset.get("platform_variant") or ""

    if not variant or variant == "UNKNOWN":
        logger.warning(
            "Asset %s first seen with platform_variant=%r — registering "
            "without baseline (lifecycle=REGISTERED)",
            asset_id, variant,
        )
        # Emit a minimal record so the asset is at least visible to the COP.
        record = AsMaintainedRecord(
            asset_id=asset_id,
            lifecycle=am.LIFECYCLE_REGISTERED,
            last_observed_at_ns=now_ns,
        )
        ctx.set(_KEY_AM_STATE, _record_to_dict(record))
        await _emit_asset_cm_state(ctx, record)
        return None

    baseline = _baselines().get(variant)
    if baseline is None:
        logger.warning(
            "Asset %s has variant %r but no baseline is loaded — registering "
            "without baseline (lifecycle=REGISTERED)",
            asset_id, variant,
        )
        record = AsMaintainedRecord(
            asset_id=asset_id,
            lifecycle=am.LIFECYCLE_REGISTERED,
            last_observed_at_ns=now_ns,
        )
        ctx.set(_KEY_AM_STATE, _record_to_dict(record))
        await _emit_asset_cm_state(ctx, record)
        return None

    proto = initialize_from_baseline(asset_id, baseline, ingest_time_ns=now_ns)
    proto.lifecycle = am.LIFECYCLE_ACTIVE
    proto.last_observed_at.FromNanoseconds(now_ns)
    record = proto_to_record(proto)
    logger.info(
        "Asset %s initialized against baseline %s (variant=%s)",
        asset_id, baseline.baseline_id, variant,
    )
    return record


# ---------------------------------------------------------------------------
# CM event application
# ---------------------------------------------------------------------------

def _apply_event_to_record(
    record: AsMaintainedRecord,
    event: dict,
    now_ns: int,
) -> None:
    """Mutate `record` in place based on the oneof event variant."""
    # ProtobufToDict (camelCase by default) and our own json-dumps shapes
    # can both appear; accept either.
    def _get(field: str) -> Any:
        for key in (field, _to_camel(field)):
            if key in event:
                return event[key]
        return None

    if _get("part_replaced"):
        pr = _get("part_replaced")
        slot_id = pr.get("slotId") or pr.get("slot_id")
        new_ci_id = pr.get("newCiId") or pr.get("new_ci_id")
        for inst in record.installed:
            if inst.slot_id == slot_id:
                inst.ci_id = new_ci_id
                inst.installed_at_ns = now_ns
                return
        # Slot didn't exist in record — add it
        record.installed.append(InstalledCiRecord(
            slot_id=slot_id, ci_id=new_ci_id, installed_at_ns=now_ns,
        ))
        return

    if _get("mod_applied"):
        ma = _get("mod_applied")
        mod_id = ma.get("modId") or ma.get("mod_id")
        applied_at = ma.get("appliedAt") or ma.get("applied_at")
        applied_ns = _iso_to_ns(applied_at) if applied_at else now_ns
        for comp in record.mod_status:
            if comp.mod_id == mod_id:
                comp.state = am.MOD_STATE_APPLIED
                comp.applied_at_ns = applied_ns
                return
        # Mod not tracked — append as applied
        record.mod_status.append(ModComplianceRecord(
            mod_id=mod_id, state=am.MOD_STATE_APPLIED, applied_at_ns=applied_ns,
        ))
        return

    if _get("inspection_completed"):
        ic = _get("inspection_completed")
        if ic.get("passed"):
            # Treat passing inspection as verifying current install state
            record.last_observed_at_ns = now_ns
        return

    if _get("serial_corrected"):
        # CI serial corrections don't change the analyzer's view of slot/ci_id
        # mapping; the corrected serial is stored on the CI record itself
        # (out of scope for the AsMaintainedRecord which only tracks ci_id).
        return

    if _get("baseline_assigned"):
        ba = _get("baseline_assigned")
        new_baseline_id = ba.get("baselineId") or ba.get("baseline_id")
        if new_baseline_id and new_baseline_id != record.baseline_id:
            record.baseline_id = new_baseline_id
            # Reset installed/mod_status to baseline defaults; preserve
            # discrepancy ids by recomputing in _reanalyze.
            new_bl = _baseline_by_id(new_baseline_id)
            if new_bl is not None:
                fresh = initialize_from_baseline(
                    record.asset_id, new_bl, ingest_time_ns=now_ns,
                )
                refreshed = proto_to_record(fresh)
                record.baseline_id = refreshed.baseline_id
                record.installed = refreshed.installed
                record.mod_status = refreshed.mod_status
        return

    if _get("manual_discrepancy"):
        md = _get("manual_discrepancy")
        severity_str = (md.get("severity") or "MINOR").upper()
        severity = {
            "INFO": disc.SEVERITY_INFO,
            "MINOR": disc.SEVERITY_MINOR,
            "MAJOR": disc.SEVERITY_MAJOR,
            "CRITICAL": disc.SEVERITY_CRITICAL,
        }.get(severity_str, disc.SEVERITY_MINOR)
        # Manual discrepancies persist in a dedicated list so reanalysis
        # (which rebuilds `record.discrepancies` from baseline) does not
        # clobber human-raised findings. Merged into the wire form by
        # store.record_to_proto.
        record.manual_discrepancies.append(DiscrepancyRecord(
            discrepancy_id=str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"manual|{record.asset_id}|{md.get('description', '')}",
            )),
            type=disc.DISCREPANCY_UNSPECIFIED,
            description=md.get("description", "manual discrepancy"),
            severity=severity,
            recommended_action=md.get("recommendedAction")
                or md.get("recommended_action", ""),
            detected_at_ns=now_ns,
        ))
        return


def _baseline_by_id(baseline_id: str) -> cb.ConfigurationBaseline | None:
    for variant in _baselines().all_variants():
        b = _baselines().get(variant)
        if b is not None and b.baseline_id == baseline_id:
            return b
    return None


# ---------------------------------------------------------------------------
# Reanalysis and persistence
# ---------------------------------------------------------------------------

def _reanalyze(record: AsMaintainedRecord, *, now_ns: int) -> AsMaintainedRecord:
    """Recompute analyzer-derived discrepancies + overall_status.

    Manual discrepancies (record.manual_discrepancies) are preserved across
    the recompute and factored into overall_status. Only the analyzer's own
    output lands in record.discrepancies — manual entries stay in their
    dedicated list and only merge into the proto wire form via
    store.record_to_proto.
    """
    preserved_manual = list(record.manual_discrepancies)

    if not record.baseline_id:
        record.manual_discrepancies = preserved_manual
        return record  # registered but no baseline assigned

    baseline = _baseline_by_id(record.baseline_id)
    if baseline is None:
        logger.warning(
            "Asset %s references baseline %s which is no longer loaded",
            record.asset_id, record.baseline_id,
        )
        record.manual_discrepancies = preserved_manual
        return record

    proto = record_to_proto(record)
    new_discs = compute_discrepancies(proto, baseline, now_override=now_ns)
    proto.ClearField("discrepancies")
    proto.discrepancies.extend(new_discs)

    # Overall status must consider BOTH analyzer-computed and human-raised
    # discrepancies. Merge for the severity reduction only; the analyzer
    # output stays separately addressable on the record.
    manual_protos = [_disc_record_to_proto_local(d) for d in preserved_manual]
    proto.overall_status = overall_status(list(new_discs) + manual_protos)
    proto.as_of.FromNanoseconds(now_ns)

    out = proto_to_record(proto)
    out.manual_discrepancies = preserved_manual
    return out


def _disc_record_to_proto_local(d: DiscrepancyRecord):
    """Local import-friendly converter (avoids store.py private name)."""
    from openddil.configuration.v1 import discrepancy_pb2 as _disc
    p = _disc.ConfigurationDiscrepancy()
    p.discrepancy_id = d.discrepancy_id
    p.type = d.type
    p.description = d.description
    p.severity = d.severity
    p.recommended_action = d.recommended_action
    p.related_ci_id = d.related_ci_id
    p.related_mod_id = d.related_mod_id
    if d.detected_at_ns:
        from google.protobuf.timestamp_pb2 import Timestamp
        ts = Timestamp()
        ts.FromNanoseconds(d.detected_at_ns)
        p.detected_at.CopyFrom(ts)
    return p


async def _persist_and_emit_transitions(
    ctx: restate.ObjectContext,
    record: AsMaintainedRecord,
    now_ns: int,
) -> None:
    """Save state and emit a transition CloudEvent if overall_status changed."""
    prev_alerted = record.last_alerted_status

    # Should we alert? Two cases:
    #   - Transition into a worse-than-IN_COMPLIANCE state (worsening alert)
    #   - Transition back to IN_COMPLIANCE from worse (resolved alert)
    current = record.overall_status
    transition_kind: str | None = None
    if current != prev_alerted:
        if current >= am.CONFIG_STATUS_MAJOR_DISCREPANCY \
                and prev_alerted < am.CONFIG_STATUS_MAJOR_DISCREPANCY:
            transition_kind = "detected"
        elif current == am.CONFIG_STATUS_IN_COMPLIANCE \
                and prev_alerted > am.CONFIG_STATUS_IN_COMPLIANCE:
            transition_kind = "resolved"

    if transition_kind:
        record.last_alerted_status = current
        cloud_event_bytes = _build_cloud_event(record, prev_alerted, current,
                                                transition_kind, now_ns)
        logger.info(
            "Alert %s for %s: %s -> %s",
            transition_kind, record.asset_id,
            _status_name(prev_alerted), _status_name(current),
        )
        # Emit to tactical-events via the installed Kafka publisher (main.py
        # wires it up; ctx.run journaling makes the publish idempotent under
        # Restate retries).
        await _emit_tactical_event(ctx, cloud_event_bytes)

    ctx.set(_KEY_AM_STATE, _record_to_dict(record))
    await _emit_asset_cm_state(ctx, record)


async def _emit_asset_cm_state(
    ctx: restate.ObjectContext,
    record: AsMaintainedRecord,
) -> None:
    """Publish the updated AsMaintainedConfiguration to asset-cm-state.

    Uses the side-effecting producer wired in main.py. ctx.run() ensures the
    publish is journaled, so retries don't double-publish.
    """
    payload = _record_to_dict(record)
    await ctx.run(
        "publish-asset-cm-state",
        lambda: _publish_kafka(
            topic="asset-cm-state",
            key=record.asset_id,
            value=json.dumps(payload).encode("utf-8"),
        ),
    )


async def _emit_tactical_event(
    ctx: restate.ObjectContext,
    cloud_event_bytes: bytes,
) -> None:
    await ctx.run(
        "publish-tactical-event",
        lambda: _publish_kafka(
            topic="tactical-events",
            key=ctx.key(),
            value=cloud_event_bytes,
        ),
    )


# Producer hook — main.py installs the real Kafka client; tests stub it.
_publish_kafka_fn = None


def set_kafka_publisher(fn) -> None:
    """Install the Kafka publish callable. Signature: fn(topic, key, value)."""
    global _publish_kafka_fn
    _publish_kafka_fn = fn


def _publish_kafka(*, topic: str, key: str, value: bytes) -> None:
    if _publish_kafka_fn is None:
        raise RuntimeError("Kafka publisher not installed; call set_kafka_publisher()")
    _publish_kafka_fn(topic, key, value)


# ---------------------------------------------------------------------------
# Scheduled recheck
# ---------------------------------------------------------------------------

async def _schedule_next_recheck(
    ctx: restate.ObjectContext,
    record: AsMaintainedRecord,
    now_ns: int,
) -> None:
    """Compute the soonest moment we might need to recheck this asset and
    schedule a delayed self-call.

    Two triggers drive future rechecks:
      1. A required mod's due_date passes — we need to fire an overdue alert.
      2. The staleness window expires without new telemetry — transition to
         LIFECYCLE_STALE.

    We pick the soonest of those. To avoid scheduling storms, we debounce
    via `next_recheck_ns`: if a recheck is already scheduled for an earlier
    time we don't schedule another.
    """
    if record.lifecycle == am.LIFECYCLE_DECOMMISSIONED:
        return

    candidates: list[int] = []

    # Next due_date among pending mods
    if record.baseline_id:
        baseline = _baseline_by_id(record.baseline_id)
        if baseline is not None:
            pending_ids = {
                m.mod_id for m in record.mod_status
                if m.state in (am.MOD_STATE_PENDING, am.MOD_STATE_UNSPECIFIED)
            }
            for mod in baseline.required_mods:
                if mod.mod_id in pending_ids \
                        and mod.due_date.seconds > 0:
                    due_ns = mod.due_date.seconds * 1_000_000_000
                    if due_ns > now_ns:
                        candidates.append(due_ns)

    # Staleness check
    if record.last_observed_at_ns and record.lifecycle == am.LIFECYCLE_ACTIVE:
        stale_at = record.last_observed_at_ns + STALENESS_WINDOW_S * 1_000_000_000
        if stale_at > now_ns:
            candidates.append(stale_at)

    if not candidates:
        return

    target_ns = min(candidates)
    # Floor the delay
    delay_s = max(RECHECK_MIN_DELAY_S, (target_ns - now_ns) // 1_000_000_000)

    # Debounce: if we already have a recheck scheduled at <= target, skip
    existing = await ctx.get(_KEY_NEXT_RECHECK, type_hint=int)
    if existing and existing <= target_ns:
        return

    ctx.set(_KEY_NEXT_RECHECK, target_ns)
    ctx.object_send(
        recheck_compliance,
        key=record.asset_id,
        arg={},
        send_delay=timedelta(seconds=int(delay_s)),
    )


# ---------------------------------------------------------------------------
# CloudEvent construction
# ---------------------------------------------------------------------------

def _build_cloud_event(
    record: AsMaintainedRecord,
    prev_status: int,
    current_status: int,
    transition_kind: str,
    now_ns: int,
) -> bytes:
    event_type = (
        "openddil.configuration.discrepancy.detected"
        if transition_kind == "detected"
        else "openddil.configuration.discrepancy.resolved"
    )
    data = {
        "asset_id": record.asset_id,
        "baseline_id": record.baseline_id,
        "lifecycle": _lifecycle_name(record.lifecycle),
        "previous_status": _status_name(prev_status),
        "current_status": _status_name(current_status),
        "discrepancies": [dataclasses.asdict(d) for d in record.discrepancies],
        "transition_at": _ns_to_iso(now_ns),
    }
    ce = CloudEvent(
        attributes={
            "specversion": "1.0",
            "id": str(uuid.uuid4()),
            "source": "/openddil/cm-service",
            "type": event_type,
            "subject": record.asset_id,
            "time": _ns_to_iso(now_ns),
            "datacontenttype": "application/json",
        },
        data=data,
    )
    return cloud_event_to_json(ce)


# ---------------------------------------------------------------------------
# Dict <-> Record bridge (for Restate state serialization)
# ---------------------------------------------------------------------------

def _record_to_dict(rec: AsMaintainedRecord) -> dict:
    return dataclasses.asdict(rec)


def _dict_to_record(d: dict) -> AsMaintainedRecord:
    return AsMaintainedRecord(
        asset_id=d.get("asset_id", ""),
        baseline_id=d.get("baseline_id", ""),
        as_of_ns=d.get("as_of_ns", 0),
        installed=[InstalledCiRecord(**i) for i in d.get("installed", [])],
        mod_status=[ModComplianceRecord(**m) for m in d.get("mod_status", [])],
        discrepancies=[DiscrepancyRecord(**x) for x in d.get("discrepancies", [])],
        overall_status=d.get("overall_status", 0),
        lifecycle=d.get("lifecycle", 0),
        last_observed_at_ns=d.get("last_observed_at_ns", 0),
        last_alerted_status=d.get("last_alerted_status", 0),
        manual_discrepancies=[
            DiscrepancyRecord(**x) for x in d.get("manual_discrepancies", [])
        ],
    )


# ---------------------------------------------------------------------------
# Bytes -> dict decoders for Kafka-subscription handler inputs
# ---------------------------------------------------------------------------

def _decode_silver_event(raw: bytes | dict | None) -> dict:
    """Restate's Kafka subscription delivers the message value as bytes
    (the body of the Kafka record). raw-sensor-stream carries binary
    protobuf EntityTelemetryEvent; decode to a dict so downstream handler
    code stays simple."""
    if isinstance(raw, dict):
        return raw  # already-decoded (e.g., unit tests)
    if not raw:
        return {}
    try:
        evt = tel.EntityTelemetryEvent()
        evt.ParseFromString(raw if isinstance(raw, (bytes, bytearray)) else bytes(raw))
        # protobuf 6.x renamed `including_default_value_fields` →
        # `always_print_fields_with_no_presence`. We don't want defaults at
        # all here, so omitting the kwarg is correct on both 5.x and 6.x.
        return MessageToDict(evt, preserving_proto_field_name=False)
    except Exception as exc:
        logger.warning("Failed to decode Silver event as protobuf "
                        "(len=%d): %s", len(raw) if raw else 0, exc)
        return {}


def _decode_cm_event(raw: bytes | dict | None) -> dict:
    """Decode a binary CmEvent payload to dict. Producers (the cli helper
    and external systems) emit protobuf binary; handler logic operates on
    dict for ergonomic oneof-variant access."""
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        evt = cme.CmEvent()
        evt.ParseFromString(raw if isinstance(raw, (bytes, bytearray)) else bytes(raw))
        # protobuf 6.x renamed `including_default_value_fields` →
        # `always_print_fields_with_no_presence`. We don't want defaults at
        # all here, so omitting the kwarg is correct on both 5.x and 6.x.
        return MessageToDict(evt, preserving_proto_field_name=False)
    except Exception as exc:
        logger.warning("Failed to decode CmEvent (len=%d): %s",
                        len(raw) if raw else 0, exc)
        return {}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _now_ns(ctx: restate.ObjectContext) -> int:
    # ctx.time() returns a `datetime` aligned to Restate's wall clock for
    # deterministic replay. Fall back to system clock outside Restate (tests).
    try:
        return int(ctx.time().timestamp() * 1_000_000_000)
    except Exception:
        return int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)


def _ns_to_iso(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).isoformat()


def _iso_to_ns(iso: str) -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
               * 1_000_000_000)


def _to_camel(snake: str) -> str:
    parts = snake.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


def _status_name(s: int) -> str:
    return {
        am.CONFIG_STATUS_UNSPECIFIED:         "CONFIG_STATUS_UNSPECIFIED",
        am.CONFIG_STATUS_IN_COMPLIANCE:       "CONFIG_STATUS_IN_COMPLIANCE",
        am.CONFIG_STATUS_MINOR_DISCREPANCY:   "CONFIG_STATUS_MINOR_DISCREPANCY",
        am.CONFIG_STATUS_MAJOR_DISCREPANCY:   "CONFIG_STATUS_MAJOR_DISCREPANCY",
        am.CONFIG_STATUS_NOT_MISSION_CAPABLE: "CONFIG_STATUS_NOT_MISSION_CAPABLE",
    }.get(s, f"UNKNOWN({s})")


def _lifecycle_name(s: int) -> str:
    return {
        am.LIFECYCLE_UNSPECIFIED:    "UNSPECIFIED",
        am.LIFECYCLE_REGISTERED:     "REGISTERED",
        am.LIFECYCLE_ACTIVE:         "ACTIVE",
        am.LIFECYCLE_STALE:          "STALE",
        am.LIFECYCLE_DECOMMISSIONED: "DECOMMISSIONED",
    }.get(s, f"UNKNOWN({s})")
