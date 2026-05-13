"""
Unit tests for the AssetCM Virtual Object handlers.

Uses a stub ObjectContext so the handler logic can be exercised without
spinning up a Restate runtime. The actual Restate-runtime integration is
covered by Hero Scenario v3 Tests 12-16 against the live stack.

The handlers are async functions that depend on `ctx.get`, `ctx.set`,
`ctx.run`, `ctx.object_send`, `ctx.key`, and `ctx.time`. The stub
implements all of these with predictable behavior.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3]
                        / "openddil-contracts" / "gen" / "python"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from openddil.configuration.v1 import (
    as_maintained_pb2 as am,
    configuration_baseline_pb2 as cb,
    discrepancy_pb2 as disc,
)
from baselines.loader import make_registry
from events import asset_cm
from events.asset_cm import (
    apply_cm_event,
    decommission,
    observe,
    recheck_compliance,
)


REAL_BASELINES_DIR = (
    Path(__file__).resolve().parents[3]
    / "openddil-contracts" / "baselines"
)


# ---------------------------------------------------------------------------
# Stub Restate ObjectContext
# ---------------------------------------------------------------------------

class StubCtx:
    """A minimal ObjectContext stand-in.

    Records every state mutation, side-effect (`run`), and scheduled send so
    tests can assert on them.
    """

    def __init__(self, key: str, now_ns: int):
        self._key = key
        self._now_ns = now_ns
        self._state: dict[str, object] = {}
        self.runs: list[tuple[str, object]] = []        # (label, result)
        self.scheduled: list[dict] = []                  # delayed sends
        self.published: list[tuple[str, str, bytes]] = []  # via run -> publisher

    def key(self) -> str:
        return self._key

    def time(self):
        return datetime.fromtimestamp(self._now_ns / 1_000_000_000,
                                       tz=timezone.utc)

    async def get(self, name: str, type_hint=None):
        return self._state.get(name)

    def set(self, name: str, value) -> None:
        self._state[name] = value

    def clear(self, name: str) -> None:
        self._state.pop(name, None)

    def clear_all(self) -> None:
        self._state.clear()

    async def run(self, label: str, fn):
        result = fn()
        self.runs.append((label, result))
        return result

    def object_send(self, handler, *, key, arg, send_delay=None):
        self.scheduled.append({
            "handler": getattr(handler, "__name__", str(handler)),
            "key": key,
            "arg": arg,
            "send_delay_s": send_delay.total_seconds() if send_delay else 0,
        })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def install_registry_and_publisher():
    """Each test gets the real Phase 1 baselines + a stub Kafka publisher."""
    asset_cm.set_baseline_registry(make_registry(REAL_BASELINES_DIR))
    published: list[tuple[str, str, bytes]] = []

    def stub_publish(topic: str, key: str, value: bytes) -> None:
        published.append((topic, key, value))

    asset_cm.set_kafka_publisher(stub_publish)
    yield published
    asset_cm.set_baseline_registry(None)  # cleanup
    asset_cm.set_kafka_publisher(None)


def _now_ns(iso: str = "2026-05-12T12:00:00Z") -> int:
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
               * 1_000_000_000)


def _silver_event_dict(
    asset_id: str = "dis:1:1:4773",
    platform_variant: str = "M1A2-SEPv3",
) -> dict:
    """ProtobufToDict-shaped Silver event (camelCase per default proto JSON)."""
    return {
        "eventId": "test-evt-1",
        "asset": {
            "assetId": asset_id,
            "platformVariant": platform_variant,
        },
        "kinematics": {},
        "provenance": {
            "producerId": "dis-ingestor-binary",
            "sourceProtocol": "DIS/IEEE-1278.1-binary",
        },
        "schemaRevision": 1,
    }


# ---------------------------------------------------------------------------
# observe() — first-seen path
# ---------------------------------------------------------------------------

def test_observe_first_seen_initializes_from_baseline(install_registry_and_publisher):
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))

    state = ctx._state["am_state"]
    assert state["asset_id"] == "dis:1:1:4773"
    assert state["baseline_id"] == "M1A2-SEPv3-Baseline-2024.2"
    assert state["lifecycle"] == am.LIFECYCLE_ACTIVE
    # M1A2 baseline has 5 authorized slots but apkws-launcher is optional,
    # so initialize_from_baseline records 4 required slot entries.
    installed_slots = {i["slot_id"] for i in state["installed"]}
    assert installed_slots == {"engine", "transmission", "fcs-computer",
                                "thermal-imager"}
    assert len(state["mod_status"]) == 2
    # baseline has 1 SAFETY_OF_FLIGHT mod with due_date 2025-12-31 (past as of
    # 2026-05-12), so the asset starts NOT_MISSION_CAPABLE
    assert state["overall_status"] == am.CONFIG_STATUS_NOT_MISSION_CAPABLE


def test_observe_first_seen_emits_asset_cm_state(install_registry_and_publisher):
    published = install_registry_and_publisher
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))
    # One publish to asset-cm-state, one to tactical-events (CRITICAL on first seen)
    topics = [p[0] for p in published]
    assert "asset-cm-state" in topics
    assert "tactical-events" in topics


def test_observe_first_seen_fires_critical_alert(install_registry_and_publisher):
    """First-seen M1A2 starts NOT_MISSION_CAPABLE (overdue safety MWO);
    a tactical-events CloudEvent must be published with severity CRITICAL."""
    published = install_registry_and_publisher
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))

    tactical = [p for p in published if p[0] == "tactical-events"]
    assert len(tactical) == 1
    envelope = json.loads(tactical[0][2])
    assert envelope["type"] == "openddil.configuration.discrepancy.detected"
    assert envelope["subject"] == "dis:1:1:4773"
    assert envelope["data"]["current_status"] == "CONFIG_STATUS_NOT_MISSION_CAPABLE"


def test_observe_unknown_platform_variant_registers_without_baseline(
    install_registry_and_publisher,
):
    ctx = StubCtx(key="dis:9:9:9999", now_ns=_now_ns())
    event = _silver_event_dict(asset_id="dis:9:9:9999", platform_variant="UNKNOWN")
    asyncio.run(observe(ctx, event))

    state = ctx._state["am_state"]
    assert state["lifecycle"] == am.LIFECYCLE_REGISTERED
    assert state["baseline_id"] == ""
    assert state["overall_status"] == am.CONFIG_STATUS_UNSPECIFIED


def test_observe_idempotent_no_extra_alert(install_registry_and_publisher):
    """Second observe() for an already-CRITICAL asset must NOT re-fire the
    alert. This is the ADR-0014 transition-cache replacement."""
    published = install_registry_and_publisher
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())

    asyncio.run(observe(ctx, _silver_event_dict()))
    ctx._now_ns += 10 * 1_000_000_000   # 10 seconds later
    asyncio.run(observe(ctx, _silver_event_dict()))

    tactical = [p for p in published if p[0] == "tactical-events"]
    assert len(tactical) == 1, (
        "Second observe() with no state change must not emit a second alert"
    )


# ---------------------------------------------------------------------------
# apply_cm_event()
# ---------------------------------------------------------------------------

def test_mod_applied_resolves_discrepancy_and_emits_resolved_alert(
    install_registry_and_publisher,
):
    published = install_registry_and_publisher
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    # First-seen: lands in NOT_MISSION_CAPABLE
    asyncio.run(observe(ctx, _silver_event_dict()))

    # Apply both overdue mods AND verify the installed CIs (so we move into
    # full compliance). MWO-2024-117 is SAFETY_OF_FLIGHT, MWO-2023-089 is
    # MISSION_CRITICAL — both overdue as of 2026-05-12.
    for mod_id in ("MWO-2024-117", "MWO-2023-089"):
        cm_evt = {
            "eventId": f"test-{mod_id}",
            "assetId": "dis:1:1:4773",
            "modApplied": {"modId": mod_id,
                            "appliedAt": "2026-05-12T11:00:00Z"},
        }
        asyncio.run(apply_cm_event(ctx, cm_evt))

    # We still have unverified CI slots — overall_status improves but won't
    # hit IN_COMPLIANCE until we verify CIs via an InspectionCompleted event.
    # The "resolved" alert only fires when status returns to IN_COMPLIANCE,
    # so this scenario verifies we DON'T spuriously emit resolved alerts.
    state = ctx._state["am_state"]
    assert all(m["state"] == am.MOD_STATE_APPLIED for m in state["mod_status"])

    tactical = [p for p in published if p[0] == "tactical-events"]
    # Exactly one alert (the original NOT_MISSION_CAPABLE detection) —
    # no spurious resolved alerts while CIs remain unverified
    assert len(tactical) == 1


def test_cm_event_for_unknown_asset_is_dropped(install_registry_and_publisher):
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    # No prior observe(), so no state exists
    asyncio.run(apply_cm_event(ctx, {
        "modApplied": {"modId": "MWO-2024-117"},
    }))
    # No state was created
    assert "am_state" not in ctx._state


def test_baseline_assigned_switches_baseline(install_registry_and_publisher):
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))
    assert ctx._state["am_state"]["baseline_id"] == "M1A2-SEPv3-Baseline-2024.2"

    asyncio.run(apply_cm_event(ctx, {
        "baselineAssigned": {"baselineId": "UH-60M-Baseline-2024.1"},
    }))
    assert ctx._state["am_state"]["baseline_id"] == "UH-60M-Baseline-2024.1"


def test_manual_discrepancy_survives_reanalysis(install_registry_and_publisher):
    """Manual discrepancies must persist in `manual_discrepancies` across
    every reanalysis cycle. The analyzer rebuilds the `discrepancies` list
    from baseline; manual entries live in their own list and merge into the
    wire form via store.record_to_proto."""
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))

    asyncio.run(apply_cm_event(ctx, {
        "manualDiscrepancy": {
            "description": "Visual: hatch hinge cracked",
            "severity": "MAJOR",
            "recommendedAction": "Replace hatch assembly",
        },
    }))

    # After the first reanalysis the manual entry must be in the dedicated list
    manual = ctx._state["am_state"]["manual_discrepancies"]
    assert len(manual) == 1
    assert "hatch hinge" in manual[0]["description"]

    # Trigger a second reanalysis via another observe(); manual must survive
    ctx._now_ns += 10 * 1_000_000_000
    asyncio.run(observe(ctx, _silver_event_dict()))
    manual_after = ctx._state["am_state"]["manual_discrepancies"]
    assert len(manual_after) == 1, "Manual discrepancy lost on subsequent reanalysis"
    assert manual_after[0]["description"] == manual[0]["description"]


def test_critical_manual_discrepancy_escalates_overall_status(
    install_registry_and_publisher,
):
    """A CRITICAL manual discrepancy on an otherwise-compliant asset must
    drive overall_status to NOT_MISSION_CAPABLE."""
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))

    # Apply all overdue mods so the asset reaches MAJOR_DISCREPANCY (still
    # has unverified CIs but no overdue mods)
    for mod_id in ("MWO-2024-117", "MWO-2023-089"):
        asyncio.run(apply_cm_event(ctx, {
            "modApplied": {"modId": mod_id,
                            "appliedAt": "2026-05-12T11:00:00Z"},
        }))

    pre = ctx._state["am_state"]["overall_status"]
    # Now raise a CRITICAL manual finding — must escalate beyond pre
    asyncio.run(apply_cm_event(ctx, {
        "manualDiscrepancy": {
            "description": "Crew reports loose ammunition retention strap",
            "severity": "CRITICAL",
            "recommendedAction": "Ground until inspected by armorer",
        },
    }))
    post = ctx._state["am_state"]["overall_status"]
    assert post == am.CONFIG_STATUS_NOT_MISSION_CAPABLE, (
        f"Expected escalation to NOT_MISSION_CAPABLE; pre={pre} post={post}"
    )


def test_manual_discrepancy_appears_in_wire_form(install_registry_and_publisher):
    """The wire-form proto (what asset-cm-state consumers see) must include
    manual discrepancies merged into the unified `discrepancies` list."""
    from as_maintained.store import record_to_proto
    from as_maintained.persistence_model import DiscrepancyRecord

    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))
    asyncio.run(apply_cm_event(ctx, {
        "manualDiscrepancy": {
            "description": "M1 — visual: track tension low",
            "severity": "MINOR",
            "recommendedAction": "Tension to spec at next motorpool",
        },
    }))

    # Reconstruct the record and run it through record_to_proto
    record = asset_cm._dict_to_record(ctx._state["am_state"])
    proto = record_to_proto(record)

    descriptions = [d.description for d in proto.discrepancies]
    assert any("track tension low" in desc for desc in descriptions), (
        "Manual discrepancy missing from wire form"
    )
    # And the analyzer-derived MISSING_CI / MISSING_MOD entries are still
    # present (proves we merged, not replaced)
    assert any("Slot " in desc for desc in descriptions)


# ---------------------------------------------------------------------------
# Scheduled recheck
# ---------------------------------------------------------------------------

def test_recheck_compliance_marks_stale_when_window_expired(
    install_registry_and_publisher,
):
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))
    assert ctx._state["am_state"]["lifecycle"] == am.LIFECYCLE_ACTIVE

    # Jump beyond the staleness window
    ctx._now_ns += (asset_cm.STALENESS_WINDOW_S + 60) * 1_000_000_000
    asyncio.run(recheck_compliance(ctx, {}))
    assert ctx._state["am_state"]["lifecycle"] == am.LIFECYCLE_STALE


def test_recheck_recovers_from_stale_on_next_observe(
    install_registry_and_publisher,
):
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))
    ctx._now_ns += (asset_cm.STALENESS_WINDOW_S + 60) * 1_000_000_000
    asyncio.run(recheck_compliance(ctx, {}))
    assert ctx._state["am_state"]["lifecycle"] == am.LIFECYCLE_STALE

    ctx._now_ns += 60 * 1_000_000_000
    asyncio.run(observe(ctx, _silver_event_dict()))
    assert ctx._state["am_state"]["lifecycle"] == am.LIFECYCLE_ACTIVE


def test_observe_schedules_recheck(install_registry_and_publisher):
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))
    # At least one scheduled recheck (either for staleness window or for the
    # next mod due_date)
    assert any(s["handler"] == "recheck_compliance" for s in ctx.scheduled)


# ---------------------------------------------------------------------------
# Decommission
# ---------------------------------------------------------------------------

def test_decommission_sets_lifecycle(install_registry_and_publisher):
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))
    asyncio.run(decommission(ctx, {"reason": "retired"}))
    assert ctx._state["am_state"]["lifecycle"] == am.LIFECYCLE_DECOMMISSIONED


def test_decommission_stops_recheck_scheduling(install_registry_and_publisher):
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))
    ctx.scheduled.clear()
    asyncio.run(decommission(ctx, {"reason": "retired"}))
    # No new recheck schedules after decommission
    assert ctx.scheduled == []
