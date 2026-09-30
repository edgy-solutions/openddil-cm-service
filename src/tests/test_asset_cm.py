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
        # _now_ns in production goes through ctx.run("now_ns", lambda:
        # int(datetime.now(...).timestamp() * 1e9)). Tests stub the
        # wall clock via StubCtx._now_ns so assertions on
        # last_observed_at_ns can compare exact values; honor that
        # label here so tests stay deterministic without forcing
        # the production code to add a test-friendly side door (which
        # broke replay correctness; see asset_cm.py:_now_ns comment).
        if label == "now_ns":
            result = self._now_ns
        else:
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


def _removal_event_dict(asset_id: str = "dis:1:1:4773") -> dict:
    """A DIS Remove Entity PDU decoded to the Silver event shape: no
    kinematics, no platform_variant, operational_state.operational_status
    = OPERATIONAL_STATUS_REMOVED. This is the shape `_decode_silver_event`
    produces for protobuf-binary input (MessageToDict camelCase, enum
    rendered as its NAME string) — see asset_cm._is_removal's docstring
    for the other shapes the guard also has to tolerate."""
    return {
        "eventId": "test-evt-removal",
        "asset": {
            "assetId": asset_id,
        },
        "operationalState": {
            "operationalStatus": "OPERATIONAL_STATUS_REMOVED",
        },
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
# observe() — Remove Entity (operational_status = OPERATIONAL_STATUS_REMOVED)
# ---------------------------------------------------------------------------

def test_observe_removal_for_unknown_asset_is_dropped(install_registry_and_publisher):
    """A Remove Entity for an asset_id with no existing AssetCM state must be
    dropped before `_load_or_init` runs, not treated as first-seen
    registration. Without the guard, `_load_or_init` sees no
    platform_variant (removals carry none) and registers a minimal visible
    record — a removal would CREATE an asset."""
    published = install_registry_and_publisher
    from metrics import cm_removal_unknown_asset_dropped_total as counter
    before = counter._value.get()

    ctx = StubCtx(key="dis:9:9:0001", now_ns=_now_ns())
    asyncio.run(observe(ctx, _removal_event_dict(asset_id="dis:9:9:0001")))

    assert "am_state" not in ctx._state, "no state may be set for the dropped removal"
    assert published == [], "no emission for the dropped removal"
    assert counter._value.get() == before + 1


def test_observe_removal_for_existing_asset_follows_existing_path(
    install_registry_and_publisher,
):
    """A Remove Entity for an asset we already have AssetCM state for is
    NOT covered by the new guard — it must still update and persist state
    the same way any other observe() call does today."""
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))
    first_observed = ctx._state["am_state"]["last_observed_at_ns"]

    ctx._now_ns += 10 * 1_000_000_000   # 10 seconds later
    asyncio.run(observe(ctx, _removal_event_dict(asset_id="dis:1:1:4773")))

    assert ctx._state["am_state"]["last_observed_at_ns"] > first_observed, (
        "existing-key path must still update/persist state as today"
    )


def test_observe_non_removal_first_seen_still_initializes(
    install_registry_and_publisher,
):
    """Sanity check the new guard is removal-specific: an ordinary
    first-seen event (not a removal) must still take the existing
    first-seen registration path unchanged."""
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))

    assert "am_state" in ctx._state
    assert ctx._state["am_state"]["lifecycle"] == am.LIFECYCLE_ACTIVE


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


# ---------------------------------------------------------------------------
# Releasability labels (ADR-0029 §3) — propagated, never derived
# ---------------------------------------------------------------------------

def _labelled_event(nation: str = "ATL", releasable=("BDR",)) -> dict:
    ev = _silver_event_dict()
    ev["provenance"]["originatorNation"] = nation
    ev["provenance"]["releasableTo"] = list(releasable)
    return ev


def test_observe_stores_releasability_labels(install_registry_and_publisher):
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _labelled_event()))
    state = ctx._state["am_state"]
    assert state["originator_nation"] == "ATL"
    assert state["releasable_to"] == ["BDR"]


def test_unlabelled_event_leaves_record_unlabelled(install_registry_and_publisher):
    """cm-service must NOT invent a nation for an asset nobody declared. The
    §7 completeness gate is supposed to catch that; a default here would hide
    exactly what the gate exists to surface, and the fix belongs at the
    ingress that failed to declare the asset."""
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))
    state = ctx._state["am_state"]
    assert state["originator_nation"] == ""
    assert state["releasable_to"] == []


def test_labels_reach_the_emitted_cm_state_envelope(install_registry_and_publisher):
    """asset-cm-state is JSON (ADR-0018) built by dataclasses.asdict, so the
    labels land at the ENVELOPE'S TOP LEVEL — which is the exact shape the
    projector's cm_state handler reads. Pinned here because the two repos
    agree on that shape by convention and nothing else enforces it."""
    published = install_registry_and_publisher
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _labelled_event()))
    payload = next(json.loads(p[2]) for p in published if p[0] == "asset-cm-state")
    assert payload["originator_nation"] == "ATL"
    assert payload["releasable_to"] == ["BDR"]


def test_labels_reach_the_tactical_event(install_registry_and_publisher):
    """THE DEFECT OF 2026-09-17, pinned.

    The record carried originator_nation/releasable_to from ingest, the
    cm-state envelope carried them (test above), the recompute preserved them
    (test below) -- and the ONE dict that builds the tactical-events
    CloudEvent did not include them. Three rows reached `tactical_events`
    with both label columns NULL.

    The projector cannot compensate and must not: `releasability_from` has no
    fallback by design (ADR-0029 §3), so an unstamped event stays unlabelled
    forever.

    WHY NOTHING CAUGHT IT FOR MONTHS: `tactical_events` was EMPTY the whole
    time the derive stage was dead, so the §7 completeness gate -- which does
    check this table -- had never seen a row from this producer. The gate was
    green over zero rows. A table that is empty for the wrong reason is not
    covered by the check that reads it.
    """
    published = install_registry_and_publisher
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _labelled_event()))

    tactical = [p for p in published if p[0] == "tactical-events"]
    assert len(tactical) == 1
    data = json.loads(tactical[0][2])["data"]
    assert data["originator_nation"] == "ATL"
    assert data["releasable_to"] == ["BDR"]


def test_tactical_event_keeps_empty_releasable_to_as_a_value(
    install_registry_and_publisher,
):
    """An empty releasable_to is a REAL LABEL, not a gap.

    A declared nation with no additional release is the ordinary coalition
    posture -- the originator's own access comes from the first clause of the
    ADR-0029 §4 disjunction. The row must be labelled on BOTH columns so the
    completeness gate can tell "releasable to nobody else" from "nobody ever
    said", which are different facts with different remedies.

    Guards against a well-meaning future edit that treats [] as missing and
    substitutes the nation, silently widening an asset's audience.
    """
    published = install_registry_and_publisher
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _labelled_event(nation="ATL", releasable=())))

    data = json.loads(
        next(p[2] for p in published if p[0] == "tactical-events")
    )["data"]
    assert data["originator_nation"] == "ATL"
    assert data["releasable_to"] == []


def test_unlabelled_asset_yields_unlabelled_tactical_event(
    install_registry_and_publisher,
):
    """The must-NOT-fire half. cm-service may not invent a nation for an
    asset nobody declared -- not on the record, and not on the way out.

    Without this, a fix for the defect above could be written as "default the
    nation" and pass, which would hide undeclared assets from the gate that
    exists to find them. The remedy for an unlabelled event is at the ingress
    that failed to declare the asset.
    """
    published = install_registry_and_publisher
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict()))

    data = json.loads(
        next(p[2] for p in published if p[0] == "tactical-events")
    )["data"]
    assert data["originator_nation"] == ""
    assert data["releasable_to"] == []


def test_labels_survive_recompute(install_registry_and_publisher):
    """THE TRAP. `record -> proto -> record` drops every dataclass-only
    field, so labels survive a recompute only by being named in the
    preservation block. Forgetting one is silent partial propagation: labels
    correct until the first recompute, then NULL, and a §7 gate that passes
    and later fails with no code change in between.

    recheck_compliance is the cheapest path through _recompute; edge_id is
    asserted alongside so a future edit that drops BOTH cannot pass by
    accident."""
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _labelled_event()))
    assert ctx._state["am_state"]["originator_nation"] == "ATL"

    asyncio.run(recheck_compliance(ctx, b""))
    state = ctx._state["am_state"]
    assert state["originator_nation"] == "ATL", "labels dropped by recompute"
    assert state["releasable_to"] == ["BDR"]


def test_labels_are_sticky_across_a_thin_event(install_registry_and_publisher):
    """An asset does not change nationality because one message was thin.
    Clearing on absence would make the §7 gate flicker with feed hiccups."""
    ctx = StubCtx(key="dis:1:1:4773", now_ns=_now_ns())
    asyncio.run(observe(ctx, _labelled_event()))
    asyncio.run(observe(ctx, _silver_event_dict()))     # no labels this time
    assert ctx._state["am_state"]["originator_nation"] == "ATL"
