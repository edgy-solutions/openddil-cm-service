"""Golden bytes of the asset-cm-state payload (wire shape must not change).

A fixed AsMaintainedRecord carrying closed episodes and episode context goes
through `_emit_asset_cm_state`; the published value must equal the stored
golden bytes. Those two durable-only fields must stay off the wire.
Set REGEN_GOLDEN=1 to rewrite the file (never in CI).
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]
                        / "openddil-contracts" / "gen" / "python"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from events import asset_cm
from as_maintained.persistence_model import (
    AsMaintainedRecord, ClosedEpisodeRecord, DiscrepancySourceRecord,
    EpisodeContextRecord, InstalledCiRecord, ModComplianceRecord,
)
from tests.test_asset_cm import StubCtx

GOLDEN = Path(__file__).parent / "golden" / "asset_cm_state_payload.json"


def _record() -> AsMaintainedRecord:
    mods = [ModComplianceRecord(mod_id="MWO-T-1", state=2, applied_at_ns=1_700_000_000_000_000_000)]
    closed = [
        ClosedEpisodeRecord(
            discrepancy_id="ep-t-0001", detected_at_ns=1_700_000_100_000_000_000,
            component="tr_module", fault_code="F-1001",
            sources=[DiscrepancySourceRecord(source="operator_report", reported_by="operator-a")],
            cleared_at_ns=1_700_000_200_000_000_000, cleared_by="operator-b",
            resolution="swapped module", baseline_id="BL-T-1", mod_compliance=mods),
        ClosedEpisodeRecord(
            discrepancy_id="ep-t-0002", detected_at_ns=1_700_000_300_000_000_000,
            component="cooling_fan", fault_code="F-1002",
            cleared_at_ns=1_700_000_400_000_000_000, cleared_by="operator-a",
            resolution="cleaned"),
    ]
    return AsMaintainedRecord(
        asset_id="asset-t-0001", baseline_id="BL-T-1", as_of_ns=1_700_000_500_000_000_000,
        installed=[InstalledCiRecord(slot_id="engine", ci_id="ci-t-1", installed_at_ns=1_700_000_000_000_000_000),
                   InstalledCiRecord(slot_id="transmission", ci_id="ci-t-2")],
        mod_status=mods, overall_status=1, lifecycle=1,
        edge_id="edge-a", region_id="region-a",
        originator_nation="ATL", releasable_to=["BDR"],
        closed_episodes=closed,
        episode_context={"ep-t-0003": EpisodeContextRecord(
            detected_at_ns=1_700_000_450_000_000_000, baseline_id="BL-T-1", mod_compliance=mods)},
    )


def test_asset_cm_state_payload_is_golden():
    published = []
    asset_cm.set_kafka_publisher(lambda t, k, v: published.append((t, k, v)))
    try:
        ctx = StubCtx(key="asset-t-0001", now_ns=1_700_000_500_000_000_000)
        asyncio.run(asset_cm._emit_asset_cm_state(ctx, _record()))
    finally:
        asset_cm.set_kafka_publisher(None)
    assert len(published) == 1
    topic, key, value = published[0]
    assert (topic, key) == ("asset-cm-state", "asset-t-0001")
    if os.environ.get("REGEN_GOLDEN") == "1":
        GOLDEN.write_bytes(value)
    assert value == GOLDEN.read_bytes()
