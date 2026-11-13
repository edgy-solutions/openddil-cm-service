"""failure-episodes: the wire record and the per-close emit (slice 3a)."""
from __future__ import annotations

import asyncio
import copy
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3]
                        / "openddil-contracts" / "gen" / "python"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from events import asset_cm
from events import failure_episodes as fe
from events.asset_cm import apply_cm_event, observe
from tests.test_asset_cm import (
    StubCtx, _episode_cm_event, _now_ns, _resolve_cm_event, _silver_event_dict,
    REAL_BASELINES_DIR,
)
from baselines.loader import make_registry

GOLDEN = Path(__file__).parent / "golden" / "failure_episode_record.json"
ASSET = "asset-t-0001"


@pytest.fixture(autouse=True)
def published():
    asset_cm.set_baseline_registry(make_registry(REAL_BASELINES_DIR))
    out: list[tuple[str, str, bytes]] = []
    asset_cm.set_kafka_publisher(lambda t, k, v: out.append((t, k, v)))
    yield out
    asset_cm.set_baseline_registry(None)
    asset_cm.set_kafka_publisher(None)


def _episode(**over) -> dict:
    ep = {
        "discrepancy_id": "ep-t-0001", "type": 1, "description": "d", "severity": 2,
        "recommended_action": "r", "detected_at_ns": 1_700_000_100_000_000_000,
        "component": "tr_module", "fault_code": "F-1001",
        "sources": [{"source": "operator_report", "reported_by": "operator-a"},
                    {"source": "inspection", "reported_by": "operator-b"}],
        "cleared_at_ns": 1_700_000_200_000_000_000, "cleared_by": "operator-b",
        "resolution": "swapped module", "baseline_id": "BL-T-1",
        "mod_compliance": [{"mod_id": "MWO-T-1", "state": 2,
                            "applied_at_ns": 1_700_000_000_000_000_000,
                            "applied_by_work_order": ""}],
    }
    ep.update(over)
    return ep


LABELS = {"originator_nation": "ATL", "releasable_to": ["BDR"]}
TIER = {"edge_id": "edge-a", "region_id": "region-a"}


# -- the wire record --------------------------------------------------------

def test_golden_wire_record():
    value, refusal = fe.build_record(
        ASSET, {"originator_nation": "ATL", "releasable_to": ["BDR"]}, TIER, _episode())
    assert refusal is None
    if os.environ.get("REGEN_GOLDEN") == "1":
        GOLDEN.write_bytes(value)
    assert value == GOLDEN.read_bytes()
    assert set(json.loads(value)) == {
        "episode_id", "detected_at_ns", "asset_id", "component", "fault_code",
        "cleared_at_ns", "cleared_by", "resolution", "sources", "baseline_id",
        "mod_compliance", "originator_nation", "releasable_to", "edge_id",
        "region_id"}


def test_build_record_is_deterministic():
    a = fe.build_record(ASSET, LABELS, TIER, _episode())
    b = fe.build_record(ASSET, dict(LABELS), dict(TIER), _episode())
    assert a == b


def test_empty_releasable_to_with_a_nation_loads():
    value, refusal = fe.build_record(
        ASSET, {"originator_nation": "ATL", "releasable_to": []}, TIER, _episode())
    assert refusal is None
    assert json.loads(value)["releasable_to"] == []
    assert json.loads(value)["originator_nation"] == "ATL"


def test_refusals():
    no_detected = {k: v for k, v in _episode().items() if k != "detected_at_ns"}
    cases = [
        (LABELS, _episode(detected_at_ns=0), "no_detected_at_ns"),
        (LABELS, no_detected, "no_detected_at_ns"),
        (LABELS, _episode(discrepancy_id=""), "no_discrepancy_id"),
        (None, _episode(), "labels_never_exported"),
        ({"originator_nation": "", "releasable_to": []}, _episode(), "unlabelled"),
    ]
    for labels, ep, reason in cases:
        value, refusal = fe.build_record(ASSET, labels, TIER, ep)
        assert value is None
        assert refusal == {"reason": reason, "asset_id": ASSET,
                           "discrepancy_id": ep.get("discrepancy_id", "")}


# -- the per-close emit -----------------------------------------------------

def _labelled_ctx(now_ns=None):
    ctx = StubCtx(key=ASSET, now_ns=now_ns or _now_ns())
    asyncio.run(observe(ctx, _silver_event_dict(asset_id=ASSET)))
    ctx._state["am_state"].update(
        originator_nation="ATL", releasable_to=["BDR"],
        edge_id="edge-a", region_id="region-a")
    return ctx


def _open(ctx, event_id="e1", fault_code="F-1234"):
    asyncio.run(apply_cm_event(ctx, _episode_cm_event(
        event_id=event_id, recorded_by="operator-a", source="operator_report",
        component="tr_module", fault_code=fault_code)))


def _close(ctx):
    asyncio.run(apply_cm_event(ctx, _resolve_cm_event()))


def _fe(published):
    return [p for p in published if p[0] == fe.TOPIC]


def test_close_publishes_one_record_matching_build_record(published):
    ctx = _labelled_ctx()
    _open(ctx)
    assert _fe(published) == []
    ctx._now_ns += 60 * 1_000_000_000
    _close(ctx)
    got = _fe(published)
    assert len(got) == 1
    st = ctx._state["am_state"]
    want, _ = fe.build_record(ASSET, LABELS, TIER, st["closed_episodes"][0])
    assert got[0] == (fe.TOPIC, ASSET, want)
    assert any(label == "publish-failure-episode" for label, _ in ctx.runs)


class JournalCtx(StubCtx):
    """Replays a recorded ctx.run result instead of running its fn again."""

    def __init__(self, *a, journal=None, **k):
        super().__init__(*a, **k)
        self.journal = journal if journal is not None else {}
        self._n: dict[str, int] = {}

    async def run(self, label, fn):
        n = self._n[label] = self._n.get(label, 0) + 1
        if (label, n) in self.journal:
            self.runs.append((label, self.journal[(label, n)]))
            return self.journal[(label, n)]
        result = await super().run(label, fn)
        self.journal[(label, n)] = result
        return result


def test_replay_of_the_same_close_publishes_no_second_record(published):
    ctx = _labelled_ctx()
    _open(ctx)
    before = copy.deepcopy(ctx._state)
    first = JournalCtx(ASSET, _now_ns() + 10**9)
    first._state = copy.deepcopy(before)
    asyncio.run(apply_cm_event(first, _resolve_cm_event()))
    assert len(_fe(published)) == 1
    # Retry: state as before the attempt, same journal.
    retry = JournalCtx(ASSET, _now_ns() + 10**9, journal=first.journal)
    retry._state = copy.deepcopy(before)
    asyncio.run(apply_cm_event(retry, _resolve_cm_event()))
    assert len(_fe(published)) == 1


def test_1001_closes_publish_1001_distinct_episodes(published):
    ctx = _labelled_ctx()
    for i in range(1001):
        ctx._now_ns += 60 * 1_000_000_000
        _open(ctx, event_id=f"e{i}")
        ctx._now_ns += 60 * 1_000_000_000
        _close(ctx)
    got = _fe(published)
    assert len(got) == 1001
    keys = {(json.loads(v)["episode_id"], json.loads(v)["detected_at_ns"])
            for _, _, v in got}
    assert len(keys) == 1001
    assert {k for _, k, _ in got} == {ASSET}


def test_refused_close_publishes_nothing_and_still_closes(published, caplog):
    ctx = StubCtx(key=ASSET, now_ns=_now_ns())     # no labels declared
    asyncio.run(observe(ctx, _silver_event_dict(asset_id=ASSET)))
    _open(ctx)
    ctx._now_ns += 60 * 1_000_000_000
    with caplog.at_level("WARNING"):
        _close(ctx)
    assert _fe(published) == []
    assert len(ctx._state["am_state"]["closed_episodes"]) == 1
    assert f"failure_episode_refused reason=unlabelled asset_id={ASSET}" in caplog.text
