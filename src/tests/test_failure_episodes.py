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


# -- the backfill -----------------------------------------------------------

import backfill_failure_episodes as bf  # noqa: E402


def _state_row(asset, n, nation="ATL", labels=True, start=0):
    st = {"asset_id": asset, "edge_id": "edge-a", "region_id": "region-a",
          "closed_episodes": [
              _episode(discrepancy_id=f"ep-{asset}-{i}",
                       detected_at_ns=1_700_000_000_000_000_000 + (start + i) * 10**9)
              for i in range(n)]}
    if labels:
        st["originator_nation"] = nation
        st["releasable_to"] = []
    return {"service_key": asset, "value_utf8": json.dumps(st)}


def _fetch(rows):
    return lambda admin: rows


def _run(admins=(), files=(), fetch=None, **kw):
    out: list = []
    summary = bf.run(list(admins), list(files), fetch=fetch or _fetch([]),
                     publish=lambda t, k, v: out.append((t, k, v)), **kw)
    return summary, out


def _file_line(asset, i, **over):
    obj = {"admin": "a", "asset": asset,
           "asset_labels": {"originator_nation": "ATL", "releasable_to": ["BDR"]},
           "asset_tier": TIER, "asset_as_of_ns": 1,
           "episode": _episode(discrepancy_id=f"ep-f-{i}",
                               detected_at_ns=1_700_000_000_000_000_000 + i)}
    obj.update(over)
    return json.dumps(obj)


def test_backfill_live_state_emits_n_and_is_repeatable():
    rows = [_state_row("asset-t-0001", 3), _state_row("asset-t-0002", 2)]
    s1, out1 = _run(["http://admin-a:9070"], fetch=_fetch(rows))
    s2, out2 = _run(["http://admin-a:9070"], fetch=_fetch(rows))
    assert s1["emitted"] == 5 and s1["duplicate"] == 0 and s1["refusals"] == []
    assert out1 == out2 and len(out1) == 5
    assert {t for t, _, _ in out1} == {"failure-episodes"}
    assert {k for _, k, _ in out1} == {"asset-t-0001", "asset-t-0002"}
    rec = json.loads(out1[0][2])
    assert rec["releasable_to"] == [] and rec["originator_nation"] == "ATL"
    assert rec["edge_id"] == "edge-a"


def test_backfill_state_and_file_overlap_emits_once(tmp_path):
    rows = [_state_row("asset-t-0001", 2)]
    f = tmp_path / "ep.jsonl"
    ep = rows and json.loads(rows[0]["value_utf8"])["closed_episodes"][0]
    f.write_text(_file_line("asset-t-0001", 0, episode=ep) + "\n")
    s, out = _run(["http://admin-a:9070"], [str(f)], fetch=_fetch(rows))
    assert s["emitted"] == 2 and s["duplicate"] == 1 and len(out) == 2


def test_backfill_same_file_twice_emits_once_per_line(tmp_path):
    f = tmp_path / "ep.jsonl"
    f.write_text("\n".join(_file_line("asset-t-0001", i) for i in range(3)) + "\n")
    s, out = _run(files=[str(f), str(f)])
    assert s["emitted"] == 3 and s["duplicate"] == 3 and len(out) == 3


def test_backfill_export_file_k_lines(tmp_path):
    f = tmp_path / "ep.jsonl"
    lines = [_file_line("asset-t-0001", i) for i in range(7)]
    lines.insert(3, json.dumps({"summary": {"lines": 7}}))
    f.write_text("\n".join(lines) + "\n")
    s, out = _run(files=[str(f)])
    assert s["emitted"] == 7 and len(out) == 7
    assert s["skipped_summary_lines"] == 1 and s["refused"] == {}


def test_backfill_empty_releasable_to_loads(tmp_path):
    f = tmp_path / "ep.jsonl"
    f.write_text(_file_line("asset-t-0001", 0, asset_labels={
        "originator_nation": "ATL", "releasable_to": []}) + "\n")
    s, out = _run(files=[str(f)])
    assert s["emitted"] == 1
    assert json.loads(out[0][2])["releasable_to"] == []


def test_backfill_red_lines_are_refused_counted_named(tmp_path):
    f = tmp_path / "ep.jsonl"
    no_detected = json.loads(_file_line("asset-t-0001", 1))
    del no_detected["episode"]["detected_at_ns"]
    no_labels = json.loads(_file_line("asset-t-0001", 2))
    del no_labels["asset_labels"]
    f.write_text("\n".join([
        json.dumps(no_detected), json.dumps(no_labels),
        _file_line("asset-t-0001", 3, asset_labels={
            "originator_nation": "", "releasable_to": []}),
        "{not json",
        _file_line("asset-t-0001", 4),
    ]) + "\n")
    s, out = _run(files=[str(f)])
    assert s["emitted"] == 1 and len([o for o in out if o[0] == fe.TOPIC]) == 1
    assert s["refused"] == {"no_detected_at_ns": 1, "labels_never_exported": 1,
                            "unlabelled": 1, "unparseable": 1}
    assert [r["source"] for r in s["refusals"]] == [f"{f}:{n}" for n in (1, 2, 3, 4)]
    assert s["refusals"][0]["asset_id"] == "asset-t-0001"
    assert s["refusals"][0]["discrepancy_id"] == "ep-f-1"


def test_backfill_state_row_without_label_key_or_nation_is_refused():
    rows = [_state_row("asset-t-0001", 1, labels=False),
            _state_row("asset-t-0002", 1, nation="")]
    s, out = _run(["http://user:secret@admin-a:9070/x"], fetch=_fetch(rows))
    assert [o for o in out if o[0] == fe.TOPIC] == [] and s["refused"] == {"labels_never_exported": 1, "unlabelled": 1}
    assert s["refusals"][0]["source"] == "state:http://admin-a:9070"
    assert "secret" not in json.dumps(s)


def test_backfill_dry_run_publishes_nothing():
    s, out = _run(["http://admin-a:9070"], fetch=_fetch([_state_row("asset-t-0001", 2)]),
                  dry_run=True)
    assert s["emitted"] == 2 and out == []


# -- the withheld record (S6) -----------------------------------------------

WITHHELD_GOLDEN = Path(__file__).parent / "golden" / "failure_episode_withheld.json"


def _withheld(published):
    return [p for p in published if p[0] == fe.WITHHELD_TOPIC]


def _counter(stage, reason):
    from metrics import cm_failure_episode_withheld_total as c
    return c.labels(stage=stage, reason=reason)._value.get()


def _unlabelled_close():
    ctx = StubCtx(key=ASSET, now_ns=_now_ns())
    asyncio.run(observe(ctx, _silver_event_dict(asset_id=ASSET)))
    _open(ctx)
    ctx._now_ns += 60 * 1_000_000_000
    return ctx


def test_unlabelled_close_publishes_one_withheld_record(published):
    before = _counter("emit", "unlabelled")
    ctx = _unlabelled_close()
    _close(ctx)
    assert _fe(published) == []
    w = _withheld(published)
    assert len(w) == 1 and w[0][1] == ASSET
    obj = json.loads(w[0][2])
    assert obj["stage"] == "emit" and obj["reason"] == "unlabelled"
    stored = ctx._state["am_state"]["closed_episodes"][0]
    assert json.dumps(obj["episode"], sort_keys=True) == json.dumps(stored, sort_keys=True)
    assert _counter("emit", "unlabelled") == before + 1


def test_withheld_record_is_not_republished_on_replay(published):
    ctx = _unlabelled_close()
    before = copy.deepcopy(ctx._state)
    first = JournalCtx(ASSET, _now_ns() + 10**9)
    first._state = copy.deepcopy(before)
    asyncio.run(apply_cm_event(first, _resolve_cm_event()))
    assert len(_withheld(published)) == 1
    retry = JournalCtx(ASSET, _now_ns() + 10**9, journal=first.journal)
    retry._state = copy.deepcopy(before)
    asyncio.run(apply_cm_event(retry, _resolve_cm_event()))
    assert len(_withheld(published)) == 1


def test_golden_withheld_record():
    refusal = {"reason": "unlabelled", "asset_id": ASSET, "discrepancy_id": "ep-t-0001"}
    value = fe.build_withheld(
        ASSET, {"originator_nation": "", "releasable_to": []}, TIER,
        _episode(), refusal, "emit")
    if os.environ.get("REGEN_GOLDEN") == "1":
        WITHHELD_GOLDEN.write_bytes(value)
    assert value == WITHHELD_GOLDEN.read_bytes()


def test_build_withheld_defaults_and_never_refuses():
    v = fe.build_withheld("a", None, None, {}, {"reason": "unparseable"}, "backfill")
    o = json.loads(v)
    assert o["labels"] is None and o["discrepancy_id"] == "" and o["detected_at_ns"] == 0
    assert o["tier"] == {"edge_id": "", "region_id": ""}


def _wh(out):
    return [o for o in out if o[0] == fe.WITHHELD_TOPIC]


def test_backfill_publishes_refusals_to_withheld(tmp_path):
    f = tmp_path / "ep.jsonl"
    unlabelled = json.loads(_file_line("asset-t-0001", 2))
    unlabelled["asset_labels"] = {"originator_nation": "", "releasable_to": []}
    f.write_text("\n".join([
        _file_line("asset-t-0001", 1), json.dumps(unlabelled), "{not json",
    ]) + "\n")
    b_unl = _counter("backfill", "unlabelled")
    b_unp = _counter("backfill", "unparseable")
    s, out = _run(files=[str(f)])
    assert s["emitted"] == 1 and len([o for o in out if o[0] == fe.TOPIC]) == 1
    assert s["withheld_published"] == 2 and len(_wh(out)) == 2
    objs = [json.loads(o[2]) for o in _wh(out)]
    assert [o["reason"] for o in objs] == ["unlabelled", "unparseable"]
    assert all(o["stage"] == "backfill" for o in objs)
    assert _wh(out)[0][1] == "asset-t-0001" and _wh(out)[1][1] == ""
    assert objs[0]["episode"] == unlabelled["episode"]
    assert objs[1]["episode"]["raw"].strip() == chr(123) + "not json"
    assert objs[1]["discrepancy_id"] == ""
    assert _counter("backfill", "unlabelled") == b_unl + 1
    assert _counter("backfill", "unparseable") == b_unp + 1


def test_backfill_dry_run_publishes_no_withheld(tmp_path):
    f = tmp_path / "ep.jsonl"
    f.write_text("{not json\n")
    s, out = _run(files=[str(f)], dry_run=True)
    assert out == [] and s["withheld_published"] == 0 and s["refused"] == {"unparseable": 1}


def test_backfill_withholds_an_unparseable_state_row():
    good = _state_row("asset-t-0001", 1)
    bad = {"service_key": "asset-t-0002", "value_utf8": chr(123) + "not json"}
    b = _counter("backfill", "unparseable")
    s, out = _run(["http://admin-a:9070"], fetch=_fetch([good, bad]))
    assert s["emitted"] == 1 and len([o for o in out if o[0] == fe.TOPIC]) == 1
    assert s["withheld_published"] == 1 and len(_wh(out)) == 1
    assert _wh(out)[0][1] == "asset-t-0002"
    obj = json.loads(_wh(out)[0][2])
    assert obj["stage"] == "backfill" and obj["reason"] == "unparseable"
    assert obj["asset_id"] == "asset-t-0002"
    assert obj["episode"] == {"raw": chr(123) + "not json"}
    assert _counter("backfill", "unparseable") == b + 1


def test_backfill_dry_run_publishes_no_withheld_state_row():
    bad = {"service_key": "asset-t-0002", "value_utf8": chr(123) + "not json"}
    s, out = _run(["http://admin-a:9070"], fetch=_fetch([_state_row("asset-t-0001", 1), bad]),
                  dry_run=True)
    assert out == [] and s["withheld_published"] == 0
    assert s["refused"] == {"unparseable": 1}


# -- the tier rule (contracts #64 section 3.2) -------------------------------

@pytest.mark.parametrize("tier", [
    None, {}, {"edge_id": "", "region_id": ""}, {"edge_id": None, "region_id": None},
    {"edge_id": 7, "region_id": ["r"]}, "edge-a"])
def test_build_record_refuses_no_tier(tier):
    value, refusal = fe.build_record(ASSET, LABELS, tier, _episode())
    assert value is None
    assert refusal == {"reason": "no_tier", "asset_id": ASSET,
                       "discrepancy_id": "ep-t-0001"}


def test_build_record_accepts_edge_only_and_region_only():
    v, r = fe.build_record(ASSET, LABELS, {"edge_id": "edge-a"}, _episode())
    assert r is None and json.loads(v)["region_id"] == ""
    v, r = fe.build_record(ASSET, LABELS, {"region_id": "region-a"}, _episode())
    assert r is None and json.loads(v)["edge_id"] == ""


def test_no_tier_is_checked_after_the_other_refusals():
    _, r = fe.build_record(ASSET, None, {}, _episode())
    assert r["reason"] == "labels_never_exported"


def test_close_with_no_tier_withholds_and_publishes_no_record(published):
    before = _counter("emit", "no_tier")
    ctx = _labelled_ctx()
    ctx._state["am_state"].update(edge_id="", region_id="")
    _open(ctx)
    ctx._now_ns += 60 * 1_000_000_000
    _close(ctx)
    assert _fe(published) == []
    w = _withheld(published)
    assert len(w) == 1
    obj = json.loads(w[0][2])
    assert obj["stage"] == "emit" and obj["reason"] == "no_tier"
    assert _counter("emit", "no_tier") == before + 1


def _tfile(tmp_path, lines):
    f = tmp_path / "t.jsonl"
    f.write_text("\n".join(lines) + "\n")
    return str(f)


def _notier_ep(i=0, **over):
    return _episode(discrepancy_id=f"ep-f-{i}",
                    detected_at_ns=1_700_000_000_000_000_000 + i, **over)


def test_backfill_file_takes_tier_from_asset_record(tmp_path):
    f = _tfile(tmp_path, [_file_line(
        "asset-t-0001", 0, episode=_notier_ep(),
        asset_tier={"edge_id": "edge-z", "region_id": "region-z"})])
    s, out = _run(files=[f])
    assert s["emitted"] == 1 and len(out) == 1
    rec = json.loads(out[0][2])
    assert (rec["edge_id"], rec["region_id"]) == ("edge-z", "region-z")
    assert s["tier_source"] == {"close_record": 0, "asset_record": 1}


def test_backfill_file_close_record_tier_wins(tmp_path):
    f = _tfile(tmp_path, [_file_line(
        "asset-t-0001", 0, episode=_notier_ep(edge_id="edge-e", region_id="region-e"),
        asset_tier={"edge_id": "edge-z", "region_id": "region-z"})])
    s, out = _run(files=[f])
    rec = json.loads(out[0][2])
    assert (rec["edge_id"], rec["region_id"]) == ("edge-e", "region-e")
    assert s["tier_source"] == {"close_record": 1, "asset_record": 0}


def test_backfill_tier_is_one_whole_block_never_mixed(tmp_path):
    f = _tfile(tmp_path, [_file_line(
        "asset-t-0001", 0, episode=_notier_ep(edge_id="edge-e"),
        asset_tier={"region_id": "region-z"})])
    s, out = _run(files=[f])
    rec = json.loads(out[0][2])
    assert (rec["edge_id"], rec["region_id"]) == ("edge-e", "")
    assert s["tier_source"]["close_record"] == 1


@pytest.mark.parametrize("asset_tier", [
    "ABSENT", {}, {"edge_id": "", "region_id": ""}, "edge-a"])
def test_backfill_file_no_tier_anywhere_is_refused(tmp_path, asset_tier):
    over = {} if asset_tier != "ABSENT" else {}
    line = json.loads(_file_line("asset-t-0001", 0, episode=_notier_ep(), **over))
    if asset_tier == "ABSENT":
        del line["asset_tier"]
    else:
        line["asset_tier"] = asset_tier
    f = _tfile(tmp_path, [json.dumps(line)])
    before = _counter("backfill", "no_tier")
    s, out = _run(files=[f])
    assert s["emitted"] == 0
    assert s["refused"] == {"no_tier": 1}
    assert [t for t, _, _ in out] == [fe.WITHHELD_TOPIC]
    assert s["withheld_published"] == 1
    assert _counter("backfill", "no_tier") == before + 1
    assert s["tier_source"] == {"close_record": 0, "asset_record": 0}


def test_backfill_state_row_without_ids_refused_with_ids_emits():
    row = json.loads(_state_row("asset-t-0001", 2)["value_utf8"])
    for e in row["closed_episodes"]:
        pass
    bare = dict(row, edge_id="", region_id="")
    s, out = _run(["http://a:9070"], fetch=_fetch(
        [{"service_key": "asset-t-0001", "value_utf8": json.dumps(bare)}]))
    assert s["emitted"] == 0 and s["refused"] == {"no_tier": 2}
    s, out = _run(["http://a:9070"], fetch=_fetch([_state_row("asset-t-0001", 2)]))
    assert s["emitted"] == 2
    assert s["tier_source"] == {"close_record": 0, "asset_record": 2}


def test_backfill_dry_run_counts_no_tier_and_publishes_nothing(tmp_path):
    line = json.loads(_file_line("asset-t-0001", 0, episode=_notier_ep()))
    line["asset_tier"] = {}
    f = _tfile(tmp_path, [json.dumps(line)])
    before = _counter("backfill", "no_tier")
    s, out = _run(files=[f], dry_run=True)
    assert s["refused"] == {"no_tier": 1} and out == []
    assert _counter("backfill", "no_tier") == before


# -- a first fault after a state wipe ---------------------------------------
#
# The upgrade-time Restate wipe deletes the AssetCM record; a fresh StubCtx
# is that wiped state. The record is rebuilt by the next telemetry event, and
# its labels come from that event's provenance only (ADR-0029 s3). The ingest
# stamps the provenance from the asset's declaration, which the wipe keeps.
# So the label after a wipe is whatever the feed states now: a declared asset
# is labelled again by its first event, and an asset the feed no longer labels
# stays unlabelled however it was labelled before.

def _feed_event(nation=None, releasable=()):
    ev = _silver_event_dict(asset_id=ASSET)
    ev["provenance"].update(edgeId="edge-a", regionId="region-a")
    if nation:
        ev["provenance"]["originatorNation"] = nation
        ev["provenance"]["releasableTo"] = list(releasable)
    return ev


def _life(ctx, event, fault_code="F-1234", event_id="e1"):
    """One telemetry event, then a fault filed and closed a minute later."""
    asyncio.run(observe(ctx, event))
    _open(ctx, event_id=event_id, fault_code=fault_code)
    ctx._now_ns += 60 * 1_000_000_000
    _close(ctx)


def test_declared_asset_first_fault_after_a_wipe_is_labelled(published):
    before = StubCtx(key=ASSET, now_ns=_now_ns())
    _life(before, _feed_event("ATL"))
    assert len(_fe(published)) == 1

    wiped = StubCtx(key=ASSET, now_ns=_now_ns() + 3600 * 10**9)
    assert wiped._state == {}
    _life(wiped, _feed_event("ATL"), event_id="e2")

    got = _fe(published)
    assert len(got) == 2 and _withheld(published) == []
    rec = json.loads(got[1][2])
    assert rec["originator_nation"] == "ATL" and rec["releasable_to"] == []
    st = wiped._state["am_state"]
    assert st["originator_nation"] == "ATL" and len(st["closed_episodes"]) == 1


def test_label_the_feed_no_longer_states_is_not_carried_across_a_wipe(published):
    """An asset labelled before the wipe whose feed now carries no nation
    (no declaration at its edge, no stamp, no file row) is unlabelled after
    it: its close is withheld as unlabelled, not published under the old
    label. A last-known label left in the tier store is not a source."""
    before = StubCtx(key=ASSET, now_ns=_now_ns())
    _life(before, _feed_event("ATL"))
    assert len(_fe(published)) == 1

    wiped = StubCtx(key=ASSET, now_ns=_now_ns() + 3600 * 10**9)
    _life(wiped, _feed_event(), event_id="e2")

    assert len(_fe(published)) == 1
    w = _withheld(published)
    assert len(w) == 1 and json.loads(w[0][2])["reason"] == "unlabelled"
    assert wiped._state["am_state"]["originator_nation"] == ""


def test_declaration_after_a_refusal_labels_the_next_fault(published):
    """Undeclared: the first close is withheld. Once the declaration reaches
    the feed, the next event labels the record and the next close publishes."""
    ctx = StubCtx(key=ASSET, now_ns=_now_ns())
    _life(ctx, _feed_event())
    assert _fe(published) == [] and len(_withheld(published)) == 1

    ctx._now_ns += 60 * 1_000_000_000
    _life(ctx, _feed_event("ATL"), event_id="e2")

    got = _fe(published)
    assert len(got) == 1 and len(_withheld(published)) == 1
    assert json.loads(got[0][2])["originator_nation"] == "ATL"


def test_cm_event_before_the_first_event_after_a_wipe_is_dropped(published, caplog):
    """The window between a wipe and the asset's first telemetry event: a CM
    event finds no record and is dropped with a warning, so no episode opens
    and nothing is published or withheld. The cm-events topic keeps it for
    replay."""
    wiped = StubCtx(key=ASSET, now_ns=_now_ns())
    with caplog.at_level("WARNING"):
        _open(wiped)
    assert wiped._state == {}
    assert published == []
    assert f"Dropping CM event for unknown asset {ASSET}" in caplog.text
