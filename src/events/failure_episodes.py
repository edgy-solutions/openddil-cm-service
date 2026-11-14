"""The `failure-episodes` wire record: one closed manual-discrepancy episode.

Shared by the per-close emit (events/asset_cm.py) and the backfill
(backfill_failure_episodes.py), so both write the same bytes for the same
episode. The projector copies the shape byte-for-byte; the golden file
src/tests/golden/failure_episode_record.json is the contract.

A record is never written without a declared label (ADR-0029 section 7,
ADR-0055 section 1a: a default is not a declaration). A refusal is returned,
never dropped silently.
"""
from __future__ import annotations

import json

TOPIC = "failure-episodes"
WITHHELD_TOPIC = "failure-episodes-withheld"

REFUSAL_NO_DISCREPANCY_ID = "no_discrepancy_id"
REFUSAL_NO_DETECTED_AT_NS = "no_detected_at_ns"
REFUSAL_LABELS_NEVER_EXPORTED = "labels_never_exported"
REFUSAL_UNLABELLED = "unlabelled"


def build_record(
    asset_id: str,
    labels: dict | None,
    tier: dict | None,
    episode: dict,
) -> tuple[bytes | None, dict | None]:
    """Return (bytes, None) or (None, {"reason", "asset_id", "discrepancy_id"}).

    labels: {"originator_nation", "releasable_to"} or None (never exported).
    tier: {"edge_id", "region_id"}.
    episode: a ClosedEpisodeRecord as a dict (the stored form).
    """
    ep = episode or {}
    tier = tier or {}
    did = ep.get("discrepancy_id") or ""

    def refuse(reason: str) -> tuple[None, dict]:
        return None, {"reason": reason, "asset_id": asset_id, "discrepancy_id": did}

    if not did:
        return refuse(REFUSAL_NO_DISCREPANCY_ID)
    detected = ep.get("detected_at_ns") or 0
    if not detected:
        return refuse(REFUSAL_NO_DETECTED_AT_NS)
    if labels is None:
        return refuse(REFUSAL_LABELS_NEVER_EXPORTED)
    nation = labels.get("originator_nation") or ""
    if not nation:
        return refuse(REFUSAL_UNLABELLED)

    obj = {
        "episode_id": did,
        "detected_at_ns": int(detected),
        "asset_id": asset_id,
        "component": ep.get("component", ""),
        "fault_code": ep.get("fault_code", ""),
        "cleared_at_ns": int(ep.get("cleared_at_ns", 0) or 0),
        "cleared_by": ep.get("cleared_by", ""),
        "resolution": ep.get("resolution", ""),
        "sources": list(ep.get("sources") or []),
        "baseline_id": ep.get("baseline_id", ""),
        "mod_compliance": list(ep.get("mod_compliance") or []),
        "originator_nation": nation,
        "releasable_to": list(labels.get("releasable_to") or []),
        "edge_id": tier.get("edge_id", "") or "",
        "region_id": tier.get("region_id", "") or "",
    }
    return json.dumps(obj, sort_keys=True).encode("utf-8"), None


def build_withheld(
    asset_id: str,
    labels: dict | None,
    tier: dict | None,
    episode: dict,
    refusal: dict,
    stage: str,
) -> bytes:
    """A refused episode, kept with its reason (ADR-0055: never dropped).

    stage is "emit" or "backfill". Never refuses: everything is kept.
    """
    ep = episode or {}
    tier = tier or {}
    obj = {
        "stage": stage,
        "reason": refusal["reason"],
        "asset_id": asset_id,
        "discrepancy_id": refusal.get("discrepancy_id") or "",
        "detected_at_ns": int(ep.get("detected_at_ns") or 0),
        "labels": labels,
        "tier": {"edge_id": tier.get("edge_id", "") or "",
                 "region_id": tier.get("region_id", "") or ""},
        "episode": episode,
    }
    return json.dumps(obj, sort_keys=True).encode("utf-8")
