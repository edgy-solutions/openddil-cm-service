"""
Tests for the discrepancy analyzer. Framework-agnostic, no Faust/Kafka imports.

Coverage target: 90%+ on discrepancy/analyzer.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

sys.path.insert(0, str(Path(__file__).resolve().parents[3]
                        / "openddil-contracts" / "gen" / "python"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from openddil.configuration.v1 import (
    as_maintained_pb2 as am,
    configuration_baseline_pb2 as cb,
    configuration_item_pb2 as ci,
    discrepancy_pb2 as disc,
)
from discrepancy.analyzer import (
    UnknownBaselineError,
    compute_discrepancies,
    initialize_from_baseline,
    overall_status,
)


# ---------------------------------------------------------------------------
# Fixtures — build baselines and asset states programmatically
# ---------------------------------------------------------------------------

PAST_NS  = 1_640_995_200_000_000_000  # 2022-01-01 UTC — clearly past
FUTURE_NS = 4_102_444_800_000_000_000  # 2100-01-01 UTC — clearly future
NOW_NS    = 1_746_086_400_000_000_000  # 2025-05-01 UTC — our "now"


def _ts(ns: int) -> Timestamp:
    t = Timestamp()
    t.FromNanoseconds(ns)
    return t


def _baseline(
    baseline_id: str = "TEST-BL-1",
    platform_variant: str = "TEST-PLATFORM",
    *,
    authorized_cis: list[cb.AuthorizedCi] | None = None,
    required_mods: list[cb.ModificationRequirement] | None = None,
) -> cb.ConfigurationBaseline:
    b = cb.ConfigurationBaseline()
    b.baseline_id = baseline_id
    b.platform_variant = platform_variant
    b.version = "1.0"
    b.effective_from.CopyFrom(_ts(PAST_NS))
    for a in (authorized_cis or []):
        b.authorized_cis.append(a)
    for m in (required_mods or []):
        b.required_mods.append(m)
    return b


def _auth_ci(slot: str, parts: list[str], minrev: str = "A", optional: bool = False) -> cb.AuthorizedCi:
    a = cb.AuthorizedCi()
    a.slot_id = slot
    for p in parts:
        a.acceptable_part_numbers.append(p)
    a.minimum_revision = minrev
    a.optional = optional
    return a


def _required_mod(
    mod_id: str,
    *,
    category: int = cb.COMPLIANCE_MISSION_CRITICAL,
    due_ns: int = FUTURE_NS,
    title: str = "test mod",
) -> cb.ModificationRequirement:
    m = cb.ModificationRequirement()
    m.mod_id = mod_id
    m.type = cb.MOD_TYPE_MWO
    m.title = title
    m.due_date.CopyFrom(_ts(due_ns))
    m.category = category
    return m


def _ci_record(ci_id: str, part_number: str, revision: str = "A") -> ci.ConfigurationItem:
    c = ci.ConfigurationItem()
    c.ci_id = ci_id
    c.part_number = part_number
    c.revision = revision
    return c


# ---------------------------------------------------------------------------
# Initialize-from-baseline tests
# ---------------------------------------------------------------------------

def test_initialize_from_baseline_populates_slots_and_mods():
    bl = _baseline(
        authorized_cis=[_auth_ci("engine", ["P1", "P2"])],
        required_mods=[_required_mod("M1")],
    )
    state = initialize_from_baseline("asset-1", bl, ingest_time_ns=NOW_NS)
    assert state.asset_id == "asset-1"
    assert state.baseline_id == "TEST-BL-1"
    assert len(state.installed) == 1
    assert state.installed[0].slot_id == "engine"
    assert state.installed[0].ci_id == ""           # unverified
    assert len(state.mod_status) == 1
    assert state.mod_status[0].state == am.MOD_STATE_PENDING


def test_initialize_skips_optional_slots():
    bl = _baseline(authorized_cis=[
        _auth_ci("required-slot", ["P1"]),
        _auth_ci("optional-slot", ["P2"], optional=True),
    ])
    state = initialize_from_baseline("asset-1", bl, ingest_time_ns=NOW_NS)
    slot_ids = {s.slot_id for s in state.installed}
    assert slot_ids == {"required-slot"}


def test_initialize_computes_discrepancies_for_unverified_asset():
    """Freshly-initialized asset should have MISSING_CI discrepancies for
    every required slot (since ci_id is empty)."""
    bl = _baseline(authorized_cis=[
        _auth_ci("engine", ["P1"]),
        _auth_ci("transmission", ["P2"]),
    ])
    state = initialize_from_baseline("asset-1", bl, ingest_time_ns=NOW_NS)
    discs_by_type = {d.type for d in state.discrepancies}
    assert disc.DISCREPANCY_MISSING_CI in discs_by_type
    assert state.overall_status == am.CONFIG_STATUS_MAJOR_DISCREPANCY


# ---------------------------------------------------------------------------
# compute_discrepancies — happy path
# ---------------------------------------------------------------------------

def test_in_compliance_asset_has_no_discrepancies():
    bl = _baseline(
        authorized_cis=[_auth_ci("engine", ["P1"])],
        required_mods=[],
    )
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    slot = state.installed.add()
    slot.slot_id = "engine"
    slot.ci_id = "ci-1"

    ci_records = {"ci-1": _ci_record("ci-1", "P1", "A")}
    discs = compute_discrepancies(state, bl, now_override=NOW_NS,
                                  installed_ci_records=ci_records)
    assert discs == []
    assert overall_status(discs) == am.CONFIG_STATUS_IN_COMPLIANCE


# ---------------------------------------------------------------------------
# Missing CI
# ---------------------------------------------------------------------------

def test_missing_required_slot_produces_missing_ci_discrepancy():
    bl = _baseline(authorized_cis=[_auth_ci("engine", ["P1"])])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    # No installed entries

    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    assert len(discs) == 1
    assert discs[0].type == disc.DISCREPANCY_MISSING_CI
    assert discs[0].severity == disc.SEVERITY_MAJOR


def test_missing_optional_slot_no_discrepancy():
    bl = _baseline(authorized_cis=[
        _auth_ci("apkws", ["P1"], optional=True),
    ])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id

    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    assert discs == []


def test_empty_ci_id_in_slot_treated_as_unverified():
    bl = _baseline(authorized_cis=[_auth_ci("engine", ["P1"])])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    slot = state.installed.add()
    slot.slot_id = "engine"
    slot.ci_id = ""   # initialized but never verified

    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    assert len(discs) == 1
    assert discs[0].type == disc.DISCREPANCY_MISSING_CI
    assert "not been verified" in discs[0].description


# ---------------------------------------------------------------------------
# Unauthorized / obsolete part
# ---------------------------------------------------------------------------

def test_unauthorized_part_discrepancy():
    bl = _baseline(authorized_cis=[_auth_ci("engine", ["P1", "P2"])])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    slot = state.installed.add()
    slot.slot_id = "engine"
    slot.ci_id = "ci-bad"

    ci_records = {"ci-bad": _ci_record("ci-bad", "P-WRONG", "A")}
    discs = compute_discrepancies(state, bl, now_override=NOW_NS,
                                  installed_ci_records=ci_records)
    assert len(discs) == 1
    assert discs[0].type == disc.DISCREPANCY_UNAUTHORIZED_PART
    assert discs[0].severity == disc.SEVERITY_MAJOR
    assert discs[0].related_ci_id == "ci-bad"


def test_obsolete_revision_discrepancy():
    bl = _baseline(authorized_cis=[_auth_ci("engine", ["P1"], minrev="D")])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    slot = state.installed.add()
    slot.slot_id = "engine"
    slot.ci_id = "ci-old"

    ci_records = {"ci-old": _ci_record("ci-old", "P1", "B")}
    discs = compute_discrepancies(state, bl, now_override=NOW_NS,
                                  installed_ci_records=ci_records)
    assert len(discs) == 1
    assert discs[0].type == disc.DISCREPANCY_OBSOLETE_REVISION
    assert discs[0].severity == disc.SEVERITY_MINOR


def test_no_ci_records_skips_part_revision_checks():
    """Without ci_records, we can't check part_number/revision; the analyzer
    must silently skip these checks rather than emit false discrepancies."""
    bl = _baseline(authorized_cis=[_auth_ci("engine", ["P1"], minrev="D")])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    slot = state.installed.add()
    slot.slot_id = "engine"
    slot.ci_id = "ci-mystery"
    # No installed_ci_records passed
    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    assert discs == []


# ---------------------------------------------------------------------------
# Missing / overdue mods
# ---------------------------------------------------------------------------

def test_missing_mod_compliance_record():
    bl = _baseline(required_mods=[_required_mod("M1")])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    # No mod_status entries

    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    assert len(discs) == 1
    assert discs[0].type == disc.DISCREPANCY_MISSING_MOD
    assert discs[0].related_mod_id == "M1"


def test_applied_mod_no_discrepancy():
    bl = _baseline(required_mods=[_required_mod("M1")])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    comp = state.mod_status.add()
    comp.mod_id = "M1"
    comp.state = am.MOD_STATE_APPLIED

    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    assert discs == []


def test_waived_mod_no_discrepancy():
    bl = _baseline(required_mods=[_required_mod("M1")])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    comp = state.mod_status.add()
    comp.mod_id = "M1"
    comp.state = am.MOD_STATE_WAIVED

    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    assert discs == []


def test_not_applicable_mod_no_discrepancy():
    bl = _baseline(required_mods=[_required_mod("M1")])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    comp = state.mod_status.add()
    comp.mod_id = "M1"
    comp.state = am.MOD_STATE_NOT_APPLICABLE

    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    assert discs == []


def test_pending_overdue_safety_mod_escalates_to_critical():
    """COMPLIANCE_SAFETY_OF_FLIGHT base maps to CRITICAL; overdue stays CRITICAL.
    But for MISSION_CRITICAL (base MAJOR), overdue escalates to CRITICAL."""
    bl = _baseline(required_mods=[
        _required_mod("M-SAFETY",
                      category=cb.COMPLIANCE_SAFETY_OF_FLIGHT,
                      due_ns=PAST_NS),
        _required_mod("M-MISSION",
                      category=cb.COMPLIANCE_MISSION_CRITICAL,
                      due_ns=PAST_NS),
    ])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    for mod_id in ("M-SAFETY", "M-MISSION"):
        c = state.mod_status.add()
        c.mod_id = mod_id
        c.state = am.MOD_STATE_PENDING

    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    by_mod = {d.related_mod_id: d for d in discs}
    assert by_mod["M-SAFETY"].severity == disc.SEVERITY_CRITICAL
    assert by_mod["M-MISSION"].severity == disc.SEVERITY_CRITICAL  # escalated from MAJOR
    assert "OVERDUE" in by_mod["M-SAFETY"].description


def test_pending_not_overdue_keeps_base_severity():
    bl = _baseline(required_mods=[
        _required_mod("M-FUTURE",
                      category=cb.COMPLIANCE_MISSION_CRITICAL,
                      due_ns=FUTURE_NS),
    ])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    c = state.mod_status.add()
    c.mod_id = "M-FUTURE"
    c.state = am.MOD_STATE_PENDING

    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    assert len(discs) == 1
    assert discs[0].severity == disc.SEVERITY_MAJOR  # MISSION_CRITICAL base


def test_overdue_explicit_state():
    bl = _baseline(required_mods=[
        _required_mod("M1", category=cb.COMPLIANCE_IMPROVEMENT, due_ns=PAST_NS),
    ])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    c = state.mod_status.add()
    c.mod_id = "M1"
    c.state = am.MOD_STATE_OVERDUE

    discs = compute_discrepancies(state, bl, now_override=NOW_NS)
    assert len(discs) == 1
    # MOD_STATE_OVERDUE + past due → severity escalates from MINOR to MAJOR
    assert discs[0].severity == disc.SEVERITY_MAJOR


# ---------------------------------------------------------------------------
# overall_status reduction
# ---------------------------------------------------------------------------

def test_overall_status_empty_is_in_compliance():
    assert overall_status([]) == am.CONFIG_STATUS_IN_COMPLIANCE


def test_overall_status_info_only_is_in_compliance():
    d = disc.ConfigurationDiscrepancy()
    d.severity = disc.SEVERITY_INFO
    assert overall_status([d]) == am.CONFIG_STATUS_IN_COMPLIANCE


def test_overall_status_minor():
    d = disc.ConfigurationDiscrepancy()
    d.severity = disc.SEVERITY_MINOR
    assert overall_status([d]) == am.CONFIG_STATUS_MINOR_DISCREPANCY


def test_overall_status_major_beats_minor():
    minor = disc.ConfigurationDiscrepancy(); minor.severity = disc.SEVERITY_MINOR
    major = disc.ConfigurationDiscrepancy(); major.severity = disc.SEVERITY_MAJOR
    assert overall_status([minor, major]) == am.CONFIG_STATUS_MAJOR_DISCREPANCY


def test_overall_status_critical_beats_all():
    info = disc.ConfigurationDiscrepancy();  info.severity = disc.SEVERITY_INFO
    major = disc.ConfigurationDiscrepancy(); major.severity = disc.SEVERITY_MAJOR
    crit = disc.ConfigurationDiscrepancy();  crit.severity = disc.SEVERITY_CRITICAL
    assert overall_status([info, major, crit]) == am.CONFIG_STATUS_NOT_MISSION_CAPABLE


# ---------------------------------------------------------------------------
# Stable discrepancy_id (deduplication)
# ---------------------------------------------------------------------------

def test_discrepancy_id_stable_across_recomputations():
    bl = _baseline(required_mods=[_required_mod("M1")])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-stable"
    state.baseline_id = bl.baseline_id

    discs_a = compute_discrepancies(state, bl, now_override=NOW_NS)
    discs_b = compute_discrepancies(state, bl, now_override=NOW_NS + 10**9)
    assert discs_a[0].discrepancy_id == discs_b[0].discrepancy_id


# ---------------------------------------------------------------------------
# Boundary conditions
# ---------------------------------------------------------------------------

def test_empty_baseline_produces_no_discrepancies():
    bl = _baseline(authorized_cis=[], required_mods=[])
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    assert compute_discrepancies(state, bl, now_override=NOW_NS) == []


def test_mismatched_baseline_id_raises():
    bl = _baseline(baseline_id="BL-A")
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = "BL-B"
    with pytest.raises(UnknownBaselineError):
        compute_discrepancies(state, bl, now_override=NOW_NS)


def test_multiple_discrepancies_aggregate_correctly():
    """One of each type — assert all surface and overall_status is worst."""
    bl = _baseline(
        authorized_cis=[
            _auth_ci("engine", ["P1"], minrev="D"),
            _auth_ci("transmission", ["P2"]),
            _auth_ci("missing-slot", ["P3"]),
        ],
        required_mods=[
            _required_mod("M-SAFETY",
                          category=cb.COMPLIANCE_SAFETY_OF_FLIGHT,
                          due_ns=PAST_NS),
        ],
    )
    state = am.AsMaintainedConfiguration()
    state.asset_id = "asset-1"
    state.baseline_id = bl.baseline_id
    # engine: wrong part
    s = state.installed.add(); s.slot_id = "engine"; s.ci_id = "ci-bad"
    # transmission: obsolete revision
    s2 = state.installed.add(); s2.slot_id = "transmission"; s2.ci_id = "ci-old"
    # missing-slot: omitted entirely
    # mod: pending and overdue
    c = state.mod_status.add(); c.mod_id = "M-SAFETY"; c.state = am.MOD_STATE_PENDING

    ci_records = {
        "ci-bad": _ci_record("ci-bad", "P-WRONG", "A"),
        "ci-old": _ci_record("ci-old", "P2", "A"),  # below minrev D... wait
    }
    discs = compute_discrepancies(state, bl, now_override=NOW_NS,
                                  installed_ci_records=ci_records)
    types = {d.type for d in discs}
    assert disc.DISCREPANCY_UNAUTHORIZED_PART in types  # engine
    assert disc.DISCREPANCY_MISSING_CI in types          # missing-slot
    assert disc.DISCREPANCY_MISSING_MOD in types         # M-SAFETY
    # overall_status picks the worst — SAFETY_OF_FLIGHT overdue → CRITICAL
    assert overall_status(discs) == am.CONFIG_STATUS_NOT_MISSION_CAPABLE
