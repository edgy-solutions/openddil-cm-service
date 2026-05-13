"""
Discrepancy analyzer — pure Python, framework-agnostic.

Boundary discipline (ADR-0006 pattern):
  - This module imports ONLY from openddil.configuration.v1 protobuf bindings.
  - No faust, no kafka, no asyncio, no logging frameworks.
  - Inputs and outputs are protobuf messages or plain dataclasses.
  - Algorithm developers can test this module without standing up Kafka.

Public API:
  - compute_discrepancies(as_maintained, baseline) -> list[ConfigurationDiscrepancy]
  - overall_status(discrepancies) -> ConfigurationStatus
  - initialize_from_baseline(asset_id, baseline, ingest_time_ns) -> AsMaintainedConfiguration

Severity mapping (per ADR-0009 / Phase 3 task spec):
  COMPLIANCE_SAFETY_OF_FLIGHT -> SEVERITY_CRITICAL
  COMPLIANCE_MISSION_CRITICAL -> SEVERITY_MAJOR
  COMPLIANCE_IMPROVEMENT      -> SEVERITY_MINOR
  COMPLIANCE_RECORDKEEPING    -> SEVERITY_INFO

Status reduction:
  Any CRITICAL -> CONFIG_STATUS_NOT_MISSION_CAPABLE
  Any MAJOR    -> CONFIG_STATUS_MAJOR_DISCREPANCY
  Any MINOR    -> CONFIG_STATUS_MINOR_DISCREPANCY
  Only INFO or empty -> CONFIG_STATUS_IN_COMPLIANCE
"""
from __future__ import annotations

import datetime
import uuid
from typing import Iterable

from google.protobuf.timestamp_pb2 import Timestamp

from openddil.configuration.v1 import (
    as_maintained_pb2 as am,
    configuration_baseline_pb2 as cb,
    configuration_item_pb2 as ci,
    discrepancy_pb2 as disc,
)


class UnknownBaselineError(RuntimeError):
    """Raised when caller asks for discrepancies without a matching baseline."""


# ---------------------------------------------------------------------------
# Severity mapping
# ---------------------------------------------------------------------------

_COMPLIANCE_TO_SEVERITY: dict[int, int] = {
    cb.COMPLIANCE_SAFETY_OF_FLIGHT: disc.SEVERITY_CRITICAL,
    cb.COMPLIANCE_MISSION_CRITICAL: disc.SEVERITY_MAJOR,
    cb.COMPLIANCE_IMPROVEMENT:      disc.SEVERITY_MINOR,
    cb.COMPLIANCE_RECORDKEEPING:    disc.SEVERITY_INFO,
}

# CI categories that cause MAJOR (vs MINOR) when missing.
_MAJOR_MISSING_CATEGORIES: set[int] = {
    ci.CI_CATEGORY_PLATFORM,
    ci.CI_CATEGORY_MAJOR_ASSEMBLY,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def initialize_from_baseline(
    asset_id: str,
    baseline: cb.ConfigurationBaseline,
    ingest_time_ns: int | None = None,
) -> am.AsMaintainedConfiguration:
    """Build an initial AsMaintainedConfiguration from baseline defaults.

    The result represents an asset that has never been verified:
      - For each non-optional AuthorizedCi slot, an empty InstalledCi entry
        is recorded (slot present but ci_id="").
      - For each required mod, ModCompliance with state=MOD_STATE_PENDING.
      - Discrepancies are computed against the baseline, so an unverified
        asset begins with one DISCREPANCY_MISSING_CI per non-optional slot
        plus discrepancies for any overdue required mods.
    """
    state = am.AsMaintainedConfiguration()
    state.asset_id = asset_id
    state.baseline_id = baseline.baseline_id

    now = _ingest_timestamp(ingest_time_ns)
    state.as_of.CopyFrom(now)

    # Install slot entries — empty ci_id means "slot exists, contents unknown"
    for auth in baseline.authorized_cis:
        if auth.optional:
            continue
        slot = state.installed.add()
        slot.slot_id = auth.slot_id
        # ci_id intentionally left empty; installed_at left unset

    # Mod compliance entries — all PENDING until proven applied
    for mod in baseline.required_mods:
        comp = state.mod_status.add()
        comp.mod_id = mod.mod_id
        comp.state = am.MOD_STATE_PENDING

    # Compute discrepancies + overall status against the baseline
    discs = compute_discrepancies(state, baseline, now_override=now.ToNanoseconds())
    state.discrepancies.extend(discs)
    state.overall_status = overall_status(list(discs))

    return state


def compute_discrepancies(
    as_maintained: am.AsMaintainedConfiguration,
    baseline: cb.ConfigurationBaseline,
    *,
    now_override: int | None = None,
    installed_ci_records: dict[str, ci.ConfigurationItem] | None = None,
) -> list[disc.ConfigurationDiscrepancy]:
    """Compute the delta between as-maintained and authorized configuration.

    Pure function. Does not mutate inputs.

    Args:
      as_maintained: the actual current state
      baseline: the authorized reference configuration
      now_override: nanoseconds since epoch; used for time-based checks (overdue
        mods) so unit tests can pin the clock. Defaults to real now.
      installed_ci_records: optional dict mapping installed.ci_id -> the CI
        record (so we can check part_number / revision against authorized_cis).
        If omitted, UNAUTHORIZED_PART and OBSOLETE_REVISION checks are skipped
        (because we have no part-number data to compare).

    Returns:
      A list of ConfigurationDiscrepancy messages. Stable discrepancy_id is
      derived from a hash of (asset_id, type, related_ref) so the same
      discrepancy across recomputations has the same id.
    """
    if baseline.baseline_id and as_maintained.baseline_id \
            and baseline.baseline_id != as_maintained.baseline_id:
        # Caller passed mismatched baseline. Treat as programmer error.
        raise UnknownBaselineError(
            f"asset {as_maintained.asset_id} is measured against "
            f"{as_maintained.baseline_id!r} but caller passed baseline "
            f"{baseline.baseline_id!r}"
        )

    now_ns = now_override if now_override is not None else _now_ns()
    installed_by_slot = {i.slot_id: i for i in as_maintained.installed}
    mod_status_by_id = {m.mod_id: m for m in as_maintained.mod_status}
    ci_records = installed_ci_records or {}

    out: list[disc.ConfigurationDiscrepancy] = []

    # --- AuthorizedCi checks -------------------------------------------
    for auth in baseline.authorized_cis:
        installed = installed_by_slot.get(auth.slot_id)

        # Missing CI: slot has no entry OR slot entry has empty ci_id
        if installed is None and not auth.optional:
            out.append(_make_discrepancy(
                asset_id=as_maintained.asset_id,
                dtype=disc.DISCREPANCY_MISSING_CI,
                description=f"Slot {auth.slot_id!r} has no installed CI",
                severity=disc.SEVERITY_MAJOR,
                recommended_action=f"Install authorized part in slot {auth.slot_id}",
                related_ci_id="",
                detected_at_ns=now_ns,
            ))
            continue

        if installed is None and auth.optional:
            continue  # optional slot, absence is fine

        # Slot has an entry, but maybe with empty ci_id (initialized-but-unverified)
        if not installed.ci_id and not auth.optional:
            severity = disc.SEVERITY_MAJOR  # unverified slot is major until inspected
            out.append(_make_discrepancy(
                asset_id=as_maintained.asset_id,
                dtype=disc.DISCREPANCY_MISSING_CI,
                description=f"Slot {auth.slot_id!r} present but ci_id unknown "
                            "(asset has not been verified)",
                severity=severity,
                recommended_action=f"Perform inspection to record installed CI in "
                                   f"slot {auth.slot_id}",
                related_ci_id="",
                detected_at_ns=now_ns,
            ))
            continue

        # CI is recorded; check it against authorized list (if we have the CI record)
        ci_record = ci_records.get(installed.ci_id)
        if ci_record is None:
            # We don't have the part-number/revision data; skip these checks
            continue

        if ci_record.part_number not in auth.acceptable_part_numbers:
            out.append(_make_discrepancy(
                asset_id=as_maintained.asset_id,
                dtype=disc.DISCREPANCY_UNAUTHORIZED_PART,
                description=(f"Slot {auth.slot_id!r} has part_number "
                             f"{ci_record.part_number!r}; authorized: "
                             f"{list(auth.acceptable_part_numbers)}"),
                severity=disc.SEVERITY_MAJOR,
                recommended_action=(f"Replace with authorized part: one of "
                                    f"{list(auth.acceptable_part_numbers)}"),
                related_ci_id=installed.ci_id,
                detected_at_ns=now_ns,
            ))
            continue  # if unauthorized, revision check is moot

        if auth.minimum_revision and ci_record.revision < auth.minimum_revision:
            out.append(_make_discrepancy(
                asset_id=as_maintained.asset_id,
                dtype=disc.DISCREPANCY_OBSOLETE_REVISION,
                description=(f"Slot {auth.slot_id!r}: revision "
                             f"{ci_record.revision!r} below minimum "
                             f"{auth.minimum_revision!r}"),
                severity=disc.SEVERITY_MINOR,
                recommended_action=(f"Upgrade CI to revision "
                                    f"{auth.minimum_revision} or later"),
                related_ci_id=installed.ci_id,
                detected_at_ns=now_ns,
            ))

    # --- ModificationRequirement checks --------------------------------
    for mod in baseline.required_mods:
        comp = mod_status_by_id.get(mod.mod_id)
        base_severity = _COMPLIANCE_TO_SEVERITY.get(mod.category, disc.SEVERITY_MINOR)

        # Mod absent from tracking entirely
        if comp is None:
            out.append(_make_discrepancy(
                asset_id=as_maintained.asset_id,
                dtype=disc.DISCREPANCY_MISSING_MOD,
                description=(f"Required mod {mod.mod_id!r} ({mod.title!r}) "
                             "has no compliance record"),
                severity=base_severity,
                recommended_action=f"Schedule application of {mod.mod_id}",
                related_mod_id=mod.mod_id,
                detected_at_ns=now_ns,
            ))
            continue

        # Mod tracked but not applied
        is_overdue = (
            mod.due_date.seconds > 0
            and (now_ns // 1_000_000_000) > mod.due_date.seconds
        )

        if comp.state == am.MOD_STATE_APPLIED or comp.state == am.MOD_STATE_WAIVED:
            continue  # compliant

        if comp.state == am.MOD_STATE_NOT_APPLICABLE:
            continue  # explicitly excluded

        # PENDING or OVERDUE or UNSPECIFIED — discrepancy fires
        severity = _escalate(base_severity) if is_overdue else base_severity
        action = (f"OVERDUE: apply {mod.mod_id} immediately"
                  if is_overdue else f"Apply {mod.mod_id} before "
                  f"{_iso_or_unknown(mod.due_date)}")
        out.append(_make_discrepancy(
            asset_id=as_maintained.asset_id,
            dtype=disc.DISCREPANCY_MISSING_MOD,
            description=(f"Mod {mod.mod_id!r} ({mod.title!r}) state="
                         f"{_mod_state_name(comp.state)}"
                         + (" (OVERDUE)" if is_overdue else "")),
            severity=severity,
            recommended_action=action,
            related_mod_id=mod.mod_id,
            detected_at_ns=now_ns,
        ))

    return out


def overall_status(
    discrepancies: list[disc.ConfigurationDiscrepancy],
) -> int:
    """Reduce a discrepancy list to a single ConfigurationStatus value."""
    severities = {d.severity for d in discrepancies}
    if disc.SEVERITY_CRITICAL in severities:
        return am.CONFIG_STATUS_NOT_MISSION_CAPABLE
    if disc.SEVERITY_MAJOR in severities:
        return am.CONFIG_STATUS_MAJOR_DISCREPANCY
    if disc.SEVERITY_MINOR in severities:
        return am.CONFIG_STATUS_MINOR_DISCREPANCY
    return am.CONFIG_STATUS_IN_COMPLIANCE


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------

def _make_discrepancy(
    *,
    asset_id: str,
    dtype: int,
    description: str,
    severity: int,
    recommended_action: str,
    detected_at_ns: int,
    related_ci_id: str = "",
    related_mod_id: str = "",
) -> disc.ConfigurationDiscrepancy:
    d = disc.ConfigurationDiscrepancy()
    # Stable id from (asset, type, related ref) so the same finding across
    # recomputations carries the same discrepancy_id — enables deduplication.
    seed = f"{asset_id}|{dtype}|{related_ci_id}|{related_mod_id}"
    d.discrepancy_id = str(uuid.uuid5(uuid.NAMESPACE_URL, seed))
    d.type = dtype
    d.description = description
    d.severity = severity
    d.recommended_action = recommended_action
    if related_ci_id:
        d.related_ci_id = related_ci_id
    if related_mod_id:
        d.related_mod_id = related_mod_id
    ts = Timestamp()
    ts.FromNanoseconds(detected_at_ns)
    d.detected_at.CopyFrom(ts)
    return d


def _escalate(severity: int) -> int:
    """Bump severity by one level (used for PENDING+overdue mods)."""
    table = {
        disc.SEVERITY_INFO:     disc.SEVERITY_MINOR,
        disc.SEVERITY_MINOR:    disc.SEVERITY_MAJOR,
        disc.SEVERITY_MAJOR:    disc.SEVERITY_CRITICAL,
        disc.SEVERITY_CRITICAL: disc.SEVERITY_CRITICAL,  # already max
    }
    return table.get(severity, severity)


def _ingest_timestamp(ingest_time_ns: int | None) -> Timestamp:
    ts = Timestamp()
    if ingest_time_ns is None:
        ts.GetCurrentTime()
    else:
        ts.FromNanoseconds(ingest_time_ns)
    return ts


def _now_ns() -> int:
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1_000_000_000)


def _iso_or_unknown(ts: Timestamp) -> str:
    if ts.seconds == 0 and ts.nanos == 0:
        return "<no due date>"
    return ts.ToJsonString()


def _mod_state_name(state: int) -> str:
    return {
        am.MOD_STATE_UNSPECIFIED:    "UNSPECIFIED",
        am.MOD_STATE_NOT_APPLICABLE: "NOT_APPLICABLE",
        am.MOD_STATE_PENDING:        "PENDING",
        am.MOD_STATE_OVERDUE:        "OVERDUE",
        am.MOD_STATE_APPLIED:        "APPLIED",
        am.MOD_STATE_WAIVED:         "WAIVED",
    }.get(state, f"UNKNOWN({state})")
