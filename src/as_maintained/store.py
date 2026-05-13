"""
Translation between AsMaintainedRecord (Restate-durable dataclasses) and
AsMaintainedConfiguration (Protobuf form).

ADR-0006 / ADR-0014: this is the ONLY place that crosses the persistence/
computation boundary. Restate handlers call these functions at the boundary;
the analyzer never sees dataclasses, the Virtual Object state never sees
protobuf.
"""
from __future__ import annotations

from google.protobuf.timestamp_pb2 import Timestamp

from openddil.configuration.v1 import (
    as_maintained_pb2 as am,
    discrepancy_pb2 as disc,
)

from .persistence_model import (
    AsMaintainedRecord,
    DiscrepancyRecord,
    InstalledCiRecord,
    ModComplianceRecord,
)


# ---------------------------------------------------------------------------
# Proto -> Record
# ---------------------------------------------------------------------------

def proto_to_record(proto: am.AsMaintainedConfiguration) -> AsMaintainedRecord:
    # NOTE: proto does not distinguish analyzer-computed vs manual
    # discrepancies. All entries land in `discrepancies`; the manual list is
    # initialized empty. Callers that need the manual list intact across a
    # reanalysis must preserve it themselves (see _reanalyze in asset_cm.py).
    return AsMaintainedRecord(
        asset_id=proto.asset_id,
        baseline_id=proto.baseline_id,
        as_of_ns=_ts_to_ns(proto.as_of),
        installed=[_installed_proto_to_record(i) for i in proto.installed],
        mod_status=[_mod_proto_to_record(m) for m in proto.mod_status],
        discrepancies=[_disc_proto_to_record(d) for d in proto.discrepancies],
        overall_status=int(proto.overall_status),
        lifecycle=int(proto.lifecycle),
        last_observed_at_ns=_ts_to_ns(proto.last_observed_at),
        last_alerted_status=int(proto.last_alerted_status),
        manual_discrepancies=[],
    )


def _installed_proto_to_record(p: am.InstalledCi) -> InstalledCiRecord:
    return InstalledCiRecord(
        slot_id=p.slot_id,
        ci_id=p.ci_id,
        installed_at_ns=_ts_to_ns(p.installed_at),
    )


def _mod_proto_to_record(p: am.ModCompliance) -> ModComplianceRecord:
    return ModComplianceRecord(
        mod_id=p.mod_id,
        state=int(p.state),
        applied_at_ns=_ts_to_ns(p.applied_at),
        applied_by_work_order=p.applied_by_work_order,
    )


def _disc_proto_to_record(p: disc.ConfigurationDiscrepancy) -> DiscrepancyRecord:
    return DiscrepancyRecord(
        discrepancy_id=p.discrepancy_id,
        type=int(p.type),
        description=p.description,
        severity=int(p.severity),
        recommended_action=p.recommended_action,
        related_ci_id=p.related_ci_id,
        related_mod_id=p.related_mod_id,
        detected_at_ns=_ts_to_ns(p.detected_at),
    )


# ---------------------------------------------------------------------------
# Record -> Proto
# ---------------------------------------------------------------------------

def record_to_proto(rec: AsMaintainedRecord) -> am.AsMaintainedConfiguration:
    out = am.AsMaintainedConfiguration()
    out.asset_id = rec.asset_id
    out.baseline_id = rec.baseline_id
    if rec.as_of_ns:
        out.as_of.CopyFrom(_ns_to_ts(rec.as_of_ns))
    for i in rec.installed:
        out.installed.append(_installed_record_to_proto(i))
    for m in rec.mod_status:
        out.mod_status.append(_mod_record_to_proto(m))
    # Wire form merges analyzer-computed and manual discrepancies into one
    # list. The proto has no source distinction; manual entries are
    # identifiable by their `discrepancy_id` prefix ("manual|..." uuid5).
    for d in rec.discrepancies:
        out.discrepancies.append(_disc_record_to_proto(d))
    for d in rec.manual_discrepancies:
        out.discrepancies.append(_disc_record_to_proto(d))
    out.overall_status = rec.overall_status
    out.lifecycle = rec.lifecycle
    if rec.last_observed_at_ns:
        out.last_observed_at.CopyFrom(_ns_to_ts(rec.last_observed_at_ns))
    out.last_alerted_status = rec.last_alerted_status
    return out


def _installed_record_to_proto(r: InstalledCiRecord) -> am.InstalledCi:
    p = am.InstalledCi()
    p.slot_id = r.slot_id
    p.ci_id = r.ci_id
    if r.installed_at_ns:
        p.installed_at.CopyFrom(_ns_to_ts(r.installed_at_ns))
    return p


def _mod_record_to_proto(r: ModComplianceRecord) -> am.ModCompliance:
    p = am.ModCompliance()
    p.mod_id = r.mod_id
    p.state = r.state
    if r.applied_at_ns:
        p.applied_at.CopyFrom(_ns_to_ts(r.applied_at_ns))
    p.applied_by_work_order = r.applied_by_work_order
    return p


def _disc_record_to_proto(r: DiscrepancyRecord) -> disc.ConfigurationDiscrepancy:
    p = disc.ConfigurationDiscrepancy()
    p.discrepancy_id = r.discrepancy_id
    p.type = r.type
    p.description = r.description
    p.severity = r.severity
    p.recommended_action = r.recommended_action
    p.related_ci_id = r.related_ci_id
    p.related_mod_id = r.related_mod_id
    if r.detected_at_ns:
        p.detected_at.CopyFrom(_ns_to_ts(r.detected_at_ns))
    return p


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _ts_to_ns(ts: Timestamp) -> int:
    return ts.seconds * 1_000_000_000 + ts.nanos


def _ns_to_ts(ns: int) -> Timestamp:
    out = Timestamp()
    out.FromNanoseconds(ns)
    return out
