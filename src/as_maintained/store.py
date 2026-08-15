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
    AdvisoryProvenanceRecord,
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


def _adv_proto_to_record(p: disc.AdvisoryProvenance) -> AdvisoryProvenanceRecord:
    return AdvisoryProvenanceRecord(
        basis=int(p.basis),
        producer=p.producer,
        producer_version=p.producer_version,
        config_hash=p.config_hash,
        model_artifact_hash=p.model_artifact_hash,
        rule_id=p.rule_id,
        inputs=list(p.inputs),
        confidence=float(p.confidence),
        confidence_kind=int(p.confidence_kind),
        limitations=[int(x) for x in p.limitations],
        generated_at_ns=_ts_to_ns(p.generated_at),
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
        advisory_provenance=_adv_proto_to_record(p.advisory_provenance),
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
        out.discrepancies.append(disc_record_to_proto(d))
    for d in rec.manual_discrepancies:
        out.discrepancies.append(disc_record_to_proto(d))
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


def disc_record_to_proto(r: DiscrepancyRecord) -> disc.ConfigurationDiscrepancy:
    """Record -> proto for a single discrepancy.

    PUBLIC deliberately. `events/asset_cm.py` previously carried a private
    duplicate of this function (`_disc_record_to_proto_local`) whose only
    reason to exist was avoiding an underscore-prefixed import — and it
    silently contradicted this module's own docstring, which claims to be
    the ONLY place crossing the persistence/computation boundary. Two
    converters means the next field addition has two chances to be
    half-applied, and the failure mode is silent partial propagation:
    manual discrepancies would have lost their provenance while
    analyzer-computed ones kept it, with nothing failing. Deleted as part
    of ADR-0038 C4(a), which is the change that exercises every site.
    """
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
    # Only materialize the submessage when there is provenance to carry.
    # Setting ANY field on `p.advisory_provenance` marks it PRESENT, so
    # writing an all-default provenance would turn an absent submessage into
    # an empty-but-present one and break byte-identical round-tripping —
    # caught by test_round_trip_preserves_full_state, which is exactly the
    # kind of thing that assertion exists for. Absent stays absent, which is
    # also the honest encoding: no claim, rather than an empty claim.
    if not _adv_is_default(r.advisory_provenance):
        _adv_record_to_proto_into(r.advisory_provenance, p.advisory_provenance)
    return p


def _adv_is_default(r: AdvisoryProvenanceRecord | dict | None) -> bool:
    """True when the record carries no provenance claim at all."""
    if r is None:
        return True
    if isinstance(r, dict):
        r = AdvisoryProvenanceRecord(**r)
    return r == AdvisoryProvenanceRecord()


def _adv_record_to_proto_into(r: AdvisoryProvenanceRecord | None,
                                p: disc.AdvisoryProvenance) -> None:
    """Fill an AdvisoryProvenance submessage in place.

    Tolerates None and a raw dict: durable state written before this field
    existed decodes to the dataclass default via `_dict_to_record`, but a
    caller holding a hand-built record may pass either.
    """
    if r is None:
        return
    if isinstance(r, dict):  # defensive: un-narrowed durable state
        r = AdvisoryProvenanceRecord(**r)
    p.basis = r.basis
    p.producer = r.producer
    p.producer_version = r.producer_version
    p.config_hash = r.config_hash
    p.model_artifact_hash = r.model_artifact_hash
    p.rule_id = r.rule_id
    p.inputs.extend(r.inputs)
    p.confidence = r.confidence
    p.confidence_kind = r.confidence_kind
    p.limitations.extend(r.limitations)
    if r.generated_at_ns:
        p.generated_at.CopyFrom(_ns_to_ts(r.generated_at_ns))


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _ts_to_ns(ts: Timestamp) -> int:
    return ts.seconds * 1_000_000_000 + ts.nanos


def _ns_to_ts(ns: int) -> Timestamp:
    out = Timestamp()
    out.FromNanoseconds(ns)
    return out
