"""
Persistence/Computation model round-trip tests.

ADR-0006: proto -> record -> proto must produce an equivalent message.
This protects against accidental field drops when either form evolves.
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
    discrepancy_pb2 as disc,
)
from as_maintained.store import proto_to_record, record_to_proto


def _ts(ns: int) -> Timestamp:
    t = Timestamp()
    t.FromNanoseconds(ns)
    return t


def _full_state() -> am.AsMaintainedConfiguration:
    """Build a fully-populated AsMaintainedConfiguration that exercises every
    sub-message and every optional field."""
    s = am.AsMaintainedConfiguration()
    s.asset_id = "dis:1:1:4773"
    s.baseline_id = "M1A2-SEPv3-Baseline-2024.2"
    s.as_of.CopyFrom(_ts(1_746_086_400_000_000_000))

    i1 = s.installed.add()
    i1.slot_id = "engine"
    i1.ci_id = "AGT-1500-SN-12345"
    i1.installed_at.CopyFrom(_ts(1_700_000_000_000_000_000))

    i2 = s.installed.add()
    i2.slot_id = "transmission"
    i2.ci_id = ""  # unverified

    m1 = s.mod_status.add()
    m1.mod_id = "MWO-2024-117"
    m1.state = am.MOD_STATE_APPLIED
    m1.applied_at.CopyFrom(_ts(1_740_000_000_000_000_000))
    m1.applied_by_work_order = "WO-9876"

    m2 = s.mod_status.add()
    m2.mod_id = "MWO-2023-089"
    m2.state = am.MOD_STATE_PENDING

    d1 = s.discrepancies.add()
    d1.discrepancy_id = "abc-def"
    d1.type = disc.DISCREPANCY_MISSING_MOD
    d1.description = "MWO-2023-089 pending"
    d1.severity = disc.SEVERITY_MAJOR
    d1.recommended_action = "Apply MWO-2023-089"
    d1.related_mod_id = "MWO-2023-089"
    d1.detected_at.CopyFrom(_ts(1_746_086_400_000_000_000))

    s.overall_status = am.CONFIG_STATUS_MAJOR_DISCREPANCY
    s.lifecycle = am.LIFECYCLE_ACTIVE
    s.last_observed_at.CopyFrom(_ts(1_746_086_300_000_000_000))
    s.last_alerted_status = am.CONFIG_STATUS_MINOR_DISCREPANCY
    return s


def test_round_trip_preserves_full_state():
    original = _full_state()
    record = proto_to_record(original)
    round_tripped = record_to_proto(record)

    # Serialized form must be byte-identical for equivalence
    assert original.SerializeToString() == round_tripped.SerializeToString()


def test_round_trip_empty_state():
    empty = am.AsMaintainedConfiguration()
    empty.asset_id = "empty-asset"
    record = proto_to_record(empty)
    round_tripped = record_to_proto(record)
    assert round_tripped.asset_id == "empty-asset"
    assert len(round_tripped.installed) == 0
    assert len(round_tripped.mod_status) == 0
    assert len(round_tripped.discrepancies) == 0


def test_record_field_count_matches_proto():
    """Sanity check that we haven't forgotten to map any proto field. If
    AsMaintainedConfiguration grows a field, this test fails loudly so the
    persistence model gets updated in lockstep."""
    proto_fields = set(
        am.AsMaintainedConfiguration.DESCRIPTOR.fields_by_name.keys()
    )
    expected = {
        "asset_id", "baseline_id", "as_of",
        "installed", "mod_status", "discrepancies", "overall_status",
        "lifecycle", "last_observed_at", "last_alerted_status",
    }
    # If this fails, AsMaintainedRecord needs a matching field, and
    # store.proto_to_record / store.record_to_proto need to map it.
    assert proto_fields == expected, (
        f"AsMaintainedConfiguration has fields {proto_fields} but the "
        f"persistence model was built for {expected}. Update "
        f"as_maintained/persistence_model.py and as_maintained/store.py."
    )


def test_record_preserves_zero_timestamp_correctly():
    """A proto with unset Timestamp fields (seconds=0, nanos=0) must round-trip
    without spurious timestamps appearing on the proto side."""
    s = am.AsMaintainedConfiguration()
    s.asset_id = "no-timestamps"
    # Don't set as_of
    i = s.installed.add()
    i.slot_id = "slot-a"
    # Don't set installed_at
    record = proto_to_record(s)
    round_tripped = record_to_proto(record)
    assert not round_tripped.HasField("as_of")
    assert not round_tripped.installed[0].HasField("installed_at")


# ---------------------------------------------------------------------------
# Advisory provenance reaches the WIRE — ADR-0038 C4(a) / ADR-0018 amendment
# ---------------------------------------------------------------------------
# These exist because of a specific trap: `discrepancy.proto` DOCUMENTS the
# asset-cm-state path, it is not that path's contract. The wire shape is the
# DiscrepancyRecord dataclass via `dataclasses.asdict`, so a proto-only change
# ships a no-op that passes every other test in this file. The round-trip
# tests above would stay green while nothing reached a consumer.
#
# So these assert against the dict a consumer actually receives, not against
# the proto. See ADR-0018 §Amendment 2026-08-15.

def test_advisory_provenance_reaches_the_serialized_wire_shape():
    """The emission path is asdict(record) -> json.dumps. Provenance must
    survive it, or the whole C4(a) change is invisible in production."""
    import dataclasses
    import json

    from as_maintained.persistence_model import AdvisoryProvenanceRecord

    s = am.AsMaintainedConfiguration(asset_id="wire-check")
    d = s.discrepancies.add()
    d.discrepancy_id = "abc"
    d.recommended_action = "Schedule application of MWO-2026-001"
    d.advisory_provenance.basis = disc.ADVISORY_BASIS_RULE
    d.advisory_provenance.producer = "cm-service/discrepancy-analyzer"
    d.advisory_provenance.producer_version = "1.0.0"
    d.advisory_provenance.rule_id = "required-mod.untracked"
    d.advisory_provenance.limitations.append(
        disc.ADVISORY_LIMITATION_NOT_AUTHORITATIVE_ORDER
    )

    record = proto_to_record(s)
    assert isinstance(record.discrepancies[0].advisory_provenance,
                      AdvisoryProvenanceRecord)

    payload = dataclasses.asdict(record)
    prov = payload["discrepancies"][0]["advisory_provenance"]
    assert prov["basis"] == disc.ADVISORY_BASIS_RULE
    assert prov["producer"] == "cm-service/discrepancy-analyzer"
    assert prov["rule_id"] == "required-mod.untracked"
    assert disc.ADVISORY_LIMITATION_NOT_AUTHORITATIVE_ORDER in prov["limitations"]

    # And it must survive JSON encoding, which is what actually goes on Kafka.
    assert "required-mod.untracked" in json.dumps(payload)


def test_absent_provenance_stays_absent_on_the_proto():
    """A discrepancy with no provenance must NOT gain an empty-but-present
    submessage. Setting any field on a proto3 submessage marks it present, so
    a careless record->proto would turn "no claim" into "empty claim" and
    break byte-identical round-tripping for every pre-existing row."""
    s = am.AsMaintainedConfiguration(asset_id="no-prov")
    d = s.discrepancies.add()
    d.discrepancy_id = "abc"
    d.recommended_action = "Install authorized part in slot turret-optics"

    round_tripped = record_to_proto(proto_to_record(s))
    assert not round_tripped.discrepancies[0].HasField("advisory_provenance")
    assert s.SerializeToString() == round_tripped.SerializeToString()


def test_durable_state_written_before_provenance_existed_still_loads():
    """The additive-only constraint (ADR-0018 §Amendment). Restate replays
    dicts serialized before this field existed; they must decode to the
    default, which reads as "no claim" rather than failing or asserting."""
    from events.asset_cm import _disc_from_dict

    legacy = {
        "discrepancy_id": "abc", "type": 1, "description": "old row",
        "severity": 3, "recommended_action": "Apply MWO-2026-001",
        "related_ci_id": "", "related_mod_id": "MWO-2026-001",
        "detected_at_ns": 1_700_000_000_000_000_000,
    }
    rec = _disc_from_dict(legacy)
    assert rec.advisory_provenance.basis == disc.ADVISORY_BASIS_UNSPECIFIED
    assert rec.advisory_provenance.producer == ""

    # And a nested dict (what asdict writes today) narrows back to the
    # dataclass rather than staying a dict — otherwise record->proto would
    # fail on attribute access.
    from as_maintained.persistence_model import AdvisoryProvenanceRecord
    current = dict(legacy)
    current["advisory_provenance"] = {"basis": 1, "producer": "x",
                                       "producer_version": "", "config_hash": "",
                                       "model_artifact_hash": "", "rule_id": "",
                                       "inputs": [], "confidence": 0.0,
                                       "confidence_kind": 0, "limitations": [],
                                       "generated_at_ns": 0}
    rec2 = _disc_from_dict(current)
    assert isinstance(rec2.advisory_provenance, AdvisoryProvenanceRecord)
    assert rec2.advisory_provenance.producer == "x"
