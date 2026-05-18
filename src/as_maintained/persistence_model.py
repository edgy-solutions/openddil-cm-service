"""
Persistence model — plain Python dataclasses for AssetCM Virtual Object state.

ADR-0014: the CM service is built on Restate Virtual Objects. Restate's
durable state is serialized via JSON by default; plain dataclasses with
JSON-friendly field types are the natural shape.

ADR-0006 boundary (preserved): this module owns serialization. The analyzer
in `discrepancy/` owns the protobuf form. Translation functions in `store.py`
cross the boundary; nothing else does.

Why dataclasses (not protobuf) for Virtual Object state?
  - Restate's Python SDK serializes state via JSON by default. Dataclasses
    serialize cleanly. Protobuf would require custom JSON marshalling.
  - The Virtual Object state shape can evolve without proto field-number
    pressure.
  - Algorithm code (analyzer.py) still receives protobuf, which is the
    stable contract toward the rest of the system.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class InstalledCiRecord:
    slot_id: str
    ci_id: str = ""
    installed_at_ns: int = 0   # 0 = unset


@dataclass
class ModComplianceRecord:
    mod_id: str
    state: int = 0             # ModComplianceState enum value
    applied_at_ns: int = 0
    applied_by_work_order: str = ""


@dataclass
class DiscrepancyRecord:
    discrepancy_id: str
    type: int                  # DiscrepancyType enum value
    description: str
    severity: int              # Severity enum value
    recommended_action: str
    related_ci_id: str = ""
    related_mod_id: str = ""
    detected_at_ns: int = 0


@dataclass
class AsMaintainedRecord:
    """The full as-maintained configuration for one asset, in
    Restate-durable form. Stored as a single state key on the AssetCM
    Virtual Object keyed by asset_id."""
    asset_id: str
    baseline_id: str = ""
    as_of_ns: int = 0
    installed: list[InstalledCiRecord] = field(default_factory=list)
    mod_status: list[ModComplianceRecord] = field(default_factory=list)
    # Analyzer-computed discrepancies. Rebuilt from scratch on every
    # reanalysis — DO NOT append manual entries here.
    discrepancies: list[DiscrepancyRecord] = field(default_factory=list)
    overall_status: int = 0                # ConfigurationStatus enum value

    # Restate-specific fields (do not exist on faust.Record form; live here
    # because they replace the in-memory transition cache from Agent C):
    lifecycle: int = 0                     # LifecycleState enum value
    last_observed_at_ns: int = 0
    last_alerted_status: int = 0           # ConfigurationStatus enum value

    # Manual discrepancies raised by humans/external systems. Persisted
    # separately so the analyzer's recompute (which derives discrepancies
    # purely from baseline + installed/mod_status) does not clobber them.
    # Merged with `discrepancies` when emitting to the wire via
    # `record_to_proto`; downstream consumers see one unified list keyed
    # by `discrepancy_id` (manual entries use uuid5 with a 'manual|' seed
    # so they're identifiable without a schema change).
    manual_discrepancies: list[DiscrepancyRecord] = field(default_factory=list)

    # Origin-node provenance (ADR-0022 / ADR-0023 Phase 6b §A). Populated
    # from the first observe() event's provenance.edge_id / region_id
    # (raw-sensor-stream events carry this after Phase 6a). Persisted on
    # the asset record so subsequent emissions from recheck_compliance /
    # decommission (which have no fresh inbound event) inherit the
    # asset's edge attribution. Empty string default for pre-6b records
    # — projector falls back to its env default with rate-limited WARN.
    edge_id: str = ""
    region_id: str = ""
