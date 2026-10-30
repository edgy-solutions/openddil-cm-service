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
class AdvisoryProvenanceRecord:
    """Provenance for a discrepancy's `recommended_action` (ADR-0038 C4).

    THIS DATACLASS IS THE WIRE SHAPE, not the proto — asset-cm-state is
    serialized by `dataclasses.asdict` (ADR-0018 §Amendment 2026-08-15).
    Field names must match `AdvisoryProvenance` in discrepancy.proto, and
    every field must have a default: `_dict_to_record` reconstructs from
    Restate-durable state, so rows written before this existed decode with
    the defaults. All-default means NO CLAIM — never "trusted", never
    "human-authored".
    """
    basis: int = 0                 # AdvisoryBasis enum value
    producer: str = ""
    producer_version: str = ""
    config_hash: str = ""
    model_artifact_hash: str = ""
    rule_id: str = ""
    inputs: list[str] = field(default_factory=list)
    confidence: float = 0.0
    confidence_kind: int = 0       # ConfidenceKind enum value
    limitations: list[int] = field(default_factory=list)  # AdvisoryLimitation
    generated_at_ns: int = 0


@dataclass
class DiscrepancySourceRecord:
    """One report that contributed to an episode-keyed DiscrepancyRecord
    (ADR-0018 §Amendment 2026-08-15).

    THIS DATACLASS IS THE WIRE SHAPE, not the proto — see
    AdvisoryProvenanceRecord's docstring above for why that matters here too.
    `source` is an opaque string cm-service stores and never interprets; it
    names no particular consumer or system.
    """
    source: str = ""
    reported_by: str = ""
    event_id: str = ""
    reported_at_ns: int = 0
    description: str = ""


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
    # ADR-0038 C4. Defaulted so pre-provenance durable state still decodes.
    advisory_provenance: AdvisoryProvenanceRecord = field(
        default_factory=AdvisoryProvenanceRecord
    )
    # Episode keying. component is a BOM slot_id; a non-empty fault_code
    # groups every report of the same (asset, component, fault_code) into one
    # discrepancy instead of one per report. Defaulted so durable state from
    # before episode keying still decodes (empty fault_code == today's unkeyed manual
    # discrepancy, unchanged).
    component: str = ""
    fault_code: str = ""
    # Every report that contributed to this episode. Empty for the unkeyed
    # (fault_code == "") path and for all earlier durable state.
    sources: list[DiscrepancySourceRecord] = field(default_factory=list)


@dataclass
class EpisodeContextRecord:
    """The asset's configuration frozen at the moment an episode OPENED
    (ADR-0051 §2, baseline and mod list only).

    Kept OFF DiscrepancyRecord on purpose: that dataclass is the wire shape
    of `discrepancies` / `manual_discrepancies` on asset-cm-state, and
    existing readers must see no change. Lives in
    AsMaintainedRecord.episode_context keyed by discrepancy_id, and moves
    into the ClosedEpisodeRecord when the episode closes. Every field has a
    default so durable state written before this existed decodes.
    """
    detected_at_ns: int = 0
    baseline_id: str = ""
    mod_compliance: list[ModComplianceRecord] = field(default_factory=list)


@dataclass
class ClosedEpisodeRecord:
    """A manual-discrepancy episode after `manual_discrepancy_resolved`
    closed it (ADR-0051 §1). Append-only on AsMaintainedRecord.

    IDENTITY IS (discrepancy_id, detected_at_ns), NOT discrepancy_id.
    The episode id is uuid5(asset|component|fault_code), so a refile after
    closure gets the SAME id as the episode it follows. ADR-0051 §1 says
    the closed set is keyed by the existing episode id; that cannot be a
    unique key on its own. The episode key itself is unchanged (the gate and
    egress depend on it); only this list needs the pair.

    Every field has a default so durable state written before this existed
    decodes.
    """
    discrepancy_id: str = ""
    type: int = 0
    description: str = ""
    severity: int = 0
    recommended_action: str = ""
    detected_at_ns: int = 0
    component: str = ""
    fault_code: str = ""
    sources: list[DiscrepancySourceRecord] = field(default_factory=list)
    cleared_at_ns: int = 0
    cleared_by: str = ""
    resolution: str = ""
    # Frozen at open (EpisodeContextRecord); empty when the episode had no
    # context entry (re-seeded from the store after a state wipe).
    baseline_id: str = ""
    mod_compliance: list[ModComplianceRecord] = field(default_factory=list)


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

    # Coalition releasability labels (ADR-0029 §3). Same lifecycle as
    # edge_id / region_id above: taken from the first observe() event's
    # provenance and persisted on the record, so emissions from
    # recheck_compliance / decommission — which have no fresh inbound event
    # — still carry them.
    #
    # PROPAGATED, NEVER DERIVED. cm-service holds no releasability
    # declaration and must not acquire one; the label is stamped once at
    # ingress. What cm-service contributes is the join from asset to
    # configuration record.
    #
    # Empty default, and empty means UNLABELLED — the ADR-0029 §7
    # completeness gate counts such a row. That is deliberate: a
    # configuration record for an asset nobody declared should fail the
    # gate, not quietly acquire a nation from whichever service noticed it
    # last. Pre-ADR-0029 records decode with the default, as with edge_id.
    originator_nation: str = ""
    releasable_to: list[str] = field(default_factory=list)

    # ADR-0051 slice item 1. Closed manual-discrepancy episodes, append-only
    # (`manual_discrepancies` keeps meaning "open episodes only"), and the
    # context frozen at open for each OPEN episode, keyed by discrepancy_id.
    # Neither is published on asset-cm-state; ADR-0051 item 2 (the store
    # table) decides what is projected.
    closed_episodes: list[ClosedEpisodeRecord] = field(default_factory=list)
    episode_context: dict[str, EpisodeContextRecord] = field(default_factory=dict)
