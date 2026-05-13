"""
CLI: produce a CmEvent to the cm-events Kafka topic.

Usage (from any host with kafka access):
    python submit_cm_event.py \
        --asset-id dis:1:1:4773 \
        --mod-applied MWO-2024-117

    python submit_cm_event.py \
        --asset-id dis:1:1:4773 \
        --part-replaced engine:AGT-1500-RevE-SN-12345 \
        --recorded-by maintainer-alice

The script encodes a `CmEvent` protobuf and publishes via confluent-kafka.
Intended for tests and ad-hoc operator use, not as a production API
(production CM events come from the work-order system via Restate's
Kafka subscription).
"""
from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Generated proto bindings — try several common locations so this CLI works
# both inside the cm-service container and from host with the repo cloned.
for candidate in [
    "/proto",
    str(Path(__file__).resolve().parents[2] / "openddil-contracts" / "gen" / "python"),
]:
    if Path(candidate).is_dir():
        sys.path.insert(0, candidate)
        break

from confluent_kafka import Producer
from google.protobuf.timestamp_pb2 import Timestamp
from openddil.configuration.v1 import cm_events_pb2 as cme  # type: ignore


def _now_ts() -> Timestamp:
    t = Timestamp()
    t.FromDatetime(datetime.now(timezone.utc))
    return t


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--brokers", default="redpanda-edge:9092",
                   help="Kafka bootstrap servers")
    p.add_argument("--topic", default="cm-events",
                   help="Topic to produce to")
    p.add_argument("--asset-id", required=True,
                   help="Canonical OpenDDIL asset_id")
    p.add_argument("--recorded-by", default="cli",
                   help="Identifier of the actor recording the event")
    p.add_argument("--work-order-ref", default="",
                   help="Optional ALCS/EAGLE work-order reference")

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--mod-applied",
                   help="Record a ModApplied event with this mod_id "
                        "(e.g. MWO-2024-117)")
    g.add_argument("--part-replaced",
                   help="Record a PartReplaced event as slot_id:new_ci_id")
    g.add_argument("--inspection",
                   choices=["passed", "failed"],
                   help="Record an InspectionCompleted event")
    g.add_argument("--baseline-assigned",
                   help="Switch the asset to a different baseline (baseline_id)")
    g.add_argument("--manual-discrepancy",
                   help="Raise a manual discrepancy: 'severity|description'")
    return p.parse_args()


def _build_event(args: argparse.Namespace) -> cme.CmEvent:
    evt = cme.CmEvent()
    evt.event_id = str(uuid.uuid4())
    evt.asset_id = args.asset_id
    evt.recorded_at.CopyFrom(_now_ts())
    evt.recorded_by = args.recorded_by
    evt.work_order_ref = args.work_order_ref

    if args.mod_applied:
        evt.mod_applied.mod_id = args.mod_applied
        evt.mod_applied.applied_at.CopyFrom(_now_ts())
    elif args.part_replaced:
        if ":" not in args.part_replaced:
            raise SystemExit("--part-replaced must be 'slot_id:new_ci_id'")
        slot, new_ci = args.part_replaced.split(":", 1)
        evt.part_replaced.slot_id = slot
        evt.part_replaced.new_ci_id = new_ci
        evt.part_replaced.reason = "cli-submitted"
    elif args.inspection:
        evt.inspection_completed.inspection_type = "operator-inspection"
        evt.inspection_completed.completed_at.CopyFrom(_now_ts())
        evt.inspection_completed.passed = (args.inspection == "passed")
    elif args.baseline_assigned:
        evt.baseline_assigned.baseline_id = args.baseline_assigned
        evt.baseline_assigned.reason = "cli-submitted"
    elif args.manual_discrepancy:
        if "|" not in args.manual_discrepancy:
            raise SystemExit("--manual-discrepancy must be 'SEVERITY|description'")
        severity, desc = args.manual_discrepancy.split("|", 1)
        evt.manual_discrepancy.severity = severity
        evt.manual_discrepancy.description = desc
        evt.manual_discrepancy.recommended_action = "review"
    return evt


def main() -> int:
    args = _parse_args()
    evt = _build_event(args)

    producer = Producer({"bootstrap.servers": args.brokers, "acks": "all"})
    producer.produce(
        topic=args.topic,
        key=args.asset_id.encode("utf-8"),
        value=evt.SerializeToString(),
    )
    producer.flush(timeout=10)
    print(f"Published {args.asset_id}: {evt.WhichOneof('event')} (event_id={evt.event_id})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
