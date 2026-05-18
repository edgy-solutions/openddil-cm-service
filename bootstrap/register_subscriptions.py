"""
cm-service bootstrap — thin wrapper around the shared library.

Owns cm-service's subscription list. Restate-side plumbing lives in
`openddil_bootstrap.restate_subscriptions`.

ADR-0023 Phase 6b §A: cm-service subscribes to each of the 3 per-edge
Kafka clusters. Deployment registration is idempotent (409s handled);
each cluster gets its own subscriptions with edge-suffixed consumer-group
names so per-edge consumer-group lag is readable in rpk.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time

from openddil_bootstrap.restate_subscriptions import (
    Subscription,
    bootstrap_restate_service,
)

logger = logging.getLogger("cm_service.bootstrap")


RESTATE_ADMIN_URL   = os.getenv("RESTATE_ADMIN_URL",   "http://restate-server:9070")
CM_SERVICE_ENDPOINT = os.getenv("CM_SERVICE_ENDPOINT", "http://cm-service:9080")
BOOTSTRAP_TIMEOUT_S = int(os.getenv("CM_BOOTSTRAP_TIMEOUT_S", "120"))

DEFAULT_EDGE_CLUSTERS = (
    "edge-01=redpanda-edge-01:9092,"
    "edge-02=redpanda-edge-02:9092,"
    "edge-03=redpanda-edge-03:9092"
)
EDGE_CLUSTERS = os.getenv("CM_EDGE_CLUSTERS", DEFAULT_EDGE_CLUSTERS)


def _parse_clusters(spec: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise RuntimeError(
                f"CM_EDGE_CLUSTERS entry {entry!r} must be 'edge_id=host:port'"
            )
        edge_id, brokers = entry.split("=", 1)
        out.append((edge_id.strip(), brokers.strip()))
    return out


def _per_edge_subscriptions(edge_id: str) -> list[Subscription]:
    override = os.getenv("CM_SUBSCRIPTIONS")
    if override:
        try:
            entries = json.loads(override)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"CM_SUBSCRIPTIONS is not valid JSON: {exc}"
            ) from exc
        return [Subscription(**e) for e in entries]

    return [
        Subscription(
            topic="raw-sensor-stream",
            handler="AssetCM/observe",
            consumer_group=f"cm-service-silver-{edge_id}",
        ),
        Subscription(
            topic="cm-events",
            handler="AssetCM/apply_cm_event",
            consumer_group=f"cm-service-cm-events-{edge_id}",
        ),
    ]


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )

    edge_clusters = _parse_clusters(EDGE_CLUSTERS)
    # ADR-0023 Phase 6b §A — Restate 1.6.2 subscription_controller race:
    # concurrent multi-cluster subscription registration triggered a
    # "worker is unreachable" task-fail and self-shutdown. Sequentializing
    # registrations with a small inter-cluster sleep works around it.
    # Env-tunable; 2s default proven sufficient. See
    # tests/hero_scenario_v3/README.md follow-up for the Restate-upgrade
    # note to revisit.
    inter_cluster_sleep = float(os.getenv("BOOTSTRAP_INTER_CLUSTER_SLEEP_S", "2"))
    logger.info("[cm-service] bootstrapping %d edge cluster(s) "
                "(inter-cluster sleep=%.1fs)",
                len(edge_clusters), inter_cluster_sleep)

    for i, (edge_id, brokers) in enumerate(edge_clusters):
        if i > 0 and inter_cluster_sleep > 0:
            time.sleep(inter_cluster_sleep)
        cluster_name = f"openddil-{edge_id}"
        bootstrap_restate_service(
            service_label=f"cm-service[{edge_id}]",
            restate_admin_url=RESTATE_ADMIN_URL,
            service_endpoint=CM_SERVICE_ENDPOINT,
            kafka_cluster_name=cluster_name,
            kafka_brokers=brokers,
            subscriptions=_per_edge_subscriptions(edge_id),
            timeout_s=BOOTSTRAP_TIMEOUT_S,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
