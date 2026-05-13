"""
cm-service bootstrap — thin wrapper around the shared library.

This file used to carry the full Restate registration plumbing
(deployment register, Kafka cluster register, subscription dedup-and-create).
That logic moved to `openddil_bootstrap.restate_subscriptions` so other
Restate services (logistics-fusion-service, future identity-resolver, etc.)
reuse it instead of copy-pasting it.

This wrapper:
  - Owns cm-service's subscription list (what topics this service consumes).
  - Resolves config from env so deployment can change brokers/endpoints
    without code changes.
  - Calls `bootstrap_restate_service` and exits.
"""
from __future__ import annotations

import json
import logging
import os
import sys

from openddil_bootstrap.restate_subscriptions import (
    Subscription,
    bootstrap_restate_service,
)

logger = logging.getLogger("cm_service.bootstrap")


# ---------------------------------------------------------------------------
# Configuration via env (no hardcoded topic names per ADR-0010)
# ---------------------------------------------------------------------------
RESTATE_ADMIN_URL   = os.getenv("RESTATE_ADMIN_URL",   "http://restate-server:9070")
CM_SERVICE_ENDPOINT = os.getenv("CM_SERVICE_ENDPOINT", "http://cm-service:9080")
KAFKA_CLUSTER_NAME  = os.getenv("KAFKA_CLUSTER_NAME",  "openddil-edge")
KAFKA_BROKERS       = os.getenv("KAFKA_BROKERS",       "redpanda-edge:9092")
BOOTSTRAP_TIMEOUT_S = int(os.getenv("CM_BOOTSTRAP_TIMEOUT_S", "120"))


def _subscriptions() -> list[Subscription]:
    """cm-service's subscription list.

    Override via CM_SUBSCRIPTIONS (JSON list of {topic, handler, consumer_group})
    to add feeds without code changes.
    """
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
            topic=os.getenv("CM_TOPIC_SILVER", "raw-sensor-stream"),
            handler="AssetCM/observe",
            consumer_group=os.getenv("CM_GROUP_SILVER", "cm-service-silver"),
        ),
        Subscription(
            topic=os.getenv("CM_TOPIC_CM_EVENTS", "cm-events"),
            handler="AssetCM/apply_cm_event",
            consumer_group=os.getenv("CM_GROUP_CM_EVENTS", "cm-service-cm-events"),
        ),
    ]


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )
    return bootstrap_restate_service(
        service_label="cm-service",
        restate_admin_url=RESTATE_ADMIN_URL,
        service_endpoint=CM_SERVICE_ENDPOINT,
        kafka_cluster_name=KAFKA_CLUSTER_NAME,
        kafka_brokers=KAFKA_BROKERS,
        subscriptions=_subscriptions(),
        timeout_s=BOOTSTRAP_TIMEOUT_S,
    )


if __name__ == "__main__":
    sys.exit(main())
