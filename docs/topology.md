# CM Service — Topic and Subscription Topology

This document describes how the Configuration Management service is wired
into the OpenDDIL Kafka topology. Per ADR-0014 the service runs as a
Restate Virtual Object (`AssetCM`); per ADR-0009 it owns the
`asset-cm-state` and `cm-events` topics; per ADR-0010 it never branches
on feed source — Silver is feed-agnostic.

## Diagram

```
┌──────────────────────────┐        ┌──────────────────────────┐
│ raw-sensor-stream        │        │ cm-events                │
│ (Silver, Protobuf)       │        │ (CM actions, Protobuf)   │
│ — feeds: DIS, proprietary│        │ — actors: maintainers,   │
│   & future protocols     │        │   work-order systems     │
└─────────────┬────────────┘        └─────────────┬────────────┘
              │                                   │
              │  Kafka subscription               │  Kafka subscription
              │  (managed by Restate)             │  (managed by Restate)
              ▼                                   ▼
        ┌───────────────────────────────────────────────────┐
        │ Restate Virtual Object: AssetCM                   │
        │ keyed by asset_id                                 │
        │                                                   │
        │  observe(silver_event)     ◄── raw-sensor-stream  │
        │  apply_cm_event(cm_event)  ◄── cm-events          │
        │  recheck_compliance()      ◄── self, scheduled    │
        │  decommission(reason)      ◄── operator           │
        │                                                   │
        │  Durable state per asset_id:                      │
        │    am_state, next_recheck_ns                      │
        └─────────────┬───────────────────┬─────────────────┘
                      │                   │
                      │  egress (via ctx.run → confluent-kafka producer)
                      ▼                   ▼
        ┌─────────────────────────┐  ┌──────────────────────────┐
        │ asset-cm-state          │  │ tactical-events          │
        │ (compacted, keyed by    │  │ (CloudEvents v1, JSON)   │
        │  asset_id)              │  │ — discrepancy.detected   │
        │ — readers: COP UI,      │  │   / discrepancy.resolved │
        │   future analytics      │  │ — readers: alerters,     │
        │                         │  │   ALCS/EAGLE bridges     │
        └─────────────────────────┘  └──────────────────────────┘
```

## Subscriptions (registered by `bootstrap/register_subscriptions.py`)

| # | Source topic         | Restate handler            | Consumer group           | Notes |
|---|----------------------|----------------------------|--------------------------|-------|
| 1 | `raw-sensor-stream`  | `AssetCM/observe`          | `cm-service-silver`      | Triggers first-seen path; updates `last_observed_at` thereafter |
| 2 | `cm-events`          | `AssetCM/apply_cm_event`   | `cm-service-cm-events`   | Each `CmEvent.oneof` variant routes to a different state mutation |

Subscriptions are created with `auto.offset.reset = earliest`. The
bootstrap script is idempotent — 409 responses from Restate's admin API
are treated as "already exists" and ignored.

**Adding a new feed**: append an entry to `_DEFAULT_SUBSCRIPTIONS` in
the bootstrap script, or set `CM_SUBSCRIPTIONS` env var to a JSON list
of `{topic, handler, consumer_group}` triplets. No code change in the
AssetCM Virtual Object — it only sees Silver-shape events.

## Topics owned vs consumed

| Topic                  | Owned by               | Cleanup policy       | Schema             |
|------------------------|------------------------|----------------------|--------------------|
| `raw-sensor-stream`    | DIS sidecar + Connect  | retention            | Silver protobuf    |
| `cm-events`            | external producers     | compact,delete (30d) | `CmEvent` protobuf |
| `asset-cm-state`       | **cm-service**         | compact              | `AsMaintainedConfiguration` JSON |
| `cm-items`             | cm-service (future)    | compact              | `ConfigurationItem` protobuf |
| `tactical-events`      | shared egress          | retention            | CloudEvents v1 JSON |

`asset-cm-state` uses JSON-of-dataclass-asdict, not protobuf binary, for
two reasons:

1. Restate journals state as JSON internally, so emitting JSON keeps the
   wire form and the durable state shape identical (no double conversion).
2. Downstream readers (the future COP UI via ElectricSQL) want shape-stable
   JSON they can query without a protobuf decoder.

The Bronze-to-Silver protobuf chain (DIS sidecar → Bloblang → Silver) is
unaffected; `asset-cm-state` is downstream of Silver and is a distinct
contract.

## Egress: Kafka publisher

`cm-service` publishes to `asset-cm-state` and `tactical-events` via a
confluent-kafka producer wrapped in `ctx.run()`. The Restate journal
guarantees the side effect is replayed at most once per handler
invocation — so re-execution under retry doesn't emit duplicate
CloudEvents.

`acks=all`, `enable.idempotence=true`, `compression.type=zstd`, same
config pattern established by the DIS sidecar in Phase 2.

## Failure modes and what they look like

| Failure | Symptom | Recovery |
|---|---|---|
| Restate not ready when bootstrap runs | Bootstrap waits up to `CM_BOOTSTRAP_TIMEOUT_S` (default 120s) | Increase the timeout or run bootstrap manually after stack is healthy |
| CM service endpoint slow to start | Bootstrap waits on `/discover` | Same |
| Subscription already exists | Bootstrap logs "already exists" and continues | Nothing — designed behavior |
| Kafka broker down during egress | `ctx.run()` returns exception, Restate retries the handler | Producer's idempotence + Restate's at-least-once → no duplicates |
| `am_state` corrupted on disk | Asset re-initializes on next `observe()` (first-seen path) | Acceptable: state is derivable from baseline + recent CM events |
| Baseline missing for an asset's `platform_variant` | Asset registers in `LIFECYCLE_REGISTERED` with no baseline; logged at WARN | Add baseline YAML, send SIGHUP to cm-service, next `observe()` re-initializes |

## Operational commands

```bash
# Inspect a specific asset's CM state
docker exec openddil-demo-redpanda-edge-1 \
  rpk topic consume asset-cm-state \
    --group inspect-$$ \
    --offset start \
    --format '%v\n' \
  | jq 'select(.asset_id=="dis:1:1:4773")'

# Reload baselines without restarting cm-service
docker exec openddil-demo-cm-service kill -HUP 1

# Re-run subscription bootstrap (idempotent)
docker compose run --rm cm-service-bootstrap

# Submit a manual CM event (test/dev only — production goes via the
# work-order system)
echo '{"event_id":"manual-1","asset_id":"dis:1:1:4773","mod_applied":{"mod_id":"MWO-2024-117","applied_at":"2026-05-12T00:00:00Z"}}' \
  | docker exec -i openddil-demo-redpanda-edge-1 \
      rpk topic produce cm-events --key=dis:1:1:4773
```
