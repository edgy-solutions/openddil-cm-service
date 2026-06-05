# openddil-cm-service

Configuration Management service for OpenDDIL — owns As-Maintained state
per asset, runs the discrepancy analyzer, emits CloudEvents on
worsening/resolving status transitions.

Built on **Restate Virtual Objects** per ADR-0014. One `AssetCM` instance
per `asset_id`, durable state across restarts, per-object scheduled
re-evaluation timers.

## Layout

```
openddil-cm-service/
├── Dockerfile                          uv-based two-stage build
├── pyproject.toml                      pinned deps (restate-sdk, hypercorn, …)
├── README.md                           this file
├── docs/topology.md                    Kafka topic ↔ handler wiring
├── bootstrap/
│   └── register_subscriptions.py       idempotent admin-API bootstrap
├── cli/
│   └── submit_cm_event.py              ad-hoc CmEvent producer
└── src/
    ├── main.py                         ASGI entry; Kafka publisher + baselines
    ├── baselines/loader.py             SIGHUP-reloadable YAML loader
    ├── discrepancy/analyzer.py         pure-Python; no Faust/Kafka imports
    ├── as_maintained/
    │   ├── persistence_model.py        dataclasses (Restate durable form)
    │   └── store.py                    proto ↔ dataclass translation
    ├── events/
    │   └── asset_cm.py                 the Virtual Object + 4 handlers
    └── tests/                          55 unit tests, 97% coverage on analyzer
```

## Handlers

| Handler | Trigger | Effect |
|---|---|---|
| `observe(silver_event)` | Kafka subscription on `raw-sensor-stream` | First-seen → initialize from baseline; subsequent → update `last_observed_at`, recover from STALE |
| `apply_cm_event(cm_event)` | Kafka subscription on `cm-events` | Mutate as-maintained state per oneof variant (mod applied, part replaced, etc.) |
| `recheck_compliance()` | Self-scheduled via `ctx.object_send(send_delay=...)` | Re-run analyzer; transition to STALE if telemetry stopped; fire overdue-mod alerts |
| `decommission(reason)` | Operator-invoked | Set `LIFECYCLE_DECOMMISSIONED`; stop scheduling rechecks |

## Lifecycle states

```
LIFECYCLE_UNSPECIFIED
LIFECYCLE_REGISTERED       Virtual Object exists, baseline lookup pending/failed
LIFECYCLE_ACTIVE           Initialized against baseline, receiving telemetry
LIFECYCLE_STALE            No telemetry within CM_STALENESS_WINDOW_S
LIFECYCLE_DECOMMISSIONED   Explicit retirement
```

Orthogonal to `ConfigurationStatus` — an asset can be `ACTIVE` and
`NOT_MISSION_CAPABLE` simultaneously, which is the picture maintainers
want.

## Runtime

Local dev (host outside Docker):

```powershell
cd openddil-cm-service
uv venv .venv
.venv\Scripts\activate
uv pip install -e .[dev]

# Compile protobuf bindings (already done in CI; here for local edits)
cd ..\openddil-contracts
py -m grpc_tools.protoc -I proto --python_out=gen/python --pyi_out=gen/python `
  proto/openddil/configuration/v1/*.proto `
  proto/openddil/telemetry/v1/telemetry.proto

# Run tests (no Kafka required)
cd ..\openddil-cm-service
py -m pytest src/tests --no-cov
```

Inside docker-compose stack:

```powershell
cd openddil-demo
docker compose up --build cm-service cm-service-bootstrap
```

The bootstrap container exits 0 once the Restate subscriptions are
registered. Re-run safely (`docker compose run --rm cm-service-bootstrap`)
to recreate subscriptions after a Restate restart.

## Configuration (env)

| Variable | Default | Notes |
|---|---|---|
| `CM_BASELINES_DIR` | `/baselines` | Single-directory mount (back-compat). Mounted from `openddil-contracts/baselines/`. |
| `CM_BASELINES_DIRS` | _(unset)_ | Colon-separated multi-directory list (e.g. `/baselines:/baselines-customer-overlay`). When set, REPLACES `CM_BASELINES_DIR`. Lets customer overlays layer their own baselines on top of the OSS set. Duplicate `platform_variant` across directories is a hard error. |
| `CM_KAFKA_BROKERS` | `redpanda-edge:9092` | For egress: `asset-cm-state` + `tactical-events` |
| `CM_HTTP_PORT` | `9080` | Restate ASGI endpoint |
| `CM_STALENESS_WINDOW_S` | `900` | Telemetry-gap threshold for `LIFECYCLE_STALE` |
| `CM_RECHECK_MIN_DELAY_S` | `30` | Lower bound on scheduled-recheck delay |
| `LOG_LEVEL` | `INFO` | |

Bootstrap container env (see `bootstrap/register_subscriptions.py`):

| Variable | Default | Notes |
|---|---|---|
| `RESTATE_ADMIN_URL` | `http://restate-server:9070` | Where the bootstrap POSTs |
| `CM_SERVICE_ENDPOINT` | `http://cm-service:9080` | Where Restate fetches handlers |
| `KAFKA_CLUSTER_NAME` | `openddil-edge` | Logical name in Restate |
| `KAFKA_BROKERS` | `redpanda-edge:9092` | Bootstrap servers for the Kafka source |
| `CM_SUBSCRIPTIONS` | (default list) | Override JSON list to add feeds without code changes |

## Verifying state on the running stack

```powershell
# Most-recent state for a known asset
docker compose exec redpanda-edge `
  rpk topic consume asset-cm-state --offset start -n 200 --format '%v\n' `
  | findstr "dis:1:1:4773"

# Hot-reload baselines after a YAML edit
docker compose exec cm-service kill -HUP 1

# Submit a test CM event
docker compose exec cm-service `
  python /app/cli/submit_cm_event.py `
    --asset-id dis:1:1:4773 `
    --mod-applied MWO-2024-117
```

## ADRs that matter for this service

- ADR-0006 — Persistence/Computation Model Separation
- ADR-0007 — Carry Units, Defer Conversion
- ADR-0009 — CM data model
- ADR-0010 — Feed integration: external feeds adapt to Silver, not vice versa
- ADR-0014 — Restate vs Faust placement (why this is Restate, not Faust)

## Known limitations

- `cm-items` topic is created but not yet produced to. CI tracking
  (serialized component instances) is a Phase 3.5 epic.
- Software/Firmware CM (CI_CATEGORY_SOFTWARE) is in the proto but not
  yet exercised by any flow.
- Manual discrepancies survive analyzer recompute (Phase 3 fix) but do
  NOT auto-resolve when programmatic actions occur — a maintainer must
  explicitly clear them via a future `clear_manual_discrepancy` handler
  (planned for Phase 3.5).
