# AGENTS.md — OpenDDIL CM Service

Guidelines and safety constraints for AI agents working in this repository.

## Repository Scope

This repo contains the **Configuration Management service** for OpenDDIL —
a Restate Virtual Object (`AssetCM`) that owns per-asset As-Maintained
configuration state, runs the pure-Python discrepancy analyzer, and emits
transition alerts. One durable instance per `asset_id`; state survives
worker restarts via Restate's journal.

Phase 3 of the OpenDDIL build (see ADR-0014 for Restate-vs-Faust placement).

## What You CAN Do

- **Extend the AssetCM Virtual Object** with new handlers, new state keys,
  new scheduled callbacks.
- **Add new evaluators** to `src/discrepancy/analyzer.py` for new
  discrepancy types — keep them pure-Python, no Faust/Kafka/Restate imports.
- **Add new CmEvent variants** (in `openddil-contracts`, not here) and
  wire them into `apply_cm_event` here.
- **Update the baseline registry** in `src/baselines/loader.py` for new
  baseline storage backends.
- **Add tests** in `src/tests/` — coverage target ≥90% on analyzer.py
  and asset_cm.py.

## What You MUST NOT Do

- ❌ **Never put streaming or Kafka code inside the analyzer or store**.
  ADR-0006 boundary: analyzer is framework-free; Kafka/Restate plumbing
  lives in `src/events/asset_cm.py` and `src/main.py`.
- ❌ **Never collapse `discrepancies` and `manual_discrepancies`** in
  `AsMaintainedRecord`. Reanalysis rebuilds `discrepancies` from scratch;
  `manual_discrepancies` survives. See ADR-0009 addendum.
- ❌ **Never bypass the shared bootstrap library**. The
  `bootstrap/register_subscriptions.py` wrapper MUST call
  `openddil_bootstrap.restate_subscriptions.bootstrap_restate_service`.
  Don't reimplement deployment registration or subscription dedup here.
- ❌ **Never assume Restate state was cleared by a Kafka topic purge**.
  Restate state is durable on the restate-server side; clear it
  explicitly with `restate state clear "AssetCM/<asset_id>" --force --yes`
  (Phase 3 bug #9).

## Service Lifecycle

```
  REGISTERED ──first observe──► ACTIVE ──no telemetry for STALENESS_WINDOW──► STALE
                                   │
                                   │ explicit decommission
                                   ▼
                            DECOMMISSIONED
```

`recheck_compliance` is the scheduled callback that re-evaluates
discrepancies on a timer (for due-date checks) and transitions
ACTIVE → STALE when telemetry stops arriving.

## Docker Compose Conventions (cross-repo rule)

When this service is consumed by `openddil-demo/docker-compose.yml`:

- The base compose references this image by `image: ghcr.io/edgy-solutions/openddil/cm-service:latest`.
  It MUST NOT contain a `build:` directive for cm-service.
- `openddil-demo/docker-compose.override.yml` has the matching
  `build: { context: ../openddil-cm-service }` and source mounts for
  developer hot-reload.
- **When you change the Dockerfile or pyproject.toml here**, you must
  publish a new image to `ghcr.io/edgy-solutions/openddil/cm-service:latest` (or bump
  a tag) so the base compose works for non-developer consumers.

## Tests

`pytest` from the repo root runs:
- `src/tests/test_analyzer.py` — pure-Python discrepancy logic
- `src/tests/test_asset_cm.py` — Virtual Object handler behavior
- `src/tests/test_baselines.py` — YAML registry + SIGHUP reload
- `src/tests/test_persistence_model.py` — dataclass invariants

Coverage target ≥90% on `src/discrepancy/analyzer.py` and
`src/events/asset_cm.py`.

## Documentation Maintenance

After ANY structural change, update:
1. `README.md` — service overview, handler list, env vars.
2. `llms.txt` — high-level summary for downstream LLM context.
3. `.cursorrules` — only if new conventions are introduced.
4. This file — only if new safety constraints apply.
