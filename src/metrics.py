"""
Prometheus metrics for openddil-cm-service.

Exposed on a plain HTTP server (prometheus_client.start_http_server),
started once by main.py at process boot — separate from the Restate
ASGI endpoint on CM_HTTP_PORT. Port is METRICS_PORT (default 9464, the
Prometheus-assigned default exporter port).

Counters here are incremented directly from handler bodies in
events/asset_cm.py, NOT via ctx.run(). Restate's at-least-once delivery
means a crashed-and-retried invocation re-executes the handler function
from the top, including any `.inc()` call that already fired before the
crash point — so these counters count HANDLER INVOCATIONS, not distinct
logical events. A retried invocation for the same inbound message can
increment the same counter twice. Treat these as liveness/volume signals,
not an exact event tally.
"""
from __future__ import annotations

from prometheus_client import Counter

cm_removal_unknown_asset_dropped_total = Counter(
    "cm_removal_unknown_asset_dropped_total",
    "Remove Entity claims for an asset_id with no AssetCM state, dropped",
)
