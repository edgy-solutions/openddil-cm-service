"""
openddil-cm-service entrypoint.

Boots:
  1. The baseline registry (loads ConfigurationBaseline YAMLs from /baselines)
  2. The Kafka publisher (confluent-kafka producer for asset-cm-state + tactical-events)
  3. The Restate AssetCM Virtual Object app on hypercorn ASGI server

Environment:
  CM_BASELINES_DIR        single baseline directory; default /baselines.
                          Back-compat; new deploys should prefer
                          CM_BASELINES_DIRS for multi-overlay support.
  CM_BASELINES_DIRS       colon-separated list of baseline directories
                          (e.g. "/baselines:/baselines-customer-overlay").
                          When set, REPLACES CM_BASELINES_DIR -- the two
                          are not merged. Duplicate platform_variant
                          across directories is a hard error.
  CM_KAFKA_BROKERS        Kafka bootstrap servers (default: redpanda-edge:9092)
  CM_HTTP_PORT            port the Restate endpoint listens on (default: 9080)
  CM_STALENESS_WINDOW_S   staleness threshold in seconds (default: 900)
  CM_RECHECK_MIN_DELAY_S  minimum delay between rechecks (default: 30)
  LOG_LEVEL               INFO / DEBUG / WARN (default: INFO)
  METRICS_PORT            HTTP port (default: 9464) serving Prometheus /metrics
                          and /healthz/live (503 when the producer has
                          attempted to publish but confirmed no delivery
                          within CM_PRODUCER_STALL_S; wire it to the
                          container's livenessProbe)
  CM_PRODUCER_STALL_S     window in seconds for the producer stall check
                          (default: 180)
  CM_PRODUCER_GRACE_S     seconds after start before the stall check may
                          report stalled (default: 120)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

from confluent_kafka import Producer, KafkaException
from prometheus_client.exposition import MetricsHandler

# Generated proto bindings are on PYTHONPATH (set by Dockerfile)
sys.path.insert(0, str(Path(__file__).parent))

from baselines.loader import make_registry, parse_dirs_env
from events import asset_cm
import producer_health

logger = logging.getLogger("cm_service.main")


def _configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )


def _error_cb(err) -> None:  # noqa: ANN001
    if producer_health.die_if_fatal(err, "error_cb"):
        return
    logger.warning("Kafka client error: %s", err)


def _on_delivery(err, _msg) -> None:  # noqa: ANN001
    if err is not None:
        if not producer_health.die_if_fatal(err, "delivery callback"):
            logger.warning("Kafka delivery failed: %s", err)
        return
    producer_health.note_delivered()


def _build_producer() -> Producer:
    brokers = os.getenv("CM_KAFKA_BROKERS", "redpanda-edge:9092")
    conf = {
        "bootstrap.servers": brokers,
        "acks":               "all",
        "linger.ms":          20,
        "compression.type":   "lz4",   # NOT zstd: Restate's librdkafka has no
                                      # zstd, and one zstd batch kills its
                                      # consumer task forever (it restarts
                                      # at the same batch and dies again).
                                      # The chart's lz4 topic pin is a
                                      # broker-side mitigation of THIS line;
                                      # keep both, but this is the real fix.
        "enable.idempotence": True,
        "error_cb":           _error_cb,
    }
    producer = Producer(conf)
    # Probe connectivity but don't crash on transient failures.
    try:
        producer.list_topics(timeout=5)
        logger.info("Kafka producer ready (brokers=%s)", brokers)
    except KafkaException as exc:
        logger.warning(
            "Kafka producer init returned warning (will retry on first send): %s",
            exc,
        )
    return producer


_poll_stop = threading.Event()


def _poll_loop(producer: Producer) -> None:
    # Serves delivery callbacks between publishes. Without it a callback
    # waits for the next publish, and a quiet period after one produce would
    # read as "produced, not delivered" on a healthy pod.
    while not _poll_stop.is_set():
        try:
            producer.poll(0.5)
        except Exception as exc:  # noqa: BLE001
            logger.warning("producer poll failed: %s", exc)
            _poll_stop.wait(0.5)


def _install_kafka_publisher(producer: Producer) -> None:
    def publish(topic: str, key: str, value: bytes) -> None:
        producer_health.note_produce()
        try:
            producer.produce(
                topic=topic,
                key=(key or "").encode("utf-8") if isinstance(key, str) else key,
                value=value,
                on_delivery=_on_delivery,
            )
        except KafkaException as exc:
            if exc.args:
                producer_health.die_if_fatal(exc.args[0], "produce")
            raise
        producer.poll(0)  # service delivery callbacks
    asset_cm.set_kafka_publisher(publish)
    _poll_stop.clear()
    threading.Thread(target=_poll_loop, args=(producer,), daemon=True,
                     name="producer-poll").start()
    logger.info("Kafka publisher installed on AssetCM handlers")


def _install_store_dsn() -> None:
    dsn = os.getenv("CM_STORE_DSN", "")
    asset_cm.set_store_dsn(dsn)
    if dsn:
        logger.info("store connection: CM_STORE_DSN (compose fallback)")
    elif asset_cm.store_configured():
        logger.info(
            "store connection: PG* environment (host=%s db=%s user=%s)",
            os.getenv("PGHOST", ""), os.getenv("PGDATABASE", ""),
            os.getenv("PGUSER", ""),
        )
    else:
        logger.warning(
            "store connection unset (no PGHOST, no CM_STORE_DSN): open "
            "faults are re-opened as new episodes after a Restate state wipe"
        )


def _install_baselines() -> None:
    # CM_BASELINES_DIRS (multi) takes precedence over CM_BASELINES_DIR
    # (single, back-compat default). When neither is set, fall back to
    # the historical default of /baselines so existing single-mount
    # deploys keep working with no env change.
    dirs = parse_dirs_env(os.getenv("CM_BASELINES_DIRS"))
    if not dirs:
        dirs = [os.getenv("CM_BASELINES_DIR", "/baselines")]
    registry = make_registry(dirs)
    registry.install_sighup_handler()
    asset_cm.set_baseline_registry(registry)
    logger.info("Baseline registry installed (%d baseline(s) from %d dir(s) %s)",
                len(registry.all_variants()),
                len(registry.directories()),
                [str(d) for d in registry.directories()])


async def _run_server(producer: Producer) -> None:
    """Run the Restate ASGI app under hypercorn."""
    import restate
    from hypercorn.asyncio import serve
    from hypercorn.config import Config

    app = restate.app([asset_cm.asset_cm])

    cfg = Config()
    port = int(os.getenv("CM_HTTP_PORT", "9080"))
    cfg.bind = [f"0.0.0.0:{port}"]
    cfg.accesslog = "-"
    cfg.errorlog = "-"

    # Flush Kafka producer on shutdown so in-flight messages aren't lost.
    shutdown_event = asyncio.Event()

    def _on_signal(*_args):
        logger.info("Shutdown signal received; flushing producer...")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            # Windows does not support add_signal_handler for SIGTERM. The
            # container will use SIGTERM (Linux). For host-dev we fall back.
            signal.signal(sig, lambda *_a: _on_signal())

    logger.info("Starting Restate AssetCM endpoint on :%d", port)
    server_task = asyncio.create_task(serve(app, cfg,
                                            shutdown_trigger=shutdown_event.wait))
    try:
        await server_task
    finally:
        _poll_stop.set()
        logger.info("Flushing Kafka producer (timeout=10 s)...")
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("%d producer message(s) not flushed before shutdown",
                            remaining)


class _HealthHandler(MetricsHandler):
    """/metrics (unchanged) plus /healthz/live on the same port."""

    def do_GET(self):  # noqa: N802
        if self.path.startswith("/healthz/live"):
            st = producer_health.state()
            body = json.dumps(st).encode()
            # 503 is what makes the kubelet restart the container.
            self.send_response(503 if st["stalled"] else 200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()

    def log_message(self, *_a):  # noqa: ANN002
        pass  # probes arrive every few seconds; do not narrate them


def _start_metrics_server(port: int | None = None) -> ThreadingHTTPServer | None:
    if port is None:
        port = int(os.environ.get("METRICS_PORT", "9464"))
    try:
        srv = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    except OSError as exc:
        # Non-fatal: a taken port (e.g. a second local instance during dev)
        # should not stop the service from serving its actual traffic.
        logger.warning("Could not start metrics server on :%d: %s", port, exc)
        return None
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True,
                     name="metrics-health").start()
    logger.info("Metrics/health server listening on :%d", srv.server_address[1])
    return srv


def main() -> None:
    _configure_logging()
    _start_metrics_server()
    _install_baselines()
    _install_store_dsn()
    producer = _build_producer()
    _install_kafka_publisher(producer)
    asyncio.run(_run_server(producer))


if __name__ == "__main__":
    main()
