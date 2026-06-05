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
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

from confluent_kafka import Producer, KafkaException

# Generated proto bindings are on PYTHONPATH (set by Dockerfile)
sys.path.insert(0, str(Path(__file__).parent))

from baselines.loader import make_registry, parse_dirs_env
from events import asset_cm

logger = logging.getLogger("cm_service.main")


def _configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )


def _build_producer() -> Producer:
    brokers = os.getenv("CM_KAFKA_BROKERS", "redpanda-edge:9092")
    conf = {
        "bootstrap.servers": brokers,
        "acks":               "all",
        "linger.ms":          20,
        "compression.type":   "zstd",
        "enable.idempotence": True,
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


def _install_kafka_publisher(producer: Producer) -> None:
    def publish(topic: str, key: str, value: bytes) -> None:
        producer.produce(
            topic=topic,
            key=(key or "").encode("utf-8") if isinstance(key, str) else key,
            value=value,
        )
        producer.poll(0)  # service delivery callbacks
    asset_cm.set_kafka_publisher(publish)
    logger.info("Kafka publisher installed on AssetCM handlers")


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
        logger.info("Flushing Kafka producer (timeout=10 s)...")
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("%d producer message(s) not flushed before shutdown",
                            remaining)


def main() -> None:
    _configure_logging()
    _install_baselines()
    producer = _build_producer()
    _install_kafka_publisher(producer)
    asyncio.run(_run_server(producer))


if __name__ == "__main__":
    main()
