"""Backfill `failure-episodes` from live AssetCM state and from export files.

Not wired into startup or a Job (a chart follow-up). Run from src/:

    python backfill_failure_episodes.py --admin http://restate-admin:9070 \
        --file ep-1.jsonl --dry-run

Every episode, live or from a file, goes through
`events.failure_episodes.build_record`, the same builder the per-close emit
uses. Within one run each (episode_id, detected_at_ns) is emitted once.
Prints one JSON summary line on stdout. Never prints a DSN, token or URL
credentials.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).parent))

from events import failure_episodes as fe  # noqa: E402
from metrics import cm_failure_episode_withheld_total  # noqa: E402

QUERY = ("select service_key, value_utf8 from state "
         "where service_name='AssetCM' and key='am_state'")


def _safe_url(url: str) -> str:
    """scheme://host[:port] only: no credentials, path or query."""
    p = urlsplit(url)
    host = p.hostname or ""
    return f"{p.scheme}://{host}" + (f":{p.port}" if p.port else "")


def fetch_state_rows(admin: str) -> list[dict]:
    """POST the state query to <admin>/query; rows as dicts. Injectable."""
    req = urllib.request.Request(
        admin.rstrip("/") + "/query",
        data=json.dumps({"query": QUERY}).encode("utf-8"),
        headers={"content-type": "application/json", "accept": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
        body = json.loads(resp.read().decode("utf-8"))
    return list(body.get("rows") or [])


def build_kafka_publisher():
    """The same producer config main.py uses (its builder is reused)."""
    import main as cm_main  # imports confluent_kafka; only on the real path
    producer = cm_main._build_producer()

    def publish(topic: str, key: str, value: bytes) -> None:
        # produces: failure-episodes
        producer.produce(topic=topic, key=key.encode("utf-8"), value=value)
        producer.poll(0)

    publish.flush = lambda: producer.flush(30)  # type: ignore[attr-defined]
    return publish


def _iter_state(admin: str, fetch):
    for row in fetch(admin):
        asset_id = row.get("service_key") or ""
        raw = row.get("value_utf8")
        try:
            st = json.loads(raw or "")
        except (TypeError, ValueError):
            # eps is None: the caller withholds the raw value, the only copy.
            yield asset_id, None, None, None, raw if isinstance(raw, str) else ""
            continue
        labels = None
        if "originator_nation" in st:
            labels = {"originator_nation": st.get("originator_nation") or "",
                      "releasable_to": list(st.get("releasable_to") or [])}
        tier = {"edge_id": st.get("edge_id", ""), "region_id": st.get("region_id", "")}
        eps = st.get("closed_episodes") or []
        yield asset_id, labels, tier, eps, None


def run(admins, files, dry_run=False, fetch=fetch_state_rows, publish=None) -> dict:
    emitted = 0
    withheld_published = 0
    duplicate = 0
    refusals: list[dict] = []
    skipped_summary = 0
    seen: set[tuple[str, int]] = set()

    def withhold(asset_id, labels, tier, episode, refusal):
        nonlocal withheld_published
        if dry_run:
            return
        publish(fe.WITHHELD_TOPIC, asset_id, fe.build_withheld(
            asset_id, labels, tier, episode, refusal, "backfill"))
        cm_failure_episode_withheld_total.labels(
            stage="backfill", reason=refusal["reason"]).inc()
        withheld_published += 1

    def handle(source, asset_id, labels, tier, episode):
        nonlocal emitted, duplicate
        value, refusal = fe.build_record(asset_id, labels, tier, episode)
        if refusal is not None:
            refusals.append({"source": source, **refusal})
            withhold(asset_id, labels, tier, episode, refusal)
            return
        ident = (episode["discrepancy_id"], int(episode["detected_at_ns"]))
        if ident in seen:
            duplicate += 1
            return
        seen.add(ident)
        if not dry_run:
            publish(fe.TOPIC, asset_id, value)
        emitted += 1

    for admin in admins:
        src = "state:" + _safe_url(admin)
        for asset_id, labels, tier, eps, raw in _iter_state(admin, fetch):
            if eps is None:
                refusal = {"reason": "unparseable", "asset_id": asset_id,
                           "discrepancy_id": ""}
                refusals.append({"source": src, **refusal})
                withhold(asset_id, None, None, {"raw": raw}, refusal)
                continue
            for ep in eps:
                handle(src, asset_id, labels, tier, ep)

    for path in files:
        with open(path, "rb") as fh:
            for lineno, raw in enumerate(fh, 1):
                if not raw.strip():
                    continue
                src = f"{path}:{lineno}"
                try:
                    obj = json.loads(raw.decode("utf-8"))
                    if not isinstance(obj, dict):
                        raise ValueError("not an object")
                except ValueError:
                    refusal = {"reason": "unparseable", "asset_id": "",
                               "discrepancy_id": ""}
                    refusals.append({"source": src, **refusal})
                    withhold("", None, None,
                             {"raw": raw.decode("utf-8", errors="replace")}, refusal)
                    continue
                if "summary" in obj:
                    skipped_summary += 1
                    continue
                handle(src, obj.get("asset") or "", obj.get("asset_labels"),
                       obj.get("asset_tier"), obj.get("episode") or {})

    counts: dict[str, int] = {}
    for r in refusals:
        counts[r["reason"]] = counts.get(r["reason"], 0) + 1
    return {"emitted": emitted, "duplicate": duplicate, "refused": counts,
            "refusals": refusals, "skipped_summary_lines": skipped_summary,
            "withheld_published": withheld_published}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--admin", action="append", default=[])
    ap.add_argument("--file", action="append", default=[])
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    publish = None if a.dry_run else build_kafka_publisher()
    summary = run(a.admin, a.file, a.dry_run, publish=publish)
    if publish is not None:
        publish.flush()
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
