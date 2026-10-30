"""Read-only access to a tier's own asset_cm_state row.

Restate holds an asset's whole record, and the upgrade-time Restate wipe
destroys it. The tier store survives that wipe, and its row carries the same
manual discrepancies, installed parts and mod history this service last
emitted, so the store (not Restate) is where a fault's first-observed time and
the applied mods and replaced parts are kept. This is the only module that
talks to postgres, and it only reads: the connection is read-only by
construction and the single statement is a SELECT. Manual discrepancies, parts
and mod history are restored; analyzer discrepancies are still recomputed from
the baseline, so they are not custody records. A reset empties the store, so after a reset nothing
is found here and a fault opens a new episode, as intended.
"""

from __future__ import annotations

import json
import logging

import asyncpg

logger = logging.getLogger(__name__)


async def read_manual_discrepancies(
    dsn: str | None, asset_id: str,
) -> list[dict] | None:
    """Return the stored manual discrepancies, or None when there is no row.

    `dsn` None means the standard PG* environment (asyncpg reads it).
    Errors propagate: the caller must not treat an unreachable store as an
    empty one.
    """
    conn = await asyncpg.connect(
        dsn, server_settings={"default_transaction_read_only": "on"}, timeout=5,
    )
    try:
        row = await conn.fetchrow(
            "SELECT manual_discrepancies FROM asset_cm_state WHERE asset_id = $1",
            asset_id,
        )
    finally:
        await conn.close()
    if row is None:
        return None
    value = row["manual_discrepancies"]
    if isinstance(value, (str, bytes)):
        value = json.loads(value)
    return value if isinstance(value, list) else []


async def read_maintained_history(dsn: str | None, asset_id: str) -> dict | None:
    """Return the stored baseline id, installed parts and mod history.

    None when there is no row. The result is
    {"baseline_id": str | None, "installed": list, "mod_status": list}.
    Errors propagate, as in read_manual_discrepancies.
    """
    conn = await asyncpg.connect(
        dsn, server_settings={"default_transaction_read_only": "on"}, timeout=5,
    )
    try:
        row = await conn.fetchrow(
            "SELECT baseline_id, installed, mod_status FROM asset_cm_state "
            "WHERE asset_id = $1",
            asset_id,
        )
    finally:
        await conn.close()
    if row is None:
        return None

    def _as_list(value):
        if isinstance(value, (str, bytes)):
            value = json.loads(value)
        return value if isinstance(value, list) else []

    return {
        "baseline_id": row["baseline_id"],
        "installed": _as_list(row["installed"]),
        "mod_status": _as_list(row["mod_status"]),
    }


async def read_declared_baseline(dsn: str | None, asset_id: str) -> str | None:
    """Return the baseline id the asset's declaration names, or None.

    None covers no declaration row, an empty baseline id, and a store older
    than the declaration schema (the table is absent). `dsn` None means the
    standard PG* environment. Any other error propagates: the caller must not
    treat an unreachable store as "no declaration".
    """
    conn = await asyncpg.connect(
        dsn, server_settings={"default_transaction_read_only": "on"}, timeout=5,
    )
    try:
        row = await conn.fetchrow(
            "SELECT baseline_id FROM asset_declaration WHERE asset_id = $1",
            asset_id,
        )
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("asset_declaration table is absent; no declared baselines")
        return None
    finally:
        await conn.close()
    if row is None:
        return None
    return row["baseline_id"] or None
