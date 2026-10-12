"""Read-only access to a tier's own asset_cm_state row.

Restate holds an asset's whole record, and the upgrade-time Restate wipe
destroys it. The tier store survives that wipe, and its row carries the same
manual discrepancies this service last emitted, so the store (not Restate) is
where a fault's first-observed time is kept. This is the only module that
talks to postgres, and it only reads: the connection is read-only by
construction and the single statement is a SELECT. Only manual discrepancies
are restored; analyzer discrepancies are recomputed from the baseline, so they
are not custody records. A reset empties the store, so after a reset nothing
is found here and a fault opens a new episode, as intended.
"""

from __future__ import annotations

import json

import asyncpg


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
