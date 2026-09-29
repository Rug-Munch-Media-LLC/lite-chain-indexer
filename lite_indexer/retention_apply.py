"""Idempotent TTL enforcement for existing ClickHouse tables.

Split out of :mod:`lite_indexer.retention` (the drift COMPARISON half) so
that module stays under its shrink-only size ratchet. Applying a policy and
reporting whether one is satisfied are separate concerns that happen to share
a table list.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import CH_DATABASE
from lite_indexer.config.storage import RETENTION_POLICY
from lite_indexer.logging import get_logger
from lite_indexer.retention import (
    fetch_live_ttls,
    parse_live_ttl,
    ttl_expression,
)

logger = get_logger(__name__)


def ensure_retention_policies(client: Any, database: str = CH_DATABASE) -> list[str]:
    """Idempotently apply table-level TTLs for every existing policy table.

    Non-fatal: a missing table or insufficient privileges degrades to a debug
    log so a startup cycle never breaks on retention DDL.

    Returns:
        The tables whose ``MODIFY TTL`` succeeded.
    """
    try:
        result = client.execute(
            "SELECT name FROM system.tables WHERE database = %(db)s AND name IN %(tables)s",
            {"db": database, "tables": tuple(sorted(RETENTION_POLICY))},
        )
        existing = {str(r[0]) for r in result}
    except Exception as exc:  # noqa: BLE001
        logger.debug("retention_table_probe_failed", error=str(exc)[:80])
        return []
    # Compare against the LIVE TTL first: an unconditional MODIFY TTL is a
    # metadata mutation on tables of 10s-100s of GiB, and this function is
    # called from the write path (ch_dex.py / ch_transfers.py). Measured
    # 2026-09-23: 218 identical ALTERs per table per 30 min saturated the
    # ext4 journal and wedged dockerd. Only ALTER on real drift.
    try:
        live = fetch_live_ttls(client, database)
    except Exception as exc:  # noqa: BLE001
        logger.debug("retention_live_ttl_probe_failed", error=str(exc)[:80])
        live = {}
    applied: list[str] = []
    for table in sorted(existing):
        rule = RETENTION_POLICY.get(table)
        if rule is None:
            continue
        parsed = parse_live_ttl(live[table]) if live.get(table) else None
        if parsed is not None and parsed[0] == rule.time_column and parsed[1] == rule.days:
            continue  # already matches the policy — no ALTER, no mutation
        try:
            client.execute(f"ALTER TABLE {database}.{table} MODIFY TTL {ttl_expression(rule)}")
            applied.append(table)
            logger.info("retention_ttl_applied", table=table, days=rule.days, column=rule.time_column)
        except Exception as exc:  # noqa: BLE001
            logger.debug("retention_ttl_skip", table=table, error=str(exc)[:80])
    return applied


__all__ = ["ensure_retention_policies"]
