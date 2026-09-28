"""Hot/cold retention enforcement + drift detection.

Single source of truth is :data:`lite_indexer.config.RETENTION_POLICY`
(mirrored by ``migrations/021_retention_classes.sql`` and documented in
``docs/CLICKHOUSE_RETENTION.md``).

* :func:`ensure_retention_policies` — idempotently applies the table-level
  ``MODIFY TTL`` for every policy table that exists, so a fresh deployment
  gets the same TTLs the migration applies. Missing tables / privileges are
  non-fatal (mirrors ``SchemaMixin.ensure_ttl_policies``).
* :func:`parse_live_ttl` / :func:`evaluate_drift` — pure helpers that parse
  the live ``system.tables.engine_full`` / ``create_table_query`` and diff it
  against the policy.
* :func:`main` — CLI drift checker. Exit 0 = no drift OR ClickHouse
  unreachable (a gate that cannot reach CH must not red the build); exit 1 =
  a provisioned table diverges from the policy.

Run ``python3 -m lite_indexer.retention`` or
``scripts/check-retention-drift.sh``.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from lite_indexer.config import CH_DATABASE, RETENTION_POLICY, RetentionRule
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Matches a TTL clause in a ClickHouse DDL string. ClickHouse normalises
# INTERVAL literals to toIntervalDay()/toIntervalMonth() in SHOW CREATE
# output, but accept both spellings. Column names may be backticked.
_TTL_RE = re.compile(
    r"\bTTL\s+"
    r"`?(?P<col>[A-Za-z_][A-Za-z0-9_]*)`?\s*\+\s*"
    r"(?:toIntervalDay\((?P<days>\d+)\)"
    r"|INTERVAL\s+(?P<days_iv>\d+)\s+DAY"
    r"|toIntervalMonth\((?P<months>\d+)\)"
    r"|INTERVAL\s+(?P<months_iv>\d+)\s+MONTH)",
    re.IGNORECASE,
)

_DAYS_PER_MONTH = 30


@dataclass(frozen=True)
class DriftRow:
    """One policy table's live-vs-expected retention comparison."""

    table: str
    expected_column: str
    expected_days: int
    ok: bool
    status: str  # ok | drift | no-ttl | absent
    actual_column: str = ""
    actual_days: int | None = None


def ttl_expression(rule: RetentionRule) -> str:
    """Return the canonical table-level TTL expression for ``rule``."""
    return f"{rule.time_column} + INTERVAL {rule.days} DAY"


def _days_from_match(match: re.Match[str]) -> int:
    for group in ("days", "days_iv"):
        value = match.group(group)
        if value is not None:
            return int(value)
    for group in ("months", "months_iv"):
        value = match.group(group)
        if value is not None:
            return int(value) * _DAYS_PER_MONTH
    raise ValueError(f"unparseable TTL interval: {match.group(0)!r}")


def parse_live_ttl(ddl: str) -> tuple[str, int] | None:
    """Parse the table-level ``(time_column, days)`` TTL from a DDL string.

    ClickHouse emits column-level TTLs *before* the table-level TTL in
    ``SHOW CREATE`` output, so the LAST match is the table-level clause.

    Returns:
        ``(time_column, days)`` or ``None`` when no TTL exists.
        ``toIntervalMonth(n)`` is normalised as ``n * 30`` days.
    """
    matches = list(_TTL_RE.finditer(ddl))
    if not matches:
        return None
    last = matches[-1]
    return last.group("col"), _days_from_match(last)


def evaluate_drift(live: Mapping[str, str]) -> list[DriftRow]:
    """Diff live DDLs (``table -> engine_full``) against the policy.

    Tables absent from ``live`` are reported as ``absent`` and are **not**
    drift: a fresh deployment legitimately lacks them, and the runtime
    ``ensure_*`` DDL (or migration 013) creates them with the policy TTL.
    """
    rows: list[DriftRow] = []
    for table in sorted(RETENTION_POLICY):
        rule = RETENTION_POLICY[table]
        ddl = live.get(table)
        if ddl is None:
            rows.append(DriftRow(table, rule.time_column, rule.days, ok=True, status="absent"))
            continue
        parsed = parse_live_ttl(ddl)
        if parsed is None:
            rows.append(DriftRow(table, rule.time_column, rule.days, ok=False, status="no-ttl"))
            continue
        actual_column, actual_days = parsed
        ok = actual_column == rule.time_column and actual_days == rule.days
        rows.append(
            DriftRow(
                table,
                rule.time_column,
                rule.days,
                ok=ok,
                status="ok" if ok else "drift",
                actual_column=actual_column,
                actual_days=actual_days,
            )
        )
    return rows


def fetch_live_ttls(client: Any, database: str = CH_DATABASE) -> dict[str, str]:
    """Read ``engine_full`` (fallback ``create_table_query``) per policy table."""
    result = client.execute(
        "SELECT name, engine_full, create_table_query FROM system.tables "
        "WHERE database = %(db)s AND name IN %(tables)s",
        {"db": database, "tables": tuple(sorted(RETENTION_POLICY))},
    )
    live: dict[str, str] = {}
    for row in result:
        name, engine_full, create_query = row[0], row[1], row[2]
        live[str(name)] = str(engine_full or create_query or "")
    return live


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


def format_report(rows: list[DriftRow]) -> str:
    """Render the drift comparison as a fixed-width text table."""
    lines = [f"{'TABLE':<22} {'STATUS':<8} {'EXPECTED':<34} ACTUAL"]
    for row in rows:
        expected = f"{row.expected_column} + {row.expected_days}d"
        actual = "-" if row.actual_days is None else f"{row.actual_column} + {row.actual_days}d"
        lines.append(f"{row.table:<22} {row.status:<8} {expected:<34} {actual}")
    return "\n".join(lines)


# CLI half — the process entry point lives in retention_cli (size ratchet);
# re-exported so ``from lite_indexer.retention import main`` keeps working.
from lite_indexer.retention_cli import _connect, main  # noqa: E402

__all__ = [
    "DriftRow",
    "_connect",
    "ensure_retention_policies",
    "evaluate_drift",
    "fetch_live_ttls",
    "format_report",
    "main",
    "parse_live_ttl",
    "ttl_expression",
]


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())
