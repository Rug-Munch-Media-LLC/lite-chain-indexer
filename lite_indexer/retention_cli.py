"""Retention drift CLI — the ``python3 -m lite_indexer.retention`` entry point.

Extracted from ``lite_indexer/retention.py`` (shrink-only size ratchet): the
ClickHouse connection helper, the drift report renderer and the process exit
code live here. The pure parsing/diffing helpers stay in
:mod:`lite_indexer.retention`, which re-exports ``main``/``_connect`` so
existing importers keep working.

Exit 0 = no drift OR ClickHouse unreachable (a gate that cannot reach CH must
not red the build); exit 1 = a provisioned table diverges from the policy.
"""

from __future__ import annotations

import os
import sys
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.retention import evaluate_drift, fetch_live_ttls, format_report

logger = get_logger(__name__)


def _connect() -> Any | None:
    """Build a ClickHouse client, or None when the driver is unavailable."""
    try:
        from clickhouse_driver import Client as CHClient
    except Exception as exc:  # noqa: BLE001
        logger.debug("retention_clickhouse_import_failed", error=str(exc)[:80])
        return None
    try:
        return CHClient(
            host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(os.environ.get("CLICKHOUSE_PORT", "9000")),
            user=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("retention_clickhouse_client_failed", error=str(exc)[:80])
        return None


def main() -> int:
    """Run the retention drift check. Returns the process exit code."""
    # EXIT CODES: 0 = verified, 1 = real drift, 2 = INDETERMINATE (could not
    # measure). Previously an unreachable ClickHouse and a zero-row read BOTH
    # exited 0 while printing "skipped" or "OK - 0/10 tables present", so a
    # gate that had verified nothing reported success. 2 is distinct from 1 so
    # a CI policy can red on real drift while not red on an outage.
    client = _connect()
    if client is None:
        print("CH unavailable — retention drift INDETERMINATE (exit 2)")  # noqa: T201
        return 2
    try:
        live = fetch_live_ttls(client)
    except Exception as exc:  # noqa: BLE001 — reported as indeterminate, not ok
        print(f"CH unavailable — retention drift INDETERMINATE (exit 2): {str(exc)[:200]}")  # noqa: T201
        return 2
    rows = evaluate_drift(live)
    print(format_report(rows))  # noqa: T201
    unknown = [row for row in rows if row.status == "indeterminate"]
    if unknown:
        print(  # noqa: T201
            f"\nRETENTION INDETERMINATE: the read returned no rows for "
            f"{len(unknown)} policy table(s). That is a zero-row read, not a "
            "fresh deployment — cannot distinguish a wrong database or a "
            "filtered query from tables that genuinely do not exist."
        )
        return 2
    drift = [row for row in rows if not row.ok]
    if drift:
        print(f"\nRETENTION DRIFT: {len(drift)} table(s) diverge from the policy.")  # noqa: T201
        print("  apply migrations/021_retention_classes.sql (docs/CLICKHOUSE_RETENTION.md).")  # noqa: T201
        return 1
    present = sum(1 for row in rows if row.status != "absent")
    if present == 0:
        # Defensive: evaluate_drift already returns indeterminate for this, so
        # reaching here would mean the two disagree.
        print("\nRETENTION INDETERMINATE: no policy table was verified present (exit 2).")  # noqa: T201
        return 2
    print(f"\nRETENTION OK — {present}/{len(rows)} policy tables present and matching.")  # noqa: T201
    return 0


if __name__ == "__main__":
    sys.exit(main())
