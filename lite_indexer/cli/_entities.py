"""``entities`` subcommand handler — wallet-entity clustering."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from lite_indexer.entities import cex_deposit_writer as cex_writer


def run_entities(args: argparse.Namespace) -> int:
    """Run a clustering heuristic and write scored rows.

    Returns:
        Process exit code (0 = success, 1 = the run itself failed).
    """
    chains = tuple(args.chains) if args.chains else cex_writer.DEFAULT_CHAINS
    result = cex_writer.run(_connect, chains=chains, dry_run=bool(args.dry_run))
    print(json.dumps(result, indent=2, sort_keys=True))  # noqa: T201
    return 0 if result.get("ok") else 1


def _connect() -> Any:
    """Open a ClickHouse client, or None when unreachable.

    The 5 s connect timeout is deliberate: an operator running this by hand
    should learn the warehouse is down immediately, not after a hang.
    """
    import os

    try:
        from clickhouse_driver import Client

        client = Client(
            host=os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
            port=int(os.getenv("CH_PORT") or os.getenv("CLICKHOUSE_PORT") or "9000"),
            user=os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or "",
            database=os.getenv("CH_DATABASE", "wallet_memory"),
            connect_timeout=5,
            send_receive_timeout=300,
        )
        client.execute("SELECT 1")
        return client
    except Exception as exc:  # noqa: BLE001 — any connect failure means "no warehouse"
        print(f"clickhouse unavailable: {exc}", file=sys.stderr)  # noqa: T201
        return None
