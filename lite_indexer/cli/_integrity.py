"""integrity-check / reconcile / --recover CLI handlers.

Daily durable-store audit + state-recovery family. Extracted from
``lite_indexer.cli`` when it was split into a package.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from lite_indexer.storage.clickhouse import ClickHouseWriter
from lite_indexer.storage.reconciliation import run_reconciliation


async def run_reconcile_cli(args: argparse.Namespace) -> int:
    """Run the reconcile subcommand (TrueBlocks-style balance reconciliation).

    Args:
        args: Parsed CLI args (chain, sample_size).

    Returns:
        Process exit code (0 = ok, 1 = ClickHouse unavailable, 2 = mismatches found).
    """
    from lite_indexer.rpc import RpcClient

    writer = ClickHouseWriter()
    ch_client = writer.client()
    if ch_client is None:
        print(json.dumps({"status": "error", "error": "clickhouse_unavailable"}))  # noqa: T201
        return 1
    results = await run_reconciliation(
        args.chain,
        RpcClient(),
        ch_client,
        limit=args.sample_size,
    )
    status = "ok"
    if results and any(i["status"] == "MISMATCH" for i in results.values()):
        status = "mismatches_found"
    print(json.dumps({"status": status, "checked": len(results), "results": results}, default=str))  # noqa: T201
    return 0 if status == "ok" else 2


def run_integrity_check_cli(args: argparse.Namespace) -> int:
    """Run the daily integrity-check subcommand (synchronous).

    Args:
        args: Parsed CLI args (``--chains``).

    Returns:
        Process exit code (0 = clean, 1 = ClickHouse unavailable,
        2 = gaps found).
    """
    from lite_indexer.recovery import run_integrity_check, summarize_report
    from lite_indexer.strategies import SUPPORTED_CHAINS

    writer = ClickHouseWriter()
    ch_client = writer.client()
    if ch_client is None:
        print(json.dumps({"status": "error", "error": "clickhouse_unavailable"}))  # noqa: T201
        return 1

    chains = [c.strip() for c in args.chains.split(",") if c.strip()]
    chain_types = {c: SUPPORTED_CHAINS.get(c, {}).get("type", "evm") for c in chains}
    report = run_integrity_check(ch_client, chains, chain_types)
    summary = summarize_report(report)
    out: dict[str, Any] = {"status": "ok" if summary["gap_count"] == 0 else "gaps_found", **summary}
    out["chains"] = report
    print(json.dumps(out, default=str))  # noqa: T201
    return 0 if summary["gap_count"] == 0 else 2


async def run_recover_cli(
    chains: list[str],
    data_dir: str | None,
    backfill: bool,
) -> int:
    """Run the --recover state-recovery mode (async).

    Queries ClickHouse for the actual max block per chain and resets the
    state file + high-water mark to it. With ``backfill`` also rewinds
    state to the earliest missing range so the next sync re-fetches it.

    Args:
        chains: Chains to recover.
        data_dir: Optional data directory override (state files live
            under ``{data_dir}/state``).
        backfill: Whether to rewind to missing ranges for re-fetch.

    Returns:
        Process exit code (0 = all recovered, 1 = ClickHouse unavailable,
        2 = some chains failed).
    """
    from lite_indexer.recovery import recover_chain_state
    from lite_indexer.strategies import SUPPORTED_CHAINS

    writer = ClickHouseWriter()
    ch_client = writer.client()
    if ch_client is None:
        print(json.dumps({"status": "error", "error": "clickhouse_unavailable"}))  # noqa: T201
        return 1

    state_dir = Path(data_dir) / "state" if data_dir else None
    results: dict[str, Any] = {}
    failed = False
    for chain in chains:
        chain_type = SUPPORTED_CHAINS.get(chain, {}).get("type", "evm")
        res = recover_chain_state(
            ch_client,
            chain,
            chain_type=chain_type,
            state_dir=state_dir,
            backfill=backfill,
        )
        results[chain] = res
        if not res.get("ok"):
            failed = True

    out: dict[str, Any] = {
        "status": "ok" if not failed else "partial_failure",
        "backfill": backfill,
        "chains": results,
    }
    print(json.dumps(out, default=str))  # noqa: T201
    return 0 if not failed else 2
