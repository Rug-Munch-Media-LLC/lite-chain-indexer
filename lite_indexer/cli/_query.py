"""query subcommand handler (on-demand historical RPC)."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any


async def run_query_cli(args: argparse.Namespace) -> int:
    """Run the on-demand historical RPC ``query`` subcommand (async).

    Builds a :class:`lite_indexer.query.QueryRouter` over ClickHouse (hot),
    the B2 parquet cold store, and the live RPC pool, then answers exactly
    one of: --tx / --address / --block / --deployer.

    Args:
        args: Parsed CLI args.

    Returns:
        Process exit code (0 = ok, 1 = runtime error, 2 = usage error).
    """
    from lite_indexer.config import HISTORICAL_PARQUET_REMOTE, ON_DEMAND_RPC_ENABLED
    from lite_indexer.query import QueryRouter
    from lite_indexer.storage.clickhouse import ClickHouseWriter
    from lite_indexer.storage.cold_store import ColdStore

    modes = [bool(args.tx), bool(args.address), args.block is not None, bool(args.deployer)]
    if sum(modes) != 1:
        print("error: exactly one of --tx / --address / --block / --deployer is required", file=sys.stderr)  # noqa: T201
        return 2
    if (args.from_block is not None or args.to_block is not None) and not args.address:
        print("error: --from-block/--to-block only apply to --address queries", file=sys.stderr)  # noqa: T201
        return 2

    writer = ClickHouseWriter()
    ch_client = writer.client()
    router = QueryRouter(
        ch_client=ch_client,
        cold_store=ColdStore(remote=HISTORICAL_PARQUET_REMOTE),
        on_demand_enabled=ON_DEMAND_RPC_ENABLED and not args.no_rpc,
    )

    out: dict[str, Any] = {"chain": args.chain, "on_demand_rpc": router.on_demand_enabled}
    if args.tx:
        row = await router.get_transfer(args.chain, args.tx)
        out["kind"] = "transfer"
        out["found"] = row is not None
        out["result"] = row
    elif args.address:
        rows = await router.get_transfers(
            args.chain,
            args.address,
            from_block=args.from_block,
            to_block=args.to_block,
            limit=args.limit,
        )
        out["kind"] = "transfers"
        out["count"] = len(rows)
        out["results"] = rows
    elif args.block is not None:
        block = await router.get_block(args.chain, args.block)
        out["kind"] = "block"
        out["found"] = bool(block)
        out["result"] = block
    elif args.deployer:
        info = await router.get_deployer_info(args.chain, args.deployer)
        out["kind"] = "deployer"
        out["found"] = info is not None
        out["result"] = info

    print(json.dumps(out, default=str))  # noqa: T201
    return 0
