"""CLI for the wallet trace and the deployer lineage.

    python -m lite_indexer.query.trace_cli trace 0xADDR --chain base
    python -m lite_indexer.query.trace_cli trace FHpc... --chain solana --depth 2
    python -m lite_indexer.query.trace_cli deployer <token-or-deployer> --chain base

Prints the JSON contract from :func:`lite_indexer.query.trace.trace` /
:func:`lite_indexer.query.trace_deployer.deployer_path`. Exit code is 0 for
a completed trace (even when the graph is empty — "nothing found" is a
result) and 2 for a usage error.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from lite_indexer.query.trace import trace
from lite_indexer.query.trace_deployer import deployer_path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trace", description="Wallet / deployer tracing over owned data"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    walk = sub.add_parser("trace", help="depth-bounded wallet graph")
    walk.add_argument("address")
    walk.add_argument("--chain", required=True)
    walk.add_argument("--depth", type=int, default=3)
    walk.add_argument("--direction", default="both", choices=("both", "in", "out"))
    walk.add_argument("--since-days", type=int, default=7)
    walk.add_argument("--max-nodes", type=int, default=40)
    walk.add_argument("--fanout", type=int, default=6)
    walk.add_argument("--limit", type=int, default=400)
    walk.add_argument(
        "--lake-dates",
        nargs="*",
        default=None,
        help="BigLake YYYY-MM-DD partitions (ethereum only)",
    )
    walk.add_argument(
        "--trim", type=int, default=0, help="print only the first N nodes and edges (0 = all)"
    )

    line = sub.add_parser("deployer", help="token-or-deployer lineage")
    line.add_argument("target")
    line.add_argument("--chain", required=True)
    line.add_argument("--depth", type=int, default=2)
    line.add_argument("--since-days", type=int, default=30)
    line.add_argument("--trim", type=int, default=0)
    return parser


def _trim(payload: dict[str, Any], trim: int) -> dict[str, Any]:
    if trim <= 0:
        return payload
    graph = payload.get("graph") or payload
    if isinstance(graph.get("nodes"), list):
        graph["nodes"] = graph["nodes"][:trim]
    if isinstance(graph.get("edges"), list):
        graph["edges"] = graph["edges"][:trim]
    return payload


def main(argv: list[str] | None = None) -> int:
    """Entry point — never raises on data errors, only on bad arguments."""
    args = _parser().parse_args(argv)
    if args.command == "trace":
        payload = trace(
            args.address,
            args.chain,
            depth=args.depth,
            direction=args.direction,
            since_days=args.since_days,
            max_nodes=args.max_nodes,
            fanout=args.fanout,
            limit=args.limit,
            lake_dates=args.lake_dates,
        )
    else:
        payload = deployer_path(
            args.target,
            args.chain,
            depth=args.depth,
            since_days=args.since_days,
        )
    json.dump(_trim(payload, args.trim), sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
