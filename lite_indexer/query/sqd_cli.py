"""CLI for :mod:`lite_indexer.query.sqd_source`.

``python -m lite_indexer.query.sqd_source <cmd> ...`` — head / stream / the
per-chain readers. Extracted so the facade stays a portal client.
"""

from __future__ import annotations

import argparse
import json
import logging

from lite_indexer.query.sqd_source import SQDSource


def _cmd_evm_traces(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.evm_traces(
        args.chain,
        int(args.from_block, 0),
        int(args.to_block, 0),
        address=args.address,
        limit=args.limit,
    )
    print(json.dumps(rows[: args.limit], default=str, indent=2))  # noqa: T201
    return 0


def _cmd_head(args: argparse.Namespace) -> int:
    src = SQDSource()
    number = src.head(args.dataset)
    print(json.dumps({"dataset": args.dataset, "head": number}))  # noqa: T201
    return 0 if number is not None else 1


def _cmd_stream(args: argparse.Namespace) -> int:
    src = SQDSource()
    fields = json.loads(args.fields)
    selectors = json.loads(args.selectors) if args.selectors else None
    blocks = src.stream(
        args.dataset,
        int(args.from_block, 0),
        int(args.to_block, 0) if args.to_block else None,
        fields=fields,
        selectors=selectors,
        max_batches=args.max_batches,
    )
    print(json.dumps(blocks[: args.limit], default=str, indent=2))  # noqa: T201
    return 0


def _cmd_owner_activity(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.solana_owner_activity(args.owner, int(args.from_slot, 0), int(args.to_slot, 0), limit=args.limit)
    print(json.dumps(rows[: args.limit], default=str, indent=2))  # noqa: T201
    return 0


def _cmd_token_balances(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.solana_token_balances_for_accounts(
        list(args.accounts), int(args.from_slot, 0), int(args.to_slot, 0), limit=args.limit
    )
    print(json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


def _cmd_tron_transfers(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.tron_trc20_transfers(args.address, int(args.from_block, 0), int(args.to_block, 0), limit=args.limit)
    print(json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


def _cmd_tron_internal(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.tron_internal_transfers(args.address, int(args.from_block, 0), int(args.to_block, 0), limit=args.limit)
    print(json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


def _cmd_hl_fills(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.hyperliquid_owner_fills(args.user, int(args.from_block, 0), int(args.to_block, 0), limit=args.limit)
    print(json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


def _silence_console_logs() -> None:
    """CLI stdout must stay machine-parseable (JSON only); logs go to stderr."""
    try:
        from loguru import logger as _loguru

        _loguru.remove()
    except ImportError:
        logging.getLogger(__name__).debug("loguru_absent_default_logger_kept")


def _cli() -> int:
    _silence_console_logs()
    parser = argparse.ArgumentParser(
        prog="sqd_source",
        description="SQDSource — on-demand history via the SQD Portal API (portal.sqd.dev).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_h = sub.add_parser("head", help="latest block of a dataset")
    p_h.add_argument("dataset")
    p_h.set_defaults(fn=_cmd_head)

    p_s = sub.add_parser("stream", help="raw paginated stream")
    p_s.add_argument("dataset")
    p_s.add_argument("--from-block", required=True)
    p_s.add_argument("--to-block", default=None)
    p_s.add_argument("--fields", required=True, help='JSON, e.g. \'{"block":{"number":true}}\'')
    p_s.add_argument(
        "--selectors",
        default=None,
        help='JSON, e.g. \'{"tokenBalances":[{"postOwner":["addr"]}]}\'',
    )
    p_s.add_argument("--max-batches", type=int, default=4)
    p_s.add_argument("--limit", type=int, default=20)
    p_s.set_defaults(fn=_cmd_stream)

    p_o = sub.add_parser("owner-activity", help="Solana instructions mentioning an owner")
    p_o.add_argument("owner")
    p_o.add_argument("--from-slot", required=True)
    p_o.add_argument("--to-slot", required=True)
    p_o.add_argument("--limit", type=int, default=200)
    p_o.set_defaults(fn=_cmd_owner_activity)

    p_b = sub.add_parser("token-balances", help="Solana token balance diffs for token accounts")
    p_b.add_argument("accounts", nargs="+")
    p_b.add_argument("--from-slot", required=True)
    p_b.add_argument("--to-slot", required=True)
    p_b.add_argument("--limit", type=int, default=200)
    p_b.set_defaults(fn=_cmd_token_balances)

    p_tt = sub.add_parser("tron-transfers", help="Tron TRC-20 wallet transfers")
    p_tt.add_argument("address")
    p_tt.add_argument("--from-block", required=True)
    p_tt.add_argument("--to-block", required=True)
    p_tt.add_argument("--limit", type=int, default=100)
    p_tt.set_defaults(fn=_cmd_tron_transfers)

    p_ti = sub.add_parser("tron-internal", help="Tron native internal transactions for a wallet")
    p_ti.add_argument("address")
    p_ti.add_argument("--from-block", required=True)
    p_ti.add_argument("--to-block", required=True)
    p_ti.add_argument("--limit", type=int, default=100)
    p_ti.set_defaults(fn=_cmd_tron_internal)

    p_hf = sub.add_parser("hl-fills", help="Hyperliquid fills for a user")
    p_hf.add_argument("user")
    p_hf.add_argument("--from-block", required=True)
    p_hf.add_argument("--to-block", required=True)
    p_hf.add_argument("--limit", type=int, default=100)
    p_hf.set_defaults(fn=_cmd_hl_fills)

    p_et = sub.add_parser("evm-traces", help="Full-history EVM traces (internal transfers)")
    p_et.add_argument("chain", choices=sorted(SQDSource.EVM_DATASETS))
    p_et.add_argument("--from-block", required=True)
    p_et.add_argument("--to-block", required=True)
    p_et.add_argument("--address", default=None, help="wallet filter (callFrom OR callTo)")
    p_et.add_argument("--limit", type=int, default=100)
    p_et.set_defaults(fn=_cmd_evm_traces)

    args = parser.parse_args()
    return int(args.fn(args))


def cli() -> int:
    """Console entry point (also used by ``python -m lite_indexer.query.sqd_source``)."""
    return _cli()
