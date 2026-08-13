"""bootstrap subcommand handler (public B2 snapshot replay)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


async def run_bootstrap_cli(args: argparse.Namespace) -> int:
    """Run the bootstrap subcommand (async).

    Handles ``--list`` (enumerate snapshots), ``--chain`` (one chain),
    ``--all`` (every supported chain), ``--from-snapshot`` (date or
    latest), and ``--dry-run`` (preview without downloading/importing).

    Args:
        args: Parsed CLI args.

    Returns:
        Process exit code (0 = success, 1 = partial/full failure,
        2 = usage error).
    """
    from lite_indexer.bootstrap import SnapshotBootstrap
    from lite_indexer.config import BOOTSTRAP_B2_BUCKET, BOOTSTRAP_PARQUET_PATH, PARQUET_RCLONE_BIN
    from lite_indexer.strategies import SUPPORTED_CHAINS

    if args.all and args.chain != "ethereum":
        print("error: --all and --chain are mutually exclusive", file=sys.stderr)  # noqa: T201
        return 2
    if args.list and args.all:
        print("error: --list and --all are mutually exclusive", file=sys.stderr)  # noqa: T201
        return 2

    from lite_indexer.config import DATA_DIR

    data_path = Path(args.data_dir) if args.data_dir else DATA_DIR
    bootstrapper = SnapshotBootstrap(
        bucket=args.bucket or BOOTSTRAP_B2_BUCKET,
        snapshots_path=args.snapshots_path or BOOTSTRAP_PARQUET_PATH,
        rclone_bin=args.rclone_bin or PARQUET_RCLONE_BIN,
        local_dir=data_path / "bootstrap",
        state_dir=data_path / "state",
    )

    if args.list:
        snapshots = await bootstrapper.list_snapshots(chain=None)
        out: dict[str, Any] = {"status": "ok", "count": len(snapshots), "snapshots": snapshots}
        print(json.dumps(out, default=str))  # noqa: T201
        return 0

    if args.all:
        chains = sorted(SUPPORTED_CHAINS.keys())
        result = await bootstrapper.bootstrap_all(
            chains=chains,
            snapshot_date=args.from_snapshot,
            target_db=args.target_db,
            dry_run=args.dry_run,
        )
    else:
        result = await bootstrapper.bootstrap_chain(
            args.chain,
            snapshot_date=args.from_snapshot,
            target_db=args.target_db,
            dry_run=args.dry_run,
        )

    print(json.dumps(result, default=str))  # noqa: T201
    status = result.get("status", "error") if isinstance(result, dict) else "error"
    return 0 if status in ("ok", "dry_run", "no_snapshot") else 1
