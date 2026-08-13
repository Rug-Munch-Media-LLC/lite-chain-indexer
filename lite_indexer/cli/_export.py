"""export-parquet cold-storage subcommand handler."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any


def run_export_parquet(args: argparse.Namespace) -> int:
    """Run the export-parquet cold-storage subcommand (synchronous).

    Args:
        args: Parsed CLI args (chain, date, days, upload,
            delete_after_upload, dry_run, remote, publish_snapshot,
            snapshot_bucket, snapshot_path, data_dir).

    Returns:
        Process exit code (0 = success, 2 = usage error).
    """
    from lite_indexer.config import (
        BOOTSTRAP_B2_BUCKET,
        BOOTSTRAP_PARQUET_PATH,
        PARQUET_COLD_DAYS,
        PARQUET_DIR,
        PARQUET_REMOTE,
    )
    from lite_indexer.storage.parquet import ParquetExporter
    from lite_indexer.strategies import SUPPORTED_CHAINS

    if args.delete_after_upload and not args.upload:
        print("error: --delete-after-upload requires --upload", file=sys.stderr)  # noqa: T201
        return 2
    if args.date and args.days:
        print("error: --date and --days are mutually exclusive", file=sys.stderr)  # noqa: T201
        return 2

    day: date | None = None
    if args.date:
        try:
            day = date.fromisoformat(args.date)
        except ValueError:
            print(f"error: invalid --date {args.date!r}; expected YYYY-MM-DD", file=sys.stderr)  # noqa: T201
            return 2
    elif not args.days:
        # Default: yesterday.
        day = datetime.now(UTC).date() - timedelta(days=1)

    data_dir = Path(args.data_dir) if args.data_dir else PARQUET_DIR
    remote = args.remote or PARQUET_REMOTE
    exporter = ParquetExporter(data_dir=data_dir, remote=remote)

    chains = sorted(SUPPORTED_CHAINS.keys()) if args.chain == "all" else [args.chain]

    summaries: list[dict[str, Any]] = []
    failed = False
    for chain in chains:
        summary = exporter.export_chain(
            chain,
            dates=[day] if day is not None else None,
            cold_days=args.days if args.days and args.days > 0 else PARQUET_COLD_DAYS,
            upload=args.upload,
            delete=args.delete_after_upload,
            dry_run=args.dry_run,
            publish_snapshot=args.publish_snapshot,
            snapshot_bucket=args.snapshot_bucket or BOOTSTRAP_B2_BUCKET,
            snapshot_path=args.snapshot_path or BOOTSTRAP_PARQUET_PATH,
        )
        summaries.append(summary.to_dict())
        if summary.errors:
            failed = True

    out: dict[str, Any] | list[dict[str, Any]] = summaries[0] if len(summaries) == 1 else {"chains": summaries}
    print(json.dumps(out, default=str))  # noqa: T201
    return 1 if failed else 0
