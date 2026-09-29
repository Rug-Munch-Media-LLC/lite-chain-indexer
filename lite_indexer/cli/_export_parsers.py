"""export-parquet subparser — ClickHouse → Parquet cold-storage tier."""

from __future__ import annotations

import argparse

from lite_indexer.cli._parser_types import SubParsers
from lite_indexer.strategies import SUPPORTED_CHAINS


def add_export_parsers(sub: SubParsers) -> None:
    """Register the export-parquet subparser (cold-storage export)."""
    _add_export_parquet_parser(sub)


def _add_export_parquet_parser(sub: SubParsers) -> None:
    """Register the export-parquet subparser (ClickHouse → Parquet export)."""
    # ── export-parquet subcommand ────────────────────────────────────
    export_parser = sub.add_parser(
        "export-parquet",
        help="Export ClickHouse rows to Parquet (cold storage tier)",
        description=(
            "Export a (chain, date) into per-chain/per-date Parquet files, "
            "optionally upload them to the B2 permanent tier and delete them "
            "from ClickHouse only after the upload is verified. "
            "Idempotent: dates already exported are skipped."
        ),
    )
    export_parser.add_argument(
        "--chain",
        default="all",
        choices=sorted(SUPPORTED_CHAINS.keys()) + ["all"],
        help="Chain to export or 'all' (default: all)",
    )
    export_parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to export as YYYY-MM-DD (default: yesterday)",
    )
    export_parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Backfill mode: min row age in days before export (default: PARQUET_COLD_DAYS env or 7). "
        "Mutually exclusive with --date.",
    )
    export_parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload Parquet files to the B2 remote via rclone",
    )
    export_parser.add_argument(
        "--delete-after-upload",
        dest="delete_after_upload",
        action="store_true",
        help="Delete exported rows from ClickHouse only after the upload is verified",
    )
    export_parser.add_argument(
        "--delete",
        dest="delete_after_upload",
        action="store_true",
        help=argparse.SUPPRESS,  # legacy alias for --delete-after-upload
    )
    export_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the export without writing, uploading, or deleting",
    )
    export_parser.add_argument(
        "--remote",
        type=str,
        default=None,
        help="rclone remote root (default: PARQUET_REMOTE env or b2-crypt-v2:chain-data/parquet)",
    )
    export_parser.add_argument(
        "--publish-snapshot",
        action="store_true",
        help="Mirror each exported (chain, date) to the bootstrap snapshot "
        "location with a manifest.json so `lite-indexer bootstrap` can "
        "discover and replay it",
    )
    export_parser.add_argument(
        "--snapshot-bucket",
        type=str,
        default=None,
        help="rclone remote root for bootstrap snapshots (default: BOOTSTRAP_B2_BUCKET env or b2-crypt:chain-data)",
    )
    export_parser.add_argument(
        "--snapshot-path",
        type=str,
        default=None,
        help="Path prefix inside the snapshot bucket (default: BOOTSTRAP_PARQUET_PATH env or snapshots)",
    )
    export_parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Data directory for local Parquet output + state (default: DATA_DIR)",
    )
