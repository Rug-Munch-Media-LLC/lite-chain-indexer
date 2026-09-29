"""bootstrap subparser — replay public B2 parquet snapshots into ClickHouse."""

from __future__ import annotations

from lite_indexer.cli._parser_types import SubParsers
from lite_indexer.strategies import SUPPORTED_CHAINS


def add_bootstrap_parsers(sub: SubParsers) -> None:
    """Register the bootstrap subparser (snapshot replay)."""
    _add_bootstrap_parser(sub)


def _add_bootstrap_parser(sub: SubParsers) -> None:
    """Register the bootstrap subparser (B2 snapshot replay)."""
    # ── bootstrap subcommand ─────────────────────────────────────────
    # Replay public B2 parquet snapshots into ClickHouse instead of
    # re-indexing from genesis over free RPCs (days → minutes). This is
    # the answer to "your backfill is RPC-bound": download the free public
    # snapshot, import via ClickHouse file(), verify row counts, and set
    # the state high-water mark so `lite-indexer daemon` resumes live.
    bootstrap_parser = sub.add_parser(
        "bootstrap",
        help="Bootstrap indexer state from public B2 parquet snapshots",
        description=(
            "Replay a public B2 parquet snapshot into ClickHouse instead "
            "of re-indexing from genesis over free RPCs. Downloads the "
            "snapshot manifest + parquet files, imports them via "
            "INSERT INTO ... SELECT * FROM file('*.parquet'), verifies "
            "row counts against the manifest, and advances the state "
            "high-water mark so the daemon resumes live from the snapshot "
            "end block."
        ),
    )
    bootstrap_parser.add_argument(
        "--chain",
        default="ethereum",
        choices=sorted(SUPPORTED_CHAINS.keys()),
        help="Chain to bootstrap (default: ethereum)",
    )
    bootstrap_parser.add_argument(
        "--all",
        action="store_true",
        help="Bootstrap every supported chain",
    )
    bootstrap_parser.add_argument(
        "--list",
        action="store_true",
        help="List available snapshots on B2 (no import)",
    )
    bootstrap_parser.add_argument(
        "--from-snapshot",
        type=str,
        default="latest",
        help="Snapshot date to use, YYYY-MM-DD, or 'latest' (default: latest)",
    )
    bootstrap_parser.add_argument(
        "--target-db",
        type=str,
        default="wallet_memory",
        help="ClickHouse database to import into (default: wallet_memory)",
    )
    bootstrap_parser.add_argument(
        "--bucket",
        type=str,
        default=None,
        help="rclone remote root for snapshots (default: BOOTSTRAP_B2_BUCKET env)",
    )
    bootstrap_parser.add_argument(
        "--snapshots-path",
        type=str,
        default=None,
        help="Path prefix inside the bucket (default: BOOTSTRAP_PARQUET_PATH env)",
    )
    bootstrap_parser.add_argument(
        "--rclone-bin",
        type=str,
        default=None,
        help="Path to the rclone binary (default: PARQUET_RCLONE_BIN env or 'rclone')",
    )
    bootstrap_parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Data directory for state files + download staging (default: DATA_DIR)",
    )
    bootstrap_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the import plan without downloading or writing",
    )
    bootstrap_parser.add_argument(
        "--source",
        type=str,
        default=None,
        help=(
            "Provider-agnostic external flat-file dataset: a local directory, an "
            "http(s) base URL, or a pyarrow.fs URL (s3://, gs://). When set, imports "
            "from it instead of the B2 snapshot (default: LITE_INDEXER_FLATFILE_SOURCE env)"
        ),
    )
    bootstrap_parser.add_argument(
        "--force",
        action="store_true",
        help="With --source: import even when the dataset overlaps the chain high-water",
    )
