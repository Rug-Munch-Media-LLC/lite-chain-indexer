"""
Snapshot manifest — the discoverability contract for B2 bootstrap snapshots.

The parquet export pipeline (``ParquetExporter.export_chain`` with
``publish_snapshot=True``, or the daily cron with ``--publish-snapshot``)
mirrors every exported ``(chain, date)`` into::

    {BOOTSTRAP_B2_BUCKET}/{BOOTSTRAP_PARQUET_PATH}/{date}/{chain}/
        manifest.json
        {chain}_{date}.parquet
        {chain}_dex_{date}.parquet

and writes a ``manifest.json`` describing what the snapshot contains::

    {
      "chain": "ethereum",
      "date": "2026-08-01",
      "start_block": 100,
      "end_block": 25400000,
      "tables": {
        "evm_transfers": {
          "rows": 359000000,
          "size_bytes": 21474836480,
          "parquet_files": ["ethereum_2026-08-01.parquet"]
        }
      }
    }

The manifest is what makes bootstrap discoverable: ``SnapshotBootstrap.
list_snapshots`` enumerates ``manifest.json`` files under the snapshot
path, ``bootstrap_chain`` reads the manifest for a ``(chain, date)`` to
learn the file list + expected row counts, imports the parquet files, and
verifies the imported row count against ``tables[*].rows``.

The ``{chain}`` component lives in the path (not the bare
``snapshots/{date}/manifest.json``) because a daily export covers 16
chains — a single date-level manifest per chain avoids 16 processes
clobbering one file. ``parse_manifest`` still accepts the single-chain
schema exactly as documented.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from lite_indexer.config import BOOTSTRAP_B2_BUCKET, BOOTSTRAP_PARQUET_PATH
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

MANIFEST_NAME = "manifest.json"


@dataclass(frozen=True)
class SnapshotTable:
    """Per-table snapshot stats from the manifest."""

    name: str
    rows: int
    size_bytes: int
    parquet_files: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Serializable form (spec schema)."""
        return {
            "rows": self.rows,
            "size_bytes": self.size_bytes,
            "parquet_files": list(self.parquet_files),
        }


@dataclass(frozen=True)
class SnapshotManifest:
    """A single (chain, date) bootstrap snapshot manifest.

    Attributes:
        chain: Chain name (``ethereum``, ``base``, ...).
        date: Snapshot date as ``YYYY-MM-DD``.
        start_block: First block included in the snapshot.
        end_block: Last block included in the snapshot.
        tables: Per-table stats keyed by ClickHouse table name.
        manifest_url: Remote path of the manifest (set by the bootstrap
            listing; empty when built locally for export).
    """

    chain: str
    date: str
    start_block: int
    end_block: int
    tables: dict[str, SnapshotTable]
    manifest_url: str = ""

    # ── builders ──────────────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        """Serializable dict matching the documented manifest schema."""
        return {
            "chain": self.chain,
            "date": self.date,
            "start_block": self.start_block,
            "end_block": self.end_block,
            "tables": {name: table.to_dict() for name, table in self.tables.items()},
        }

    def total_rows(self) -> int:
        """Sum of rows across all tables in the snapshot."""
        return sum(t.rows for t in self.tables.values())

    def total_files(self) -> int:
        """Total parquet files across all tables."""
        return sum(len(t.parquet_files) for t in self.tables.values())

    def total_size_bytes(self) -> int:
        """Total parquet payload size across all tables."""
        return sum(t.size_bytes for t in self.tables.values())

    def block_range(self) -> tuple[int, int]:
        """Return ``(start_block, end_block)``."""
        return self.start_block, self.end_block

    # ── remote path helpers ───────────────────────────────────────

    def snapshot_dir(self, bucket: str = BOOTSTRAP_B2_BUCKET, path: str = BOOTSTRAP_PARQUET_PATH) -> str:
        """Remote directory for this snapshot's parquet files.

        Args:
            bucket: rclone remote root (default ``BOOTSTRAP_B2_BUCKET``).
            path: Snapshot path prefix (default ``BOOTSTRAP_PARQUET_PATH``).

        Returns:
            Remote directory string, e.g.
            ``b2-crypt:chain-data/snapshots/2026-08-01/ethereum``.
        """
        return f"{bucket.rstrip('/')}/{path.strip('/')}/{self.date}/{self.chain}"

    def manifest_path(self, bucket: str = BOOTSTRAP_B2_BUCKET, path: str = BOOTSTRAP_PARQUET_PATH) -> str:
        """Remote path to this snapshot's ``manifest.json``.

        Args:
            bucket: rclone remote root.
            path: Snapshot path prefix.

        Returns:
            Full remote path to the manifest.
        """
        return f"{self.snapshot_dir(bucket, path)}/{MANIFEST_NAME}"


def parse_manifest(data: dict[str, Any] | str) -> SnapshotManifest:
    """Parse + validate a manifest dict or JSON string.

    Accepts the documented single-chain schema::

        {chain, date, start_block, end_block, tables: {name: {rows,
        size_bytes, parquet_files}}}

    ``tables`` entries may also be plain dicts with ``rows`` /
    ``size_bytes`` / ``parquet_files`` (missing keys default to 0/[]).

    Args:
        data: Manifest dict, or JSON string to parse.

    Returns:
        A validated :class:`SnapshotManifest`.

    Raises:
        ValueError: If the manifest is malformed (missing required keys,
            bad types, non-numeric blocks).
    """
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError as exc:
            raise ValueError(f"manifest is not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"manifest must be an object, got {type(data).__name__}")

    required = ("chain", "date", "start_block", "end_block", "tables")
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"manifest missing required keys: {', '.join(missing)}")

    chain = data["chain"]
    date = data["date"]
    if not isinstance(chain, str) or not chain:
        raise ValueError("manifest 'chain' must be a non-empty string")
    if not isinstance(date, str) or not date:
        raise ValueError("manifest 'date' must be a non-empty string")

    try:
        start_block = int(data["start_block"])
        end_block = int(data["end_block"])
    except (TypeError, ValueError) as exc:
        raise ValueError("manifest start_block/end_block must be integers") from exc
    if end_block < start_block:
        raise ValueError(f"manifest end_block ({end_block}) is below start_block ({start_block})")

    tables_raw = data["tables"]
    if not isinstance(tables_raw, dict):
        raise ValueError("manifest 'tables' must be an object")

    tables: dict[str, SnapshotTable] = {}
    for name, spec in tables_raw.items():
        if not isinstance(spec, dict):
            raise ValueError(f"table {name!r} manifest entry must be an object")
        try:
            rows = int(spec.get("rows", 0))
            size_bytes = int(spec.get("size_bytes", 0))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"table {name!r} rows/size_bytes must be integers") from exc
        files_raw = spec.get("parquet_files", [])
        files = tuple(str(f) for f in files_raw) if isinstance(files_raw, list) else ()
        tables[name] = SnapshotTable(name=name, rows=rows, size_bytes=size_bytes, parquet_files=files)

    return SnapshotManifest(
        chain=chain,
        date=date,
        start_block=start_block,
        end_block=end_block,
        tables=tables,
        manifest_url=str(data.get("manifest_url", "")),
    )


def build_manifest(
    chain: str,
    date: str,
    start_block: int,
    end_block: int,
    tables: dict[str, dict[str, Any]] | dict[str, SnapshotTable],
) -> SnapshotManifest:
    """Build a :class:`SnapshotManifest` from simple inputs.

    Args:
        chain: Chain name.
        date: Snapshot date as ``YYYY-MM-DD``.
        start_block: First block in the snapshot.
        end_block: Last block in the snapshot.
        tables: Either ``{name: SnapshotTable}`` or ``{name: {rows,
            size_bytes, parquet_files}}`` (spec schema).

    Returns:
        A validated manifest.

    Raises:
        ValueError: On invalid inputs (delegates to :func:`parse_manifest`
            semantics via ``to_dict`` + re-parse).
    """
    normalized: dict[str, SnapshotTable] = {}
    for name, spec in tables.items():
        if isinstance(spec, SnapshotTable):
            normalized[name] = spec
        elif isinstance(spec, dict):
            normalized[name] = SnapshotTable(
                name=name,
                rows=int(spec.get("rows", 0)),
                size_bytes=int(spec.get("size_bytes", 0)),
                parquet_files=tuple(str(f) for f in spec.get("parquet_files", [])),
            )
        else:
            raise ValueError(f"table {name!r} must be SnapshotTable or dict")

    manifest = SnapshotManifest(
        chain=chain,
        date=date,
        start_block=int(start_block),
        end_block=int(end_block),
        tables=normalized,
    )
    # Validate through the same code path used for untrusted input.
    parse_manifest(manifest.to_dict())
    return manifest


def manifest_json(manifest: SnapshotManifest) -> str:
    """Serialize a manifest to a pretty JSON string."""
    return json.dumps(manifest.to_dict(), indent=2, sort_keys=True)


__all__ = [
    "MANIFEST_NAME",
    "SnapshotManifest",
    "SnapshotTable",
    "build_manifest",
    "manifest_json",
    "parse_manifest",
]
