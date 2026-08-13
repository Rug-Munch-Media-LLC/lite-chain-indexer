"""
Parquet staging compaction — collapse hourly micro-batches into hive files.

The daily export (``scripts/lite-indexer-parquet-daily.sh`` /
``lite_indexer.storage.parquet``) writes one small file per chain/date/hour
into the staging tree::

    b2-crypt:chain-data/parquet/{chain}/{YYYY}/{MM}/{DD}/{HH}/{chain}_{YYYY-MM-DD}.parquet
    b2-crypt:chain-data/parquet/{chain}/{YYYY}/{MM}/{DD}/{HH}/{chain}_dex_{YYYY-MM-DD}.parquet

Thousands of tiny files are slow and expensive to scan. This module
compacts one partition (a month, or a day for high-volume chains) of
staging micro-batches into a small number of large files under a hive
partition layout::

    {prefix}/hive/chain={chain_id}/month={YYYY-MM}/part-00000.parquet
    {prefix}/hive/chain={chain_id}/day={YYYY-MM-DD}/part-00000.parquet

Compacted files are:

* **sorted by block_number** (ascending) so range scans are local,
* **zstd compressed, level 3** (good ratio, cheap decode),
* **fixed_size_binary(20)** for EVM addresses and **fixed_size_binary(32)**
  for tx/block hashes (via pyarrow), saving ~50% vs 42-char hex strings.

Idempotency + crash safety
--------------------------
The compaction is write-first, delete-second:

1. all parts are written to ``.tmp-*`` names and verified (row count read
   back from the parquet footer),
2. verified parts are moved to their final ``part-*.parquet`` names,
3. a ``_manifest.json`` is written recording the exact staging keys that
   were consumed,
4. only then are the staging micro-batches deleted (each delete is
   idempotent — missing keys are ignored),
5. the manifest's ``staging_deleted`` flag is flipped to ``true``.

A crash at any point leaves either orphaned ``.tmp-*`` files (cleaned at
the start of the next run), or final files + a manifest whose staging keys
still exist (the next run finishes the cleanup). Rows are never deleted
from staging before the compacted file has been written and verified.

Connection settings are reused from the rest of the codebase (see
``lite_indexer.storage.cold_store`` httpfs mode): ``B2_ACCOUNT_ID`` /
``B2_APPLICATION_KEY`` with optional ``B2_S3_ENDPOINT`` /
``B2_S3_REGION`` overrides.

NOTE: the production ``b2-crypt:`` remote is rclone-crypt *encrypted*, so
S3FileSystem can only be used against an unencrypted bucket. Point
``COMPACTED_PARQUET_REMOTE`` (or ``COMPACTED_B2_BUCKET`` +
``COMPACTED_PARQUET_PREFIX``) at a plaintext B2 bucket for this job.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import sys
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

try:
    import pyarrow as pa
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:  # pragma: no cover — optional dependency
    pa = None  # type: ignore[assignment]
    pafs = None  # type: ignore[assignment]
    pq = None  # type: ignore[assignment]
    PYARROW_AVAILABLE = False

from lite_indexer.config import PARQUET_REMOTE
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Manifest file recorded per hive partition (tracks consumed staging keys).
_MANIFEST_NAME = "_manifest.json"
# Prefix for in-flight write files (cleaned on the next run if orphaned).
_TMP_PREFIX = ".tmp-"

# Table -> staging file kind (filename marker). Same mapping as
# lite_indexer.storage.cold_store.TABLE_KINDS. Only EVM tables are
# supported: solana transfers use base58 strings + slot ordering and are
# out of scope for the fixed_size_binary compaction.
_TABLE_KINDS: dict[str, str] = {
    "evm_transfers": "transfers",
    "dex_events": "dex",
    "deployer_events": "deployer",
    "internal_transfers": "internal",
}

# Filename marker per staging kind (matches the daily export naming).
# "transfers" is the catch-all: files without any kind marker.
_KIND_MARKERS: dict[str, str] = {
    "dex": "_dex_",
    "deployer": "_deployer_",
    "internal": "_internal_",
}

# EVM columns that hold 20-byte addresses after compaction.
_EVM_ADDRESS_COLUMNS = frozenset(
    {
        "from_address",
        "to_address",
        "token_address",
        "contract_address",
        "token0",
        "token1",
        "sender",
        "recipient",
        "deployer",
        "deployer_eoa",
        "pair_address",
        "factory",
    }
)
# EVM columns that hold 32-byte tx/block hashes after compaction.
_EVM_HASH_COLUMNS = frozenset({"tx_hash", "block_hash", "event_hash"})

# Per-file size target for compacted parts (128-512MB per the contract).
_DEFAULT_MAX_BYTES = 512 * 1024 * 1024
_MIN_TARGET_BYTES = 128 * 1024 * 1024

# Heuristic: zstd level 3 typically compresses these wide rows ~10x vs raw
# Arrow bytes. Used only to split oversized partitions into parts that land
# in the 128-512MB target range.
_ZSTD_SIZE_FACTOR = 10.0

_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_DAY_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")


@dataclass
class CompactionSummary:
    """Outcome of one partition compaction run."""

    table: str
    chain_id: str
    period: str
    grain: str
    status: str = "pending"  # compacted | skipped | empty | pending_cleanup | error
    staging_files: int = 0
    staging_rows: int = 0
    output_files: int = 0
    output_rows: int = 0
    bytes_written: int = 0
    staging_deleted: bool = False
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serializable summary for CLI output."""
        return {
            "table": self.table,
            "chain_id": self.chain_id,
            "grain": self.grain,
            "period": self.period,
            "status": self.status,
            "staging_files": self.staging_files,
            "staging_rows": self.staging_rows,
            "output_files": self.output_files,
            "output_rows": self.output_rows,
            "bytes_written": self.bytes_written,
            "staging_deleted": self.staging_deleted,
            "errors": self.errors,
        }


# ── Connection (reuse the shared B2 settings, no new client) ───────────


def _s3_fs() -> Any:
    """Build the pyarrow S3FileSystem from the shared B2 env settings.

    Mirrors the connection settings used by
    ``lite_indexer.storage.cold_store`` (httpfs mode) and
    ``lite_indexer.storage.duckdb_query.query_httpfs``: Backblaze B2 S3
    credentials come from ``B2_ACCOUNT_ID`` / ``B2_APPLICATION_KEY`` with
    the ``B2_S3_ENDPOINT`` / ``B2_S3_REGION`` overrides.

    Raises:
        RuntimeError: When pyarrow is missing or no credentials are set
            (so callers fail gracefully instead of crashing mid-run).
    """
    if not PYARROW_AVAILABLE:
        raise RuntimeError("pyarrow is not installed — run: pip install pyarrow")
    access_key = os.getenv("B2_ACCOUNT_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("B2_APPLICATION_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError(
            "B2 credentials missing: set B2_ACCOUNT_ID and B2_APPLICATION_KEY "
            "(or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY) before compacting."
        )
    endpoint = os.getenv("B2_S3_ENDPOINT", "https://s3.us-west-004.backblazeb2.com")
    region = os.getenv("B2_S3_REGION", "us-west-004")
    return pafs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        endpoint_override=endpoint,
    )


def _remote_parts(remote: str) -> tuple[str, str]:
    """Split an rclone-style remote into ``(bucket, prefix)``.

    ``"b2-crypt:chain-data/parquet"`` -> ``("chain-data", "parquet")``.

    Args:
        remote: rclone remote root (e.g. ``PARQUET_REMOTE``).

    Returns:
        ``(bucket, prefix)`` with leading/trailing slashes stripped.
    """
    if ":" in remote:
        bucket, _, prefix = remote.partition(":")
    else:
        bucket, prefix = remote, ""
    return bucket.strip("/"), prefix.strip("/")


def _bucket_and_prefixes() -> tuple[str, str, str]:
    """Return ``(bucket, staging_prefix, compacted_prefix)``.

    Staging micro-batches live under the canonical parquet remote
    (``PARQUET_REMOTE``); compacted hive files live under
    ``{staging_prefix}/hive`` unless overridden with
    ``COMPACTED_PARQUET_REMOTE`` (point at an *unencrypted* B2 remote) or
    ``COMPACTED_B2_BUCKET`` / ``COMPACTED_PARQUET_PREFIX``.
    """
    bucket, prefix = _remote_parts(os.getenv("COMPACTED_PARQUET_REMOTE", PARQUET_REMOTE))
    bucket = os.getenv("COMPACTED_B2_BUCKET", bucket)
    compacted_prefix = os.getenv("COMPACTED_PARQUET_PREFIX", "hive").strip("/")
    if prefix:
        compacted_prefix = f"{prefix}/{compacted_prefix}"
    return bucket, prefix, compacted_prefix


# ── Path helpers ───────────────────────────────────────────────────────


def _parse_period(period: str, grain: str) -> str:
    """Validate ``period`` against ``grain``; returns the canonical value."""
    if grain == "month":
        if not _MONTH_RE.match(period):
            raise ValueError(f"period must be YYYY-MM for grain=month, got {period!r}")
        return period
    if grain == "day":
        if not _DAY_RE.match(period):
            raise ValueError(f"period must be YYYY-MM-DD for grain=day, got {period!r}")
        return period
    raise ValueError(f"unsupported grain {grain!r} (expected 'month' or 'day')")


def _staging_root(prefix: str, chain_id: str, period: str, grain: str) -> str:
    """Build the staging tree root for a (chain, period, grain)."""
    year, month = period.split("-")[:2]
    if grain == "day":
        return f"{prefix}/{chain_id}/{year}/{month}/{period.split('-')[2]}"
    return f"{prefix}/{chain_id}/{year}/{month}"


def _partition_dir(compacted_prefix: str, chain_id: str, grain: str, period: str) -> str:
    """Hive partition dir: ``{prefix}/chain={id}/{grain}={period}``."""
    return f"{compacted_prefix}/chain={chain_id}/{grain}={period}"


def _manifest_path(partition_dir: str) -> str:
    return f"{partition_dir}/{_MANIFEST_NAME}"


# ── B2 listing / state ─────────────────────────────────────────────────


def _list_staging_files(fs: Any, root: str, kind: str) -> list[str]:
    """Return staging parquet keys under ``root`` matching ``kind``.

    Args:
        fs: pyarrow filesystem.
        root: Staging tree root for the partition.
        kind: File kind — ``"dex"``/``"deployer"``/``"internal"`` match
            their filename markers; ``"transfers"`` matches files without
            any kind marker.

    Returns:
        Sorted list of object keys (empty when the root does not exist).
    """
    try:
        infos = fs.get_file_info(pafs.FileSelector(root, recursive=True))
    except FileNotFoundError:
        return []
    marker = _KIND_MARKERS.get(kind, "")
    out: list[str] = []
    for info in infos:
        if info.type != pafs.FileType.File or not info.path.endswith(".parquet"):
            continue
        name = os.path.basename(info.path)
        if name.startswith(_TMP_PREFIX) or name.endswith("~"):
            continue
        if marker:
            if marker in name:
                out.append(info.path)
        elif not any(m in name for m in _KIND_MARKERS.values()):
            out.append(info.path)
    return sorted(out)


def _read_manifest(fs: Any, partition_dir: str) -> dict[str, Any] | None:
    """Read the partition manifest, or None when absent."""
    path = _manifest_path(partition_dir)
    try:
        with fs.open_input_file(path) as fh:
            return cast(dict[str, Any], json.loads(fh.read().decode("utf-8")))
    except FileNotFoundError:
        return None


def _ensure_dir(fs: Any, path: str) -> None:
    """Create ``path`` recursively; no-op when it already exists.

    S3 is a flat namespace so this is a no-op there; local filesystems
    (tests, dev) need the partition directory present before writing.
    """
    with contextlib.suppress(OSError, NotImplementedError):
        fs.create_dir(path, recursive=True)


def _write_manifest(fs: Any, partition_dir: str, manifest: dict[str, Any]) -> None:
    """Write the partition manifest atomically (tmp + move)."""
    _ensure_dir(fs, partition_dir)
    key = _manifest_path(partition_dir)
    tmp = f"{partition_dir}/{_TMP_PREFIX}manifest-{uuid.uuid4().hex}.json"
    with fs.open_output_stream(tmp) as fh:
        fh.write(json.dumps(manifest, indent=2).encode("utf-8"))
    fs.move(tmp, key)


def _list_final_parts(fs: Any, partition_dir: str) -> list[str]:
    """Return promoted ``part-*.parquet`` keys in the hive partition dir.

    These hold previously-compacted rows; when the manifest says staging
    was already deleted, they are the only home of those rows and must be
    merged back in when new staging files arrive.
    """
    try:
        infos = fs.get_file_info(pafs.FileSelector(partition_dir, recursive=False))
    except FileNotFoundError:
        return []
    return sorted(
        info.path
        for info in infos
        if info.type == pafs.FileType.File
        and info.path.endswith(".parquet")
        and os.path.basename(info.path).startswith("part-")
    )


def _delete_keys(fs: Any, keys: list[str]) -> None:
    """Delete object keys; missing keys are ignored (idempotent)."""
    for key in keys:
        try:
            fs.delete_file(key)
            logger.info("compaction_staging_deleted", key=key)
        except FileNotFoundError:
            pass  # already gone


def _cleanup_tmp(fs: Any, partition_dir: str) -> None:
    """Remove orphaned ``.tmp-*`` files from a crashed run."""
    try:
        infos = fs.get_file_info(pafs.FileSelector(partition_dir, recursive=True))
    except FileNotFoundError:
        return
    for info in infos:
        if info.type == pafs.FileType.File and os.path.basename(info.path).startswith(_TMP_PREFIX):
            with contextlib.suppress(OSError):
                fs.delete_file(info.path)


# ── Schema conversion (fixed_size_binary) ──────────────────────────────


def _hex_to_fixed_binary(array: pa.Array, width: int, column: str) -> pa.Array:
    """Convert a string array of 0x-hex values to ``fixed_size_binary(width)``.

    Empty/None values become zero-filled binary (keeps the fixed width
    schema intact); non-hex or wrong-length values raise so schema drift
    surfaces instead of silently corrupting data.

    Args:
        array: String (utf8) column.
        width: Fixed binary width (20 for addresses, 32 for hashes).
        column: Column name (for error messages).

    Returns:
        A ``pa.binary(width)`` (fixed_size_binary) array.
    """
    values: list[bytes] = []
    for value in array.to_pylist():
        if value is None or value == "":
            values.append(b"\x00" * width)
            continue
        if isinstance(value, bytes):
            raw = value
        else:
            text = str(value)
            if text.startswith(("0x", "0X")):
                text = text[2:]
            try:
                raw = bytes.fromhex(text)
            except ValueError as exc:
                raise ValueError(f"compaction: column {column!r} holds non-hex value {str(value)[:32]!r}") from exc
        if len(raw) == width:
            values.append(raw)
        elif len(raw) < width:
            values.append(raw + b"\x00" * (width - len(raw)))
        else:
            raise ValueError(f"compaction: column {column!r} value decodes to {len(raw)} bytes, expected {width}")
    return pa.array(values, type=pa.binary(width))


def _apply_fixed_binary(table: pa.Table) -> pa.Table:
    """Cast EVM address/hash string columns to fixed-size binary.

    Address columns (``from_address``, ``to_address``, ...) become
    20-byte ``fixed_size_binary``; tx/block hash columns become 32-byte
    ``fixed_size_binary``. Columns that are already binary (or not in the
    mapping) are left untouched.
    """
    for column in table.column_names:
        arr = table.column(column)
        if arr.type not in (pa.string(), pa.large_string()):
            continue
        if column in _EVM_ADDRESS_COLUMNS:
            table = table.set_column(
                table.schema.get_field_index(column),
                column,
                _hex_to_fixed_binary(arr, 20, column),
            )
        elif column in _EVM_HASH_COLUMNS:
            table = table.set_column(
                table.schema.get_field_index(column),
                column,
                _hex_to_fixed_binary(arr, 32, column),
            )
    return table


# ── Write + verify ─────────────────────────────────────────────────────


def _plan_parts(table: pa.Table, max_bytes: int) -> list[pa.Table]:
    """Split a table into parts that each fit ~``max_bytes`` on disk.

    Uses a zstd size heuristic (``_ZSTD_SIZE_FACTOR``) to estimate the
    compressed size; partitions smaller than the target produce one part.
    """
    if table.num_rows == 0:
        return []
    est_bytes = max(1, int(table.nbytes / _ZSTD_SIZE_FACTOR))
    if est_bytes <= max_bytes:
        return [table]
    parts = max(1, -(-est_bytes // max_bytes))  # ceil division
    rows_per_part = max(1, table.num_rows // parts)
    out: list[pa.Table] = []
    for start in range(0, table.num_rows, rows_per_part):
        out.append(table.slice(start, rows_per_part))
    return out


def _write_part(fs: Any, key: str, table: pa.Table) -> int:
    """Write one part (zstd level 3); returns bytes written."""
    pq.write_table(table, key, filesystem=fs, compression="zstd", compression_level=3)
    return cast(int, fs.get_file_info(key).size)


def _verify_part(fs: Any, key: str, expected_rows: int) -> bool:
    """Verify a written part by reading its footer row count back."""
    try:
        metadata = pq.read_metadata(key, filesystem=fs)
        return cast(bool, metadata.num_rows == expected_rows)
    except Exception as exc:  # noqa: BLE001 — verification is best-effort
        logger.warning("compaction_verify_failed", key=key, error=str(exc)[:150])
        return False


# ── Compaction orchestration ───────────────────────────────────────────


def compact_staging(
    table: str,
    chain_id: str,
    period: str,
    grain: str = "month",
    *,
    delete_staging: bool = False,
    dry_run: bool = False,
    max_bytes: int = _DEFAULT_MAX_BYTES,
    fs: Any | None = None,
) -> CompactionSummary:
    """Compact staging micro-batches for a partition into hive files.

    Reads every staging micro-batch under
    ``{prefix}/{chain_id}/{YYYY}/{MM}[/{DD}]`` for ``table``, merges and
    sorts them by ``block_number``, casts address/hash columns to
    ``fixed_size_binary``, and writes one (or more, for oversized
    partitions) zstd-3 parquet part under
    ``{prefix}/hive/chain={chain_id}/{grain}={period}/``.

    Write-first / delete-second: parts are written to ``.tmp-*`` names,
    verified, promoted to ``part-*.parquet``, a ``_manifest.json`` records
    the consumed staging keys, and only then is staging deleted (gated by
    ``delete_staging``, matching the repo's ``--delete-after-upload``
    convention). The run is idempotent: an already-compacted partition is
    skipped unless new staging files arrived for it.

    Args:
        table: Table name (``evm_transfers``, ``dex_events``, ...).
        chain_id: Chain id/name used in ``chain={chain_id}``.
        period: ``YYYY-MM`` for month grain, ``YYYY-MM-DD`` for day grain.
        grain: Partition grain — ``"month"`` (default) or ``"day"``.
        delete_staging: Delete consumed staging micro-batches after the
            compacted file is written and verified. Without it the run
            writes + records state and a later run finishes the cleanup.
        dry_run: Plan only — list staging, log the target, write/delete
            nothing.
        max_bytes: Target maximum size (bytes) per compacted part.
        fs: Optional pyarrow filesystem override (tests); defaults to the
            shared B2 S3FileSystem.

    Returns:
        :class:`CompactionSummary` describing what happened.

    Raises:
        ValueError: Unknown table, bad period/grain, or non-hex data.
        RuntimeError: Missing B2 credentials / pyarrow (fails gracefully).
    """
    summary = CompactionSummary(table=table, chain_id=chain_id, period=period, grain=grain)

    if table not in _TABLE_KINDS:
        summary.status = "error"
        message = f"unsupported table {table!r} (supported: {sorted(_TABLE_KINDS)})"
        summary.errors.append(message)
        raise ValueError(message)

    period = _parse_period(period, grain)
    summary.period = period
    if fs is None:
        fs = _s3_fs()

    kind = _TABLE_KINDS[table]
    _bucket, staging_prefix, compacted_prefix = _bucket_and_prefixes()
    staging_root = _staging_root(staging_prefix, chain_id, period, grain)
    partition_dir = _partition_dir(compacted_prefix, chain_id, grain, period)

    staging_keys = _list_staging_files(fs, staging_root, kind)
    summary.staging_files = len(staging_keys)

    manifest = _read_manifest(fs, partition_dir)
    known = set(manifest.get("staging", [])) if manifest else set()
    current = set(staging_keys)
    new_data = current - known if manifest else current

    # ── Idempotency / crash-recovery short-circuits ────────────────────
    if manifest is not None:
        if manifest.get("staging_deleted") and not new_data:
            summary.status = "skipped"
            logger.info(
                "compaction_skipped",
                table=table,
                chain=chain_id,
                partition=partition_dir,
                reason="already_compacted",
            )
            return summary
        if not new_data and current:
            # Every staging key present is already recorded in the
            # manifest → the compacted files cover the partition. Either
            # finish a crash-interrupted cleanup or report pending.
            if current == known and not manifest.get("staging_deleted") and delete_staging:
                _delete_keys(fs, sorted(current))
                _write_manifest(
                    fs,
                    partition_dir,
                    {**manifest, "staging": sorted(current), "staging_deleted": True},
                )
                summary.status = "compacted"
                summary.staging_deleted = True
                return summary
            if current < known:
                if delete_staging:
                    _delete_keys(fs, sorted(known - current))
                    _write_manifest(
                        fs,
                        partition_dir,
                        {**manifest, "staging": sorted(current), "staging_deleted": True},
                    )
                    summary.status = "compacted"
                    summary.staging_deleted = True
                else:
                    summary.status = "pending_cleanup"
                return summary
            summary.status = "pending_cleanup" if not manifest.get("staging_deleted") else "skipped"
            return summary
        if not current and manifest.get("staging") and not manifest.get("staging_deleted"):
            # Crash between staging deletion and the manifest flag.
            manifest["staging_deleted"] = True
            _write_manifest(fs, partition_dir, manifest)
            summary.status = "skipped"
            return summary

    # ── Normal compaction path ─────────────────────────────────────────
    if not staging_keys:
        if manifest is None:
            logger.info("compaction_nothing_to_do", table=table, chain=chain_id, root=staging_root)
            summary.status = "empty"
        else:
            # Manual staging removal edge: finals exist but no manifest
            # claims them; leave everything untouched.
            summary.status = "skipped"
        return summary

    if dry_run:
        summary.status = "dry_run"
        logger.info(
            "compaction_dry_run",
            table=table,
            chain=chain_id,
            staging_root=staging_root,
            partition=partition_dir,
            files=len(staging_keys),
            delete_staging=delete_staging,
        )
        return summary

    _cleanup_tmp(fs, partition_dir)

    # Read + merge + sort + convert.
    # When a previous run already compacted + deleted staging (manifest
    # staging_deleted=true), the existing final parts are the only home of
    # those rows — merge them with the new staging files instead of
    # recomputing from staging alone (which would drop the old rows).
    source_keys = staging_keys
    if manifest is not None and manifest.get("staging_deleted"):
        source_keys = staging_keys + [key for key in _list_final_parts(fs, partition_dir) if key not in staging_keys]
    try:
        # ParquetFile.read() (not pq.read_table) so hive-partitioned final
        # parts read back cleanly — pyarrow 24's dataset layer trips on
        # single-file paths under chain=/month= dirs (string vs dictionary
        # merge quirk), but ParquetFile reads the file directly.
        tables = [pq.ParquetFile(key, filesystem=fs).read() for key in source_keys]
    except Exception as exc:  # noqa: BLE001 — surface as a run error
        summary.status = "error"
        summary.errors.append(f"read staging failed: {str(exc)[:200]}")
        return summary

    merged = pa.concat_tables(tables, promote_options="permissive")
    summary.staging_rows = merged.num_rows
    if "block_number" in merged.column_names:
        merged = merged.sort_by("block_number")
    merged = _apply_fixed_binary(merged)

    part_tables = _plan_parts(merged, max_bytes)
    if not part_tables:
        summary.status = "empty"
        return summary

    part_names = [f"part-{i:05d}.parquet" for i in range(len(part_tables))]
    tmp_keys = [f"{partition_dir}/{_TMP_PREFIX}{uuid.uuid4().hex}-{name}" for name in part_names]
    final_keys = [f"{partition_dir}/{name}" for name in part_names]

    # Write all parts to tmp names, verifying each before promotion.
    _ensure_dir(fs, partition_dir)
    for tmp_key, part in zip(tmp_keys, part_tables, strict=True):
        try:
            summary.bytes_written += _write_part(fs, tmp_key, part)
        except Exception as exc:  # noqa: BLE001 — surface as a run error
            summary.status = "error"
            summary.errors.append(f"write failed for {tmp_key}: {str(exc)[:200]}")
            _cleanup_tmp(fs, partition_dir)
            return summary
        if not _verify_part(fs, tmp_key, part.num_rows):
            summary.status = "error"
            summary.errors.append(f"verify failed for {tmp_key}")
            _cleanup_tmp(fs, partition_dir)
            return summary

    # Promote verified parts to final names.
    for tmp_key, final_key in zip(tmp_keys, final_keys, strict=True):
        try:
            fs.move(tmp_key, final_key)
        except Exception as exc:  # noqa: BLE001 — surface as a run error
            summary.status = "error"
            summary.errors.append(f"promote failed for {final_key}: {str(exc)[:200]}")
            return summary

    summary.output_files = len(final_keys)
    summary.output_rows = merged.num_rows

    # Record state BEFORE any staging deletion (crash-safe ordering).
    _write_manifest(
        fs,
        partition_dir,
        {
            "table": table,
            "chain_id": chain_id,
            "grain": grain,
            "period": period,
            "files": final_keys,
            "rows": merged.num_rows,
            "staging": staging_keys,
            "staging_deleted": False,
            "compacted_at": datetime.now(UTC).isoformat(),
        },
    )

    if delete_staging:
        _delete_keys(fs, staging_keys)
        manifest = _read_manifest(fs, partition_dir) or {}
        manifest["staging_deleted"] = True
        _write_manifest(fs, partition_dir, manifest)
        summary.staging_deleted = True

    summary.status = "compacted"
    logger.info(
        "compaction_complete",
        table=table,
        chain=chain_id,
        partition=partition_dir,
        files=len(staging_keys),
        rows=merged.num_rows,
        parts=len(final_keys),
        bytes_written=summary.bytes_written,
        staging_deleted=summary.staging_deleted,
    )
    return summary


# ── CLI (``python -m lite_indexer.storage.compaction``) ────────────────


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI args for the compaction entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m lite_indexer.storage.compaction",
        description="Compact staging micro-batches into hive-partitioned parquet on B2.",
    )
    parser.add_argument("--table", required=True, choices=sorted(_TABLE_KINDS), help="Table to compact.")
    parser.add_argument(
        "--chain",
        "--chain-id",
        dest="chain_id",
        required=True,
        help="Chain id/name (used in chain={chain_id}), e.g. ethereum.",
    )
    parser.add_argument(
        "--period",
        required=True,
        help="Partition value: YYYY-MM (month grain) or YYYY-MM-DD (day grain).",
    )
    parser.add_argument("--grain", choices=("month", "day"), default="month", help="Partition grain.")
    parser.add_argument(
        "--delete-staging",
        action="store_true",
        help="Delete staging micro-batches after the compacted file is written and verified.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan only — write/delete nothing.")
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=_DEFAULT_MAX_BYTES,
        help=f"Target max size per compacted part in bytes (default {_DEFAULT_MAX_BYTES}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run compaction from the CLI; returns process exit code."""
    args = parse_args(argv)
    try:
        summary = compact_staging(
            args.table,
            args.chain_id,
            args.period,
            grain=args.grain,
            delete_staging=args.delete_staging,
            dry_run=args.dry_run,
            max_bytes=args.max_bytes,
        )
    except (RuntimeError, ValueError) as exc:
        logger.error("compaction_failed", error=str(exc))
        print(f"error: {exc}", file=sys.stderr)  # noqa: T201
        return 1
    print(json.dumps(summary.to_dict(), indent=2))  # noqa: T201
    return 0 if summary.status not in ("error",) else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "compact_staging",
    "CompactionSummary",
    "_s3_fs",
    "_apply_fixed_binary",
    "_plan_parts",
    "PYARROW_AVAILABLE",
    "_MIN_TARGET_BYTES",
    "_DEFAULT_MAX_BYTES",
]
