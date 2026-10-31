"""One-pass streaming export for large unpartitioned tables (``address_activity``).

``archive_export.export_unit`` issues one query per (chain, day) unit. On
``address_activity`` (262 GiB, unpartitioned, sorted by address/tx_signature)
that is one full table scan per unit — 21 x N scans of the whole table. This
module makes it ONE scan, split as it streams, in three bounded stages:

1. ``dump_sql`` — a single ``INSERT INTO FUNCTION file(...)`` writes the whole
   closed-day slice to one local parquet file. One reader, one writer, memory
   bounded (the partitioned-write variant held 10.5 GiB across ~824 open
   partition writers and died on this box; a single sink measured 331 MiB).
2. ``split_dump`` — ``clickhouse-local`` reads that file and re-emits one part
   per ``<chain>_<YYYYMMDD>`` partition, one day-range chunk per call so the
   number of simultaneously open partition writers stays small.
3. ``verify_upload`` — every staged part is counted with DuckDB and compared
   to the authoritative ClickHouse count for that closed day; only an equal (or
   larger, recorded as ``late_rows``) part is uploaded, to the same
   deterministic key the per-unit exporter uses.

A part is never uploaded twice (the manifest is the unit's seal) and a short
part aborts that unit instead of shipping a partial object.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

from lite_indexer.storage.archive_export import DB, ch, object_exists, upload
from lite_indexer.storage.archive_specs import TableSpec

STAGE_ROOT = Path("/data/clickhouse/user_files/archive-stream")
CTR_ROOT = "/var/lib/clickhouse/user_files"
CH_LOCAL = ["docker", "exec", "rmi-clickhouse", "clickhouse-local"]
CHUNK_DAYS = 7


def own(path: Path) -> None:
    """Give a staging path to the clickhouse uid (uid 101) so local writes land."""
    subprocess.run(["chown", "-R", "101:101", str(path)], check=False)


def ctr(path: Path) -> str:
    """Host path -> container path (ClickHouse runs in rmi-clickhouse)."""
    return str(path).replace("/data/clickhouse/user_files", CTR_ROOT)


def dump_sql(spec: TableSpec, dump: Path, cutoff_day: str) -> str:
    """The ONE scan: whole closed-day slice into a single local parquet file."""
    cols = ", ".join(spec.columns)
    return (
        f"INSERT INTO FUNCTION file('{ctr(dump)}', Parquet) SELECT {cols} FROM {DB}.{spec.table} "
        f"WHERE toDate({spec.cursor}) < toDate('{cutoff_day}') "
        f"SETTINGS max_memory_usage=8000000000, max_threads=2, max_block_size=65536, "
        f"max_insert_block_size=65536, output_format_parquet_row_group_size=65536, "
        f"output_format_parquet_compression_method='zstd'"
    )


def ensure_dump(spec: TableSpec, cutoff_day: str, reuse: bool) -> Path:
    """Create (or reuse) the single-pass dump for this table."""
    dump = STAGE_ROOT / f"{spec.table}-dump.parquet"
    if reuse and dump.exists() and dump.stat().st_size > 0:
        try:
            duckdb_rows(dump)  # readable footer => a completed dump, not a killed one
            return dump
        except subprocess.CalledProcessError:
            pass
    ch(dump_sql(spec, dump, cutoff_day))
    return dump


def split_chunk(dump: Path, out_dir: Path, first_day: str, last_day_exclusive: str, spec: TableSpec) -> None:
    """One local pass: every row in [first, last) lands in its own partition part."""
    cursor = spec.cursor
    part = f"concat({spec.chain_column or chr(39) + '_global' + chr(39)}, '_', toString(toYYYYMMDD({cursor})))"
    sql = (
        f"INSERT INTO FUNCTION file('{ctr(out_dir)}/{{_partition_id}}/part-000.parquet', Parquet) "
        f"PARTITION BY {part} SELECT * FROM file('{ctr(dump)}', Parquet) "
        f"WHERE toDate({spec.cursor}) >= toDate('{first_day}') AND toDate({spec.cursor}) < toDate('{last_day_exclusive}')"
    )
    subprocess.run([*CH_LOCAL, "--max_threads", "4", "--max_memory_usage", "7000000000", "--query", sql], check=True)


def split_dump(dump: Path, out_dir: Path, days: list[str], spec: TableSpec) -> None:
    """Split the dump into per-(chain, day) parts, CHUNK_DAYS at a time."""
    out_dir.mkdir(parents=True, exist_ok=True)
    own(out_dir)
    for start in range(0, len(days), CHUNK_DAYS):
        chunk = days[start : start + CHUNK_DAYS]
        end = (dt.date.fromisoformat(chunk[-1]) + dt.timedelta(days=1)).isoformat()
        split_chunk(dump, out_dir, chunk[0], end, spec)


def duckdb_rows(part: Path) -> int:
    """Row count of a staged part, read independently of ClickHouse."""
    sql = f"SELECT count(*) FROM read_parquet('{part}')"
    out = subprocess.run(["duckdb", "-noheader", "-list", "-c", sql], capture_output=True, text=True, check=True)
    return int(out.stdout.strip())


def _manifest(spec: TableSpec, chain: str, day: str, part: dict) -> dict:
    """Build the unit manifest. `part` carries rows/bytes/md5/late_rows."""
    return {
        "v": 1,
        "table": spec.table,
        "chain": chain,
        "date": day,
        "mode": spec.mode,
        "cursor": spec.cursor or None,
        "rows": part["rows"],
        "bytes": part["bytes"],
        "md5": part["md5"],
        "part": "part-000.parquet",
        "predicate": f"{spec.chain_column} = '{chain}' AND toDate({spec.cursor}) = toDate('{day}')",
        "streamed": True,
        "late_rows": part["late_rows"],
        "exported_at": dt.datetime.now(dt.UTC).isoformat(),
    }


def verify_upload(spec: TableSpec, out_dir: Path, expected: dict[tuple[str, str], int]) -> dict:
    """Verify every staged part against ClickHouse, upload the sealed ones."""
    summary = {"table": spec.table, "units": 0, "rows": 0, "bytes": 0, "late": 0, "failed": [], "ok": []}
    for part in sorted(out_dir.glob("*/part-000.parquet")):
        chain, stamp = part.parent.name.rsplit("_", 1)
        day = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:]}"
        want = expected.get((chain, day))
        got = duckdb_rows(part)
        if want is None or got < want:
            summary["failed"].append(f"{chain}/{day}: ch={want} parquet={got}")
            continue
        key_dir = f"{chain}/{spec.table}/{day}"
        if object_exists(f"{key_dir}/manifest.json") or object_exists(f"{key_dir}/part-00000.parquet"):
            summary["units"] += 1
            continue
        digest = hashlib.md5(part.read_bytes(), usedforsecurity=False).hexdigest()  # noqa: S324 — integrity, not auth
        upload(part, f"{key_dir}/part-000.parquet")
        manifest = _manifest(
            spec, chain, day,
            {"rows": got, "bytes": part.stat().st_size, "md5": digest, "late_rows": got - want},
        )
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump(manifest, fh, indent=2, sort_keys=True)
            local = Path(fh.name)
        upload(local, f"{key_dir}/manifest.json")
        local.unlink()
        summary["units"] += 1
        summary["rows"] += got
        summary["bytes"] += part.stat().st_size
        summary["late"] += got - want
        summary["ok"].append(f"{chain}/{spec.table}/{day} rows={got} bytes={part.stat().st_size}")
    return summary


def export_stream(spec: TableSpec, expected: dict[tuple[str, str], int], cutoff_day: str, reuse: bool = True) -> dict:
    """Dump once, split by day, verify against ClickHouse, upload. Returns the summary."""
    dump = ensure_dump(spec, cutoff_day, reuse)
    days = sorted({day for _, day in expected})
    out_dir = STAGE_ROOT / f"{spec.table}-units"
    split_dump(dump, out_dir, days, spec)
    return verify_upload(spec, out_dir, expected)
