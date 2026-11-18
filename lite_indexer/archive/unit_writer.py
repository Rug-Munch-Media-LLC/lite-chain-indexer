"""Verify-then-upload for one parquet unit. The only writer in the fleet.

A "unit" is the smallest independently verifiable slice of the archive:
one day for normal tables, one hour for chains that write ~1M rows/hour
(see ``backfill-evm-hours.py``). It is addressed by (chain, table, date) and
lands at::

    <remote>/<chain>/<table>/<YYYY-MM-DD>/part-000.parquet

High-volume tables keep the hour-granularity layout instead::

    <remote>/<chain>/<YYYY>/<MM>/<DD>/<HH>/<chain>_<YYYY-MM-DD>.parquet

Both shapes are produced by :func:`object_key`, because the readers already
expect them and changing a layout to tidy a writer would break the archive.

FAILURE POLICY
    Every failure is a raised :class:`WriteError` with the reason attached, not
    a returned string. The previous implementation returned ``"FAIL ..."`` and
    ``"MISMATCH ..."`` as strings, which meant a caller had to parse prose to
    know whether to continue — and one caller did not, so a mismatched unit was
    merely printed. A raised error cannot be ignored by accident.
"""

from __future__ import annotations

import subprocess
import tempfile
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

#: Where verified units land. Overridable for a dry run or a test.
DEFAULT_REMOTE = "b2-crypt-v2:chain-data/parquet"

#: DuckDB is the only writer. It is already a hard dependency of every reader in
#: this repo, so using it here adds nothing to the install surface.
DUCKDB = "duckdb"


class WriteError(RuntimeError):
    """A unit could not be verified or published. Always carries a reason."""

    def __init__(self, reason: str, *, key: str = "", detail: str = "") -> None:
        self.reason = reason
        self.key = key
        self.detail = detail
        super().__init__(f"{reason}{f' [{key}]' if key else ''}: {detail}"[:500])


@dataclass(frozen=True)
class UnitSpec:
    """Identity of one archival unit.

    Attributes:
        chain: Chain name as the archive spells it (``base``, ``ethereum``).
        table: Warehouse table name (``evm_transfers``, ``internal_transfers``).
        date: ``YYYY-MM-DD`` for day units.
        hour: ``HH`` for hour-granularity units, else ``""``.
        expected_rows: What the source says this unit holds. The upload is only
            accepted when the parquet row count equals this exactly — a unit
            that lost or duplicated rows is worse than a missing unit, because
            it silently corrupts every query over the range.
    """

    chain: str
    table: str
    date: str
    expected_rows: int
    hour: str = ""


@dataclass
class UnitResult:
    """What was published, with the evidence that it is correct."""

    spec: UnitSpec
    key: str
    rows: int
    bytes_written: int
    verified: bool
    skipped: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


def object_key(spec: UnitSpec, remote: str = DEFAULT_REMOTE) -> str:
    """B2 key for one unit, in the layout the readers already expect.

    Day layout and hour layout coexist in the archive: ``base`` writes
    ``<date>/part-000.parquet`` while ``polygon`` writes
    ``<Y>/<M>/<D>/<HH>/<chain>_<date>.parquet``. Both are correct for their
    table's volume; the choice is the caller's, via ``spec.hour``.
    """
    if spec.hour:
        y, m, d = spec.date.split("-")
        return f"{remote}/{spec.chain}/{y}/{m}/{d}/{spec.hour}/{spec.chain}_{spec.date}.parquet"
    return f"{remote}/{spec.chain}/{spec.table}/{spec.date}/part-000.parquet"


def _split_key(key: str) -> tuple[str, str]:
    """(parent_remote_path, basename) — rclone needs the parent to list."""
    parent, _, name = key.rpartition("/")
    return parent, name


def _rclone_exists(key: str) -> bool:
    """True only when the object's NAME appears in its parent listing.

    MEASURED 2026-11-18 — every single-path variant gives a false positive:
      ``rclone lsjson <file>``           -> rc 0 for a missing file
      ``rclone lsjson --stat <file>``    -> {"IsDir": true, "Size": -1} for BOTH
                                            a real object and a missing one
      ``rclone lsf --files-only <file>`` -> empty for both
      ``rclone size <file>``             -> "Total objects: 0" for both
    Only listing the PARENT and matching the basename distinguishes them.

    Consequence of getting this wrong is not a wrong log line: the writer
    returned ``verified=True, skipped=True`` for an object that did not exist, so
    a backfill would have "completed" having uploaded nothing.
    """
    parent, name = _split_key(key)
    if not parent or not name:
        return False
    proc = subprocess.run(
        ["rclone", "lsf", parent], capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        return False
    return name in proc.stdout.splitlines()



def _rclone_size(key: str) -> int:
    """Object size in bytes, or -1 when absent.

    Uses ``lsjson`` on the PARENT with ``--files-only`` and matches the name,
    because every single-path stat variant lies — see :func:`_rclone_exists` for
    the measurements. rclone is the transport that performs the upload, so
    verifying with the same tool removes the stale-mount disagreement this
    archive has hit before.
    """
    import json

    parent, name = _split_key(key)
    if not parent or not name:
        return -1
    proc = subprocess.run(
        ["rclone", "lsjson", parent, "--files-only"],
        capture_output=True, text=True, check=False,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        return -1
    try:
        for entry in json.loads(proc.stdout):
            if entry.get("Name") == name:
                return int(entry.get("Size", -1))
    except (ValueError, TypeError):
        return -1
    return -1



def _count_rows(part: Path) -> int:
    proc = subprocess.run(
        [DUCKDB, "-noheader", "-list", "-c", f"SELECT count(*) FROM read_parquet('{part}')"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise WriteError("row_count_failed", detail=proc.stderr[-200:])
    try:
        return int(proc.stdout.strip())
    except ValueError as exc:
        raise WriteError("row_count_unparseable", detail=proc.stdout[:120]) from exc


def _write_parquet(rows: Iterable[Mapping[str, Any]], part: Path) -> None:
    """Materialise rows as parquet via DuckDB, streaming through a temp NDJSON.

    DuckDB cannot create a table from a Python iterator, and buffering a day of
    a busy chain in memory is exactly what this package exists to avoid. Writing
    NDJSON to the same TemporaryDirectory and reading it back keeps memory flat
    and local disk bounded by one unit.
    """
    import json

    nd = part.with_suffix(".ndjson")
    try:
        with nd.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, default=str))
                fh.write("\n")
        sql = f"COPY (SELECT * FROM read_json_auto('{nd}')) TO '{part}' (FORMAT parquet, COMPRESSION zstd)"
        proc = subprocess.run([DUCKDB, "-c", sql], capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise WriteError("parquet_write_failed", detail=proc.stderr[-300:])
    finally:
        nd.unlink(missing_ok=True)


def write_unit(
    spec: UnitSpec,
    rows: Iterable[Mapping[str, Any]],
    *,
    remote: str = DEFAULT_REMOTE,
    skip_if_present: bool = True,
) -> UnitResult:
    """Publish one unit to B2, or report that it is already there.

    Args:
        spec: Identity plus the expected row count.
        rows: Row dicts. Consumed once; memory stays flat.
        remote: Destination root.
        skip_if_present: Return ``skipped`` when the object already exists. This
            is what makes a re-run cheap and is why a failed unit costs one
            re-fetch rather than a re-scan.

    Returns:
        A :class:`UnitResult` carrying rows, bytes, key and ``verified``.

    Raises:
        WriteError: on any step that cannot be proven correct. Callers must let
            it propagate: a swallowed WriteError is a corrupt archive.
    """
    key = object_key(spec, remote)

    if skip_if_present and _rclone_exists(key):
        return UnitResult(
            spec=spec,
            key=key,
            rows=spec.expected_rows,
            bytes_written=0,
            verified=True,
            skipped=True,
        )

    with tempfile.TemporaryDirectory(prefix=f"unit-{spec.chain}-") as tmp:
        part = Path(tmp) / "part-000.parquet"
        _write_parquet(rows, part)

        if not part.exists() or part.stat().st_size == 0:
            raise WriteError("empty_parquet", key=key)

        got = _count_rows(part)
        if got != spec.expected_rows:
            raise WriteError(
                "row_count_mismatch",
                key=key,
                detail=f"source={spec.expected_rows} written={got}",
            )

        local_bytes = part.stat().st_size
        proc = subprocess.run(
            ["rclone", "copyto", str(part), key],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise WriteError("upload_failed", key=key, detail=proc.stderr[-300:])

        # An upload that "returned 0" is not proof. The object must exist, at
        # the size we wrote. This is the gate the operator's delete step will
        # depend on, so it has to be evidence rather than an exit code.
        remote_bytes = _rclone_size(key)
        if remote_bytes < 0:
            raise WriteError("object_absent_after_upload", key=key)
        if remote_bytes != local_bytes:
            raise WriteError(
                "size_mismatch_after_upload",
                key=key,
                detail=f"local={local_bytes} remote={remote_bytes}",
            )

        return UnitResult(
            spec=spec,
            key=key,
            rows=got,
            bytes_written=local_bytes,
            verified=True,
        )
