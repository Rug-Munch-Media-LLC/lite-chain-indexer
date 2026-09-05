"""Shared path/marker helpers for the parquet cold-storage tier."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any


def _local_dir(data_dir: Path, chain: str, day: date) -> Path:
    """Build the local parquet directory for a chain/date.

    Layout: ``{chain}/{YYYY-MM-DD}/`` — one directory per chain/date
    holding both the transfers and DEX parquet files, so the whole day
    can be uploaded with a single ``rclone copy {dir} {remote}``.

    Args:
        data_dir: Local parquet root.
        chain: Chain name.
        day: Export date.

    Returns:
        Local directory path.
    """
    return data_dir / chain / day.isoformat()


def _local_path(data_dir: Path, chain: str, day: date, kind: str) -> Path:
    """Build the local parquet file path for a chain/date/kind.

    Args:
        data_dir: Local parquet root.
        chain: Chain name.
        day: Export date.
        kind: ``"transfers"`` or ``"dex"``.

    Returns:
        Local file path inside the ``{chain}/{date}/`` directory.
    """
    fname = f"{chain}_dex_{day.isoformat()}.parquet" if kind == "dex" else f"{chain}_{day.isoformat()}.parquet"
    return _local_dir(data_dir, chain, day) / fname


def _remote_dir(remote: str, chain: str, day: date) -> str:
    """Build the rclone remote directory for a chain/date.

    Args:
        remote: rclone remote root (e.g. ``b2-crypt:lite-indexer-parquet``).
        chain: Chain name.
        day: Export date.

    Returns:
        Remote directory path.
    """
    return f"{remote}/{chain}/{day.isoformat()}"


def _remote_path(remote: str, chain: str, day: date, kind: str) -> str:
    """Build the rclone remote file path for a chain/date/kind.

    Args:
        remote: rclone remote root.
        chain: Chain name.
        day: Export date.
        kind: ``"transfers"`` or ``"dex"``.

    Returns:
        Remote file path.
    """
    return f"{_remote_dir(remote, chain, day)}/{_local_path(Path(''), chain, day, kind).name}"


def _state_marker(state_dir: Path, chain: str, day: date) -> Path:
    """Return the state marker path for a (chain, date)."""
    return state_dir / chain / f"{day.isoformat()}.done"


def _block_range(transfer_rows: list[dict[str, Any]], dex_rows: list[dict[str, Any]]) -> tuple[int, int]:
    """Compute the min/max block span over exported row dicts.

    The manifest records ``start_block``/``end_block`` so bootstrap can
    set the state high-water mark to the snapshot's true coverage.

    Args:
        transfer_rows: Rows from the transfers table.
        dex_rows: Rows from the DEX table.

    Returns:
        ``(start_block, end_block)``, both 0 when no rows carry a
        ``block_number``.
    """
    start: int | None = None
    end: int | None = None
    for rows in (transfer_rows, dex_rows):
        for row in rows:
            raw = row.get("block_number")
            if raw is None:
                continue
            try:
                block = int(raw)
            except (TypeError, ValueError):
                continue
            if start is None or block < start:
                start = block
            if end is None or block > end:
                end = block
    return (start if start is not None else 0, end if end is not None else 0)


__all__ = [
    "_local_dir",
    "_local_path",
    "_remote_dir",
    "_remote_path",
    "_state_marker",
    "_block_range",
]
