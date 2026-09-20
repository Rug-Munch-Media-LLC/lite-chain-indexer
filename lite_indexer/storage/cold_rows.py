"""File selection + row decoding for the B2 parquet cold tier.

Applies the producer contract in :mod:`cold_parquet` to a collected file
set and to the rows DuckDB returns from it:

* files are routed by table kind (transfers / dex / solana) and never
  unioned across producer schemas (Solana has no ``chain`` column, so it
  must not join an EVM ``read_parquet`` call);
* binary (legacy ``FixedString``) values are hex-decoded to ``0x…`` so
  addresses match ClickHouse's ``String`` output;
* missing canonical columns take type defaults, and ``chain`` is injected
  for exports that have no chain column.
"""

from __future__ import annotations

import os
from typing import Any

from lite_indexer.storage.cold_parquet import (
    COLUMN_DEFAULTS,
    TABLE_COLUMNS,
    TABLE_KINDS,
)


def files_for_table(files: dict[str, list[Any]], table: str) -> list[Any]:
    """Select the producer files backing one logical table.

    Transfers exclude the Solana directory: those files have no ``chain``
    column and cannot union with EVM parquet in one ``read_parquet`` call.
    """
    kind = TABLE_KINDS[table]
    if kind == "solana":
        picked = list(files.get("spl_balance_changes", []))
    elif kind == "dex":
        picked = list(files.get("dex", []))
    elif kind == "transfers":
        picked = [p for p in files.get("transfers", []) if "/solana/" not in str(p).replace("\\", "/")]
    else:  # deployer / internal: no export yet — match the kind marker.
        picked = [p for bucket in files.values() for p in bucket if f"_{kind}_" in os.path.basename(str(p))]
    return sorted(picked, key=lambda p: os.path.basename(str(p)))


def decode_row(table: str, row: dict[str, Any], chain: str = "") -> dict[str, Any]:
    """Normalize one parquet row to the canonical column shape.

    Binary (FixedString) values become ``0x`` hex strings with trailing NUL
    padding stripped; missing columns take type defaults; ``chain`` is
    injected for exports that have no chain column (Solana).
    """
    out: dict[str, Any] = {}
    for col in TABLE_COLUMNS[table]:
        value = row.get(col)
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = "0x" + bytes(value).rstrip(b"\x00").hex()
        if value is None:
            value = chain if col == "chain" and chain else COLUMN_DEFAULTS.get(col, "")
        out[col] = value
    return out


__all__ = ["decode_row", "files_for_table"]
