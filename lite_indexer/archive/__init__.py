"""B2 unit writer — the one place a verified parquet object is published.

WHY THIS PACKAGE EXISTS
    Every backfill source (Subsquid, Envio HyperSync, BigQuery, BigLake,
    ClickHouse) needs the SAME last three steps: write parquet, prove the row
    count, upload, prove the object. Before this package that contract was
    embedded in `scripts/backfill-evm-hours.py`, tied to its ClickHouse SQL, so
    a second source would have had to copy it. Five sources, five copies of the
    same correctness rules.

    Correctness rules that must not be re-implemented per source:
      1. Local footprint is a TemporaryDirectory. The server must never grow
         with retention — that is the operator's explicit requirement, and it is
         enforced here rather than trusted to each caller.
      2. An upload is not a success. Success is: the object exists at the
         expected size AND the local row count matches the expected count.
      3. The local part is deleted when the context exits, whether or not the
         upload worked. A failed unit is cheaper to re-fetch than a full disk.

WHAT THIS PACKAGE IS NOT
    It knows nothing about chains, providers, or schemas. A source adapter turns
    a (chain, table, day) into an iterator of row dicts; this package publishes
    them. That split is what lets one writer serve every tier.
"""

from __future__ import annotations

from .unit_writer import (
    UnitResult,
    UnitSpec,
    WriteError,
    object_key,
    write_unit,
)

__all__ = [
    "UnitResult",
    "UnitSpec",
    "WriteError",
    "object_key",
    "write_unit",
]
