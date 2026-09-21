"""BigQuery SQL builder + ``bq`` CLI backend (split from bigquery_source).

Keeps the source module within the structure ratchet. Holds the
``crypto_ethereum`` table maps, the parameterized SQL builder, and the
subprocess shell-out to the ``bq`` CLI. Imported by
:mod:`lite_indexer.query.bigquery_source` only.
"""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Sequence
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_PROJECT = "gen-lang-client-0708984505"
_DATASET = "bigquery-public-data.crypto_ethereum"

#: Per-table SELECT lists, aliased to the warehouse column names.
#: Timestamps are exported as epoch milliseconds for deterministic mapping.
_TABLE_SELECT: dict[str, str] = {
    "transactions": (
        "block_number, UNIX_MILLIS(block_timestamp) AS block_timestamp, "
        "`hash` AS tx_hash, from_address, to_address, value"
    ),
    "token_transfers": (
        "block_number, UNIX_MILLIS(block_timestamp) AS block_timestamp, "
        "transaction_hash AS tx_hash, from_address, to_address, token_address, value, log_index"
    ),
    "logs": (
        "block_number, UNIX_MILLIS(block_timestamp) AS block_timestamp, "
        "transaction_hash AS tx_hash, address AS token_address, log_index"
    ),
    "contracts": "block_number, UNIX_MILLIS(block_timestamp) AS block_timestamp, address AS token_address",
    "blocks": "number AS block_number, UNIX_MILLIS(timestamp) AS block_timestamp, `hash` AS tx_hash, transaction_count",
}

#: Address columns usable for the ``address`` filter (empty = unsupported).
_ADDR_COLS: dict[str, tuple[str, ...]] = {
    "transactions": ("from_address", "to_address"),
    "token_transfers": ("from_address", "to_address"),
    "logs": ("address",),
    "contracts": ("address",),
    "blocks": (),
}


def build_query(
    table: str,
    *,
    address: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    dates: Sequence[str] | None = None,
    limit: int = 100,
) -> tuple[str, list[tuple[str, str, Any]]]:
    """Build parameterized SQL for one ``crypto_ethereum`` table.

    Returns ``(sql, params)`` where params are ``(name, bigquery_type, value)``.
    Raises ``ValueError`` on a malformed date.
    """
    where: list[str] = []
    params: list[tuple[str, str, Any]] = []
    if from_block is not None:
        where.append("block_number >= @from_block")
        params.append(("from_block", "INT64", int(from_block)))
    if to_block is not None:
        where.append("block_number <= @to_block")
        params.append(("to_block", "INT64", int(to_block)))
    if address:
        addr40 = str(address).strip().lower().removeprefix("0x")[-40:]
        clauses = " OR ".join(f"LOWER({c}) = @address" for c in _ADDR_COLS[table])
        where.append(f"({clauses})")
        params.append(("address", "STRING", addr40))
    if dates:
        for d in dates:
            if not _DATE_RE.match(str(d)):
                raise ValueError(f"invalid date for bigquery query: {d!r} (expected YYYY-MM-DD)")
        where.append("DATE(block_timestamp) IN UNNEST(@dates)")
        params.append(("dates", "ARRAY<DATE>", [str(d) for d in dates]))

    sql = f"SELECT {_TABLE_SELECT[table]} FROM `{_DATASET}.{table}`"  # nosemgrep: formatted-sql-query — table from fixed allowlist
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY block_number DESC LIMIT @limit"
    params.append(("limit", "INT64", max(1, min(int(limit), 1000))))
    return sql, params


def run_bq(bq_bin: str, sql: str, params: list[tuple[str, str, Any]]) -> list[dict[str, Any]]:
    """Shell out to the ``bq`` CLI with named parameters; [] on any failure."""
    cmd = [bq_bin, "--project_id", _PROJECT, "--format=json", "--quiet", "query", "--use_legacy_sql=false"]
    for name, bigquery_type, value in params:
        rendered = json.dumps(value) if bigquery_type.startswith("ARRAY<") else str(value)
        cmd.append(f"--parameter={name}:{bigquery_type}:{rendered}")
    cmd.append(sql)
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        logger.warning("bigquery_bq_failed", error=((proc.stderr or proc.stdout) or "")[:300])
        return []
    try:
        data = json.loads(proc.stdout or "[]")
    except ValueError:
        logger.warning("bigquery_bad_json", error=(proc.stdout or "")[:200])
        return []
    return data if isinstance(data, list) else []
