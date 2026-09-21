"""BigQuery SQL builder + ``bq`` CLI backend (split from bigquery_source).

Keeps the source module within the structure ratchet. Holds the
``crypto_ethereum`` table maps, the parameterized SQL builder, and the
subprocess shell-out to the ``bq`` CLI. Imported by
:mod:`lite_indexer.query.bigquery_source` only.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query.bigquery_cli import run_bq  # noqa: F401  (public re-export)

logger = get_logger(__name__)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_PROJECT = "gen-lang-client-0708984505"
_DATASET = "bigquery-public-data.crypto_ethereum"

# Solana public dataset (verified 2026-09-21 via ``bq show --schema``):
# ``Token Transfers`` and ``Instructions`` are DAY-partitioned on
# ``block_timestamp``; ``Instructions`` is clustered on ``program_id``.
# Column notes: ``Token Transfers.value``/``decimals`` are NUMERIC (bq CLI
# returns them as JSON strings); ``Instructions.accounts`` is REPEATED
# STRING (base58, case-sensitive); ``Instructions.index`` is the
# instruction index within the transaction.
_SOLANA_DATASET = "bigquery-public-data.crypto_solana_mainnet_us"

#: Solana logical table -> physical BigQuery table name.
_SOLANA_TABLES: dict[str, str] = {
    "token_transfers": "Token Transfers",
    "instructions": "Instructions",
}

#: Per-table SELECT lists for the solana dataset, aliased to warehouse names.
_SOLANA_SELECT: dict[str, str] = {
    "token_transfers": (
        "block_slot AS slot, UNIX_MILLIS(block_timestamp) AS block_timestamp, "
        "tx_signature AS tx_hash, mint, authority AS owner, "
        "CAST(value AS STRING) AS value, CAST(decimals AS STRING) AS decimals, "
        "source, destination, transfer_type"
    ),
    "instructions": (
        "block_slot AS slot, UNIX_MILLIS(block_timestamp) AS block_timestamp, "
        "tx_signature AS tx_hash, `index` AS transaction_index, "
        "program_id, accounts, data"
    ),
}

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
    chain: str = "eth",
    address: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    dates: Sequence[str] | None = None,
    limit: int = 100,
    mints: Sequence[str] | None = None,
    offset: int | None = None,
) -> tuple[str, list[tuple[str, str, Any]]]:
    """Build parameterized SQL for one public-dataset table.

    ``chain="eth"`` reads ``crypto_ethereum``; ``chain="solana"`` reads
    ``crypto_solana_mainnet_us`` (``token_transfers`` / ``instructions``).
    Returns ``(sql, params)`` where params are ``(name, bigquery_type, value)``.
    Raises ``ValueError`` on a malformed date or an unknown solana table.
    """
    if chain == "solana":
        return _build_solana_query(
            table,
            address=address,
            from_block=from_block,
            to_block=to_block,
            dates=dates,
            limit=limit,
            mints=mints,
            offset=offset,
        )
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


def _build_solana_query(
    table: str,
    *,
    address: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    dates: Sequence[str] | None = None,
    limit: int = 100,
    mints: Sequence[str] | None = None,
    offset: int | None = None,
) -> tuple[str, list[tuple[str, str, Any]]]:
    """Build parameterized SQL for one ``crypto_solana_mainnet_us`` table.

    Always filters on the ``block_timestamp`` DAY partition when dates are
    given (cost guardrail: never a full-table scan). ``from_block``/``to_block``
    are interpreted as slot bounds. For ``instructions`` the ``address``
    filter matches membership in the base58 ``accounts`` array.
    """
    if table not in _SOLANA_TABLES:
        raise ValueError(f"unsupported solana bigquery table: {table!r}")
    where: list[str] = []
    params: list[tuple[str, str, Any]] = []
    if mints:
        where.append("mint IN UNNEST(@mints)")
        params.append(("mints", "ARRAY<STRING>", [str(m) for m in mints]))
    if from_block is not None:
        where.append("block_slot >= @from_slot")
        params.append(("from_slot", "INT64", int(from_block)))
    if to_block is not None:
        where.append("block_slot <= @to_slot")
        params.append(("to_slot", "INT64", int(to_block)))
    if address:
        addr = str(address).strip()
        if table == "instructions":
            where.append("@address IN UNNEST(accounts)")
        else:
            where.append("(source = @address OR destination = @address OR authority = @address)")
        params.append(("address", "STRING", addr))
    if dates:
        for d in dates:
            if not _DATE_RE.match(str(d)):
                raise ValueError(f"invalid date for bigquery query: {d!r} (expected YYYY-MM-DD)")
        where.append("DATE(block_timestamp) IN UNNEST(@dates)")
        params.append(("dates", "ARRAY<DATE>", [str(d) for d in dates]))

    sql = f"SELECT {_SOLANA_SELECT[table]} FROM `{_SOLANA_DATASET}.{_SOLANA_TABLES[table]}`"  # nosemgrep: formatted-sql-query — table from fixed allowlist
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY block_slot DESC LIMIT @limit"
    params.append(("limit", "INT64", max(1, min(int(limit), 1000))))
    if offset is not None and int(offset) > 0:
        sql += " OFFSET @offset"
        params.append(("offset", "INT64", int(offset)))
    return sql, params

