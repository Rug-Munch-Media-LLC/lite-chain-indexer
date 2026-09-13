"""
Token metadata reference table (cryo-inspired).

``evm_transfers`` historically stored ``token_symbol`` and ``token_name``
on every row — roughly 60 bytes x 264M rows of duplicated metadata. This
module moves that to a reference table: one row per token, not per
transfer.

* :func:`ensure_token_meta_table` — idempotent DDL for
  ``wallet_memory.token_meta_ref`` (ReplacingMergeTree, so plain INSERTs
  upsert).
* :func:`upsert_token_meta` — upsert one token's metadata. Call once per
  NEW token per batch (the caller deduplicates), not once per transfer.
* :func:`resolve_token_meta` — read a token's metadata back (used by
  consumers that join the reference table).

Always enabled: ``TOKEN_META_ENABLED`` is a hard-coded ``True`` (see
:mod:`lite_indexer.config`), so ``ClickHouseWriter.write_transfers`` never
writes symbol/name to transfer rows and this reference table is upserted
once per unique token per batch instead. All DDL is idempotent and
non-fatal.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import CH_TOKEN_META_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# One row per token. ReplacingMergeTree deduplicates by
# (chain, token_address), so repeated upserts collapse to one row and the
# last write wins.
_TOKEN_META_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_TOKEN_META_TABLE} (
    chain LowCardinality(String),
    token_address String,
    symbol String DEFAULT '',
    name String DEFAULT '',
    decimals Nullable(UInt8),
    first_seen_block UInt64 DEFAULT 0,
    last_updated DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (chain, token_address)
"""

_INSERT_COLUMNS = (
    "chain",
    "token_address",
    "symbol",
    "name",
    "decimals",
    "first_seen_block",
    "last_updated",
)


async def ensure_token_meta_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.token_meta_ref`` if it does not exist.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False when the
        database is missing or privileges are insufficient.
    """
    try:
        ch_client.execute(_TOKEN_META_DDL)  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
        logger.info("token_meta_table_ensured", table=CH_TOKEN_META_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("token_meta_table_skip", error=str(exc)[:80])
        else:
            logger.warning("token_meta_table_failed", error=str(exc)[:100])
        return False


async def upsert_token_meta(
    ch_client: Any,
    chain: str,
    token_address: str,
    symbol: str | None,
    name: str | None,
    decimals: int | None = None,
    first_seen_block: int = 0,
) -> bool:
    """Upsert one token's metadata into the reference table.

    Idempotent thanks to ReplacingMergeTree — call once per NEW token per
    batch (not per transfer). ``symbol``/``name`` default to empty strings
    and ``decimals`` stays NULL when unknown.

    Args:
        ch_client: ClickHouse client.
        chain: Chain name.
        token_address: Lowercased token contract address.
        symbol: Token symbol (may be empty/None when unenriched).
        name: Token name (may be empty/None when unenriched).
        decimals: Optional token decimals.
        first_seen_block: Block number where the token was first observed.

    Returns:
        True when the upsert was accepted, False on transient failure
        (caller may retry on a later batch).
    """
    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            f"INSERT INTO {CH_TOKEN_META_TABLE} ({', '.join(_INSERT_COLUMNS)}) VALUES",
            [
                (
                    chain,
                    token_address,
                    symbol or "",
                    name or "",
                    decimals,
                    first_seen_block,
                    datetime.now(UTC),
                )
            ],
        )
        metrics_incr("token_meta_upserts")
        return True
    except Exception as exc:  # noqa: BLE001 — cache write is best-effort
        logger.debug(
            "token_meta_upsert_failed",
            chain=chain,
            token=token_address[:12],
            error=str(exc)[:80],
        )
        return False


async def resolve_token_meta(
    ch_client: Any,
    chain: str,
    token_address: str,
) -> dict[str, Any] | None:
    """Read a token's metadata back from the reference table.

    Args:
        ch_client: ClickHouse client.
        chain: Chain name.
        token_address: Token contract address.

    Returns:
        Dict with chain/token_address/symbol/name/decimals/first_seen_block
        or None when the token has not been seen.
    """
    try:
        rows = ch_client.execute(
            f"SELECT chain, token_address, symbol, name, decimals, first_seen_block "
            f"FROM {CH_TOKEN_META_TABLE} "
            f"WHERE chain = %(chain)s AND token_address = %(token)s LIMIT 1",
            {"chain": chain, "token": token_address},
        )
        if not rows:
            return None
        row = rows[0]
        return {
            "chain": row[0],
            "token_address": row[1],
            "symbol": row[2],
            "name": row[3],
            "decimals": row[4],
            "first_seen_block": row[5],
        }
    except Exception as exc:  # noqa: BLE001 — lookup is best-effort
        logger.debug(
            "token_meta_resolve_failed",
            chain=chain,
            token=token_address[:12],
            error=str(exc)[:80],
        )
        return None


__all__ = [
    "ensure_token_meta_table",
    "upsert_token_meta",
    "resolve_token_meta",
]
