"""
Vector ingestion pipeline — LanceDB from ClickHouse.

Turns freshly indexed ClickHouse rows into semantic embeddings that the
LanceDB vector store serves:

* **Wallet embeddings** — per-address behavior summaries aggregated over the
  synced block range: transfer count, first active timestamp, total volume
  and the most-traded token symbol. Text pattern::

      address {addr} on {chain} with {n} transfers, first active {date},
      volume {amount} {symbol}

* **Contract embeddings** — deployed contracts from the ``deployer_events``
  table: the deployer EOA and the number of transfer interactions observed
  with the contract in the range. Text pattern::

      contract {addr} on {chain} deployed by {deployer}, {n} interactions

The pipeline is: read (ClickHouse) → represent (text) → embed (Qwen3-Embedding-4B
via :mod:`lite_indexer.vectors.embedder`) → store
(LanceDB via :mod:`lite_indexer.vectors.lance_store`). It is best-effort by
design: the indexer wraps :func:`run_vector_ingestion` so a failure logs a
warning and never blocks the core sync loop.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from lite_indexer.config import (
    CH_DEPLOYER_TABLE,
    CH_TABLE,
    CH_TOKEN_META_TABLE,
    VECTOR_INGESTION_BATCH_SIZE,
)
from lite_indexer.logging import get_logger
from lite_indexer.vectors.config import COLLECTION_CONTRACTS, COLLECTION_WALLETS

logger = get_logger(__name__)


class VectorIngestionError(RuntimeError):
    """Raised when the ingestion pipeline cannot read from ClickHouse."""


def _date_str(value: Any) -> str:
    """Normalize a ClickHouse DateTime / ISO string to a ``YYYY-MM-DD`` date.

    Args:
        value: DateTime object, ISO string, or None.

    Returns:
        The date portion (``YYYY-MM-DD``) or an empty string when unknown.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d")
    return str(value)[:10]


def wallet_text(row: dict[str, Any]) -> str:
    """Human-readable wallet behavior summary embedded for similarity search.

    Format: ``address {addr} on {chain} with {n} transfers, first active
    {date}, volume {amount} {symbol}``

    Args:
        row: Wallet aggregate with ``address``, ``chain``,
            ``transfer_count``, ``first_active``, ``volume``, ``symbol``.

    Returns:
        The text representation (embedded verbatim by the pipeline).
    """
    return (
        f"address {row.get('address', '')} on {row.get('chain', '')} "
        f"with {int(row.get('transfer_count', 0))} transfers, "
        f"first active {_date_str(row.get('first_active'))}, "
        f"volume {int(row.get('volume', 0))} {row.get('symbol', '')}"
    )


def contract_text(row: dict[str, Any]) -> str:
    """Human-readable contract summary embedded for clone / rug detection.

    Format: ``contract {addr} on {chain} deployed by {deployer}, {n}
    interactions``

    Args:
        row: Contract row with ``contract_address``, ``chain``,
            ``deployer``, ``interaction_count``.

    Returns:
        The text representation (embedded verbatim by the pipeline).
    """
    return (
        f"contract {row.get('contract_address', '')} on {row.get('chain', '')} "
        f"deployed by {row.get('deployer', '')}, "
        f"{int(row.get('interaction_count', 0))} interactions"
    )


def read_wallet_rows(
    client: Any,
    chain: str,
    from_block: int,
    to_block: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Aggregate per-address behavior from ``evm_transfers`` over a range.

    Each unique address (appearing as ``from_address`` or ``to_address``) is
    summarized: transfer count, first active timestamp, total volume (sum of
    ``value``), and the symbol of the token it traded most (joined from
    ``token_meta_ref``).

    Args:
        client: ClickHouse client (anything with ``execute``).
        chain: Chain name.
        from_block: First block of the range.
        to_block: Last block of the range.
        limit: Max wallets to return (most active first).

    Returns:
        List of dicts with keys ``address``, ``chain``, ``transfer_count``,
        ``first_active``, ``volume``, ``symbol``.

    Raises:
        VectorIngestionError: When the ClickHouse read fails.
    """
    query = f"""
    SELECT
        address,
        sum(cnt) AS transfer_count,
        min(first_active) AS first_active,
        sum(volume) AS volume,
        argMax(symbol, cnt) AS symbol
    FROM (
        SELECT
            address,
            token_address,
            chain,
            count() AS cnt,
            min(block_timestamp) AS first_active,
            sum(toInt128(value)) AS volume
        FROM (
            SELECT
                arrayJoin([from_address, to_address]) AS address,
                token_address,
                chain,
                block_timestamp,
                value
            FROM {CH_TABLE}
            WHERE chain = %(chain)s
              AND block_number >= %(from_block)s
              AND block_number <= %(to_block)s
        )
        GROUP BY address, token_address, chain
    ) t
    LEFT JOIN {CH_TOKEN_META_TABLE} meta
        ON meta.chain = t.chain AND meta.token_address = t.token_address
    GROUP BY address
    ORDER BY transfer_count DESC
    LIMIT %(limit)s
    """
    params = {"chain": chain, "from_block": from_block, "to_block": to_block, "limit": limit}
    try:
        rows = client.execute(query, params)  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
    except Exception as exc:
        raise VectorIngestionError(f"wallet read failed: {exc}") from exc

    # Defensive cap: the SQL LIMIT handles production; slicing keeps the
    # batch bounded even with a client that ignores the LIMIT (tests, proxies).
    out: list[dict[str, Any]] = []
    for r in rows[:limit]:
        out.append(
            {
                "address": str(r[0]),
                "chain": chain,
                "transfer_count": int(r[1] or 0),
                "first_active": r[2],
                "volume": int(r[3] or 0),
                "symbol": str(r[4] or ""),
            }
        )
    return out


def read_contract_rows(
    client: Any,
    chain: str,
    from_block: int,
    to_block: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Read newly deployed contracts from ``deployer_events`` over a range.

    Each contract (``token_address``) is summarized with its deployer EOA
    and the number of transfer interactions observed with it in the range
    (counted from ``evm_transfers``).

    Args:
        client: ClickHouse client (anything with ``execute``).
        chain: Chain name.
        from_block: First block of the range.
        to_block: Last block of the range.
        limit: Max contracts to return (most interacted first).

    Returns:
        List of dicts with keys ``contract_address``, ``chain``,
        ``deployer``, ``first_active``, ``interaction_count``,
        ``contract_type``.

    Raises:
        VectorIngestionError: When the ClickHouse read fails.
    """
    query = f"""
    SELECT
        d.contract_address,
        d.chain,
        d.deployer,
        d.first_active,
        coalesce(t.transfer_count, 0) AS interaction_count,
        d.contract_type
    FROM (
        SELECT
            token_address AS contract_address,
            chain,
            deployer,
            min(block_timestamp) AS first_active,
            any(type) AS contract_type
        FROM {CH_DEPLOYER_TABLE}
        WHERE chain = %(chain)s
          AND block_number >= %(from_block)s
          AND block_number <= %(to_block)s
          AND token_address != ''
        GROUP BY token_address, chain, deployer
    ) d
    LEFT JOIN (
        SELECT chain, token_address, count() AS transfer_count
        FROM {CH_TABLE}
        WHERE chain = %(chain)s
          AND block_number >= %(from_block)s
          AND block_number <= %(to_block)s
        GROUP BY chain, token_address
    ) t
        ON t.chain = d.chain AND t.token_address = d.contract_address
    ORDER BY interaction_count DESC
    LIMIT %(limit)s
    """
    params = {"chain": chain, "from_block": from_block, "to_block": to_block, "limit": limit}
    try:
        rows = client.execute(query, params)  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
    except Exception as exc:
        raise VectorIngestionError(f"contract read failed: {exc}") from exc

    out: list[dict[str, Any]] = []
    for r in rows[:limit]:
        out.append(
            {
                "contract_address": str(r[0]),
                "chain": str(r[1]),
                "deployer": str(r[2] or ""),
                "first_active": r[3],
                "interaction_count": int(r[4] or 0),
                "contract_type": str(r[5] or ""),
            }
        )
    return out


async def ingest_wallet_vectors(
    store: Any,
    client: Any,
    chain: str,
    from_block: int,
    to_block: int,
    limit: int = VECTOR_INGESTION_BATCH_SIZE,
) -> int:
    """Read wallet aggregates, embed them, and store in LanceDB.

    Rows are keyed by address and carry the pre-built wallet metadata
    (``chain``, ``tx_count``, ``first_seen``, ``last_seen``) so SQL filters
    on the collection work. The canonical collection schema is ensured
    before writing.

    Args:
        store: :class:`LanceVectorStore` (or a compatible fake).
        client: ClickHouse client.
        chain: Chain name.
        from_block: First block of the range.
        to_block: Last block of the range.
        limit: Max wallets to embed per run.

    Returns:
        Number of rows indexed into ``wallet_embeddings`` (0 when the range
        produced no wallets).
    """
    rows = read_wallet_rows(client, chain, from_block, to_block, limit)
    if not rows:
        return 0
    await store.ensure_default_collections()

    data: list[dict[str, Any]] = []
    for row in rows:
        first_seen = _date_str(row["first_active"])
        data.append(
            {
                "id": row["address"],
                "text": wallet_text(row),
                "chain": row["chain"],
                "labels": "",
                "tx_count": int(row["transfer_count"]),
                "first_seen": first_seen,
                "last_seen": first_seen,
            }
        )
    indexed = await store.index_embeddings(COLLECTION_WALLETS, data)
    logger.info(
        "vector_wallets_indexed",
        chain=chain,
        from_block=from_block,
        to_block=to_block,
        rows=indexed,
    )
    return cast(int, indexed)


async def ingest_contract_vectors(
    store: Any,
    client: Any,
    chain: str,
    from_block: int,
    to_block: int,
    limit: int = VECTOR_INGESTION_BATCH_SIZE,
) -> int:
    """Read deployed contracts, embed them, and store in LanceDB.

    Rows are keyed by contract address and carry the pre-built contract
    metadata (``chain``, ``contract_type``, ``deployer``, ``verified``,
    ``bytecode_hash``).

    Args:
        store: :class:`LanceVectorStore` (or a compatible fake).
        client: ClickHouse client.
        chain: Chain name.
        from_block: First block of the range.
        to_block: Last block of the range.
        limit: Max contracts to embed per run.

    Returns:
        Number of rows indexed into ``contract_embeddings`` (0 when the
        range produced no contracts).
    """
    rows = read_contract_rows(client, chain, from_block, to_block, limit)
    if not rows:
        return 0
    await store.ensure_default_collections()

    data: list[dict[str, Any]] = []
    for row in rows:
        data.append(
            {
                "id": row["contract_address"],
                "text": contract_text(row),
                "chain": row["chain"],
                "contract_type": row.get("contract_type", ""),
                "deployer": row.get("deployer", ""),
                "verified": False,
                "bytecode_hash": "",
            }
        )
    indexed = await store.index_embeddings(COLLECTION_CONTRACTS, data)
    logger.info(
        "vector_contracts_indexed",
        chain=chain,
        from_block=from_block,
        to_block=to_block,
        rows=indexed,
    )
    return cast(int, indexed)


async def run_vector_ingestion(
    store: Any,
    client: Any,
    chain: str,
    from_block: int,
    to_block: int,
    limit: int = VECTOR_INGESTION_BATCH_SIZE,
) -> dict[str, int]:
    """Run the full ingestion pipeline for a block range.

    Reads wallet aggregates + deployed contracts from ClickHouse, embeds
    them (Qwen3-Embedding-4B by default), and stores them in the
    ``wallet_embeddings`` / ``contract_embeddings`` LanceDB collections.

    Best-effort per collection: a ClickHouse read failure raises
    :class:`VectorIngestionError`, an embedding failure raises
    :class:`lite_indexer.vectors.embedder.EmbeddingError` — callers (the
    indexer) are expected to guard the whole call so vector ingestion never
    blocks indexing.

    Args:
        store: :class:`LanceVectorStore` (or a compatible fake).
        client: ClickHouse client.
        chain: Chain name.
        from_block: First block of the range.
        to_block: Last block of the range.
        limit: Max wallets/contracts embedded per run.

    Returns:
        Stats dict: ``{"wallets_indexed": N, "contracts_indexed": M}``.
    """
    wallets = await ingest_wallet_vectors(store, client, chain, from_block, to_block, limit=limit)
    contracts = await ingest_contract_vectors(store, client, chain, from_block, to_block, limit=limit)
    logger.info(
        "vector_ingestion_run",
        chain=chain,
        from_block=from_block,
        to_block=to_block,
        wallets=wallets,
        contracts=contracts,
    )
    return {"wallets_indexed": wallets, "contracts_indexed": contracts}


__all__ = [
    "VectorIngestionError",
    "wallet_text",
    "contract_text",
    "read_wallet_rows",
    "read_contract_rows",
    "ingest_wallet_vectors",
    "ingest_contract_vectors",
    "run_vector_ingestion",
]
