"""
Pre-built LanceDB collection schemas for chain data.

Each collection stores rows keyed by ``id`` (address / tx hash / token)
with a ``text`` field holding the human-readable pattern that was embedded,
the ``vector`` column itself, and metadata columns used for SQL filters.

Collections:
  * ``wallet_embeddings`` — wallet behavior patterns (for wallet similarity)
  * ``contract_embeddings`` — deployed contract/bytecode signatures (for
    clone & rug detection)
  * ``transaction_embeddings`` — transaction patterns (for fraud detection)
  * ``token_embeddings`` — token metadata + holder patterns (for token
    similarity and scam screening)

Schemas are built with :func:`embedding_schema` so the vector dimension is
consistent with the configured embedding model (``EMBEDDING_DIM``).
"""

from __future__ import annotations

import pyarrow as pa

from lite_indexer.vectors.config import EMBEDDING_DIM

# Vector column type: fixed-size list of float32, one per embedding dim.
VECTOR_TYPE = pa.list_(pa.float32(), EMBEDDING_DIM)


def embedding_schema(
    metadata: dict[str, pa.DataType] | None = None,
    dim: int = EMBEDDING_DIM,
) -> pa.Schema:
    """Build a schema for a vector collection.

    Args:
        metadata: Extra metadata columns (name -> Arrow type), stored
            alongside the id/text/vector columns for SQL filters.
        dim: Embedding dimension (must match the embedding model).

    Returns:
        A PyArrow schema with ``id`` (string), ``text`` (string),
        ``vector`` (list<float32, dim>), and the given metadata fields.
    """
    fields: list[pa.Field] = [
        pa.field("id", pa.string()),
        pa.field("text", pa.string()),
        pa.field("vector", pa.list_(pa.float32(), dim)),
    ]
    if metadata:
        for name, dtype in metadata.items():
            fields.append(pa.field(name, dtype))
    return pa.schema(fields)


# Common metadata columns shared across collections.
_CHAIN_FIELD: dict[str, pa.DataType] = {"chain": pa.string()}
_ADDRESS_META: dict[str, pa.DataType] = {
    "chain": pa.string(),
    "labels": pa.string(),  # comma-separated labels (deployer, mev, …)
    "tx_count": pa.int64(),  # number of observed transactions
    "first_seen": pa.string(),  # ISO timestamp
    "last_seen": pa.string(),  # ISO timestamp
}

WALLET_SCHEMA: pa.Schema = embedding_schema(_ADDRESS_META)
CONTRACT_SCHEMA: pa.Schema = embedding_schema(
    {
        "chain": pa.string(),
        "contract_type": pa.string(),  # token, dex, bridge, …
        "deployer": pa.string(),
        "verified": pa.bool_(),
        "bytecode_hash": pa.string(),
    }
)
TRANSACTION_SCHEMA: pa.Schema = embedding_schema(
    {
        "chain": pa.string(),
        "from_address": pa.string(),
        "to_address": pa.string(),
        "token_address": pa.string(),
        "value": pa.string(),  # raw value as decimal string
        "block_number": pa.int64(),
    }
)
TOKEN_SCHEMA: pa.Schema = embedding_schema(
    {
        "chain": pa.string(),
        "symbol": pa.string(),
        "decimals": pa.int32(),
        "holders": pa.int64(),
        "total_supply": pa.string(),
    }
)

# Collection name -> schema for the pre-built collections.
DEFAULT_COLLECTIONS: dict[str, pa.Schema] = {
    "wallet_embeddings": WALLET_SCHEMA,
    "contract_embeddings": CONTRACT_SCHEMA,
    "transaction_embeddings": TRANSACTION_SCHEMA,
    "token_embeddings": TOKEN_SCHEMA,
}
