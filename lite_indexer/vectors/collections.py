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
consistent with the configured embedding model (``EMBEDDING_DIM``). Prefer
:func:`default_collections` over the module-level snapshot when the vector
dimension must reflect the *current* config value at call time.
"""

from __future__ import annotations

import pyarrow as pa

from lite_indexer.vectors import config as _vector_config
from lite_indexer.vectors.config import EMBEDDING_DIM

# Vector column type: fixed-size list of float32, one per embedding dim.
VECTOR_TYPE = pa.list_(pa.float32(), EMBEDDING_DIM)


def vector_dim_from_type(dtype: pa.DataType) -> int | None:
    """Return the fixed-size list length for a vector dtype.

    Args:
        dtype: A PyArrow data type (typically a collection's ``vector``
            column type).

    Returns:
        The fixed-size list length (the embedding dimension), or ``None``
        when ``dtype`` is not a fixed-size list.
    """
    if pa.types.is_fixed_size_list(dtype):
        size = getattr(dtype, "list_size", None)
        if isinstance(size, int):
            return size
    return None


def embedding_schema(
    metadata: dict[str, pa.DataType] | None = None,
    dim: int | None = None,
) -> pa.Schema:
    """Build a schema for a vector collection.

    Args:
        metadata: Extra metadata columns (name -> Arrow type), stored
            alongside the id/text/vector columns for SQL filters.
        dim: Embedding dimension (must match the embedding model). When
            ``None`` the current ``config.EMBEDDING_DIM`` is read at call
            time, so a runtime override takes effect without a reimport.

    Returns:
        A PyArrow schema with ``id`` (string), ``text`` (string),
        ``vector`` (list<float32, dim>), and the given metadata fields.
    """
    resolved_dim = _vector_config.EMBEDDING_DIM if dim is None else dim
    fields: list[pa.Field] = [
        pa.field("id", pa.string()),
        pa.field("text", pa.string()),
        pa.field("vector", pa.list_(pa.float32(), resolved_dim)),
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
_CONTRACT_META: dict[str, pa.DataType] = {
    "chain": pa.string(),
    "contract_type": pa.string(),  # token, dex, bridge, …
    "deployer": pa.string(),
    "verified": pa.bool_(),
    "bytecode_hash": pa.string(),
}
_TRANSACTION_META: dict[str, pa.DataType] = {
    "chain": pa.string(),
    "from_address": pa.string(),
    "to_address": pa.string(),
    "token_address": pa.string(),
    "value": pa.string(),  # raw value as decimal string
    "block_number": pa.int64(),
}
_TOKEN_META: dict[str, pa.DataType] = {
    "chain": pa.string(),
    "symbol": pa.string(),
    "decimals": pa.int32(),
    "holders": pa.int64(),
    "total_supply": pa.string(),
}

# Collection name -> metadata columns for the pre-built collections.
_COLLECTION_META: dict[str, dict[str, pa.DataType]] = {
    "wallet_embeddings": _ADDRESS_META,
    "contract_embeddings": _CONTRACT_META,
    "transaction_embeddings": _TRANSACTION_META,
    "token_embeddings": _TOKEN_META,
}


def default_collections(dim: int | None = None) -> dict[str, pa.Schema]:
    """Build the four pre-built collection schemas at ``dim``.

    Args:
        dim: Embedding dimension. When ``None`` the current
            ``config.EMBEDDING_DIM`` is read at call time.

    Returns:
        Mapping of collection name to its PyArrow schema.
    """
    resolved_dim = _vector_config.EMBEDDING_DIM if dim is None else dim
    return {name: embedding_schema(meta, dim=resolved_dim) for name, meta in _COLLECTION_META.items()}


# Import-time snapshot (kept for backwards compatibility). Use
# ``default_collections()`` when the dimension must track the live config.
WALLET_SCHEMA: pa.Schema = embedding_schema(_ADDRESS_META, dim=EMBEDDING_DIM)
CONTRACT_SCHEMA: pa.Schema = embedding_schema(_CONTRACT_META, dim=EMBEDDING_DIM)
TRANSACTION_SCHEMA: pa.Schema = embedding_schema(_TRANSACTION_META, dim=EMBEDDING_DIM)
TOKEN_SCHEMA: pa.Schema = embedding_schema(_TOKEN_META, dim=EMBEDDING_DIM)

# Collection name -> schema for the pre-built collections (import-time dim).
DEFAULT_COLLECTIONS: dict[str, pa.Schema] = default_collections(dim=EMBEDDING_DIM)
