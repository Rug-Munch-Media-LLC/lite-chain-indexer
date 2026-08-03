"""
Embedded LanceDB vector store for chain data.

LanceDB is an embedded, columnar vector database — no server process, no
cost, disk-based. It pairs with ClickHouse: ClickHouse answers analytical
SQL over the event warehouse, LanceDB answers semantic / ANN queries over
vectorized chain data (wallet behavior, contract bytecode, transactions,
token metadata).

This module provides :class:`LanceVectorStore`, an async facade over the
synchronous LanceDB API:

* :meth:`create_collection` — create a collection from a PyArrow schema
* :meth:`index_embeddings` — embed text rows and store them
* :meth:`search` — ANN vector search with an optional SQL filter
* :meth:`hybrid_search` — BM25 full-text + vector hybrid (RRF merge)
* :meth:`similar_by_id` — find rows most similar to a known id/address
* :meth:`ensure_default_collections` — create the four pre-built
  collections (wallet / contract / transaction / token embeddings)

Embedding generation is delegated to :mod:`lite_indexer.vectors.embedder`
(Ollama local by default, OpenAI-compatible as an alternative).
"""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa

from lite_indexer.vectors.collections import DEFAULT_COLLECTIONS
from lite_indexer.vectors.config import LANCE_DIR
from lite_indexer.vectors.embedder import Embedder, create_embedder

logger = logging.getLogger(__name__)

# RRF constant: standard k=60 used by BM25/vector fusion.
_RRF_K = 60


class CollectionNotFoundError(LookupError):
    """Raised when a collection does not exist in the database."""


class LanceVectorStore:
    """Embedded LanceDB vector store for chain data."""

    def __init__(self, path: str | None = None, embedder: Embedder | None = None) -> None:
        """Connect to the LanceDB database at ``path``.

        Args:
            path: LanceDB database directory (default: ``LANCE_DIR``).
            embedder: Embedding provider (default: configured via env).
        """
        import lancedb

        self.path = str(path or LANCE_DIR)
        self.db = lancedb.connect(self.path)
        self._embedder = embedder or create_embedder()
        self._owns_embedder = embedder is None

    # ── Embedding helpers ──────────────────────────────────────────────

    @property
    def embedder(self) -> Embedder:
        """The active embedding provider."""
        return self._embedder

    async def _embed(
        self,
        texts: list[str],
        embedding_model: str | None,
        on_progress: Any = None,
    ) -> list[list[float]]:
        """Embed texts, switching providers when a model override is given."""
        if embedding_model is None or embedding_model == self._embedder.model:
            return await self._embedder.embed(texts, on_progress=on_progress)
        temp = create_embedder(model=embedding_model)
        try:
            return await temp.embed(texts, on_progress=on_progress)
        finally:
            close = getattr(temp, "aclose", None)
            if close is not None:
                await close()

    # ── Collection management ──────────────────────────────────────────

    def _table_names(self) -> set[str]:
        """Return the set of collection names in the database.

        ``list_tables()`` returns a paginated wrapper (``.tables``) in some
        LanceDB versions; normalize to a plain set of names.
        """
        listing = self.db.list_tables()
        if isinstance(listing, (list, tuple, set)):
            return {str(name) for name in listing}
        tables = getattr(listing, "tables", None)
        if tables is None:
            return set()
        return {str(name) for name in tables}

    def list_collections(self) -> list[str]:
        """Return the names of all collections in the database."""
        return sorted(self._table_names())

    async def create_collection(self, name: str, schema: pa.Schema) -> bool:
        """Create a new collection with ``schema``.

        Args:
            name: Collection name.
            schema: PyArrow schema (see :mod:`lite_indexer.vectors.collections`).

        Returns:
            True when the collection was created, False when it already exists.
        """
        if name in self._table_names():
            return False
        self.db.create_table(name, schema=schema)
        logger.info("created vector collection %s at %s", name, self.path)
        return True

    async def ensure_default_collections(self) -> list[str]:
        """Create the four pre-built collections (idempotent).

        Returns:
            The names of collections that were created.
        """
        created: list[str] = []
        for name, schema in DEFAULT_COLLECTIONS.items():
            if await self.create_collection(name, schema):
                created.append(name)
        return created

    def drop_collection(self, name: str) -> bool:
        """Drop a collection; returns True when it existed."""
        if name not in self._table_names():
            return False
        self.db.drop_table(name)
        return True

    def collection_stats(self, name: str) -> dict[str, Any]:
        """Return basic stats (row count) for a collection."""
        table = self._open_table(name)
        return {"name": name, "rows": table.count_rows()}

    # ── Indexing ───────────────────────────────────────────────────────

    async def index_embeddings(
        self,
        collection: str,
        data: list[dict[str, Any]],
        embedding_model: str | None = None,
        schema: pa.Schema | None = None,
        on_progress: Any = None,
    ) -> int:
        """Generate embeddings for ``data`` and store them in ``collection``.

        Each dict in ``data`` must contain at least ``id`` and ``text``; the
        ``text`` field is embedded and the resulting ``vector`` is added to
        the row before storage. Remaining keys are stored as metadata.

        Args:
            collection: Target collection name (created if missing).
            data: Rows to embed and store.
            embedding_model: Optional model override (default: configured model).
            schema: Optional schema for a newly-created collection (inferred
                from the first row when omitted).
            on_progress: Callback ``(done, total)`` called per embedding batch.

        Returns:
            The number of rows indexed.
        """
        if not data:
            return 0
        texts = [str(row["text"]) for row in data]
        vectors = await self._embed(texts, embedding_model, on_progress=on_progress)
        rows: list[dict[str, Any]] = []
        for row, vector in zip(data, vectors, strict=True):
            enriched = dict(row)
            enriched["vector"] = vector
            rows.append(enriched)

        if collection not in self._table_names():
            self.db.create_table(collection, data=rows, schema=schema)
            logger.info("created vector collection %s (indexed %d rows)", collection, len(rows))
        else:
            self.db.open_table(collection).add(rows)
        return len(rows)

    async def add_vectors(self, collection: str, rows: list[dict[str, Any]]) -> int:
        """Store pre-embedded rows (rows must already contain ``vector``)."""
        if not rows:
            return 0
        if collection not in self._table_names():
            self.db.create_table(collection, data=rows)
        else:
            self.db.open_table(collection).add(rows)
        return len(rows)

    # ── Querying ───────────────────────────────────────────────────────

    async def search(
        self,
        collection: str,
        query_vec: list[float],
        k: int = 10,
        filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """ANN vector search with an optional SQL filter.

        Args:
            collection: Collection to search.
            query_vec: Query embedding vector.
            k: Number of results to return.
            filter: Optional LanceDB SQL filter (e.g. ``chain = 'ethereum'``).

        Returns:
            List of result rows (each includes ``id``, ``text``, metadata,
            ``_distance``) ordered by increasing distance.
        """
        table = self._open_table(collection)
        query = table.search(query_vec)
        if filter:
            query = query.where(filter)
        return [dict(row) for row in query.limit(k).to_list()]

    async def hybrid_search(
        self,
        collection: str,
        text_query: str,
        k: int = 10,
        embedding_model: str | None = None,
    ) -> list[dict[str, Any]]:
        """BM25 full-text + vector hybrid search (Reciprocal Rank Fusion).

        Requires a full-text index on the ``text`` column (created
        automatically on first call). The query text is embedded for the
        vector leg; BM25 scores come from the FTS index.

        Returns:
            List of result rows (with ``_rrf_score``) ordered by RRF score.
        """
        table = self._open_table(collection)
        # Ensure the FTS index exists (idempotent).
        from lancedb.index import FTS  # local import: lancedb is optional

        try:
            table.create_index("text", config=FTS())
        except Exception:  # noqa: BLE001 — index may already exist / be unavailable
            logger.debug("FTS index create skipped for %s", collection)

        fts_results = table.search(text_query, query_type="fts").limit(k * 2).to_list()
        (query_vec,) = await self._embed([text_query], embedding_model)
        vec_results = await self.search(collection, query_vec, k=k * 2)
        return _rrf_merge(fts_results, vec_results, limit=k)

    async def similar_by_id(
        self,
        collection: str,
        row_id: str,
        k: int = 10,
        filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """Find rows most similar to the row identified by ``row_id``.

        Looks up the stored vector for ``row_id`` and runs an ANN search
        with it. Useful for wallet/contract similarity endpoints.

        Args:
            collection: Collection to search.
            row_id: The ``id`` of the anchor row (address, tx hash, …).
            k: Number of results to return.
            filter: Optional LanceDB SQL filter.

        Returns:
            List of result rows ordered by increasing distance.
        """
        table = self._open_table(collection)
        anchor = table.search().where(f"id = '{row_id}'").limit(1).to_list()
        if not anchor:
            raise CollectionNotFoundError(f"{row_id} not found in {collection}")
        query_vec = anchor[0]["vector"]
        return await self.search(collection, query_vec, k=k, filter=filter)

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def aclose(self) -> None:
        """Close the embedding provider's HTTP client if we own it."""
        if self._owns_embedder:
            close = getattr(self._embedder, "aclose", None)
            if close is not None:
                await close()

    def _open_table(self, name: str) -> Any:
        """Open a collection, raising CollectionNotFoundError if missing."""
        if name not in self._table_names():
            raise CollectionNotFoundError(f"collection '{name}' does not exist")
        return self.db.open_table(name)


def _rrf_merge(
    fts_results: list[dict[str, Any]],
    vec_results: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    """Merge FTS and vector results with Reciprocal Rank Fusion.

    Each result list contributes ``1 / (k + rank)`` to its rows; rows are
    ranked by the summed score. Rank is 1-based.
    """
    scores: dict[Any, float] = {}
    rows: dict[Any, dict[str, Any]] = {}

    for results in (fts_results, vec_results):
        for rank, row in enumerate(results, start=1):
            row_id = row.get("id")
            if row_id is None:
                continue
            scores[row_id] = scores.get(row_id, 0.0) + 1.0 / (_RRF_K + rank)
            rows[row_id] = row

    merged = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:limit]
    out: list[dict[str, Any]] = []
    for row_id, score in merged:
        row = dict(rows[row_id])
        row["_rrf_score"] = score
        out.append(row)
    return out
