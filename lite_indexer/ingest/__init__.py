"""External ingest sources that hydrate the indexer without RPC.

- :mod:`lite_indexer.ingest.flatfile` — provider-agnostic flat-file
  bootstrap (any pre-indexed parquet dataset: local dir, http(s) base URL,
  or pyarrow.fs URL).
"""

from lite_indexer.ingest.flatfile import (
    ExternalFlatfileSource,
    FlatfileChunk,
    FlatfileDataset,
    FlatfileImportResult,
    FlatfileSourceError,
    FlatfileTable,
)

__all__ = [
    "ExternalFlatfileSource",
    "FlatfileChunk",
    "FlatfileDataset",
    "FlatfileImportResult",
    "FlatfileSourceError",
    "FlatfileTable",
]
