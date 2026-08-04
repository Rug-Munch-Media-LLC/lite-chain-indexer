"""Hermetic tests for the LanceDB vector ingestion pipeline.

Covers text generation, ClickHouse row shaping, batch-limited ingestion into
a real (tmp-dir) LanceDB store with a deterministic fake embedder, and the
indexer-level ingestion guard (failures never block indexing).
"""

from __future__ import annotations

import hashlib
import math
from typing import Any

import pytest

from lite_indexer.indexer import LiteIndexer
from lite_indexer.vectors.config import EMBEDDING_DIM
from lite_indexer.vectors.ingestion import (
    VectorIngestionError,
    contract_text,
    ingest_contract_vectors,
    ingest_wallet_vectors,
    read_contract_rows,
    read_wallet_rows,
    run_vector_ingestion,
    wallet_text,
)
from lite_indexer.vectors.lance_store import LanceVectorStore


class FakeEmbedder:
    """Deterministic embedder matching EMBEDDING_DIM (768) so the pre-built
    collection schemas accept the vectors."""

    model = "fake-embed"

    def __init__(self, dim: int = EMBEDDING_DIM) -> None:
        self.dim = dim

    async def embed(self, texts: list[str], on_progress: Any = None) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode()).digest()
            vec = [float(digest[i % len(digest)]) / 255.0 for i in range(self.dim)]
            norm = math.sqrt(sum(v * v for v in vec)) or 1.0
            vectors.append([v / norm for v in vec])
        if on_progress is not None:
            on_progress(len(texts), len(texts))
        return vectors


class FakeCHClient:
    """Minimal ClickHouse client stand-in.

    ``execute`` dispatches on the query text: queries mentioning
    ``deployer`` (the contract reader) return contract rows, everything else
    returns wallet rows. The LIMIT parameter is intentionally ignored so the
    reader's defensive cap is exercised in the batch-size test.
    """

    def __init__(
        self,
        wallet_rows: list[tuple[Any, ...]] | None = None,
        contract_rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        if wallet_rows is not None:
            self.wallet_rows = wallet_rows
        else:
            self.wallet_rows = [
                ("0xaaa", 5, "2026-08-01 10:00:00", 1000, "USDC"),
                ("0xbbb", 2, "2026-08-02 11:00:00", 250, "WETH"),
            ]
        if contract_rows is not None:
            self.contract_rows = contract_rows
        else:
            self.contract_rows = [
                ("0xc1", "ethereum", "0xdeployer", "2026-08-01 12:00:00", 7, "v2_pool"),
            ]

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        if "deployer" in sql:
            return self.contract_rows
        return self.wallet_rows


# ── Text generation ────────────────────────────────────────────────────


def test_wallet_text_format() -> None:
    row = {
        "address": "0xabc",
        "chain": "ethereum",
        "transfer_count": 42,
        "first_active": "2026-08-01 10:00:00",
        "volume": 12345,
        "symbol": "USDC",
    }
    assert wallet_text(row) == (
        "address 0xabc on ethereum with 42 transfers, first active 2026-08-01, volume 12345 USDC"
    )


def test_wallet_text_missing_fields() -> None:
    text = wallet_text({"address": "0x1", "chain": "base"})
    assert "0x1" in text
    assert "base" in text
    assert "with 0 transfers" in text


def test_wallet_text_datetime_first_active() -> None:
    from datetime import UTC, datetime

    row = {
        "address": "0xabc",
        "chain": "ethereum",
        "transfer_count": 1,
        "first_active": datetime(2026, 8, 1, 10, 30, tzinfo=UTC),
        "volume": 5,
        "symbol": "USDC",
    }
    assert wallet_text(row) == ("address 0xabc on ethereum with 1 transfers, first active 2026-08-01, volume 5 USDC")


def test_contract_text_format() -> None:
    row = {
        "contract_address": "0xc0ffee",
        "chain": "bsc",
        "deployer": "0xd00d",
        "interaction_count": 99,
    }
    assert contract_text(row) == "contract 0xc0ffee on bsc deployed by 0xd00d, 99 interactions"


def test_contract_text_missing_fields() -> None:
    text = contract_text({"contract_address": "0xc1", "chain": "ethereum"})
    assert "0xc1" in text
    assert "0 interactions" in text


# ── ClickHouse readers ─────────────────────────────────────────────────


def test_read_wallet_rows_shapes() -> None:
    client = FakeCHClient(wallet_rows=[("0xabc", 3, "2026-08-01 10:00:00", 700, "USDT")])
    rows = read_wallet_rows(client, "ethereum", 100, 200, limit=10)
    assert rows == [
        {
            "address": "0xabc",
            "chain": "ethereum",
            "transfer_count": 3,
            "first_active": "2026-08-01 10:00:00",
            "volume": 700,
            "symbol": "USDT",
        }
    ]


def test_read_contract_rows_shapes() -> None:
    client = FakeCHClient(contract_rows=[("0xc1", "ethereum", "0xdep", "2026-08-01 12:00:00", 7, "v2_pool")])
    rows = read_contract_rows(client, "ethereum", 100, 200, limit=10)
    assert len(rows) == 1
    assert rows[0]["contract_address"] == "0xc1"
    assert rows[0]["chain"] == "ethereum"
    assert rows[0]["deployer"] == "0xdep"
    assert rows[0]["interaction_count"] == 7
    assert rows[0]["contract_type"] == "v2_pool"


def test_read_wallet_rows_raises_on_ch_failure() -> None:
    class BrokenCH:
        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
            raise RuntimeError("clickhouse down")

    with pytest.raises(VectorIngestionError):
        read_wallet_rows(BrokenCH(), "ethereum", 1, 10, limit=5)


def test_read_contract_rows_raises_on_ch_failure() -> None:
    class BrokenCH:
        def execute(self, sql: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
            raise RuntimeError("clickhouse down")

    with pytest.raises(VectorIngestionError):
        read_contract_rows(BrokenCH(), "ethereum", 1, 10, limit=5)


# ── Ingestion into LanceDB ─────────────────────────────────────────────


@pytest.fixture()
def store(tmp_path) -> LanceVectorStore:
    """A real LanceVectorStore backed by a tmp dir + fake embedder."""
    return LanceVectorStore(path=str(tmp_path / "lance"), embedder=FakeEmbedder())


async def test_ingest_wallet_vectors_populates_lance(store: LanceVectorStore) -> None:
    indexed = await ingest_wallet_vectors(store, FakeCHClient(), "ethereum", 100, 200, limit=10)
    assert indexed == 2
    assert store.collection_stats("wallet_embeddings")["rows"] == 2


async def test_ingest_contract_vectors_populates_lance(store: LanceVectorStore) -> None:
    indexed = await ingest_contract_vectors(store, FakeCHClient(), "ethereum", 100, 200, limit=10)
    assert indexed == 1
    assert store.collection_stats("contract_embeddings")["rows"] == 1


async def test_run_vector_ingestion_summary(store: LanceVectorStore) -> None:
    stats = await run_vector_ingestion(store, FakeCHClient(), "ethereum", 100, 200, limit=10)
    assert stats == {"wallets_indexed": 2, "contracts_indexed": 1}
    assert store.collection_stats("wallet_embeddings")["rows"] == 2
    assert store.collection_stats("contract_embeddings")["rows"] == 1


async def test_ingest_wallet_rows_carry_metadata(store: LanceVectorStore) -> None:
    await ingest_wallet_vectors(store, FakeCHClient(), "ethereum", 100, 200, limit=10)
    (query_vec,) = await store.embedder.embed(["address 0xaaa on ethereum with 5 transfers"])
    results = await store.search("wallet_embeddings", query_vec, k=1)
    assert results[0]["id"] == "0xaaa"
    assert results[0]["tx_count"] == 5
    assert results[0]["chain"] == "ethereum"


async def test_ingest_contract_rows_carry_metadata(store: LanceVectorStore) -> None:
    await ingest_contract_vectors(store, FakeCHClient(), "ethereum", 100, 200, limit=10)
    (query_vec,) = await store.embedder.embed(["contract 0xc1 on ethereum deployed by 0xdeployer"])
    results = await store.search("contract_embeddings", query_vec, k=1)
    assert results[0]["id"] == "0xc1"
    assert results[0]["deployer"] == "0xdeployer"
    assert results[0]["verified"] is False


# ── Batch size cap ─────────────────────────────────────────────────────


async def test_batch_size_caps_wallets(store: LanceVectorStore) -> None:
    client = FakeCHClient(wallet_rows=[(f"0x{i:04x}", i, "2026-08-01 00:00:00", i * 10, "USDC") for i in range(200)])
    indexed = await ingest_wallet_vectors(store, client, "ethereum", 1, 100, limit=50)
    assert indexed == 50
    assert store.collection_stats("wallet_embeddings")["rows"] == 50


async def test_empty_range_indexes_nothing(store: LanceVectorStore) -> None:
    client = FakeCHClient(wallet_rows=[], contract_rows=[])
    stats = await run_vector_ingestion(store, client, "ethereum", 1, 10, limit=10)
    assert stats == {"wallets_indexed": 0, "contracts_indexed": 0}
    assert store.collection_stats("wallet_embeddings")["rows"] == 0
    assert store.collection_stats("contract_embeddings")["rows"] == 0


# ── Indexer ingestion guard ────────────────────────────────────────────


async def test_ingestion_failure_never_blocks_indexing(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, vector_ingestion=True)

    async def boom(from_block: int, to_block: int) -> dict[str, int]:
        raise RuntimeError("ollama down")

    monkeypatch.setattr(indexer, "_maybe_run_vector_ingestion", boom)

    summary: dict[str, Any] = {"chain": "ethereum", "status": "ok", "events_written_ch": 3}
    await indexer._run_vector_ingestion_guarded(100, 200, summary)
    # Failure is swallowed: status stays "ok", no partial stats merged.
    assert summary["status"] == "ok"
    assert "wallets_indexed" not in summary


async def test_ingestion_stats_merged_into_summary(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, vector_ingestion=True)

    async def fake_ingest(from_block: int, to_block: int) -> dict[str, int]:
        return {"wallets_indexed": 5, "contracts_indexed": 2}

    monkeypatch.setattr(indexer, "_maybe_run_vector_ingestion", fake_ingest)

    summary: dict[str, Any] = {"status": "ok"}
    await indexer._run_vector_ingestion_guarded(100, 200, summary)
    assert summary["wallets_indexed"] == 5
    assert summary["contracts_indexed"] == 2


async def test_ingestion_disabled_is_noop(tmp_path) -> None:
    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, vector_ingestion=False)
    summary: dict[str, Any] = {"status": "ok"}
    await indexer._run_vector_ingestion_guarded(100, 200, summary)
    assert summary == {"status": "ok"}


async def test_interval_gating_skips_when_not_elapsed(tmp_path) -> None:
    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, vector_ingestion=True)
    # Simulate a prior run at block 1000.
    indexer._last_vector_ingestion_block = 1000

    # 1005 - 1000 = 5 < interval (100) → real method returns None before
    # ever touching ClickHouse or the vector store.
    stats = await indexer._maybe_run_vector_ingestion(1001, 1005)
    assert stats is None
    assert indexer._last_vector_ingestion_block == 1000


async def test_interval_gating_runs_when_elapsed(tmp_path) -> None:
    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, vector_ingestion=True)
    indexer._last_vector_ingestion_block = 1000

    class FakeWriter:
        def client(self) -> FakeCHClient:
            return FakeCHClient()

    setattr(indexer, "_ch_writer", FakeWriter())
    indexer._vector_store = LanceVectorStore(path=str(tmp_path / "lance"), embedder=FakeEmbedder())

    # 1105 - 1000 = 105 >= interval (100) → pipeline runs and the marker
    # advances to the current tip.
    stats = await indexer._maybe_run_vector_ingestion(1001, 1105)
    assert stats == {"wallets_indexed": 2, "contracts_indexed": 1}
    assert indexer._last_vector_ingestion_block == 1105
