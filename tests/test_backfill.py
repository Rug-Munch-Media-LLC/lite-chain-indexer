"""Tests for the freemium-only backfill queue.

Covers:

* gap detection from state/tip + reconciliation rewinds
* persisted, idempotent, bounded :class:`BackfillQueue`
* failure retention (no chunk dropped) and memory-gated draining
* freemium-only endpoint selection (free/keyless is never chosen)

Hermetic — no network, no ClickHouse.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest

from lite_indexer.indexer.backfill import (
    BackfillChunk,
    BackfillQueue,
    detect_gaps,
    drain_backfill_queue,
)
from lite_indexer.rpc import freemium as freemium_mod
from lite_indexer.rpc.freemium import (
    freemium_endpoints,
    is_freemium_endpoint,
    select_freemium_endpoint,
)
from lite_indexer.rpc.provider_caps import PROVIDER_LIMITER


@pytest.fixture(autouse=True)
def _clean_provider_limiter():
    PROVIDER_LIMITER.reset()
    yield
    PROVIDER_LIMITER.reset()


# ── Gap detection ───────────────────────────────────────────────────────


def test_detect_forward_gap_chunks_exactly():
    chunks = detect_gaps(
        "ethereum",
        last_block=100,
        tip=120,
        rewound_ranges=[],
        chunk_size=5,
        max_chunks=0,
        include_forward_gap=True,
    )
    assert [(c.from_block, c.to_block) for c in chunks] == [
        (101, 105),
        (106, 110),
        (111, 115),
        (116, 120),
    ]
    assert all(c.chain == "ethereum" for c in chunks)


def test_detect_gap_skips_forward_when_not_stalled():
    chunks = detect_gaps(
        "ethereum",
        last_block=100,
        tip=120,
        rewound_ranges=[(200, 203)],
        chunk_size=5,
        max_chunks=0,
        include_forward_gap=False,
    )
    assert [(c.from_block, c.to_block) for c in chunks] == [(200, 203)]


def test_detect_gap_dedups_and_bounds():
    chunks = detect_gaps(
        "ethereum",
        last_block=0,
        tip=100,
        rewound_ranges=[(10, 20)],
        chunk_size=10,
        max_chunks=3,
        include_forward_gap=True,
    )
    assert len(chunks) == 3
    keys = [c.key for c in chunks]
    assert len(keys) == len(set(keys))


def test_detect_gap_ignores_inverted_ranges():
    chunks = detect_gaps(
        "ethereum",
        last_block=50,
        tip=50,
        rewound_ranges=[(90, 80)],
        chunk_size=10,
        max_chunks=0,
        include_forward_gap=True,
    )
    assert chunks == []


# ── Queue persistence + idempotency + bounds ────────────────────────────


def test_enqueue_is_idempotent_and_persisted(tmp_path: Path):
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    chunk = BackfillChunk("ethereum", 100, 199)
    assert queue.enqueue([chunk]) == 1
    assert queue.enqueue([chunk]) == 0  # duplicate is a no-op

    restored = BackfillQueue("ethereum", state_dir=tmp_path)
    assert restored.pending_count == 1
    assert restored.contains(chunk.key)

    restored.mark_done(chunk.key)
    assert restored.depth == 0
    # A completed chunk is never enqueued again (idempotency gate).
    assert restored.enqueue([chunk]) == 0
    assert BackfillQueue("ethereum", state_dir=tmp_path).done_count == 1


def test_in_flight_restored_to_pending_on_load(tmp_path: Path):
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    queue.enqueue([BackfillChunk("ethereum", 1, 10)])
    queue.mark_in_flight("ethereum:1:10")
    assert queue.depth == 1
    # Persisted before restart: a fresh instance re-queues the orphan.
    restored = BackfillQueue("ethereum", state_dir=tmp_path)
    assert restored.pending_count == 1
    assert restored.in_flight_count == 0


def test_queue_depth_is_bounded(tmp_path: Path):
    queue = BackfillQueue("ethereum", state_dir=tmp_path, max_depth=2)
    added = queue.enqueue([BackfillChunk("ethereum", i, i + 9) for i in range(0, 50, 10)])
    assert added == 2
    assert queue.depth == 2


def test_corrupt_state_file_degrades_to_empty(tmp_path: Path):
    path = tmp_path / "backfill_queue_ethereum.json"
    path.write_text("{not json")
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    assert queue.depth == 0


# ── Worker: bounded concurrency, failure retention, memory guard ────────


async def test_drain_respects_concurrency_bound(tmp_path: Path):
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    queue.enqueue([BackfillChunk("ethereum", i, i + 4) for i in range(0, 20, 5)])
    active = 0
    peak = 0

    async def executor(chunk: BackfillChunk, endpoint: str) -> bool:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.03)
        active -= 1
        return True

    stats = await drain_backfill_queue(
        queue,
        executor,
        max_concurrency=2,
        max_chunks=4,
        endpoint_selector=lambda _chain: "https://x.alchemy.com/v2/k",
    )
    assert peak == 2
    assert stats["completed"] == 4
    assert queue.depth == 0


async def test_drain_keeps_failed_chunk(tmp_path: Path):
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    chunk = BackfillChunk("ethereum", 1, 10)
    queue.enqueue([chunk])

    async def failing(chunk: BackfillChunk, endpoint: str) -> bool:
        return False

    stats = await drain_backfill_queue(
        queue,
        failing,
        max_chunks=1,
        endpoint_selector=lambda _chain: "https://x.alchemy.com/v2/k",
    )
    assert stats["failed"] == 1
    assert queue.pending_count == 1  # never dropped
    assert queue.attempts_for(chunk.key) == 1


async def test_drain_exception_does_not_drop_chunk(tmp_path: Path):
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    chunk = BackfillChunk("ethereum", 1, 10)
    queue.enqueue([chunk])

    async def exploding(chunk: BackfillChunk, endpoint: str) -> bool:
        raise ConnectionError("boom")

    stats = await drain_backfill_queue(
        queue,
        exploding,
        max_chunks=1,
        endpoint_selector=lambda _chain: "https://x.alchemy.com/v2/k",
    )
    assert stats["failed"] == 1
    assert queue.contains(chunk.key)


async def test_memory_guard_pauses_drain(tmp_path: Path):
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    queue.enqueue([BackfillChunk("ethereum", 1, 10)])
    called = False

    async def executor(chunk: BackfillChunk, endpoint: str) -> bool:
        nonlocal called
        called = True
        return True

    stats = await drain_backfill_queue(queue, executor, memory_ok=False)
    assert stats["memory_paused"] == 1
    assert called is False
    assert queue.pending_count == 1  # untouched


async def test_drain_skips_chunks_without_freemium_endpoint(tmp_path: Path):
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    chunk = BackfillChunk("ethereum", 1, 10)
    queue.enqueue([chunk])

    async def executor(chunk: BackfillChunk, endpoint: str) -> bool:
        raise AssertionError("must not run without a freemium endpoint")

    stats = await drain_backfill_queue(queue, executor, max_chunks=1, endpoint_selector=lambda _chain: None)
    assert stats["skipped"] == 1
    assert queue.contains(chunk.key)  # left queued, no attempt consumed


async def test_parked_chunk_not_selected_but_retained(tmp_path: Path):
    queue = BackfillQueue("ethereum", state_dir=tmp_path, max_attempts=1)
    chunk = BackfillChunk("ethereum", 1, 10)
    queue.enqueue([chunk])
    queue.mark_in_flight(chunk.key)
    queue.mark_failed(chunk.key)
    assert queue.next_pending(10) == []
    assert queue.parked_count == 1
    assert queue.contains(chunk.key)
    queue.requeue(chunk.key)
    assert [c.key for c in queue.next_pending(10)] == [chunk.key]


# ── Freemium classification + selection ─────────────────────────────────

FREE_ENDPOINTS = [
    "https://eth.drpc.org",
    "https://mainnet.base.org",
    "https://eth.api.pocket.network",
    "https://rpc.ankr.com/celo",  # keyless Ankr one-segment path
    "https://polygon.blockpi.network/v1/rpc/public",  # keyless BlockPI /public
]

# Realistic long API keys — classification is key-SHAPE based (a trailing
# token-shaped path segment or a recognised key query param), not a baked-in
# provider list. Placeholders here are synthetic, never real secrets.
_TEST_KEY = "k" * 24
FREEMIUM_ENDPOINTS = [
    f"https://eth-mainnet.g.alchemy.com/v2/{_TEST_KEY}",
    f"https://mainnet.infura.io/v3/{_TEST_KEY}",
    f"https://lb.drpc.org/ogrpc?network=ethereum&dkey={_TEST_KEY}",
    f"https://api.uniblock.dev/uni/v1/json-rpc?chainId=1&apiKey={_TEST_KEY}",
    f"https://rpc.ankr.com/eth/{_TEST_KEY}",
    f"https://bsc-mainnet.nodereal.io/v1/{_TEST_KEY}",
    f"https://mainnet.helius-rpc.com/?api-key={_TEST_KEY}",
    f"https://eth.nownodes.io/{_TEST_KEY}",
]


@pytest.mark.parametrize("endpoint", FREEMIUM_ENDPOINTS)
def test_keyed_endpoints_are_freemium(endpoint: str):
    assert is_freemium_endpoint(endpoint) is True


@pytest.mark.parametrize("endpoint", FREE_ENDPOINTS)
def test_keyless_endpoints_are_not_freemium(endpoint: str):
    assert is_freemium_endpoint(endpoint) is False


def test_unresolved_template_is_freemium():
    assert is_freemium_endpoint("https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}") is True


def test_select_never_returns_free_endpoint():
    selected = select_freemium_endpoint(
        FREE_ENDPOINTS + FREEMIUM_ENDPOINTS,
        base_select=lambda eps: eps[0],
    )
    assert selected in FREEMIUM_ENDPOINTS
    assert selected not in FREE_ENDPOINTS


def test_select_returns_none_when_only_free():
    assert select_freemium_endpoint(FREE_ENDPOINTS, base_select=lambda eps: eps[0]) is None


def test_select_excludes_archive_guarded_endpoint():
    endpoints = FREEMIUM_ENDPOINTS[:1] + FREEMIUM_ENDPOINTS[1:2]
    with patch.object(
        freemium_mod,
        "archive_guard_reason",
        side_effect=lambda ep, method: "blocked" if "infura" in ep else None,
    ):
        selected = select_freemium_endpoint(endpoints, base_select=lambda eps: eps[0])
    assert selected == endpoints[0]
    assert "infura" not in selected


def test_freemium_endpoints_filter_preserves_order():
    assert freemium_endpoints(FREE_ENDPOINTS + FREEMIUM_ENDPOINTS) == FREEMIUM_ENDPOINTS


# ── Sync-loop integration (BackfillMixin) ───────────────────────────────


def test_mixin_enqueues_gap_when_stalled(tmp_path: Path):
    from lite_indexer.indexer import LiteIndexer

    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path)
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    indexer._backfill_queue = queue
    indexer._record_reconcile_rewind(200, 203)

    with patch.object(indexer.state, "load", return_value={"last_block": 100}):
        indexer._backfill_seen_block = 100  # equal → stalled
        added = indexer._enqueue_backfill_gaps(tip=110)

    assert added >= 1
    assert queue.contains("ethereum:101:110")
    # Rewinds are NOT enrolled by default: the transfer tables are plain
    # MergeTree (no per-key dedup), so re-writing a rewound range duplicates.
    assert not queue.contains("ethereum:200:203")
    # Re-enqueue is a no-op.
    with patch.object(indexer.state, "load", return_value={"last_block": 100}):
        assert indexer._enqueue_backfill_gaps(tip=110) == 0


def test_mixin_enrols_rewinds_only_when_flag_on(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """With ReplacingMergeTree tables (flag on), rewinds are safe to backfill."""
    import lite_indexer.indexer._sidecars as sidecars
    from lite_indexer.indexer import LiteIndexer

    monkeypatch.setattr(sidecars, "BACKFILL_INCLUDE_REWINDS", True)
    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path)
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    indexer._backfill_queue = queue
    indexer._record_reconcile_rewind(200, 203)

    with patch.object(indexer.state, "load", return_value={"last_block": 100}):
        indexer._backfill_seen_block = 100
        indexer._enqueue_backfill_gaps(tip=110)

    assert queue.contains("ethereum:200:203")


async def test_mixin_sidecar_noop_without_executor(tmp_path: Path):
    from lite_indexer.indexer import LiteIndexer

    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path)
    indexer._backfill_queue = BackfillQueue("ethereum", state_dir=tmp_path)
    indexer._backfill_queue.enqueue([BackfillChunk("ethereum", 1, 10)])
    assert await indexer._run_backfill_sidecar() == {}


async def test_mixin_sidecar_memory_gate_pauses(tmp_path: Path):
    from lite_indexer.indexer import LiteIndexer

    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path)
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    chunk = BackfillChunk("ethereum", 1, 10)
    queue.enqueue([chunk])
    indexer._backfill_queue = queue
    indexer._backfill_executor = lambda _chunk, _endpoint: _always_true()

    with patch.object(indexer, "memory_gate_ok", return_value=False):
        stats = await indexer._run_backfill_sidecar()

    assert stats["memory_paused"] == 1
    assert queue.pending_count == 1


async def _always_true() -> bool:
    return True


def test_executor_unwired_by_default(tmp_path: Path):
    """Backfill dispatch stays dormant unless explicitly opted in."""
    from lite_indexer.indexer import LiteIndexer

    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path)
    assert indexer._backfill_executor is None


def test_executor_wired_when_flag_enabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """BACKFILL_EXECUTOR_ENABLED=1 wires the EVM executor at construction."""
    import lite_indexer.indexer as indexer_mod

    monkeypatch.setattr(indexer_mod, "BACKFILL_EXECUTOR_ENABLED", True)
    indexer = indexer_mod.LiteIndexer(chain="ethereum", data_dir=tmp_path)
    assert indexer._backfill_executor is not None
