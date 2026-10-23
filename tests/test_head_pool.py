"""Tests for the EVM WebSocket head pool.

All driven by fake websockets — no network, no monkeypatched transport. The
``ws_connect`` and ``sleep`` injections let the reconnect / rotation / fallback
paths run instantly and deterministically.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import pytest

from lite_indexer import metrics
from lite_indexer.chains._overlay import load_endpoint_overlay, merge_endpoints, ws_head_endpoints
from lite_indexer.indexer._sync_loop import SyncLoopMixin
from lite_indexer.rpc.head_pool import subscribe_heads

HEAD_HASH = "0x" + "ab" * 32


def _head_frame(number: int, block_hash: str = HEAD_HASH) -> str:
    """A ``newHeads`` ``eth_subscription`` notification frame."""
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {
                "subscription": "0x1",
                "result": {"number": hex(number), "hash": block_hash},
            },
        }
    )


class FakeWebsocket:
    """Minimal websocket double: records sends, yields scripted frames."""

    def __init__(self, frames: list[str] | None = None) -> None:
        self.frames = list(frames or [])
        self.sent: list[dict[str, Any]] = []

    async def send(self, data: str) -> None:
        self.sent.append(json.loads(data))

    def __aiter__(self) -> FakeWebsocket:
        return self

    async def __anext__(self) -> str:
        if not self.frames:
            raise StopAsyncIteration
        return self.frames.pop(0)


class FakeConnect:
    """Async context manager returned by an injected ``ws_connect``."""

    def __init__(self, ws: FakeWebsocket | None = None, error: Exception | None = None) -> None:
        self.ws = ws
        self.error = error

    async def __aenter__(self) -> FakeWebsocket:
        if self.error is not None:
            raise self.error
        assert self.ws is not None
        return self.ws

    async def __aexit__(self, *args: Any) -> bool:
        return False


async def _cancel(task: asyncio.Task[None]) -> None:
    if task.done():
        return
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


class _FakeChainIndexer(SyncLoopMixin):
    """Standalone host for the SyncLoopMixin head-pool surface."""

    chain = "ethereum"

    def __init__(self) -> None:
        self._head_pool_task: asyncio.Task[None] | None = None
        self._head_wakeup: asyncio.Event | None = None

    def _is_evm(self) -> bool:
        return True


# ═══════════════════════════════════════════════════════════════════════
# subscribe_heads — subscribe + callback
# ═══════════════════════════════════════════════════════════════════════


async def test_subscribe_invokes_on_head_and_sends_new_heads() -> None:
    metrics.reset()
    socket = FakeWebsocket([_head_frame(100)])
    heads: list[tuple[int, str]] = []
    seen = asyncio.Event()

    def on_head(number: int, block_hash: str) -> None:
        heads.append((number, block_hash))
        seen.set()

    async def sleep(_seconds: float) -> None:
        await asyncio.sleep(0)

    task = asyncio.create_task(
        subscribe_heads(
            "ethereum",
            ["wss://node.example.com/ws"],
            on_head,
            ws_connect=lambda url: FakeConnect(socket),
            sleep=sleep,
        )
    )
    await asyncio.wait_for(seen.wait(), 1.0)
    await _cancel(task)

    assert heads == [(100, HEAD_HASH)]
    assert socket.sent[0]["method"] == "eth_subscribe"
    assert socket.sent[0]["params"] == ["newHeads"]
    assert metrics.get("ws_head_connected") >= 1
    assert metrics.get("ws_head_messages") == 1


async def test_reconnects_on_drop() -> None:
    metrics.reset()
    heads: list[int] = []

    def on_head(number: int, block_hash: str) -> None:
        heads.append(number)

    async def sleep(_seconds: float) -> None:
        await asyncio.sleep(0)

    def connect(url: str) -> FakeConnect:
        return FakeConnect(FakeWebsocket([_head_frame(len(heads) + 1)]))

    task = asyncio.create_task(
        subscribe_heads("ethereum", ["wss://node.example.com/ws"], on_head, ws_connect=connect, sleep=sleep)
    )
    for _ in range(200):
        if len(heads) >= 2:
            break
        await asyncio.sleep(0)
    await _cancel(task)

    assert len(heads) >= 2
    assert metrics.get("ws_head_reconnects") >= 1


async def test_rotates_to_next_endpoint_after_repeated_failure() -> None:
    metrics.reset()
    attempts: dict[str, int] = {"wss://a.example.com/ws": 0, "wss://b.example.com/ws": 0}
    seen = asyncio.Event()

    def on_head(number: int, block_hash: str) -> None:
        seen.set()

    async def sleep(_seconds: float) -> None:
        await asyncio.sleep(0)

    def connect(url: str) -> FakeConnect:
        attempts[url] += 1
        if url.startswith("wss://a"):
            return FakeConnect(error=ConnectionError("refused"))
        return FakeConnect(FakeWebsocket([_head_frame(7)]))

    task = asyncio.create_task(
        subscribe_heads(
            "ethereum",
            ["wss://a.example.com/ws", "wss://b.example.com/ws"],
            on_head,
            failures_per_endpoint=2,
            max_endpoints=2,
            ws_connect=connect,
            sleep=sleep,
        )
    )
    await asyncio.wait_for(seen.wait(), 1.0)
    await _cancel(task)

    assert attempts["wss://a.example.com/ws"] == 2
    assert attempts["wss://b.example.com/ws"] >= 1


async def test_total_failure_falls_back_without_raising() -> None:
    metrics.reset()
    delays: list[float] = []

    async def sleep(seconds: float) -> None:
        delays.append(seconds)
        await asyncio.sleep(0)

    def connect(url: str) -> FakeConnect:
        return FakeConnect(error=ConnectionError("refused"))

    await asyncio.wait_for(
        subscribe_heads(
            "ethereum",
            ["wss://a.example.com/ws", "wss://b.example.com/ws"],
            lambda number, block_hash: None,
            failures_per_endpoint=1,
            max_endpoints=2,
            ws_connect=connect,
            sleep=sleep,
        ),
        1.0,
    )

    assert metrics.get("ws_head_fallback") == 1
    assert metrics.get("ws_head_reconnects") == 2
    # One backoff before rotating, none after the final endpoint falls back.
    assert len(delays) == 1
    assert 0.0 < delays[0] <= 1.0


async def test_disabled_is_a_noop() -> None:
    metrics.reset()
    connected: list[str] = []
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        await asyncio.sleep(0)

    def connect(url: str) -> FakeConnect:
        connected.append(url)
        return FakeConnect(FakeWebsocket([_head_frame(1)]))

    await subscribe_heads(
        "ethereum",
        ["wss://node.example.com/ws"],
        lambda number, block_hash: None,
        enabled=False,
        ws_connect=connect,
        sleep=sleep,
    )

    assert connected == []
    assert slept == []
    assert metrics.get("ws_head_connected") == 0


async def test_no_endpoints_is_a_noop() -> None:
    connected: list[str] = []

    async def sleep(_seconds: float) -> None:
        await asyncio.sleep(0)

    def connect(url: str) -> FakeConnect:
        connected.append(url)
        return FakeConnect(FakeWebsocket())

    await subscribe_heads("ethereum", [], lambda number, block_hash: None, ws_connect=connect, sleep=sleep)
    assert connected == []


# ═══════════════════════════════════════════════════════════════════════
# Overlay websocket resolution
# ═══════════════════════════════════════════════════════════════════════


def test_ws_head_endpoints_from_ws_field(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OVERLAY_WS_KEY", "s3cret")
    overlay = {
        "ethereum": {
            "endpoints": ["https://rpc.example.com/v1/${OVERLAY_WS_KEY}"],
            "ws": ["wss://rpc.example.com/ws/${OVERLAY_WS_KEY}"],
        }
    }
    assert ws_head_endpoints("ethereum", overlay) == ["wss://rpc.example.com/ws/s3cret"]


def test_ws_head_endpoints_from_endpoint_scheme() -> None:
    overlay = {"base": {"endpoints": ["wss://base.example.com/ws", "ws://base.example.com/ws"]}}
    assert ws_head_endpoints("base", overlay) == ["wss://base.example.com/ws", "ws://base.example.com/ws"]


def test_ws_head_endpoints_drops_unresolved() -> None:
    overlay = {"ethereum": {"ws": ["wss://rpc.example.com/ws/${DEFINITELY_UNSET_VAR_XYZ}"]}}
    assert ws_head_endpoints("ethereum", overlay) == []


def test_ws_head_endpoints_absent_overlay() -> None:
    assert ws_head_endpoints("ethereum", {}) == []


def test_merge_endpoints_excludes_ws_scheme() -> None:
    overlay = {"ethereum": {"endpoints": ["https://rpc.example.com", "wss://rpc.example.com/ws"]}}
    assert merge_endpoints({}, overlay)["ethereum"] == ["https://rpc.example.com"]


def test_load_endpoint_overlay_preserves_ws(tmp_path: Path) -> None:
    path = tmp_path / "overlay.json"
    path.write_text(
        json.dumps({"ethereum": {"endpoints": ["https://rpc.example.com"], "ws": ["wss://rpc.example.com/ws"]}}),
        encoding="utf-8",
    )
    overlay = load_endpoint_overlay(path)
    assert overlay["ethereum"]["ws"] == ["wss://rpc.example.com/ws"]
    assert overlay["ethereum"]["endpoints"] == ["https://rpc.example.com"]


def test_head_pool_module_is_provider_free() -> None:
    tokens = ("alchemy", "infura", "quicknode", "ankr", "helius")
    text = Path("lite_indexer/rpc/head_pool.py").read_text(encoding="utf-8").lower()
    assert not any(token in text for token in tokens)


# ═══════════════════════════════════════════════════════════════════════
# SyncLoopMixin integration (wakeup + lifecycle)
# ═══════════════════════════════════════════════════════════════════════


async def test_head_pushed_during_the_wait_wakes_wait_for_head() -> None:
    indexer = _FakeChainIndexer()
    indexer._head_wakeup = asyncio.Event()
    asyncio.get_running_loop().call_later(0.05, indexer._on_head, 123, "0xdead")
    start = time.monotonic()
    await indexer.wait_for_head(5.0)  # returns on the head pushed mid-wait
    assert time.monotonic() - start < 1.0 and not indexer._head_wakeup.is_set()


async def test_wait_for_head_without_pool_uses_timeout() -> None:
    indexer = _FakeChainIndexer()
    start = time.monotonic()
    await indexer.wait_for_head(0.05)
    assert time.monotonic() - start >= 0.04


async def test_start_head_pool_noop_without_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("lite_indexer.chains.endpoints.WS_HEAD_ENDPOINTS", {"ethereum": []})
    indexer = _FakeChainIndexer()
    await indexer.start_head_pool()
    assert indexer._head_pool_task is None
    assert indexer._head_wakeup is None


async def test_start_and_stop_head_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_subscribe(chain: str, endpoints: Any, on_head: Any, **kwargs: Any) -> None:
        await asyncio.sleep(30)

    monkeypatch.setattr("lite_indexer.rpc.head_pool.subscribe_heads", fake_subscribe)
    monkeypatch.setattr(
        "lite_indexer.chains.endpoints.WS_HEAD_ENDPOINTS",
        {"ethereum": ["wss://node.example.com/ws"]},
    )
    indexer = _FakeChainIndexer()
    await indexer.start_head_pool()
    assert indexer._head_pool_task is not None
    assert indexer._head_wakeup is not None
    await indexer.stop_head_pool()
    assert indexer._head_pool_task is None
