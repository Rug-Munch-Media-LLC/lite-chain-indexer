"""Reliability-audit fixes (D2–D6), 2026-09-14.

Hermetic. Covers:

* D3 — capability misclassification: HTTP 401/403 and HTML/bot-challenge
  bodies on the getLogs probe classify as CANNOT_SERVE_LOGS (not TRANSIENT,
  not banned).
* D4 — the health/probe path never hard-bans.
* D2 — capability/health/ban gating state persists across a simulated
  restart (SQLite).
* D5 — the verified log set survives a second cycle (no re-probe, no
  per-cycle restore).
* D6 — state does not advance when an enabled writer fails.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from lite_indexer.rpc import capabilities as cap
from lite_indexer.rpc import health as hh
from lite_indexer.rpc import pool
from lite_indexer.rpc import verify as vf
from lite_indexer.rpc.capabilities import (
    GetLogsCapability,
    classify_logs_capability,
    is_logs_capability_excluded,
    set_logs_capability,
)


@pytest.fixture(autouse=True)
def _no_live_endpoint_verify() -> Any:
    """The EVM endpoint verification probes live RPCs (~20s); stub it here."""
    from unittest.mock import AsyncMock, patch

    with patch("lite_indexer.indexer.LiteIndexer._evm_verify_endpoints", AsyncMock(return_value=None)), \
            patch("lite_indexer.indexer.LiteIndexer._publish_nats_events", AsyncMock(return_value=0)), \
            patch("lite_indexer.indexer.LiteIndexer._dispatch_plugin_events", AsyncMock(return_value={})), \
            patch("lite_indexer.indexer.LiteIndexer._run_backfill_sidecar", AsyncMock(return_value={})), \
            patch("lite_indexer.indexer.LiteIndexer._run_vector_ingestion_guarded", AsyncMock(return_value=None)):
        yield

EP = "https://probe.example"


# ── D3 — classification ────────────────────────────────────────────────


@pytest.mark.parametrize("status", [401, 403, 405])
def test_http_auth_refusals_are_capability(status):
    assert classify_logs_capability(status, {"error": {"message": "nope"}}) is GetLogsCapability.CANNOT_SERVE_LOGS


@pytest.mark.parametrize(
    "body",
    [
        "<html><title>Just a moment...</title></html>",
        "Attention Required! | Cloudflare",
        "Access denied — captcha challenge",
    ],
)
def test_html_challenge_bodies_are_capability(body):
    assert classify_logs_capability(200, body) is GetLogsCapability.CANNOT_SERVE_LOGS


def test_non_json_server_error_stays_transient():
    assert classify_logs_capability(200, "upstream failure") is GetLogsCapability.TRANSIENT
    assert classify_logs_capability(502, "bad gateway") is GetLogsCapability.TRANSIENT


class _Resp:
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text

    def json(self):  # noqa: ANN201 — deliberately non-JSON
        raise ValueError("not json")


class _Client:
    def __init__(self, resp: _Resp) -> None:
        self._resp = resp

    async def __aenter__(self) -> _Client:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def post(self, url: str, json: object = None) -> _Resp:  # noqa: A002
        return self._resp


@pytest.mark.asyncio
async def test_probe_403_html_is_capability_not_ban():
    with patch.object(cap.httpx, "AsyncClient", lambda **k: _Client(_Resp(403, "<html>Just a moment...</html>"))):
        status = await cap.probe_logs_capability(EP, 1_000_000)
    assert status is GetLogsCapability.CANNOT_SERVE_LOGS
    # A capability refusal must not be treated as an abuse signal.
    assert pool.is_endpoint_banned(EP) is False


@pytest.mark.asyncio
async def test_probe_html_200_is_capability():
    with patch.object(cap.httpx, "AsyncClient", lambda **k: _Client(_Resp(200, "<html>cloudflare</html>"))):
        assert await cap.probe_logs_capability(EP, 1_000_000) is GetLogsCapability.CANNOT_SERVE_LOGS


# ── D4 — probes never ban ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_probe_never_bans():
    banned: list[str] = []
    with (
        patch.object(hh, "_probe_endpoint", AsyncMock(return_value=403)),
        patch("lite_indexer.chains.GET_LOGS_ENDPOINTS", {"ethereum": [EP]}),
        patch.object(pool, "ban_endpoint", lambda *a, **k: banned.append(a[0] if a else "")),
    ):
        task = asyncio.create_task(hh.monitor_endpoint_health(chains=["ethereum"], interval=0.01, timeout=0.01))
        await asyncio.sleep(0.05)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert banned == []
    assert pool.is_endpoint_banned(EP) is False


# ── D2 — persistence across a simulated restart ────────────────────────


def test_capability_verdict_persists_across_restart():
    set_logs_capability(EP, "ethereum", False)
    assert is_logs_capability_excluded(EP) is True

    cap.clear_capabilities()  # simulate restart (in-memory gone, disk remains)
    assert is_logs_capability_excluded(EP) is True

    cap.clear_capabilities()
    assert cap.load_capabilities() >= 1


def test_ban_state_persists_across_restart():
    pool.ban_endpoint(EP, reason="test")
    assert pool.is_endpoint_banned(EP) is True

    pool.clear_ban_state()  # simulate restart
    assert pool.is_endpoint_banned(EP) is False
    assert pool.load_ban_state() >= 1
    assert pool.is_endpoint_banned(EP) is True


def test_health_state_persists_across_restart():
    hh._set_health(EP, True)
    hh.clear_health_state()  # simulate restart
    assert hh.load_health_state() >= 1
    assert hh._endpoint_verified_ok.get(EP) is True


# ── D5 — durable verified set ──────────────────────────────────────────


class _StubRpc:
    async def eth_block_number(self, endpoint: str) -> int:
        return 1_000_000


@pytest.mark.asyncio
async def test_verified_set_survives_second_cycle():
    endpoints = ["https://a.example", "https://b.example"]

    calls = {"n": 0}

    async def _gate(client, endpoint, chain, tip):
        calls["n"] += 1
        return GetLogsCapability.CAPABLE

    with (
        patch.object(vf, "GET_LOGS_ENDPOINTS", {"ethereum": list(endpoints)}),
        patch.object(vf, "verify_endpoints_for_chain", AsyncMock(return_value=list(endpoints))),
        patch.object(vf, "ENDPOINT_VERIFY_CONCURRENCY", 1),
        patch.object(vf, "_probe_and_record_capabilities", _gate),
    ):
        first = await vf.verify_chain_endpoints("ethereum", _StubRpc())
        second = await vf.verify_chain_endpoints("ethereum", _StubRpc())

    assert first is not None and second is first  # cached, not re-derived
    assert calls["n"] == len(endpoints)  # probed exactly once


# ── D6 — no silent state advance on writer failure ─────────────────────


@pytest.mark.asyncio
async def test_state_not_advanced_when_enabled_writer_fails(tmp_path):
    from lite_indexer.indexer import LiteIndexer

    with patch("lite_indexer.state.STATE_DIR", tmp_path / "state"):
        indexer = LiteIndexer(chain="ethereum", from_block=100, to_block=200, data_dir=tmp_path)
    indexer.state.save(99, 0)  # seed: last indexed block = 99
    indexer.deployers_enabled = True
    indexer.dex_enabled = False
    indexer.traces_enabled = False

    strategy = indexer.strategy
    strategy.fetch_block_number = AsyncMock(return_value=500)
    strategy.fetch_transfers = AsyncMock(return_value=[{"blockNumber": 150, "value": "1"}])
    strategy.fetch_block_timestamps = AsyncMock(return_value={})
    strategy._fetch_pair_created_events = AsyncMock(return_value=None)
    strategy._last_pair_events = [{"blockNumber": 150, "pair_address": "0xpair"}]
    strategy.write_clickhouse = Mock(return_value=1)  # transfer insert succeeds

    indexer._enrich_timestamps = AsyncMock(side_effect=lambda evs: [dict(e) for e in evs])
    # No-network guard: the indexer timestamp helper is the only RPC path the
    # writer-gate cycle can still reach.
    indexer.fetch_block_timestamps = AsyncMock(return_value={})
    # The deployer write mirrors pair events into evm_launches (best-effort);
    # stub it so the test never reaches a real RPC/DB.
    # No-network guard: any residual strategy RPC returns immediately instead
    # of backing off against the live pool.
    with (
        patch("lite_indexer.indexer._writers.write_evm_launch", AsyncMock()),
        patch("lite_indexer.rpc.client.RpcClient.call", AsyncMock(return_value=None)),
        patch("lite_indexer.rpc.client.RpcClient.get_logs", AsyncMock(return_value=("error", None))),
        patch("lite_indexer.rpc.client.RpcClient.eth_block_number", AsyncMock(return_value=500)),
    ):
        ch = Mock()
        ch.write_deployer_events = AsyncMock(return_value=0)  # silently drops rows
        ch.write_wallet_features = AsyncMock(return_value={})
        ch.client.return_value = None
        indexer._ch_writer = ch
        indexer._ch_health = type("_ChHealthStub", (), {"ok": lambda self: True})()

        summary = await indexer._sync_loop_inner({"status": "ok", "errors": []}, __import__("time").monotonic())

    assert summary["status"] == "writer_incomplete"
    assert summary["failed_writers"] == ["deployer"]
    # State must NOT advance past block 99 — the range is retried.
    assert indexer.state.load()["last_block"] == 99
