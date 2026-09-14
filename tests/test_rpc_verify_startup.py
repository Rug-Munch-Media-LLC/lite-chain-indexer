"""Parallel startup endpoint verification across chains.

The bottleneck this fixes: verification ran inside each chain's sync loop,
serialized by ``MAX_CONCURRENT_CHAINS``, so all but the first few chains sat
with ``rpc_pool_logs_endpoints{chain}=0`` for hours. Contracts below:

* one bounded-parallel pass verifies every configured EVM chain and buffers
  the results for one-shot consumption by the chain loops
* the concurrency bound is respected (env knob ``ENDPOINT_VERIFY_CONCURRENCY``)
* a chain with zero log-capable endpoints (or one that raises) is fail-soft
* the endpoint-health interval cache skips re-probing within the hour
* per-provider budgets are honored (cooled/exhausted identities skipped)

Hermetic — no network; verification is stubbed.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from lite_indexer.rpc import health as hh
from lite_indexer.rpc import verify as vf
from lite_indexer.rpc.capabilities import CapabilityRecord, GetLogsCapability, set_capability


@pytest.fixture(autouse=True)
def _clean_state():
    hh._endpoint_verified_ok.clear()
    hh._endpoint_last_verified.clear()
    vf.clear_startup_results()
    yield
    hh._endpoint_verified_ok.clear()
    hh._endpoint_last_verified.clear()
    vf.clear_startup_results()


def _result(chain: str, endpoints: list[str], tip_only: list[str] | None = None) -> vf.EndpointVerifyResult:
    return vf.EndpointVerifyResult(
        chain=chain,
        original_endpoints=list(endpoints),
        verified_endpoints=list(endpoints),
        tip_only_endpoints=list(tip_only or []),
    )


class _Recorder:
    """Records call order/concurrency of the stubbed chain verification."""

    def __init__(self, *, fail: set[str] | None = None, empty: set[str] | None = None) -> None:
        self.active = 0
        self.peak = 0
        self.calls: list[str] = []
        self.semaphores: list[object] = []
        self._fail = fail or set()
        self._empty = empty or set()

    async def __call__(
        self,
        chain: str,
        client: object,
        *,
        probe_semaphore: object = None,
        **kwargs: object,
    ) -> vf.EndpointVerifyResult:
        self.calls.append(chain)
        self.semaphores.append(probe_semaphore)
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            await asyncio.sleep(0.01)
            if chain in self._fail:
                raise RuntimeError(f"boom {chain}")
            if chain in self._empty:
                return vf.EndpointVerifyResult(chain=chain, original_endpoints=["https://x"], verified_endpoints=[])
            return _result(chain, [f"https://{chain}.example"])
        finally:
            self.active -= 1


_FAKE_POOL = {f"c{i}": [f"https://c{i}.example"] for i in range(8)}


# ── Parallel pass ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_parallel_pass_populates_all_chains(monkeypatch):
    monkeypatch.setattr(vf, "GET_LOGS_ENDPOINTS", _FAKE_POOL)
    rec = _Recorder()
    monkeypatch.setattr(vf, "verify_chain_endpoints", rec)

    results = await vf.verify_all_chains(list(_FAKE_POOL), rpc_client=object(), concurrency=4)

    assert set(results) == set(_FAKE_POOL)
    assert all(r is not None and r.verified_endpoints for r in results.values())
    # Buffered one-shot for the chain loops.
    for chain in _FAKE_POOL:
        assert vf.consume_startup_result(chain) is results[chain]
    assert vf.consume_startup_result("c0") is None  # consumed already


@pytest.mark.asyncio
async def test_concurrency_bound_respected(monkeypatch):
    monkeypatch.setattr(vf, "GET_LOGS_ENDPOINTS", _FAKE_POOL)
    rec = _Recorder()
    monkeypatch.setattr(vf, "verify_chain_endpoints", rec)

    await vf.verify_all_chains(list(_FAKE_POOL), rpc_client=object(), concurrency=3)

    # ONE shared global probe semaphore, sized to the requested bound.
    assert len({id(s) for s in rec.semaphores}) == 1
    assert rec.semaphores[0]._value == 3  # noqa: SLF001 — test inspects the bound
    assert len(rec.calls) == len(_FAKE_POOL)


@pytest.mark.asyncio
async def test_env_default_concurrency_used(monkeypatch):
    monkeypatch.setattr(vf, "GET_LOGS_ENDPOINTS", _FAKE_POOL)
    monkeypatch.setattr(vf, "ENDPOINT_VERIFY_CONCURRENCY", 2)
    rec = _Recorder()
    monkeypatch.setattr(vf, "verify_chain_endpoints", rec)

    await vf.verify_all_chains(list(_FAKE_POOL), rpc_client=object())

    assert rec.semaphores[0]._value == 2  # noqa: SLF001


@pytest.mark.asyncio
async def test_zero_capable_chain_does_not_crash(monkeypatch):
    monkeypatch.setattr(vf, "GET_LOGS_ENDPOINTS", _FAKE_POOL)
    rec = _Recorder(empty={"c2"})
    monkeypatch.setattr(vf, "verify_chain_endpoints", rec)

    results = await vf.verify_all_chains(list(_FAKE_POOL), rpc_client=object(), concurrency=4)

    assert results["c2"] is not None
    assert results["c2"].verified_endpoints == []
    assert all(results[c] is not None for c in _FAKE_POOL)


@pytest.mark.asyncio
async def test_chain_exception_is_soft(monkeypatch):
    monkeypatch.setattr(vf, "GET_LOGS_ENDPOINTS", _FAKE_POOL)
    rec = _Recorder(fail={"c5"})
    monkeypatch.setattr(vf, "verify_chain_endpoints", rec)

    results = await vf.verify_all_chains(list(_FAKE_POOL), rpc_client=object(), concurrency=4)

    assert results["c5"] is None
    assert results["c0"] is not None


@pytest.mark.asyncio
async def test_non_evm_chains_are_skipped(monkeypatch):
    monkeypatch.setattr(vf, "GET_LOGS_ENDPOINTS", {"ethereum": ["https://e.example"]})
    rec = _Recorder()
    monkeypatch.setattr(vf, "verify_chain_endpoints", rec)

    results = await vf.verify_all_chains(["ethereum", "solana", "tron"], rpc_client=object())

    assert set(results) == {"ethereum"}
    assert rec.calls == ["ethereum"]


# ── Cache + budgets ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cache_hit_within_interval_skips_reprobing(monkeypatch):
    ep = "https://cached.example"
    hh._endpoint_verified_ok[ep] = True
    hh._endpoint_last_verified[ep] = time.time()

    async def _boom(*args, **kwargs):
        raise AssertionError("should not re-probe a fresh endpoint")

    monkeypatch.setattr(hh, "verify_endpoint", _boom)
    verified = await hh.verify_endpoints_for_chain([ep], "ethereum", latest_block=100)
    assert verified == [ep]


@pytest.mark.asyncio
async def test_capability_record_cache_skips_reprobing(monkeypatch):
    ep = "https://cap-cached.example"
    set_capability(CapabilityRecord(endpoint=ep, chain="ethereum", logs_capable=True, probed_at=time.time()))
    calls = 0

    async def _probe(*args, **kwargs):
        nonlocal calls
        calls += 1
        return GetLogsCapability.CANNOT_SERVE_LOGS

    monkeypatch.setattr(vf, "probe_logs_capability", _probe)
    status = await vf._probe_and_record_capabilities(object(), ep, "ethereum", 100)  # noqa: SLF001
    assert status is GetLogsCapability.CAPABLE
    assert calls == 0


@pytest.mark.asyncio
async def test_budget_limits_honored_in_verification(monkeypatch):
    good = "https://good.example"
    cooled = "https://cooled.example"

    async def _always_ok(endpoint, chain, latest_block=None):
        return True

    monkeypatch.setattr(hh, "verify_endpoint", _always_ok)
    monkeypatch.setattr(hh, "budget_available", lambda ep, now=None: ep != cooled)

    verified = await hh.verify_endpoints_for_chain([good, cooled], "ethereum", latest_block=100)
    assert verified == [good]


# ── Per-chain readiness (a chain starts as soon as ITS verify lands) ────


@pytest.mark.asyncio
async def test_wait_startup_result_resolves_before_full_pass(monkeypatch):
    monkeypatch.setattr(vf, "GET_LOGS_ENDPOINTS", _FAKE_POOL)
    slow_done = asyncio.Event()

    async def _fake(chain: str, client: object, *, probe_semaphore: object = None) -> vf.EndpointVerifyResult:
        if chain == "c0":
            await slow_done.wait()  # still verifying the whole pass's tail
        return _result(chain, [f"https://{chain}.example"])

    monkeypatch.setattr(vf, "verify_chain_endpoints", _fake)
    task = asyncio.create_task(vf.verify_all_chains(list(_FAKE_POOL), rpc_client=object(), concurrency=8))
    await asyncio.sleep(0)  # let the pass register per-chain futures

    # c1 resolves while the full pass is still running (c0 is parked).
    result = await vf.wait_startup_result("c1", timeout=2.0)
    assert result is not None and result.chain == "c1"
    assert not task.done()

    slow_done.set()
    await task


@pytest.mark.asyncio
async def test_wait_startup_result_none_without_pass():
    assert await vf.wait_startup_result("never-verified") is None


@pytest.mark.asyncio
async def test_within_chain_probes_are_parallel(monkeypatch):
    endpoints = [f"https://p{i}.example" for i in range(8)]

    async def _fake_verify(*args, **kwargs):
        return endpoints

    class _Rpc:
        async def eth_block_number(self, endpoint):
            return 100

    active = 0
    peak = 0

    async def _gate(client, endpoint, chain, tip):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.01)
            return GetLogsCapability.CAPABLE
        finally:
            active -= 1

    monkeypatch.setattr(vf, "verify_endpoints_for_chain", _fake_verify)
    monkeypatch.setattr(vf, "ENDPOINT_VERIFY_CONCURRENCY", 3)
    monkeypatch.setattr(vf, "_probe_and_record_capabilities", _gate)

    result = await vf.verify_chain_endpoints("ethereum", _Rpc())

    assert result is not None
    assert result.verified_endpoints == endpoints
    assert peak <= 3
    assert peak >= 2
