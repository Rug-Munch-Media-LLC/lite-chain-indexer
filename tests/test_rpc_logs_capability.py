"""Broad eth_getLogs capability gating tests.

The CONFIGURED pool (builtin + private overlay) was never gated on the
getLogs capability the way auto-discovered endpoints are, so endpoints that
answer ``eth_blockNumber`` but structurally refuse broad topic-only
``eth_getLogs`` poisoned the log path and burned down the pool as "bans"
(proven live 2026-09-14). Contracts asserted here:

* classification: JSON-RPC ``-32701`` / HTTP 405 / auth-tenant-key /
  "invalid block range" refusals → ``CANNOT_SERVE_LOGS``; result list →
  ``CAPABLE``; anything else → ``TRANSIENT``.
* verification: a capability-refused endpoint is excluded from the log set
  but retained as tip-only; a working endpoint stays; an HTTP 405 / auth
  refusal behaves the same.
* ``apply_verified_endpoints`` applies only the log-capable set (never
  resurrects excluded endpoints).
* the on-demand fetcher never selects an excluded endpoint.
* a capability failure is NOT a ban / circuit-open.
"""

from __future__ import annotations

import pytest

from lite_indexer.rpc import capabilities as cap
from lite_indexer.rpc.capabilities import (
    CapabilityRecord,
    GetLogsCapability,
    classify_logs_capability,
    is_logs_capability_excluded,
    logs_capable_endpoints,
    set_capability,
    set_logs_capability,
)

GOOD = "https://good.example"
TIP_ONLY_32701 = "https://tiponly-32701.example"
TIP_ONLY_405 = "https://tiponly-405.example"
TIP_ONLY_AUTH = "https://tiponly-auth.example"
TRANSIENT = "https://transient.example"


@pytest.fixture(autouse=True)
def _clean_state():
    from lite_indexer.rpc import health as hh
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    cap._CAPABILITY_RECORDS.clear()
    hh._endpoint_verified_ok.clear()
    hh._endpoint_last_verified.clear()
    CIRCUIT_BREAKER._state.clear()
    yield
    cap._CAPABILITY_RECORDS.clear()
    hh._endpoint_verified_ok.clear()
    hh._endpoint_last_verified.clear()
    CIRCUIT_BREAKER._state.clear()


# ── Classification ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("status", "payload"),
    [
        (200, {"error": {"code": -32701, "message": "Please specify an address in your request"}}),
        (200, {"error": {"code": -32600, "message": "invalid block range params"}}),
        (200, {"error": "API key disabled, tenant disabled"}),
        (200, {"error": {"message": "Unauthorized: must authenticate"}}),
        (405, {"error": {"message": "method not allowed"}}),
    ],
)
def test_capability_refusals_are_cannot_serve_logs(status, payload):
    assert classify_logs_capability(status, payload) is GetLogsCapability.CANNOT_SERVE_LOGS


def test_result_list_is_capable_and_other_is_transient():
    assert classify_logs_capability(200, {"result": []}) is GetLogsCapability.CAPABLE
    assert classify_logs_capability(200, {"result": None}) is GetLogsCapability.TRANSIENT
    assert classify_logs_capability(500, {"error": {"message": "internal"}}) is GetLogsCapability.TRANSIENT
    assert classify_logs_capability(None, ["not", "a", "dict"]) is GetLogsCapability.TRANSIENT


# ── Probe (httpx stubbed) ──────────────────────────────────────────────


class _Resp:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _Client:
    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def post(self, url, json=None):  # noqa: A002
        return self._resp


@pytest.mark.asyncio
async def test_probe_logs_capability_32701(monkeypatch):
    monkeypatch.setattr(
        cap.httpx,
        "AsyncClient",
        lambda **k: _Client(_Resp(200, {"error": {"code": -32701, "message": "specify an address"}})),
    )
    assert await cap.probe_logs_capability(TIP_ONLY_32701, 1_000_000) is GetLogsCapability.CANNOT_SERVE_LOGS


@pytest.mark.asyncio
async def test_probe_logs_capability_http_405(monkeypatch):
    monkeypatch.setattr(cap.httpx, "AsyncClient", lambda **k: _Client(_Resp(405, {"error": "no"})))
    assert await cap.probe_logs_capability(TIP_ONLY_405, 1_000_000) is GetLogsCapability.CANNOT_SERVE_LOGS


@pytest.mark.asyncio
async def test_probe_logs_capability_working(monkeypatch):
    monkeypatch.setattr(cap.httpx, "AsyncClient", lambda **k: _Client(_Resp(200, {"result": []})))
    assert await cap.probe_logs_capability(GOOD, 1_000_000) is GetLogsCapability.CAPABLE


# ── Verification gate: excluded for logs, retained for tip ─────────────


class _StubRpc:
    async def eth_block_number(self, endpoint):
        return 1_000_000


async def _run_verify(monkeypatch, endpoints, classifications):
    from lite_indexer.rpc import verify as vf

    async def _fake_verify(*args, **kwargs):
        return endpoints

    async def _fake_archive(*args, **kwargs):
        return False

    async def _fake_range(*args, **kwargs):
        return 0

    monkeypatch.setattr(vf, "verify_endpoints_for_chain", _fake_verify)
    monkeypatch.setattr(vf, "probe_archive", _fake_archive)
    monkeypatch.setattr(vf, "probe_getlogs_range", _fake_range)

    async def _fake_probe(endpoint, tip, timeout=0.0):
        return classifications[endpoint]

    monkeypatch.setattr(vf, "probe_logs_capability", _fake_probe)
    return await vf.verify_chain_endpoints("ethereum", _StubRpc())


@pytest.mark.asyncio
async def test_verify_excludes_32701_and_retains_for_tip(monkeypatch):
    result = await _run_verify(
        monkeypatch,
        [GOOD, TIP_ONLY_32701],
        {GOOD: GetLogsCapability.CAPABLE, TIP_ONLY_32701: GetLogsCapability.CANNOT_SERVE_LOGS},
    )
    assert result is not None
    assert result.verified_endpoints == [GOOD]
    assert result.tip_only_endpoints == [TIP_ONLY_32701]
    assert is_logs_capability_excluded(TIP_ONLY_32701) is True


@pytest.mark.asyncio
async def test_verify_excludes_405_and_auth(monkeypatch):
    result = await _run_verify(
        monkeypatch,
        [GOOD, TIP_ONLY_405, TIP_ONLY_AUTH],
        {
            GOOD: GetLogsCapability.CAPABLE,
            TIP_ONLY_405: GetLogsCapability.CANNOT_SERVE_LOGS,
            TIP_ONLY_AUTH: GetLogsCapability.CANNOT_SERVE_LOGS,
        },
    )
    assert result is not None
    assert result.verified_endpoints == [GOOD]
    assert set(result.tip_only_endpoints) == {TIP_ONLY_405, TIP_ONLY_AUTH}


@pytest.mark.asyncio
async def test_transient_failure_stays_in_log_set(monkeypatch):
    result = await _run_verify(
        monkeypatch,
        [GOOD, TRANSIENT],
        {GOOD: GetLogsCapability.CAPABLE, TRANSIENT: GetLogsCapability.TRANSIENT},
    )
    assert result is not None
    assert result.verified_endpoints == [GOOD, TRANSIENT]
    assert result.tip_only_endpoints == []


@pytest.mark.asyncio
async def test_apply_verified_endpoints_excludes_capability_failures(monkeypatch):
    import lite_indexer.chains as chains_mod
    from lite_indexer.rpc import verify as vf

    saved = list(chains_mod.GET_LOGS_ENDPOINTS["ethereum"])
    chains_mod.GET_LOGS_ENDPOINTS["ethereum"] = [GOOD, TIP_ONLY_32701]
    try:
        result = await _run_verify(
            monkeypatch,
            [GOOD, TIP_ONLY_32701],
            {GOOD: GetLogsCapability.CAPABLE, TIP_ONLY_32701: GetLogsCapability.CANNOT_SERVE_LOGS},
        )
        assert result is not None
        vf.apply_verified_endpoints(result)
        assert chains_mod.GET_LOGS_ENDPOINTS["ethereum"] == [GOOD]
        vf.restore_original_endpoints(result)
        assert chains_mod.GET_LOGS_ENDPOINTS["ethereum"] == [GOOD, TIP_ONLY_32701]
    finally:
        chains_mod.GET_LOGS_ENDPOINTS["ethereum"] = saved


@pytest.mark.asyncio
async def test_apply_all_excluded_empties_the_log_set(monkeypatch):
    import lite_indexer.chains as chains_mod
    from lite_indexer.rpc import verify as vf

    saved = list(chains_mod.GET_LOGS_ENDPOINTS["ethereum"])
    chains_mod.GET_LOGS_ENDPOINTS["ethereum"] = [TIP_ONLY_32701]
    try:
        result = await _run_verify(
            monkeypatch,
            [TIP_ONLY_32701],
            {TIP_ONLY_32701: GetLogsCapability.CANNOT_SERVE_LOGS},
        )
        assert result is not None
        vf.apply_verified_endpoints(result)
        assert chains_mod.GET_LOGS_ENDPOINTS["ethereum"] == []
    finally:
        chains_mod.GET_LOGS_ENDPOINTS["ethereum"] = saved


# ── Selection / fetcher never picks an excluded endpoint ───────────────


def test_logs_capable_endpoints_filters_excluded():
    set_logs_capability(TIP_ONLY_32701, "ethereum", False)
    assert logs_capable_endpoints([GOOD, TIP_ONLY_32701]) == [GOOD]


@pytest.mark.asyncio
async def test_fetcher_endpoints_and_selection_never_pick_excluded():
    from lite_indexer.query.rpc_fetcher import RPCFetcher

    set_logs_capability(TIP_ONLY_32701, "ethereum", False)
    set_capability(CapabilityRecord(endpoint=GOOD, chain="ethereum", archive=True, getlogs_max_range=100))

    fetcher = RPCFetcher(endpoints={"ethereum": [GOOD, TIP_ONLY_32701]})
    assert fetcher.endpoints_for("ethereum") == [GOOD]
    assert await fetcher._select_endpoint("ethereum", "eth_getLogs") == GOOD


def test_archive_routing_never_admits_excluded_for_getlogs():
    from lite_indexer.rpc.archive_routing import is_archive_capable

    set_logs_capability(TIP_ONLY_32701, "ethereum", False)
    # Archive flag / positive range must not win once capability-excluded.
    set_capability(CapabilityRecord(endpoint=TIP_ONLY_32701, chain="ethereum", archive=True, getlogs_max_range=1000))
    # Re-apply the exclusion (set_capability above replaced the record).
    set_logs_capability(TIP_ONLY_32701, "ethereum", False)
    assert is_archive_capable(TIP_ONLY_32701, "eth_getLogs") is False


# ── Capability failure is not a ban / circuit-open ─────────────────────


def test_capability_failure_does_not_ban_or_open_circuit():
    from lite_indexer.rpc.pool import ban_endpoint, get_best_endpoint, record_endpoint_failure
    from lite_indexer.rpc.routing import is_circuit_open

    set_logs_capability(TIP_ONLY_32701, "ethereum", False)
    for _ in range(10):
        record_endpoint_failure(TIP_ONLY_32701)
    ban_endpoint(TIP_ONLY_32701, reason="probe_http_405")
    assert is_circuit_open(TIP_ONLY_32701) is False
    # Still selectable for tip/head work alongside a healthy peer.
    assert get_best_endpoint([TIP_ONLY_32701, GOOD]) in (TIP_ONLY_32701, GOOD)
