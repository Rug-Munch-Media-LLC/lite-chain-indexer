"""Archive-class method routing with fail-closed (capabilities + selector).

Coverage:

* method classification: ARCHIVE (eth_getLogs/eth_getProof/debug_*/trace_*)
  vs TIP class
* is_archive_capable: archive flag, positive getlogs_max_range for getLogs,
  unknown endpoint is NOT archive-capable (fail-closed default)
* select_endpoint_for_method: archive-class never routes to a known
  non-archive endpoint; raises NoArchiveEndpointError when none are
  archive-capable; TIP-class unaffected
* select_archive_endpoint: strict fail-closed entry point
* RpcClient.call: dispatch guard never sends an archive-class call to a
  known non-archive endpoint; TIP-class unaffected

Hermetic — no network, no disk.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from lite_indexer.rpc import capabilities as cap
from lite_indexer.rpc.capabilities import (
    CapabilityRecord,
    NoArchiveEndpointError,
    archive_capable_endpoints,
    archive_guard_reason,
    get_capability,
    is_archive_capable,
    method_requires_archive,
    set_capability,
)
from lite_indexer.rpc.provider_caps import PROVIDER_LIMITER
from lite_indexer.rpc.routing import USAGE_TRACKER, select_archive_endpoint, select_endpoint_for_method

ARCHIVE_EP = "https://archive.example"
TIP_EP = "https://tip.example"


@pytest.fixture(autouse=True)
def _clean_registry():
    cap._CAPABILITY_RECORDS.clear()
    USAGE_TRACKER.reset()
    PROVIDER_LIMITER.reset()
    yield
    cap._CAPABILITY_RECORDS.clear()
    USAGE_TRACKER.reset()
    PROVIDER_LIMITER.reset()


def _cap(endpoint: str, *, archive: bool, getlogs: int = 0, chain: str = "ethereum") -> None:
    set_capability(
        CapabilityRecord(
            endpoint=endpoint,
            chain=chain,
            archive=archive,
            getlogs_max_range=getlogs,
        )
    )


class TestMethodClassification:
    def test_archive_methods(self):
        for method in ("eth_getLogs", "eth_getProof", "debug_traceBlockByNumber", "trace_filter"):
            assert method_requires_archive(method) is True, method

    def test_tip_methods(self):
        for method in ("eth_blockNumber", "eth_call", "eth_getBalance", "eth_getBlockByNumber"):
            assert method_requires_archive(method) is False, method


class TestArchiveCapability:
    def test_archive_flag_admits(self):
        _cap(ARCHIVE_EP, archive=True)
        assert is_archive_capable(ARCHIVE_EP, "eth_getProof") is True

    def test_getlogs_range_admits_non_archive(self):
        _cap(TIP_EP, archive=False, getlogs=1000)
        assert is_archive_capable(TIP_EP, "eth_getLogs") is True
        # Range does NOT make it capable for non-getLogs archive methods.
        assert is_archive_capable(TIP_EP, "eth_getProof") is False

    def test_unknown_endpoint_not_capable(self):
        assert is_archive_capable("https://never-probed.example", "eth_getLogs") is False

    def test_archive_capable_endpoints_filters(self):
        _cap(ARCHIVE_EP, archive=True)
        _cap(TIP_EP, archive=False, getlogs=0)
        assert archive_capable_endpoints([ARCHIVE_EP, TIP_EP], "eth_getLogs") == [ARCHIVE_EP]


class TestGuardReason:
    def test_tip_method_never_guarded(self):
        _cap(TIP_EP, archive=False)
        assert archive_guard_reason(TIP_EP, "eth_blockNumber") is None

    def test_known_non_archive_guarded(self):
        _cap(TIP_EP, archive=False)
        assert archive_guard_reason(TIP_EP, "eth_getLogs") == "endpoint_not_archive_capable"

    def test_unknown_endpoint_allowed(self):
        assert archive_guard_reason("https://unprobed.example", "eth_getLogs") is None

    def test_archive_endpoint_allowed(self):
        _cap(ARCHIVE_EP, archive=True)
        assert archive_guard_reason(ARCHIVE_EP, "trace_filter") is None


class TestSelectorArchiveRouting:
    def test_archive_call_never_routes_to_non_archive(self):
        _cap(ARCHIVE_EP, archive=True)
        _cap(TIP_EP, archive=False)
        selected = select_endpoint_for_method("eth_getLogs", [TIP_EP, ARCHIVE_EP])
        assert selected == ARCHIVE_EP

    def test_fail_closed_when_no_archive_endpoint(self):
        _cap(TIP_EP, archive=False)
        with pytest.raises(NoArchiveEndpointError):
            select_endpoint_for_method("eth_getLogs", [TIP_EP], chain="ethereum")

    def test_fail_closed_for_debug_when_only_non_archive(self):
        _cap(TIP_EP, archive=False)
        with pytest.raises(NoArchiveEndpointError):
            select_endpoint_for_method("debug_traceTransaction", [TIP_EP])

    def test_tip_class_unaffected(self):
        _cap(TIP_EP, archive=False)
        selected = select_endpoint_for_method("eth_blockNumber", [TIP_EP], chain="ethereum")
        assert selected == TIP_EP

    def test_unprobed_pool_preserves_legacy_selection(self):
        # No capability records yet → pre-verification fallback (no raise).
        selected = select_endpoint_for_method("eth_getLogs", [TIP_EP], chain="ethereum")
        assert selected == TIP_EP

    def test_strict_select_archive_endpoint_requires_capability(self):
        with pytest.raises(NoArchiveEndpointError):
            select_archive_endpoint("eth_getLogs", [TIP_EP], chain="ethereum")
        _cap(ARCHIVE_EP, archive=True)
        assert select_archive_endpoint("eth_getLogs", [TIP_EP, ARCHIVE_EP]) == ARCHIVE_EP

    def test_strict_select_rejects_tip_method(self):
        with pytest.raises(ValueError):
            select_archive_endpoint("eth_blockNumber", [TIP_EP])


class _FakeResp:
    def __init__(self, payload: str = '{"result": "0x1"}'):
        self.text = payload
        self.status_code = 200
        self.content = payload.encode()
        self.headers: dict[str, str] = {}

    def raise_for_status(self) -> None:
        return None

    def json(self):
        import json

        return json.loads(self.text)


class _FakePooled:
    def __init__(self, resp: _FakeResp) -> None:
        self._resp = resp
        self.calls = 0

    async def post(self, url, json=None):  # noqa: A002
        self.calls += 1
        return self._resp


class TestClientDispatchGuard:
    @pytest.mark.asyncio
    async def test_archive_call_fails_closed_on_known_non_archive(self):
        from lite_indexer.rpc import client as rc

        _cap("https://nonarchive.example", archive=False)
        pooled = _FakePooled(_FakeResp())

        async def fake_get(timeout):
            return pooled

        with patch.object(rc, "_get_call_client", fake_get):
            out = await rc.RpcClient().call("https://nonarchive.example", "eth_getLogs")
        assert out is None
        assert pooled.calls == 0  # never hit the wire

    @pytest.mark.asyncio
    async def test_tip_call_still_dispatches(self):
        from lite_indexer.rpc import client as rc

        _cap("https://tip-dispatch.example", archive=False)
        pooled = _FakePooled(_FakeResp('{"result": "0x4b7"}'))

        async def fake_get(timeout):
            return pooled

        with patch.object(rc, "_get_call_client", fake_get):
            out = await rc.RpcClient().call("https://tip-dispatch.example", "eth_chainId")
        assert out == {"result": "0x4b7"}
        assert pooled.calls == 1

    @pytest.mark.asyncio
    async def test_archive_call_allowed_on_probed_archive(self):
        from lite_indexer.rpc import client as rc

        _cap(ARCHIVE_EP, archive=True)
        pooled = _FakePooled(_FakeResp('{"result": []}'))

        async def fake_get(timeout):
            return pooled

        with patch.object(rc, "_get_call_client", fake_get):
            out = await rc.RpcClient().call(ARCHIVE_EP, "eth_getLogs")
        assert out == {"result": []}
        assert pooled.calls == 1

    def test_get_capability_roundtrip(self):
        _cap(ARCHIVE_EP, archive=True)
        assert get_capability(ARCHIVE_EP) is not None
