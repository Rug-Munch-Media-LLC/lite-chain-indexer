"""Tests for in-call eth_getLogs range splitting (rpc/range_split.py).

Covers:

* range/size/result-limit error classification (and NON-range negatives)
* midpoint split bounds + depth cap + min-block floor
* recursive in-line split: 2 child calls, merged + deduped, order preserved
* fail-closed when a half ultimately fails
* RpcClient.get_logs drives the splitter end-to-end on a mocked range error

Hermetic — no network, no disk.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from lite_indexer.rpc import range_split as rs
from lite_indexer.rpc.range_split import (
    classify_logs_error,
    fetch_logs_split,
    is_range_error_message,
    merge_logs,
    split_bounds,
)

_PATCHERS: list[Any] = []


def _start(patcher: Any) -> Any:
    """Start a patcher; the module autouse fixture stops it after each test."""
    _PATCHERS.append(patcher)
    return patcher.start()


@pytest.fixture(autouse=True)
def _stop_patchers() -> Any:
    """Stop every patch started via ``_start`` after each test (no leaks)."""
    yield
    while _PATCHERS:
        _PATCHERS.pop().stop()



def _log(block: int, index: int) -> dict[str, Any]:
    return {"blockNumber": hex(block), "logIndex": hex(index), "data": "0x"}


class TestClassification:
    def test_range_markers_match(self):
        for msg in (
            "block range is too wide",
            "query returned more than 10000 results",
            "response size exceeded",
            "exceeds max block range",
            "too many results",
            "limit exceeded",
        ):
            assert is_range_error_message(msg) is True, msg

    def test_non_range_negatives(self):
        for msg in ("unauthorized", "invalid api key", "method not found", ""):
            assert is_range_error_message(msg) is False, msg

    def test_classify_invalid_params_code(self):
        assert classify_logs_error({"error": {"code": -32602, "message": "bad"}}) == "range"

    def test_classify_plain_error(self):
        assert classify_logs_error({"error": {"code": -32000, "message": "unauthorized"}}) == "error"
        assert classify_logs_error({"result": []}) == "error"
        assert classify_logs_error(None) == "error"


class TestBounds:
    def test_split_bounds_even(self):
        assert split_bounds(0, 10) == ((0, 5), (6, 10))

    def test_split_bounds_single_is_none(self):
        assert split_bounds(7, 7) is None

    def test_merge_dedupes_and_sorts(self):
        merged = merge_logs([[_log(10, 0), _log(2, 0)], [_log(10, 0), _log(2, 1)]])
        assert [(int(x["blockNumber"], 16), int(x["logIndex"], 16)) for x in merged] == [
            (2, 0),
            (2, 1),
            (10, 0),
        ]


class TestRecursiveDriver:
    @pytest.mark.asyncio
    async def test_splits_on_range_error_and_merges(self):
        calls: list[tuple[int, int]] = []

        async def fetch(f: int, t: int) -> rs.FetchResult:
            calls.append((f, t))
            if (f, t) == (0, 100):
                return ("range", None)
            return ("ok", [_log(f, 0), _log(t, 0)])

        result = await fetch_logs_split(fetch, 0, 100)
        assert [c for c in calls if c == (0, 100)]  # root attempted
        assert (0, 50) in calls and (51, 100) in calls  # 2 child calls
        assert result is not None
        assert [int(x["blockNumber"], 16) for x in result] == [0, 50, 51, 100]

    @pytest.mark.asyncio
    async def test_non_range_error_not_split(self):
        calls: list[tuple[int, int]] = []

        async def fetch(f: int, t: int) -> rs.FetchResult:
            calls.append((f, t))
            return ("error", None)

        assert await fetch_logs_split(fetch, 0, 100) is None
        assert calls == [(0, 100)]

    @pytest.mark.asyncio
    async def test_depth_cap_bounds_recursion(self):
        calls: list[tuple[int, int]] = []

        async def fetch(f: int, t: int) -> rs.FetchResult:
            calls.append((f, t))
            return ("range", None)

        with patch.object(rs, "GETLOGS_SPLIT_MAX_DEPTH", 1):
            assert await fetch_logs_split(fetch, 0, 7) is None
        # depth 0 splits once → two depth-1 leaves; depth cap stops there.
        assert len(calls) == 3

    @pytest.mark.asyncio
    async def test_min_block_floor_blocks_split(self):
        calls: list[tuple[int, int]] = []

        async def fetch(f: int, t: int) -> rs.FetchResult:
            calls.append((f, t))
            return ("range", None)

        with (
            patch.object(rs, "GETLOGS_SPLIT_MAX_DEPTH", 10),
            patch.object(rs, "GETLOGS_SPLIT_MIN_BLOCKS", 100),
        ):
            assert await fetch_logs_split(fetch, 0, 50) is None  # size 51 <= floor
        assert calls == [(0, 50)]

    @pytest.mark.asyncio
    async def test_any_half_failure_fails_closed(self):
        async def fetch(f: int, t: int) -> rs.FetchResult:
            if (f, t) == (0, 100):
                return ("range", None)
            if (f, t) == (0, 50):
                return ("ok", [_log(0, 0)])
            return ("error", None)

        assert await fetch_logs_split(fetch, 0, 100) is None


class _FakeResp:
    def __init__(self, payload: Any, status_code: int = 200, headers: dict[str, str] | None = None):
        self._payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _FakeHttp:
    """Rejects wide ranges; returns one log per accepted 4-block window.

    Handles both single JSON-RPC objects and BATCH arrays (sibling halves).
    """

    def __init__(self) -> None:
        self.params: list[tuple[int, int]] = []
        self.batch_posts = 0
        self.single_posts = 0

    @staticmethod
    def _one(req: dict[str, Any]) -> dict[str, Any]:
        p = req["params"][0]
        f = int(p["fromBlock"], 16)
        t = int(p["toBlock"], 16)
        if (t - f + 1) > 4:
            return {"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32000, "message": "block range too large"}}
        return {"jsonrpc": "2.0", "id": req["id"], "result": [_log(f, 0)]}

    async def post(self, url: str, **kw: Any):
        req = kw["json"]
        if isinstance(req, list):
            self.batch_posts += 1
            out = []
            for item in req:
                p = item["params"][0]
                self.params.append((int(p["fromBlock"], 16), int(p["toBlock"], 16)))
                out.append(self._one(item))
            return _FakeResp(out)
        self.single_posts += 1
        p = req["params"][0]
        self.params.append((int(p["fromBlock"], 16), int(p["toBlock"], 16)))
        return _FakeResp(self._one(req))


@pytest.mark.asyncio
async def test_rpcclient_get_logs_splits_in_call():
    import lite_indexer.rpc.client as rc

    http = _FakeHttp()

    async def _noop(*a: Any, **k: Any) -> None:
        return None

    _start(patch.object(rc, "apply_provider_rate_limit", _noop))
    _start(patch.object(rc, "provider_throttle_before", _noop))
    _start(patch.object(rc, "record_provider_response", lambda *a, **k: None))
    _start(patch.object(rc, "_safe_record", lambda *a, **k: None))
    _start(patch("lite_indexer.rpc.throttle.throttle_before", _noop))
    _start(patch("lite_indexer.rpc.throttle.record_rpc_response", lambda *a, **k: None))
    _start(patch.object(rc, "_defer_reason_for", lambda *a, **k: None))

    async def _get_client() -> _FakeHttp:
        return http

    async def _get_call_client(timeout: float) -> _FakeHttp:
        return http

    client = rc.RpcClient()
    client._get_client = _get_client  # noqa: SLF001 — test seam
    _start(patch.object(rc, "_get_call_client", _get_call_client))
    result = await client.get_logs("https://archive.example", 0, 15, "0x" + "ab" * 32)
    assert result is not None
    blocks = [int(x["blockNumber"], 16) for x in result]
    assert blocks == [0, 4, 8, 12]
    # Root range was rejected, then halved in-line; sibling halves batched.
    assert (0, 15) in http.params and (0, 7) in http.params and (8, 15) in http.params
    assert http.batch_posts >= 1


class TestGenericRangeMethod:
    @pytest.mark.asyncio
    async def test_second_method_splits_and_concatenates(self):
        calls: list[tuple[int, int]] = []

        async def fetch(f: int, t: int) -> rs.FetchResult:
            calls.append((f, t))
            if (f, t) == (0, 100):
                return ("range", None)
            return ("ok", [{"block": f}, {"block": t}])

        result = await rs.fetch_range_split(fetch, 0, 100, method="trace_filter", merge="records")
        assert calls == [(0, 100), (0, 50), (51, 100)]
        assert result == [{"block": 0}, {"block": 50}, {"block": 51}, {"block": 100}]

    @pytest.mark.asyncio
    async def test_records_merge_keeps_duplicates(self):
        async def fetch(f: int, t: int) -> rs.FetchResult:
            if (f, t) == (0, 10):
                return ("range", None)
            return ("ok", [{"dup": 1}])

        result = await rs.fetch_range_split(fetch, 0, 10, method="trace_filter", merge="records")
        assert result == [{"dup": 1}, {"dup": 1}]


class TestBatchSiblings:
    @pytest.mark.asyncio
    async def test_siblings_fetched_in_one_batch(self):
        single_calls: list[tuple[int, int]] = []
        many_calls: list[list[tuple[int, int]]] = []

        async def fetch_once(f: int, t: int) -> rs.FetchResult:
            single_calls.append((f, t))
            if (f, t) == (0, 20):
                return ("range", None)
            return ("ok", [f])

        async def fetch_many(ranges: list[tuple[int, int]]) -> list[rs.FetchResult]:
            many_calls.append(list(ranges))
            return [("ok", [f]) for f, _t in ranges]

        result = await rs.fetch_range_split(
            fetch_once, 0, 20, method="eth_getLogs", fetch_many=fetch_many, merge="records"
        )
        assert single_calls == [(0, 20)]  # children came from the batch, not singles
        assert many_calls == [[(0, 10), (11, 20)]]
        assert result == [0, 11]
