"""Tests for the silent ``eth_getLogs`` truncation guard.

A public RPC that caps its result list answers a ranged ``eth_getLogs``
with HTTP 200 and a SHORT list — no error, no warning. Accepting that
page as the complete chunk is silent data loss: the sync loop advances
its watermark past blocks it never queried. These tests cover the pure
predicate (``is_truncated_log_response``) and, more importantly, that
``_fetch_range`` actually ROTATES to a different endpoint instead of
accepting the short page. The predicate alone proves nothing if the
handler ignores it.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from lite_indexer.strategies.evm.log_budget import (
    TAIL_SLACK_BLOCKS,
    is_truncated_log_response,
)
from lite_indexer.strategies.evm.transfer_handler import _fetch_range

TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _log(block: int, index: int) -> dict[str, Any]:
    return {
        "blockNumber": hex(block),
        "logIndex": hex(index),
        "topics": [TOPIC, "0x" + "11" * 32, "0x" + "22" * 32],
        "data": "0x" + "00" * 32,
        "transactionHash": hex(index),
    }


class TestIsTruncatedLogResponse:
    def test_page_stopping_short_of_the_range_end_is_truncated(self) -> None:
        logs = [_log(100 + i, i) for i in range(50)]
        assert is_truncated_log_response(logs, 100, 200) is True

    def test_page_reaching_the_range_end_is_complete(self) -> None:
        logs = [_log(100 + i, i) for i in range(101)]
        assert is_truncated_log_response(logs, 100, 200) is False

    def test_quiet_tail_inside_slack_is_not_truncated(self) -> None:
        """The last few blocks may legitimately carry no matching logs."""
        logs = [_log(100 + i, i) for i in range(200 - TAIL_SLACK_BLOCKS)]
        assert is_truncated_log_response(logs, 100, 200) is False

    def test_empty_result_is_a_valid_answer(self) -> None:
        assert is_truncated_log_response([], 100, 200) is False

    def test_single_block_page_for_a_single_block_range_is_complete(self) -> None:
        assert is_truncated_log_response([_log(100, 0)], 100, 100) is False

    def test_int_block_numbers_are_accepted(self) -> None:
        assert is_truncated_log_response([{"blockNumber": 100}], 100, 200) is True

    def test_unparseable_block_numbers_are_treated_as_suspicious(self) -> None:
        """We cannot prove the tail is covered, so we must not accept it."""
        assert is_truncated_log_response([{"blockNumber": "zzz"}], 100, 200) is True

    def test_malformed_entries_are_ignored_not_fatal(self) -> None:
        logs = [_log(200, 0), {"no_block_number": True}, "not-a-dict"]
        assert is_truncated_log_response(logs, 100, 200) is False


def _json_response(payload: dict[str, Any], status: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code=status,
        json=payload,
        request=httpx.Request("POST", "http://rpc.invalid"),
    )


class TestFetchRangeRotatesOnTruncation:
    async def test_truncated_page_is_never_accepted_as_complete(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A capped page must cost a rotation, never a silent watermark gap.

        Whichever endpoint the pool picks first, the accepted result may only
        contain rows a healthy (full-width) page returned. If the truncated
        endpoint's short page were accepted, the watermark would advance past
        blocks that were never queried.
        """
        calls: list[str] = []
        short_url = "http://short.invalid"
        full_url = "http://full.invalid"
        short_logs = [_log(100 + i, i) for i in range(50)]
        full_logs = [_log(100 + i, i) for i in range(101)]

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(str(request.url))
            if "short.invalid" in str(request.url):
                return _json_response({"jsonrpc": "2.0", "id": 1, "result": short_logs})
            return _json_response({"jsonrpc": "2.0", "id": 1, "result": full_logs})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            result = await _fetch_range(
                "ethereum",
                100,
                200,
                client=client,
                topic=TOPIC,
                endpoints=[short_url, full_url],
            )

        # The truncated endpoint is penalised on sight, so the pool may pick
        # the healthy one first and never even reach the short one. Either way
        # the accepted page must be the complete one: only the full page's logs
        # reach block 200, so a short-page acceptance would leave the maximum
        # below the range end and the watermark would advance past a gap.
        assert full_url in calls, f"the healthy endpoint must be tried; calls={calls}"
        assert result, "a complete page must yield parsed transfers"
        assert max(e["blockNumber"] for e in result) == 200
