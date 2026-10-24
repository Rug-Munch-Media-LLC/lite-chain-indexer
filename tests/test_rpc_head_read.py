"""Hedged head reads: first VALID answer wins, stale is always labelled.

Offline: the client is a scripted stub, so no provider is ever dialled.

Contracts pinned here:

* a healthy primary answers inside the delay window and no hedge is spent;
* a slow primary loses to a fast hedge (the tip is never stalled by one peer);
* a fast *failure* (None, 0, or a raised leg) fails over instead of going
  stale — the pre-fix behaviour treated an empty answer as an answer;
* when every provider fails, the last-known-good tip is served with
  ``stale=True`` and its age, or ``RpcUnavailable`` is raised — never an
  unlabelled value and never an empty success;
* non-head methods are refused before a request is built (writes and cursor
  reads are never hedged).
"""

from __future__ import annotations

import asyncio
import time

import pytest

from lite_indexer.rpc import last_good
from lite_indexer.rpc.head_read import HeadReadSpec, read_head
from lite_indexer.rpc.rpc_unavailable import RpcUnavailable

FAST = 0.02
SLOW = 0.5


class StubHeadClient:
    """Scripted per-endpoint ``(delay, value|exception)`` responses."""

    def __init__(self, script: dict[str, tuple[float, object]]) -> None:
        self.script = script
        self.calls: list[str] = []

    async def eth_block_number(self, endpoint: str) -> object:
        self.calls.append(endpoint)
        delay, value = self.script[endpoint]
        if delay:
            await asyncio.sleep(delay)
        if isinstance(value, Exception):
            raise value
        return value


@pytest.fixture(autouse=True)
def _clean_last_good():
    last_good.reset()
    yield
    last_good.reset()


async def test_fast_primary_answers_without_spending_a_hedge() -> None:
    eps = ["https://h1.example", "https://h2.example"]
    client = StubHeadClient({eps[0]: (0.0, 100), eps[1]: (0.0, 200)})

    reading = await read_head(client, "chain-h1", eps, HeadReadSpec(hedge_delay_s=FAST))

    assert reading.value in (100, 200)
    assert reading.stale is False
    assert reading.age_s == 0.0
    # Exactly one call: the healthy primary answered inside the delay window,
    # so no hedge was spent. Which of the two is primary is the pool's call
    # (cost class + fair rotation), not this module's.
    assert len(client.calls) == 1


async def test_slow_primary_loses_to_the_hedge() -> None:
    eps = ["https://h3.example", "https://h4.example"]
    client = StubHeadClient({eps[0]: (SLOW, 100), eps[1]: (0.0, 200)})

    started = time.perf_counter()
    reading = await read_head(client, "chain-h3", eps, HeadReadSpec(hedge_delay_s=FAST))
    elapsed = time.perf_counter() - started

    assert reading.value == 200
    assert elapsed < SLOW, "the hedge must not wait for the slow leg"


async def test_empty_answer_fails_over_instead_of_going_stale() -> None:
    eps = ["https://h5.example", "https://h6.example"]
    client = StubHeadClient({eps[0]: (0.0, None), eps[1]: (0.0, 200)})

    reading = await read_head(client, "chain-h5", eps, HeadReadSpec(hedge_delay_s=FAST))

    assert reading.value == 200
    assert reading.stale is False


@pytest.mark.parametrize("bad", [0, -1, "0x10", 3.5, RuntimeError("boom")])
async def test_invalid_primary_values_fail_over(bad: object) -> None:
    eps = ["https://h7.example", "https://h8.example"]
    client = StubHeadClient({eps[0]: (0.0, bad), eps[1]: (0.0, 7)})

    reading = await read_head(client, "chain-h7", eps, HeadReadSpec(hedge_delay_s=FAST))

    assert reading.value == 7


async def test_all_providers_down_with_no_history_raises_typed_error() -> None:
    eps = ["https://h9.example", "https://h10.example"]
    client = StubHeadClient({eps[0]: (0.0, None), eps[1]: (0.0, RuntimeError("down"))})

    with pytest.raises(RpcUnavailable) as info:
        await read_head(client, "chain-h9", eps, HeadReadSpec(hedge_delay_s=FAST))

    assert info.value.chain == "chain-h9"
    assert info.value.age_s is None


async def test_total_outage_serves_last_known_good_with_age() -> None:
    eps = ["https://h11.example", "https://h12.example"]
    client = StubHeadClient({eps[0]: (0.0, 424242), eps[1]: (0.0, 424242)})
    await read_head(client, "chain-h11", eps, HeadReadSpec(hedge_delay_s=FAST))

    down = StubHeadClient({eps[0]: (0.0, None), eps[1]: (0.0, RuntimeError("down"))})
    reading = await read_head(down, "chain-h11", eps, HeadReadSpec(hedge_delay_s=FAST))

    assert reading.value == 424242
    assert reading.stale is True
    assert reading.age_s >= 0.0


async def test_stale_value_older_than_the_ceiling_is_refused() -> None:
    last_good.remember("chain-h13", 9, now=0.0)

    with pytest.raises(RpcUnavailable):
        await read_head(StubHeadClient({}), "chain-h13", [], HeadReadSpec(max_stale_age_s=1.0))


async def test_non_head_method_is_refused_before_any_call() -> None:
    eps = ["https://h14.example"]
    client = StubHeadClient({eps[0]: (0.0, 1)})

    with pytest.raises(ValueError, match="non-head method"):
        await read_head(client, "chain-h14", eps, HeadReadSpec(method="eth_getLogs"))

    assert client.calls == []


def test_last_good_store_is_bounded_and_reports_age() -> None:
    for i in range(last_good.MAX_CHAINS + 1):
        last_good.remember(f"chain-bound-{i}", i, now=100.0)

    assert last_good.recall("chain-bound-0") is None
    entry = last_good.recall(f"chain-bound-{last_good.MAX_CHAINS}")
    assert entry is not None
    assert entry.age_s(now=160.0) == 60.0
