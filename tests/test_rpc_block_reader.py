"""The acceptance test for the 2026-09-26 pool walk.

The bug it guards against is not "one endpoint failed" — it is that the
caller asked ONE endpoint chosen by a random power-of-two-choices draw, and
on hyperliquid only 2 of 6 endpoints retain the stored head. So the pool
must be WALKED until an endpoint produces a real hash, and every attempt
must be recorded with the status that actually answered.

The fake pool below is shaped like the real one: four endpoints answer
``200`` with a pruned body, one has the block. The old single-draw selector
would have had a 4-in-5 chance of stalling on any given cycle; the walk
succeeds on every cycle, in a deterministic order.

Network-free. The reader is a stand-in that satisfies the
:class:`~lite_indexer.rpc.block_reader.BlockHashReader` Protocol — injected,
never constructed or patched.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.rpc import block_serving
from lite_indexer.rpc.block_reader import MAX_BLOCK_READ_ATTEMPTS, resolve_block_hash

CHAIN = "testchain"
HEAD = 1_576_295
HEAD_HASH = "0x" + "a1" * 32

#: Four endpoints that answer 200 with nothing (pruned window), one that has
#: the block. Same shape as the hyperliquid pool that stalled.
NULL_POOL = [f"https://null-{i}.example.invalid/rpc" for i in range(4)]
HASH_ENDPOINT = "https://has-block.example.invalid/rpc"
MOSTLY_NULL_POOL = [*NULL_POOL, HASH_ENDPOINT]


class FakePool:
    """A pool whose answers are fixed up front, with a call log.

    Attributes:
        served: Endpoints that return a real block for :attr:`block`.
        statuses: Per-endpoint status to report, defaulting to 200.
        calls: Endpoints dialled, in order — the attempt record.
    """

    def __init__(self, served: list[str], *, statuses: dict[str, int] | None = None) -> None:
        self.served = served
        self.statuses = statuses or {}
        self.calls: list[str] = []

    async def eth_get_block_by_number_with_status(
        self, endpoint: str, block_number: int, full_tx: bool = False, **_: Any
    ) -> tuple[dict[str, Any] | None, int | None]:
        self.calls.append(endpoint)
        status = self.statuses.get(endpoint, 200)
        if endpoint in self.served:
            return {"number": block_number, "hash": HEAD_HASH}, status
        return None, status


@pytest.fixture(autouse=True)
def _clean_registry() -> Any:
    """Keep the served-window registry out of neighbouring tests' way."""
    block_serving.reset_windows()
    yield
    block_serving.reset_windows()


async def test_walk_finds_the_hash_when_four_of_five_return_null() -> None:
    pool = FakePool([HASH_ENDPOINT])
    result = await resolve_block_hash(pool, CHAIN, HEAD, MOSTLY_NULL_POOL)  # type: ignore[arg-type]

    assert result.ok
    assert result.block_hash == HEAD_HASH
    assert result.endpoint == HASH_ENDPOINT
    assert result.served == 1


async def test_every_attempt_is_recorded_until_one_serves() -> None:
    pool = FakePool([HASH_ENDPOINT])
    result = await resolve_block_hash(pool, CHAIN, HEAD, MOSTLY_NULL_POOL)  # type: ignore[arg-type]

    # 4 nulls then the hit: nothing is swallowed, and the walk stops there.
    assert result.tried == tuple(MOSTLY_NULL_POOL)
    assert pool.calls == MOSTLY_NULL_POOL


async def test_walk_order_is_deterministic_across_repeated_reads() -> None:
    # The pre-fix selector ended in a random draw; the fix must not
    # reintroduce one. Registry reset before each cycle so the evidence
    # state is identical every time — otherwise the walk is EXPECTED to
    # converge and the orders would legitimately differ (see the next test).
    orders = []
    for _ in range(25):
        block_serving.reset_windows()
        pool = FakePool([HASH_ENDPOINT])
        await resolve_block_hash(pool, CHAIN, HEAD, MOSTLY_NULL_POOL)  # type: ignore[arg-type]
        orders.append(tuple(pool.calls))

    assert len(set(orders)) == 1


async def test_first_attempt_follows_served_range_not_a_draw() -> None:
    # Every cycle before any evidence exists must pick the SAME first
    # candidate. A power-of-two-choices draw would vary it.
    firsts = set()
    for _ in range(30):
        block_serving.reset_windows()
        pool = FakePool([HASH_ENDPOINT])
        await resolve_block_hash(pool, CHAIN, HEAD, MOSTLY_NULL_POOL)  # type: ignore[arg-type]
        firsts.add(pool.calls[0])

    assert firsts == {NULL_POOL[0]}


async def test_known_capable_endpoint_is_offered_first_next_cycle() -> None:
    # Convergence: once an endpoint has demonstrated it serves the head, the
    # next cycle leads with it instead of re-dialing the four that cannot.
    await resolve_block_hash(FakePool([HASH_ENDPOINT]), CHAIN, HEAD, MOSTLY_NULL_POOL)  # type: ignore[arg-type]
    second = FakePool([HASH_ENDPOINT])
    result = await resolve_block_hash(second, CHAIN, HEAD, MOSTLY_NULL_POOL)  # type: ignore[arg-type]

    assert result.ok
    assert second.calls[0] == HASH_ENDPOINT
    assert len(second.calls) == 1


async def test_null_only_pool_records_200_not_refusal() -> None:
    result = await resolve_block_hash(FakePool([]), CHAIN, HEAD, NULL_POOL)  # type: ignore[arg-type]

    assert not result.ok
    assert result.block_hash is None
    # A 200 with a pruned body is a window boundary, not a refusal. If the
    # status were faked as anything else this assertion would be meaningless.
    assert result.last_status == 200


async def test_pool_wide_403_reports_the_real_status() -> None:
    refusing = FakePool([], statuses={ep: 403 for ep in NULL_POOL})
    result = await resolve_block_hash(refusing, CHAIN, HEAD, NULL_POOL)  # type: ignore[arg-type]

    assert not result.ok
    assert result.last_status == 403


async def test_transport_failure_reports_no_status() -> None:
    class DeadPool:
        async def eth_get_block_by_number_with_status(
            self, endpoint: str, block_number: int, full_tx: bool = False, **_: Any
        ) -> tuple[dict[str, Any] | None, int | None]:
            raise TimeoutError("wire died")

    result = await resolve_block_hash(DeadPool(), CHAIN, HEAD, NULL_POOL)  # type: ignore[arg-type]

    assert not result.ok
    assert result.last_status is None


async def test_attempt_cap_is_respected() -> None:
    pool = FakePool([HASH_ENDPOINT])
    result = await resolve_block_hash(  # type: ignore[arg-type]
        pool, CHAIN, HEAD, [*NULL_POOL, HASH_ENDPOINT], max_attempts=2
    )

    assert len(result.tried) == 2
    assert not result.ok
    assert MAX_BLOCK_READ_ATTEMPTS == 6


async def test_empty_pool_short_circuits() -> None:
    result = await resolve_block_hash(FakePool([]), CHAIN, HEAD, [])  # type: ignore[arg-type]

    assert not result.ok
    assert result.tried == ()
    assert result.last_status is None


async def test_block_without_a_hash_is_treated_as_unserved() -> None:
    # A body with a null hash is not a hit, and must not be reported as one.
    class NoHashPool:
        async def eth_get_block_by_number_with_status(
            self, endpoint: str, block_number: int, full_tx: bool = False, **_: Any
        ) -> tuple[dict[str, Any] | None, int | None]:
            return {"number": block_number, "hash": None}, 200

    result = await resolve_block_hash(NoHashPool(), CHAIN, HEAD, [HASH_ENDPOINT])  # type: ignore[arg-type]

    assert not result.ok
    assert result.last_status == 200
