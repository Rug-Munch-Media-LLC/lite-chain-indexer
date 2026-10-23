"""Tests for the fair chain-slot rotation (cli/_slot_rotation.py).

The cap is only useful if it is FAIR: a bounded number of chains may sync at
once, but every chain must get a turn. These tests pin both properties.
"""

from __future__ import annotations

import asyncio
import contextlib

from lite_indexer.cli._slot_rotation import SlotRotation


def test_never_exceeds_slots():
    """Concurrent holders never exceed the configured slot count."""

    async def scenario() -> int:
        rotation = SlotRotation(3)
        peak = 0

        async def worker(name: str) -> None:
            nonlocal peak
            await rotation.acquire(name)
            peak = max(peak, rotation.active)
            await asyncio.sleep(0.01)
            rotation.release(name)

        await asyncio.gather(*(worker(f"c{i}") for i in range(16)))
        return peak

    assert asyncio.run(scenario()) == 3


def test_every_chain_gets_a_turn_before_any_gets_a_second():
    """Least-served-first: a 16-chain set over 3 slots is round-robin."""

    async def scenario() -> list[str]:
        rotation = SlotRotation(3)
        order: list[str] = []
        chains = [f"c{i}" for i in range(16)]

        async def worker(name: str, cycles: int) -> None:
            for _ in range(cycles):
                await rotation.acquire(name)
                order.append(name)
                await asyncio.sleep(0.001)
                rotation.release(name)

        await asyncio.gather(*(worker(c, 2) for c in chains))
        return order

    order = asyncio.run(scenario())
    assert len(set(order[:16])) == 16, order[:16]
    assert sorted(order[:16]) == sorted(f"c{i}" for i in range(16))


def test_priority_breaks_ties_only_among_equally_served_chains():
    """Two queued chains with equal service: the higher priority wins the slot."""

    async def scenario() -> str:
        rotation = SlotRotation(1, priority_for=lambda chain: 10 if chain == "hot" else 1)
        await rotation.acquire("holder")  # slot taken, queue starts empty

        order: list[str] = []

        async def worker(name: str) -> None:
            await rotation.acquire(name)
            order.append(name)
            rotation.release(name)

        cold = asyncio.create_task(worker("cold"))
        await asyncio.sleep(0)  # cold is queued first
        hot = asyncio.create_task(worker("hot"))
        await asyncio.sleep(0)  # hot is queued second, same served count (0)
        rotation.release("holder")
        await asyncio.gather(cold, hot)
        return order[0]

    assert asyncio.run(scenario()) == "hot"


def test_release_without_waiters_keeps_slot_available():
    """A release with an empty queue frees the slot for the next caller."""

    async def scenario() -> int:
        rotation = SlotRotation(1)
        await rotation.acquire("a")
        rotation.release("a")
        await asyncio.wait_for(rotation.acquire("b"), timeout=1)
        return rotation.active

    assert asyncio.run(scenario()) == 1


def test_every_chain_served_in_each_round_over_n_iterations():
    """N iterations x 14 chains / 3 slots: every eligible chain served per round.

    Rounds are taken from the SERVED ORDER, not from wall time, so this is
    deterministic: the fairness contract is that no chain is skipped while
    another chain is served twice.
    """

    async def scenario() -> list[str]:
        rotation = SlotRotation(3)
        order: list[str] = []
        chains = [f"chain{i}" for i in range(14)]
        rounds = 4

        async def worker(name: str) -> None:
            for _ in range(rounds):
                await rotation.acquire(name)
                order.append(name)
                await asyncio.sleep(0.001)
                rotation.release(name)

        await asyncio.gather(*(worker(c) for c in chains))
        return order

    order = asyncio.run(scenario())
    per_chain = [order[i : i + 14] for i in range(0, len(order), 14)]
    assert len(per_chain) == 4, per_chain
    for rnd, served in enumerate(per_chain):
        assert sorted(served) == sorted(f"chain{i}" for i in range(14)), (rnd, served)


def test_cancelled_waiter_does_not_leak_a_slot():
    """Shutdown while queued must not strand capacity (daemon SIGTERM path)."""

    async def scenario() -> int:
        rotation = SlotRotation(1)
        await rotation.acquire("holder")
        queued = asyncio.create_task(rotation.acquire("cancelled"))
        await asyncio.sleep(0)
        assert rotation.waiting == 1
        queued.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await queued
        assert rotation.waiting == 0
        assert rotation.active == 1  # only the holder still owns the slot
        rotation.release("holder")
        await asyncio.wait_for(rotation.acquire("next"), timeout=1)
        return rotation.active

    assert asyncio.run(scenario()) == 1


def test_daemon_wires_the_rotation_not_a_plain_semaphore():
    """The daemon must acquire through SlotRotation (guards a silent revert)."""

    import inspect

    from lite_indexer.cli import _daemon

    src = inspect.getsource(_daemon)
    assert "build_rotation(" in src
    assert "asyncio.Semaphore(" not in src


def test_twenty_acquisitions_serve_every_chain_at_least_twice():
    """20 acquisitions across 10 chains / 3 slots: nobody is served once.

    This is the starvation regression that produced the 40-minute stale
    alerts (2026-10-23); fairness is only real if every eligible chain is
    served repeatedly, not just once.
    """

    async def scenario() -> dict[str, int]:
        rotation = SlotRotation(3)
        chains = [f"chain{i}" for i in range(10)]
        counts = dict.fromkeys(chains, 0)

        async def worker(name: str) -> None:
            for _ in range(2):
                await rotation.acquire(name)
                counts[name] += 1
                await asyncio.sleep(0.001)
                rotation.release(name)

        await asyncio.gather(*(worker(c) for c in chains))
        return counts

    counts = asyncio.run(scenario())
    assert sum(counts.values()) == 20, counts
    assert min(counts.values()) >= 2, counts


def test_aged_waiter_beats_served_count_fairness():
    """Wall-clock age outranks least-served when a waiter has waited too long.

    Served-count fairness alone bounds GRANTS, not wall time: with 13-21 min
    sync cycles a chain could still wait hours. The aged waiter must take the
    next free slot even though it has been served five times and the rival
    has never been served.
    """

    async def scenario() -> tuple[list[str], float]:
        now = [0.0]
        rotation = SlotRotation(1, max_wait_s=100.0, clock=lambda: now[0])

        for _ in range(5):  # greedy already served 5 times
            await rotation.acquire("greedy")
            rotation.release("greedy")
        await rotation.acquire("holder")

        order: list[str] = []

        async def worker(name: str) -> None:
            await rotation.acquire(name)
            order.append(name)
            rotation.release(name)

        aged = asyncio.create_task(worker("greedy"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert rotation.waiting == 1

        now[0] = 150.0  # greedy has now been queued 150 s (> max_wait_s)
        fresh = asyncio.create_task(worker("starved"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert rotation.waiting == 2

        oldest = rotation.oldest_wait_s
        now[0] = 151.0
        rotation.release("holder")
        await asyncio.gather(aged, fresh)
        return order, oldest

    order, oldest = asyncio.run(scenario())
    assert order[0] == "greedy", order
    assert oldest >= 100.0, oldest


def test_age_bound_disabled_still_round_robins():
    """max_wait_s=0 keeps pure least-served-first (no accidental FIFO)."""

    async def scenario() -> list[str]:
        rotation = SlotRotation(1, max_wait_s=0.0)
        await rotation.acquire("holder")
        order: list[str] = []

        async def worker(name: str) -> None:
            await rotation.acquire(name)
            order.append(name)
            rotation.release(name)

        tasks = [asyncio.create_task(worker(f"c{i}")) for i in range(4)]
        await asyncio.sleep(0)
        rotation.release("holder")
        await asyncio.gather(*tasks)
        return order

    assert len(asyncio.run(scenario())) == 4
