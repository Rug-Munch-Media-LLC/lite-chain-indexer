"""Tests for the fair chain-slot rotation (cli/_slot_rotation.py).

The cap is only useful if it is FAIR: a bounded number of chains may sync at
once, but every chain must get a turn. These tests pin both properties.
"""

from __future__ import annotations

import asyncio

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
