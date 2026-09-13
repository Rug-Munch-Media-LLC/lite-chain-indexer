#!/usr/bin/env python3
"""Micro-benchmark: ClickHouse write offload vs inline on the event loop.

Reproduces the 2026-09-13 py-spy finding that synchronous ClickHouse
serialization + row-building ran on the event-loop thread and stalled every
chain. The "insert" is a fake client that performs the same row serialization
work the native driver does (``json.dumps`` of the row tuples) but no network,
so this measures the *loop-stall* cost in isolation:

* **inline** — ``writer.write_transfers(...)`` awaited directly on the loop
  (old behaviour).
* **offload** — the same call via :func:`lite_indexer.storage.ch_write_pool.run_ch_write`
  on the bounded ``CH_WRITE_THREADS`` pool (new behaviour).

A 1 ms ticker coroutine measures scheduling lag; the p50/p95/max lag is the
head-of-line-blocking cost paid by every other chain.

Run::

    python3 scripts/bench_ch_write_offload.py
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from lite_indexer.config import CH_WRITE_THREADS
from lite_indexer.storage.ch_write_pool import run_ch_write
from lite_indexer.storage.clickhouse import ClickHouseWriter

CHAINS = ["chain-a", "chain-b", "chain-c", "chain-d"]
BATCHES_PER_CHAIN = 12
EVENTS_PER_BATCH = 2000
CONCURRENCY = 4


class _FakeClickHouse:
    """Stands in for clickhouse_driver.Client: serializes rows, no network."""

    def execute(self, query: str, params: Any = None) -> list[tuple[()]]:
        stripped = query.lstrip().upper()
        if stripped.startswith(("SELECT", "ALTER", "CREATE", "INSERT")) and params is not None:
            # Model the driver's blocking column serialization on INSERT.
            json.dumps(params, default=str, separators=(",", ":"))
        return []


def _events(n: int) -> list[dict[str, Any]]:
    return [
        {
            "blockNumber": 20_000_000 + i,
            "txHash": "0x" + f"{i:064x}",
            "from": "0x" + "aa" * 20,
            "to": "0x" + "bb" * 20,
            "tokenAddress": "0x" + "cc" * 20,
            "value": str(10**18 + i),
            "blockTimestamp": "2026-09-13T00:00:00Z",
            "logIndex": i,
        }
        for i in range(n)
    ]


async def _burst(offload: bool, jobs: list[tuple[str, list[dict[str, Any]]]]) -> tuple[float, float, float, float]:
    writer = ClickHouseWriter()
    writer._client = _FakeClickHouse()  # type: ignore[assignment]  # noqa: SLF001
    lag: list[float] = []
    stop = False

    async def ticker() -> None:
        last = time.perf_counter()
        while not stop:
            await asyncio.sleep(0.001)
            now = time.perf_counter()
            lag.append(now - last)
            last = now

    sem = asyncio.Semaphore(CONCURRENCY)

    async def job(chain: str, events: list[dict[str, Any]]) -> int:
        async with sem:
            if offload:
                return await run_ch_write(writer.write_transfers, chain, events)
            return writer.write_transfers(chain, events)

    start = time.perf_counter()
    tick = asyncio.create_task(ticker())
    await asyncio.gather(*(job(c, e) for c, e in jobs))
    stop = True
    await tick
    wall = time.perf_counter() - start
    lag.sort()
    p50 = lag[len(lag) // 2] * 1000.0
    p95 = lag[int(len(lag) * 0.95)] * 1000.0
    return wall, p50, p95, lag[-1] * 1000.0


def main() -> int:
    payload = _events(EVENTS_PER_BATCH)
    jobs = [(c, payload) for c in CHAINS for _ in range(BATCHES_PER_CHAIN)]
    print(
        f"burst: {len(jobs)} writes x {EVENTS_PER_BATCH} events "
        f"({len(CHAINS)} chains), concurrency={CONCURRENCY}, CH_WRITE_THREADS={CH_WRITE_THREADS}"
    )

    for label, offload in (("inline ", False), ("offload", True)):
        wall, p50, p95, mx = asyncio.run(_burst(offload, jobs))
        print(f"  {label}  wall {wall * 1000:8.1f} ms  tick p50 {p50:7.2f} ms  p95 {p95:7.2f} ms  max {mx:7.2f} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
