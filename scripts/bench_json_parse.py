#!/usr/bin/env python3
"""Micro-benchmarks for the hot RPC JSON decode path.

Two independent measurements:

1. **Decoder throughput** — stdlib ``json.loads`` vs optional
   ``msgspec.json.decode`` on a representative 1000-log ``eth_getLogs``
   payload (per-decoder median, best, and the speedup ratio).
2. **Event-loop stall (before/after)** — decodes a burst of
   representative ``eth_getLogs`` responses while a 1ms ticker coroutine
   measures scheduling lag. ``inline`` decodes on the loop (the old
   behaviour); ``offload`` uses
   :func:`lite_indexer.rpc.jsonparse.decode_bytes_async`, which runs the
   same decoder in the bounded JSON_DECODE_THREADS pool. The p50/max tick
   lag is the head-of-line-blocking cost: inline collapses to one giant
   gap, offload lets the loop keep ticking.

Run::

    python3 scripts/bench_json_parse.py

If msgspec is not installed the throughput section reports that (optional
``perf`` extra); the loop-stall section still runs on stdlib json. Exits 0.
"""

from __future__ import annotations

import asyncio
import json
import statistics
import time

from lite_indexer.config import JSON_DECODE_THREADS
from lite_indexer.rpc import jsonparse

REPEATS = 300
WARMUP = 30


def _payload(logs: int = 1000) -> bytes:
    return _logs_payload(logs)


def _logs_payload(logs: int) -> bytes:
    rows = [
        {
            "address": "0x" + "11" * 20,
            "topics": ["0x" + "ab" * 32, "0x" + "00" * 32, "0x" + "00" * 32],
            "data": "0x" + "00" * 32,
            "blockNumber": hex(18_000_000 + i),
            "transactionHash": "0x" + f"{i:064x}",
            "transactionIndex": hex(i % 200),
            "blockHash": "0x" + "22" * 32,
            "logIndex": hex(i),
            "removed": False,
        }
        for i in range(logs)
    ]
    return json.dumps({"jsonrpc": "2.0", "id": 1, "result": rows}).encode()


def _time(fn, data: bytes) -> float:
    for _ in range(WARMUP):
        fn(data)
    samples = []
    for _ in range(REPEATS):
        start = time.perf_counter()
        fn(data)
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


async def _loop_stall(offload: bool, payloads: list[bytes], concurrency: int = 4) -> tuple[float, float, float]:
    """Run ``payloads`` through the decoder and return (wall, p50, max) tick lag ms.

    ``offload=False`` decodes inline on the event loop (old path);
    ``offload=True`` awaits :func:`jsonparse.decode_bytes_async` (new path).
    """
    stop = False
    lag: list[float] = []

    async def ticker() -> None:
        last = time.perf_counter()
        while not stop:
            await asyncio.sleep(0.001)
            now = time.perf_counter()
            lag.append(now - last)
            last = now

    sem = asyncio.Semaphore(concurrency)

    async def guarded(data: bytes):
        async with sem:
            if offload:
                return await jsonparse.decode_bytes_async(data)
            return jsonparse.decode_bytes(data)

    start = time.perf_counter()
    tick = asyncio.create_task(ticker())
    await asyncio.gather(*(guarded(p) for p in payloads))
    stop = True
    await tick
    wall = time.perf_counter() - start
    lag.sort()
    p50 = lag[len(lag) // 2] * 1000.0
    mx = lag[-1] * 1000.0
    return wall, p50, mx


async def _loop_bench() -> None:
    payloads = [_logs_payload(n) for n in ([150] * 180 + [800] * 20)]
    total = sum(len(p) for p in payloads)
    print(
        f"\nloop-stall: {len(payloads)} responses, {total / 1e6:.1f} MB total, "
        f"JSON_DECODE_THREADS={JSON_DECODE_THREADS}"
    )
    wall, p50, mx = await _loop_stall(offload=False, payloads=payloads)
    print(f"  inline   wall {wall * 1000:7.1f} ms  tick p50 {p50:7.2f} ms  max {mx:7.2f} ms")
    wall, p50, mx = await _loop_stall(offload=True, payloads=payloads)
    print(f"  offload  wall {wall * 1000:7.1f} ms  tick p50 {p50:7.2f} ms  max {mx:7.2f} ms")


def main() -> int:
    data = _payload()
    print(f"payload: {len(data):,} bytes, {REPEATS} repeats")

    stdlib_ms = _time(json.loads, data)
    print(f"json.loads        median {stdlib_ms:.3f} ms")

    try:
        import msgspec
    except ImportError:
        print("msgspec not installed — fast path inactive (optional extra 'perf')")
    else:
        msgspec_ms = _time(msgspec.json.decode, data)
        print(f"msgspec.json.decode median {msgspec_ms:.3f} ms")
        print(f"speedup (stdlib/msgspec): {stdlib_ms / msgspec_ms:.2f}x")

    asyncio.run(_loop_bench())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
