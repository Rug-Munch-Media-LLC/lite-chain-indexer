#!/usr/bin/env python3
"""Micro-benchmark: stdlib json.loads vs optional msgspec.json.decode.

Measures the hot RPC response decode on a representative 1000-log
``eth_getLogs`` payload. Run::

    python3 scripts/bench_json_parse.py

Prints the per-decoder median (and best) time plus the stdlib/msgspec ratio.
If msgspec is not installed the script reports that and exits 0.
"""

from __future__ import annotations

import json
import statistics
import time

REPEATS = 300
WARMUP = 30


def _payload(logs: int = 1000) -> bytes:
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


def main() -> int:
    data = _payload()
    print(f"payload: {len(data):,} bytes, {REPEATS} repeats")

    stdlib_ms = _time(json.loads, data)
    print(f"json.loads        median {stdlib_ms:.3f} ms")

    try:
        import msgspec
    except ImportError:
        print("msgspec not installed — fast path inactive (optional extra 'perf')")
        return 0

    msgspec_ms = _time(msgspec.json.decode, data)
    print(f"msgspec.json.decode median {msgspec_ms:.3f} ms")
    print(f"speedup (stdlib/msgspec): {stdlib_ms / msgspec_ms:.2f}x")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
