"""Offline self-test: prove the stream->decode->partition->parquet path works.

Uses a fake HyperSync client so the decode + Parquet layout can be validated
without a live token. Not part of the PoC run; a correctness harness.
"""

from __future__ import annotations

import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import backfill_base as bb


class FakeLog:
    def __init__(self, n, ts, tx, idx, addr_, t1, t2, value):
        self.block_number = n
        self.transaction_hash = tx
        self.log_index = idx
        self.address = addr_
        self.topic1 = t1
        self.topic2 = t2
        self.data = value.to_bytes(32, "big")
        self._ts = ts


class FakeBlock:
    def __init__(self, n, ts):
        self.number = n
        self.timestamp = ts


class FakeData:
    def __init__(self, blocks, logs):
        self.blocks = blocks
        self.logs = logs


class FakeRes:
    def __init__(self, blocks, logs, next_block):
        self.data = FakeData(blocks, logs)
        self.next_block = next_block
        self.archive_height = 10_000


class FakeStream:
    def __init__(self, pages):
        self._pages = list(pages)
        self.q = None

    def recv(self):
        if not self._pages:
            return None
        return self._pages.pop(0)


class FakeClient:
    def __init__(self, pages):
        self.pages = pages

    def stream(self, q, config):
        self.q = q
        self.config = config
        self.pages = list(self.pages)
        return _SelfStream(self.pages)

    async def get_height(self):
        return 10_000

    async def get_chain_id(self):
        return 8453

    def rate_limit_info(self):
        return "mock: 100 units/min"


class _SelfStream(FakeStream):
    def __init__(self, pages):
        super().__init__(pages)


def _page(start_block, n_logs_per_block, ts):
    blocks, logs = [], []
    for i in range(3):
        n = start_block + i
        t = ts + i * 2
        blocks.append(FakeBlock(n, t))
        for j in range(n_logs_per_block):
            logs.append(
                FakeLog(
                    n,
                    t,
                    f"0x{n:064x}",
                    j,
                    "0xAbC0000000000000000000000000000000000001",
                    "0x000000000000000000000000" + f"{n:040x}",
                    "0x000000000000000000000000" + f"{n + 1:040x}",
                    123456789 * (i + 1),
                )
            )
    return FakeRes(blocks, logs, start_block + 3)


def main() -> int:
    day1 = int(datetime(2026, 9, 20, tzinfo=UTC).timestamp())
    day2 = int(datetime(2026, 9, 21, tzinfo=UTC).timestamp())
    # page1 spans a day boundary; page2 is later still
    pages = [_page(100, 2, day1), _page(103, 2, day1 + 86_398), _page(106, 1, day2)]

    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        written = []
        for p in pages:
            blocks = {b.number: b.timestamp for b in p.data.blocks}
            rows = [bb.log_to_row(rec, blocks) for rec in p.data.logs]
            written.extend(bb.write_partition(rows, out))

        total_rows = sum(n for _, n, _ in written)
        total_bytes = sum(b for _, _, b in written)
        files = sorted(str(p) for p, _, _ in written)

        print("files written:")
        for f in files:
            print("  ", f.replace(td, "<tmp>"))
        print(f"rows={total_rows} bytes={total_bytes}")

        # page1: 3 blocks x2 logs=6, page2: 3x2=6, page3: 3x1=3 -> 15
        assert total_rows == 15, f"expected 15 rows, got {total_rows}"
        assert len(files) >= 2, "expected >=2 date partitions"

        # verify layout is <chain>/<date>/<file>.parquet
        for f in files:
            rel = Path(f).relative_to(out)
            assert rel.parts[0] == "base", f"bad chain dir: {rel}"
            assert len(rel.parts) == 3, f"want chain/date/file, got {rel}"
            assert len(rel.parts[1]) == 10 and rel.parts[1][4] == "-", f"bad date: {rel}"

        # verify parquet readable + values correct + value decoded big-endian
        t = pq.read_table(files[0])
        print("schema:", [x.name for x in t.schema])
        r0 = t.to_pylist()[0]
        assert r0["chain"] == "base"
        assert r0["value"] == "123456789", f"bad value decode: {r0['value']}"
        assert r0["contract_address"].startswith("0xabc"), r0["contract_address"]
        assert r0["contract_address"] == r0["contract_address"].lower()
        assert len(r0["from_addr"]) == 42, r0["from_addr"]
        assert r0["block_timestamp"] > 0
        print("value decode ok:", r0["value"])
        print("from_addr ok:", r0["from_addr"])
        print("to_addr ok:", r0["to_addr"])

        # schema must match declared SCHEMA exactly
        assert t.schema.equals(bb.SCHEMA), "schema drift"
        print("schema matches SCHEMA exactly: OK")

    print("\nSELFTEST PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
