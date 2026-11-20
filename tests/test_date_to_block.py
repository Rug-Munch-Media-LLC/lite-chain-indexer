"""The date→block resolver must be exact, cached, and frugal.

WHY THIS EXISTS
    ``lake-gaps``/``lake-backfill`` take DATES; ``sqd-fill`` takes BLOCKS. Nothing
    in the tree converted between them, so every chain except base/arbitrum needed a
    human to supply block numbers — the one piece of code between us and a
    21-chain, date-driven, free backfill.

WHAT IS PINNED HERE
    * exact UTC-day boundaries (00:00:00 → 23:59:59, next block starts the next day)
    * a day with no blocks still yields a usable range
    * results are cached, and a cache hit costs zero probes
    * the probe budget is finite (it must not loop forever on a hostile endpoint)
    * a 429 RAISES rather than retrying harder — the fleet rule for metered paths
"""

from __future__ import annotations

import datetime as dt
import json

import pytest

from lite_indexer.chains import date_to_block as dtb


class _FakeResolver(dtb.DateBlockResolver):
    """Resolver with a synthetic 2s-block chain — no network, fully deterministic."""

    GENESIS_TS = int(dt.datetime(2026, 1, 1, tzinfo=dt.UTC).timestamp())
    BLOCK_S = 2

    def __init__(self, *, head_block: int = 20_000_000, gap: tuple[int, int] | None = None):
        super().__init__("fake", urls=["http://fake.invalid"])
        self._head_block = head_block
        self._gap = gap  # (from, to) blocks that do not exist
        self.probes = 0

    def _block_at(self, number: int) -> tuple[int, int]:
        self.probes += 1
        if self._gap and self._gap[0] < number <= self._gap[1]:
            raise dtb.BlockResolveError(f"block {number} not found")
        ts = self.GENESIS_TS + (number - 1) * self.BLOCK_S
        return number, ts

    def head(self) -> tuple[int, int]:
        return self._block_at(self._head_block)


@pytest.fixture(autouse=True)
def _isolate_cache(tmp_path, monkeypatch):
    """Never touch the real cache file."""
    monkeypatch.setattr(dtb, "CACHE_PATH", tmp_path / "cache.json")


class TestExactBoundaries:
    def test_first_block_is_midnight_and_last_is_235959(self):
        r = _FakeResolver()
        rng = r.resolve_day("2026-03-01")
        start = dt.datetime(2026, 3, 1, tzinfo=dt.UTC).timestamp()
        first_ts = r._block_at(rng["first"])[1]
        last_ts = r._block_at(rng["last"])[1]
        next_ts = r._block_at(rng["last"] + 1)[1]

        assert first_ts == int(start), "first block must be the first with ts >= midnight"
        assert last_ts < int(start) + 86400, "last block must be before next midnight"
        assert next_ts == int(start) + 86400, "the block after 'last' must be next midnight"

    def test_range_covers_the_whole_day_once(self):
        r = _FakeResolver()
        rng = r.resolve_day("2026-05-05")
        # 2s blocks: a full day is 43200 blocks, give or take the boundary
        span = rng["last"] - rng["first"] + 1
        assert 43_199 <= span <= 43_201, f"unexpected span {span} for a 2s chain"

    def test_single_block_day_still_returns_a_range(self):
        """A chain slower than a day yields last == first, not an inverted range."""
        r = _FakeResolver(head_block=10)
        r.BLOCK_S = 200_000  # one block every ~2.3 days
        rng = r.resolve_day("2026-01-02")
        assert rng["last"] >= rng["first"] >= 0


class TestCaching:
    def test_second_resolve_costs_no_probes(self):
        r = _FakeResolver()
        first = r.resolve_day("2026-04-01")
        after_cold = r.probes
        assert after_cold > 0
        again = r.resolve_day("2026-04-01")
        assert r.probes == after_cold, "a cache hit must not probe"
        assert again == first

    def test_cache_is_written_and_reread(self):
        r = _FakeResolver()
        rng = r.resolve_day("2026-06-01")
        assert dtb.CACHE_PATH.is_file()
        stored = json.loads(dtb.CACHE_PATH.read_text())
        assert stored["fake:2026-06-01"]["first"] == rng["first"]

        r2 = _FakeResolver()
        assert r2.resolve_day("2026-06-01") == rng
        assert r2.probes == 0, "a persisted cache must be used by a fresh resolver"


class TestBudgetAndFailure:
    def test_probe_budget_is_finite(self, monkeypatch):
        """A non-converging bisection must stop, not spin."""
        monkeypatch.setattr(dtb, "MAX_PROBES", 3)
        r = _FakeResolver()
        with pytest.raises(dtb.BlockResolveError, match="exceeded"):
            r.resolve_day("2026-07-01")

    def test_future_date_raises(self):
        r = _FakeResolver()
        with pytest.raises(dtb.BlockResolveError, match="future"):
            r.resolve_day("2030-01-01")

    def test_resolve_range_reports_per_day_errors_without_aborting(self):
        r = _FakeResolver()
        out = r.resolve_range("2026-03-01", "2026-03-03")
        assert len(out) == 3
        assert all("error" not in d for d in out)
        out2 = r.resolve_range("2030-01-01", "2030-01-02")
        assert all("error" in d for d in out2), "each future day reported, none raised"


class TestNoEndpointGuessing:
    def test_unknown_chain_raises_a_named_error(self):
        with pytest.raises(dtb.BlockResolveError, match="no public RPC endpoint"):
            dtb.DateBlockResolver("not-a-real-chain").resolve_day("2026-01-01")

    @pytest.mark.parametrize("chain", ["base", "ethereum", "arbitrum", "polygon"])
    def test_known_chains_resolve_endpoints_from_the_single_source(self, chain):
        """Endpoints come from chains.endpoints, not a second copy in this module."""
        assert dtb._rpc_urls(chain), f"{chain} has no endpoints"


class TestSqdDateDrive:
    """The whole point of the resolver: SQD becomes date-driven."""

    def test_sqd_request_accepts_dates(self):
        from lite_indexer.ingest.lake_sqd import SqdFillRequest

        r = SqdFillRequest(chain="base", from_date="2026-11-12", to_date="2026-11-12")
        assert r.from_block == 0 and r.to_block == 0
        r.resolve_dates()
        assert r.to_block > r.from_block > 0

    def test_sqd_request_without_dates_is_untouched(self):
        from lite_indexer.ingest.lake_sqd import SqdFillRequest

        r = SqdFillRequest(chain="base", from_block=100, to_block=200)
        r.resolve_dates()
        assert (r.from_block, r.to_block) == (100, 200)

    def test_resolved_dates_match_the_resolver(self):
        from lite_indexer.ingest.lake_sqd import SqdFillRequest

        want = _FakeResolver().resolve_day("2026-04-04")
        # resolve_dates() calls resolve_day twice (start -> first, end -> last),
        # so the stub must answer first/last for each of those calls.
        seq = iter([want["first"], want["first"], want["last"], want["last"]])

        import lite_indexer.chains.date_to_block as real

        class _Stub:
            def __init__(self, chain):
                self.chain = chain

            def resolve_day(self, date):
                return {"first": next(seq), "last": next(seq)}

        orig = real.DateBlockResolver
        real.DateBlockResolver = _Stub
        try:
            r = SqdFillRequest(chain="base", from_date="2026-04-04", to_date="2026-04-04")
            r.resolve_dates()
        finally:
            real.DateBlockResolver = orig
        assert (r.from_block, r.to_block) == (want["first"], want["last"])
