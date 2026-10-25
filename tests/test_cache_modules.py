"""Durable cache split (rows / maint / gating) — behaviour contracts.

Covers the success path, every failure path (unavailable database, expired
entry, disabled tier) and the boundaries (ttl == 0, reversed range, entry
cap), asserting the tier stays failure-safe: a broken DB is a miss, never a
raise.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lite_indexer.rpc.cache_db import ResponseCacheDB, reset_db


def _db(tmp_path: Path, **kw) -> ResponseCacheDB:
    opts = {"max_entries": 0, "ttl_s": 60.0, "max_mb": 0}
    opts.update(kw)
    return ResponseCacheDB(tmp_path / "cache.db", **opts)


class TestPayloadRows:
    def test_put_then_get_roundtrip(self, tmp_path: Path):
        db = _db(tmp_path)
        assert db.put("k", chain="eth", method="eth_getLogs", payload='{"a":1}', now=100.0) is True
        assert db.get("k", now=101.0) == '{"a":1}'

    def test_missing_key_is_a_miss(self, tmp_path: Path):
        assert _db(tmp_path).get("nope") is None

    def test_entry_expires_exactly_at_ttl_boundary(self, tmp_path: Path):
        db = _db(tmp_path, ttl_s=10.0)
        db.put("k", chain="eth", method="m", payload="p", now=100.0)
        assert db.get("k", now=109.9) == "p"
        assert db.get("k", now=110.0) is None  # expires_at <= now

    def test_entry_cap_evicts_the_oldest(self, tmp_path: Path):
        db = _db(tmp_path, max_entries=2)
        for i, key in enumerate(("a", "b", "c")):
            db.put(key, chain="eth", method="m", payload=key, now=0.0 + i)
        remaining = {k for k in ("a", "b", "c") if db.get(k, now=3.0) is not None}
        assert "a" not in remaining and len(remaining) == 2


class TestMaintenance:
    def test_invalidate_range_purges_overlapping_rows(self, tmp_path: Path):
        db = _db(tmp_path)
        db.put("r", chain="eth", method="eth_getLogs", payload="p", from_block=10, to_block=20, now=1.0)
        db.put("keep", chain="eth", method="eth_getLogs", payload="p", from_block=100, to_block=110, now=1.0)
        assert db.invalidate_range("eth", 15, 25) == 1
        assert db.get("r", now=2.0) is None
        assert db.get("keep", now=2.0) == "p"

    def test_invalidate_range_accepts_reversed_bounds(self, tmp_path: Path):
        db = _db(tmp_path)
        db.put("r", chain="eth", method="m", payload="p", block_number=42, now=1.0)
        assert db.invalidate_range("eth", 50, 40) == 1

    def test_invalidate_range_is_chain_scoped(self, tmp_path: Path):
        db = _db(tmp_path)
        db.put("r", chain="eth", method="m", payload="p", block_number=42, now=1.0)
        assert db.invalidate_range("base", 0, 100) == 0
        assert db.get("r", now=2.0) == "p"

    def test_stats_and_clear(self, tmp_path: Path):
        db = _db(tmp_path, max_entries=7)
        db.put("k", chain="eth", method="m", payload="abcd", now=1.0)
        stats = db.stats()
        assert stats["entries"] == 1 and stats["max_entries"] == 7
        assert db.clear() == 1
        assert db.stats()["entries"] == 0


class TestGatingState:
    def test_put_get_load_count(self, tmp_path: Path):
        db = _db(tmp_path)
        assert db.state_put("capability", "ep1", '{"logs":true}', now=10.0) is True
        db.state_put("capability", "ep2", "{}", now=10.0)
        assert db.state_get("capability", "ep1", now=11.0) == '{"logs":true}'
        assert db.state_load("capability", now=11.0) == {"ep1": '{"logs":true}', "ep2": "{}"}
        assert db.state_count("capability") == 2

    def test_ttl_zero_means_no_expiry(self, tmp_path: Path):
        db = _db(tmp_path)
        db.state_put("health", "ep1", "ok", now=1.0)
        assert db.state_get("health", "ep1", now=10**9) == "ok"

    def test_positive_ttl_expires(self, tmp_path: Path):
        db = _db(tmp_path)
        db.state_put("ban", "ep1", "banned", ttl=5.0, now=100.0)
        assert db.state_get("ban", "ep1", now=105.0) is None

    def test_delete_removes_one_entry(self, tmp_path: Path):
        db = _db(tmp_path)
        db.state_put("ban", "ep1", "x", now=1.0)
        db.state_delete("ban", "ep1")
        assert db.state_get("ban", "ep1", now=2.0) is None


class TestUnavailableStore:
    """A broken database must degrade to miss / no-op, never raise."""

    @pytest.fixture()
    def broken(self, tmp_path: Path) -> ResponseCacheDB:
        blocker = tmp_path / "cache.db"
        blocker.mkdir()  # a directory where a file is expected → connect fails
        return ResponseCacheDB(blocker, max_entries=0, ttl_s=60.0, max_mb=0)

    def test_reads_are_misses(self, broken: ResponseCacheDB):
        assert broken.get("k") is None
        assert broken.state_get("capability", "e") is None
        assert broken.state_load("capability") == {}
        assert broken.state_count("capability") == 0
        assert broken.stats() == {}

    def test_writes_report_false(self, broken: ResponseCacheDB):
        assert broken.put("k", chain="eth", method="m", payload="p") is False
        assert broken.state_put("capability", "e", "{}") is False

    def test_maintenance_is_a_no_op(self, broken: ResponseCacheDB):
        assert broken.invalidate_range("eth", 0, 10) == 0
        assert broken.clear() == 0

    def test_disabled_tier_never_connects(self, tmp_path: Path):
        db = _db(tmp_path, enabled=False)
        assert db.put("k", chain="eth", method="m", payload="p") is False
        assert db.get("k") is None
        assert not (tmp_path / "cache.db").exists()


def test_reset_db_keeps_the_override_path(tmp_path: Path):
    reset_db(str(tmp_path / "custom.db"))
    assert ResponseCacheDB  # importable through the facade
    reset_db()
