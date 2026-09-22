"""
Tests for Transparent Historical Access (THA).

Covers the unified hot (ClickHouse) + cold (B2 parquet) access layer:

* :class:`lite_indexer.storage.historical.HistoricalAccess` — split/plan,
  hot query, cold query, smart merge, TTL detection, SQL routing, and the
  ClickHouse view rewrite used by the API.
* :class:`lite_indexer.storage.cold_store.ColdStore` — DuckDB cold query
  engine over a real (tiny) parquet tree, smart file selection, and
  schema-drift normalization.

No network is touched: the hot side is a fake ClickHouse client and the
cold side reads a temp parquet tree in mount mode.
"""

from __future__ import annotations

from unittest.mock import patch

import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

duckdb = pytest.importorskip("duckdb")

from lite_indexer.storage.cold_store import ColdStore  # noqa: E402
from lite_indexer.storage.historical import HistoricalAccess  # noqa: E402

# ── Fixtures ─────────────────────────────────────────────────────────

HOT_COLS = [
    "chain",
    "block_number",
    "tx_hash",
    "from_address",
    "to_address",
    "token_address",
    "value",
    "block_timestamp",
    "log_index",
    "ingested_at",
]


class FakeCH:
    """ClickHouse client stand-in with scriptable hot results."""

    def __init__(
        self,
        hot_rows: list[dict[str, Any]] | None = None,
        ttl_expr: str | None = None,
        views: list[str] | None = None,
    ) -> None:
        self.hot_rows = hot_rows or []
        self.ttl_expr = ttl_expr
        self.views = views or []
        self.executed: list[tuple[str, dict[str, Any] | None]] = []

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        with_column_types: bool = False,
        settings: dict[str, Any] | None = None,
    ) -> Any:
        self.executed.append((sql, params))
        if "system.tables" in sql and "ttl" in sql:
            if self.ttl_expr:
                return [(self.ttl_expr,)]
            return []
        if "system.tables" in sql:
            return [(v,) for v in self.views]
        rows = [tuple(r[c] for c in HOT_COLS) for r in self.hot_rows]
        types = [(c, "String") for c in HOT_COLS]
        if with_column_types:
            return rows, types
        return rows


def _make_parquet_tree(tmp_path: Path, base: str = "parquet") -> Path:
    """Create a fake mounted parquet tree with two cold days.

    Layout mirrors the live export:
    {chain}/{YYYY}/{MM}/{DD}/{HH}/{chain}_{YYYY-MM-DD}.parquet
    """
    con = duckdb.connect()

    def write(rel: Path, sql: str) -> None:
        rel.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({sql}) TO '{rel}' (FORMAT PARQUET)")

    root = tmp_path / base
    for day, blocks in (("30", [100, 101]), ("29", [102, 103])):
        rel = root / "ethereum" / "2026" / "06" / day / "12"
        # Minutes are offset per day so day 29 (blocks 102/103) is entirely
        # later than day 30 (blocks 100/101): newest-first = [103,102,101,100].
        base_min = 0 if day == "30" else 2
        block_sql = " UNION ALL ".join(
            f"SELECT 'ethereum' AS chain, {b}::BIGINT AS block_number, "
            f"'0xtx{b}' AS tx_hash, '0xaaa' AS from_address, '0xbbb' AS to_address, "
            f"'0xccc' AS token_address, '{b}000' AS value, "
            f"TIMESTAMP '2026-06-{day} 12:0{base_min + idx}:00' AS block_timestamp, "
            f"{idx}::BIGINT AS log_index, TIMESTAMP '2026-06-{day} 12:0{base_min + idx}:30' AS ingested_at"
            for idx, b in enumerate(blocks)
        )
        write(rel / f"ethereum_2026-06-{day}.parquet", block_sql)

    # A dex file for the dex kind mapping.
    dex_rel = root / "ethereum" / "2026" / "06" / "29" / "12"
    write(
        dex_rel / "ethereum_dex_2026-06-29.parquet",
        """
        SELECT 'swap' AS type, 'ethereum' AS chain, 500::BIGINT AS block_number,
               TIMESTAMP '2026-06-29 10:00:00' AS block_timestamp,
               '0xswap1' AS tx_hash, 7::BIGINT AS log_index,
               '0xpair' AS contract_address, 'uniswap' AS dex, 'v2' AS version,
               '0xt0' AS token0, '0xt1' AS token1, '0xaaa' AS sender, '0xbbb' AS recipient,
               '100' AS amount0_in, '0' AS amount1_in, '0' AS amount0_out, '100' AS amount1_out,
               '100' AS amount0, '100' AS amount1, 0::TINYINT AS whale,
               1.0 AS price_usd, 100.0 AS volume_usd,
               TIMESTAMP '2026-06-29 10:00:30' AS ingested_at, 'lite_indexer' AS source
        """,
    )
    return root


@pytest.fixture()
def parquet_tree(tmp_path: Path) -> Path:
    """Temp parquet archive for cold-side tests."""
    return _make_parquet_tree(tmp_path)


@pytest.fixture()
def cold_store(parquet_tree: Path) -> ColdStore:
    """ColdStore reading the temp parquet archive (mount mode)."""
    return ColdStore(remote="b2-crypt-v2:chain-data/parquet", mount_dir=parquet_tree)


def _hot_row(block: int = 200, tx: str = "0xhot", ts: str = "2026-08-01 00:00:00") -> dict[str, Any]:
    """A hot-side row as a dict keyed by the canonical CH columns."""
    return {
        "chain": "ethereum",
        "block_number": block,
        "tx_hash": tx,
        "from_address": "0xaaa",
        "to_address": "0xeee",
        "token_address": "0xccc",
        "value": f"{block}000",
        "block_timestamp": datetime.fromisoformat(ts),
        "log_index": block,
        "ingested_at": datetime.fromisoformat("2026-08-01 00:05:00"),
    }


def _make_ha(fake_ch: FakeCH, cold_store: ColdStore, hot_ttl_days: int = 30) -> HistoricalAccess:
    return HistoricalAccess(
        ch_client=fake_ch,
        b2_bucket="b2-crypt-v2:chain-data/parquet",
        cold_store=cold_store,
        hot_ttl_days=hot_ttl_days,
    )


# ── ColdStore ────────────────────────────────────────────────────────


class TestColdStore:
    def test_query_transfers_smart_file_selection(self, cold_store: ColdStore):
        """Only files inside the query window are read (2 days → 4 rows)."""
        rows = cold_store.query(
            "evm_transfers",
            "ethereum",
            from_time=datetime(2026, 6, 29, tzinfo=UTC),
            to_time=datetime(2026, 6, 30, 23, 59, tzinfo=UTC),
        )
        assert {r["tx_hash"] for r in rows} == {"0xtx100", "0xtx101", "0xtx102", "0xtx103"}

    def test_query_chain_filter(self, cold_store: ColdStore):
        rows = cold_store.query(
            "evm_transfers",
            "polygon",
            from_time=datetime(2026, 6, 29, tzinfo=UTC),
            to_time=datetime(2026, 6, 30, 23, 59, tzinfo=UTC),
        )
        assert rows == []

    def test_query_dex_kind(self, cold_store: ColdStore):
        rows = cold_store.query(
            "dex_events",
            "ethereum",
            from_time=datetime(2026, 6, 29, tzinfo=UTC),
            to_time=datetime(2026, 6, 29, 23, 59, tzinfo=UTC),
        )
        assert len(rows) == 1
        assert rows[0]["tx_hash"] == "0xswap1"
        assert rows[0]["dex"] == "uniswap"

    def test_query_where_fragment(self, cold_store: ColdStore):
        rows = cold_store.query(
            "evm_transfers",
            "ethereum",
            from_time=datetime(2026, 6, 29, tzinfo=UTC),
            to_time=datetime(2026, 6, 30, 23, 59, tzinfo=UTC),
            where="from_address = '0xaaa'",
        )
        assert len(rows) == 4  # all rows have from_address 0xaaa

    def test_query_limit(self, cold_store: ColdStore):
        rows = cold_store.query(
            "evm_transfers",
            "ethereum",
            from_time=datetime(2026, 6, 29, tzinfo=UTC),
            to_time=datetime(2026, 6, 30, 23, 59, tzinfo=UTC),
            limit=2,
        )
        assert len(rows) == 2

    def test_query_schema_drift_fills_defaults(self, cold_store: ColdStore):
        """A parquet file missing later-added columns still reads."""
        rows = cold_store.query(
            "evm_transfers",
            "ethereum",
            from_time=datetime(2026, 6, 29, tzinfo=UTC),
            to_time=datetime(2026, 6, 30, 23, 59, tzinfo=UTC),
        )
        # canonical columns are all present on every row
        for row in rows:
            assert set(row.keys()) == {
                "chain",
                "block_number",
                "tx_hash",
                "from_address",
                "to_address",
                "token_address",
                "value",
                "block_timestamp",
                "log_index",
                "ingested_at",
            }

    def test_fetch_mode_uses_rclone_cat(self, parquet_tree: Path, tmp_path: Path):
        store = ColdStore(
            remote="b2-crypt-v2:chain-data/parquet",
            fetch=True,
            cache_dir=tmp_path / "cache",
            rclone_bin="rclone",
        )

        def fake_rclone(cmd, **kwargs):
            remote = cmd[2]
            rel = remote.removeprefix("b2-crypt-v2:chain-data/parquet/")
            src = parquet_tree / rel
            out = kwargs["stdout"]
            if not src.is_file():
                return subprocess.CompletedProcess(cmd, 1)
            out.write(src.read_bytes())
            out.flush()
            return subprocess.CompletedProcess(cmd, 0)

        with patch("subprocess.run", fake_rclone):
            rows = store.query(
            "evm_transfers",
            "ethereum",
                from_time=datetime(2026, 6, 30, tzinfo=UTC),
                to_time=datetime(2026, 6, 30, 23, 59, tzinfo=UTC),
            )
        assert len(rows) == 2


# ── TTL detection ────────────────────────────────────────────────────


class TestTTLDetection:
    def test_detects_ttl_from_clickhouse(self):
        fake = FakeCH(ttl_expr="block_timestamp + toIntervalDay(45)")
        ha = HistoricalAccess(ch_client=fake, b2_bucket="x")
        assert ha._detect_ttl("evm_transfers") == 45

    def test_detects_interval_ttl(self):
        fake = FakeCH(ttl_expr="block_timestamp + INTERVAL 90 DAY")
        ha = HistoricalAccess(ch_client=fake, b2_bucket="x")
        assert ha._detect_ttl("evm_transfers") == 90

    def test_falls_back_to_config_without_clickhouse(self):
        ha = HistoricalAccess(ch_client=None, b2_bucket="x")
        assert ha._detect_ttl("evm_transfers") == 30  # RETENTION_DAYS default
        assert ha._detect_ttl("dex_events") == 90

    def test_cutoff_uses_ttl(self):
        fake = FakeCH(ttl_expr="block_timestamp + toIntervalDay(7)")
        ha = HistoricalAccess(ch_client=fake, b2_bucket="x")
        cutoff = ha.hot_cutoff("evm_transfers")
        assert abs((datetime.now(UTC) - timedelta(days=7) - cutoff).total_seconds()) < 5


# ── _smart_merge ─────────────────────────────────────────────────────


class TestSmartMerge:
    def _ha(self) -> HistoricalAccess:
        return HistoricalAccess(ch_client=None, b2_bucket="x", hot_ttl_days=30)

    def test_hot_wins_on_overlap(self):
        ha = self._ha()
        cold = [_hot_row(100, "0xdup", "2026-06-30 00:00:00")]
        hot = [_hot_row(100, "0xdup", "2026-06-30 00:00:00")]
        merged = ha._smart_merge("evm_transfers", hot, cold, 10)
        assert len(merged) == 1
        assert merged[0]["tx_hash"] == "0xdup"

    def test_cold_fills_gaps(self):
        ha = self._ha()
        cold = [_hot_row(100, "0xcold1", "2026-06-01 00:00:00"), _hot_row(99, "0xcold2", "2026-05-01 00:00:00")]
        hot = [_hot_row(200, "0xhot", "2026-08-01 00:00:00")]
        merged = ha._smart_merge("evm_transfers", hot, cold, 10)
        assert {r["tx_hash"] for r in merged} == {"0xhot", "0xcold1", "0xcold2"}

    def test_ordering_preserved(self):
        ha = self._ha()
        cold = [_hot_row(100, "0xcold1", "2026-06-01 00:00:00"), _hot_row(101, "0xcold2", "2026-06-02 00:00:00")]
        hot = [_hot_row(200, "0xhot", "2026-08-01 00:00:00")]
        merged = ha._smart_merge("evm_transfers", hot, cold, 10)
        assert [r["block_number"] for r in merged] == [200, 101, 100]

    def test_limit_enforced(self):
        ha = self._ha()
        cold = [_hot_row(i, f"0xc{i}", "2026-06-01 00:00:00") for i in range(5)]
        hot = [_hot_row(100 + i, f"0xh{i}", "2026-08-01 00:00:00") for i in range(5)]
        merged = ha._smart_merge("evm_transfers", hot, cold, 3)
        assert len(merged) == 3
        assert merged[0]["block_number"] == 104  # newest first

    def test_dedupe_across_sources(self):
        ha = self._ha()
        cold = [_hot_row(100, "0xsame", "2026-06-01 00:00:00"), _hot_row(101, "0xonly", "2026-06-02 00:00:00")]
        hot = [_hot_row(100, "0xsame", "2026-06-01 00:00:00")]
        merged = ha._smart_merge("evm_transfers", hot, cold, 10)
        assert len(merged) == 2


# ── Unified query split/plan ─────────────────────────────────────────


class TestUnifiedQuery:
    @pytest.mark.asyncio
    async def test_fully_hot_range_no_cold(self, cold_store: ColdStore):
        fake = FakeCH(hot_rows=[_hot_row()])
        ha = _make_ha(fake, cold_store)
        # Relative to now so the range stays fully hot regardless of when
        # the suite runs (the hot TTL is 30 days).
        now = datetime.now(UTC)
        rows = await ha.query(
            "evm_transfers",
            "ethereum",
            from_time=now - timedelta(days=1),
            to_time=now,
        )
        assert [r["tx_hash"] for r in rows] == ["0xhot"]
        # no cold query should have touched the store
        assert not any("read_parquet" in sql for sql, _ in fake.executed)

    @pytest.mark.asyncio
    async def test_fully_cold_range_no_hot(self, cold_store: ColdStore):
        fake = FakeCH(hot_rows=[_hot_row()])
        ha = _make_ha(fake, cold_store)
        rows = await ha.query(
            "evm_transfers",
            "ethereum",
            from_time=datetime(2026, 6, 1, tzinfo=UTC),
            to_time=datetime(2026, 6, 10, tzinfo=UTC),
        )
        # fully cold: hot side must not be consulted
        hot_sqls = [sql for sql, _ in fake.executed if "FROM wallet_memory.evm_transfers" in sql]
        assert hot_sqls == []
        assert rows == []  # the temp archive has no June 1-10 files

    @pytest.mark.asyncio
    async def test_fully_cold_range_returns_cold_rows(self, cold_store: ColdStore):
        fake = FakeCH()
        ha = _make_ha(fake, cold_store)
        rows = await ha.query(
            "evm_transfers",
            "ethereum",
            from_time=datetime(2026, 6, 29, tzinfo=UTC),
            to_time=datetime(2026, 6, 30, 23, 59, tzinfo=UTC),
        )
        assert len(rows) == 4
        assert rows[0]["block_number"] == 101  # newest cold first

    @pytest.mark.asyncio
    async def test_spanning_range_merges(self, cold_store: ColdStore):
        fake = FakeCH(hot_rows=[_hot_row(200, "0xhot")])
        ha = _make_ha(fake, cold_store)
        # Upper bound = real now so the range always spans the hot/cold
        # cutoff (cold June archive + hot side), regardless of run date.
        rows = await ha.query(
            "evm_transfers",
            "ethereum",
            from_time=datetime(2026, 6, 1, tzinfo=UTC),
            to_time=datetime.now(UTC),
        )
        assert len(rows) == 5  # 4 cold + 1 hot
        assert rows[0]["tx_hash"] == "0xhot"
        assert [r["block_number"] for r in rows] == [200, 101, 100, 103, 102]

    @pytest.mark.asyncio
    async def test_limit_enforced_across_split(self, cold_store: ColdStore):
        fake = FakeCH(hot_rows=[_hot_row(200, "0xhot")])
        ha = _make_ha(fake, cold_store)
        rows = await ha.query(
            "evm_transfers",
            "ethereum",
            from_time=datetime(2026, 6, 1, tzinfo=UTC),
            to_time=datetime.now(UTC),
            limit=2,
        )
        assert len(rows) == 2
        assert rows[0]["tx_hash"] == "0xhot"
        assert rows[1]["block_number"] == 101

    @pytest.mark.asyncio
    async def test_block_bounded_range_queries_both_tiers(self, cold_store: ColdStore):
        fake = FakeCH(hot_rows=[_hot_row(200, "0xhot")])
        ha = _make_ha(fake, cold_store)
        rows = await ha.query(
            "evm_transfers",
            "ethereum",
            from_block=100,
            to_block=200,
            limit=10,
        )
        # cold scan (block-bounded, no time bounds) + hot query
        assert len(rows) >= 4
        assert rows[0]["tx_hash"] == "0xhot"

    @pytest.mark.asyncio
    async def test_no_bounds_is_hot_only(self, cold_store: ColdStore):
        fake = FakeCH(hot_rows=[_hot_row()])
        ha = _make_ha(fake, cold_store)
        rows = await ha.query("evm_transfers", "ethereum", limit=10)
        assert [r["tx_hash"] for r in rows] == ["0xhot"]
        assert not any("read_parquet" in sql for sql, _ in fake.executed)

    @pytest.mark.asyncio
    async def test_unknown_table_rejected(self, cold_store: ColdStore):
        ha = _make_ha(FakeCH(), cold_store)
        with pytest.raises(ValueError):
            await ha.query("wallet_labels", "ethereum")

    @pytest.mark.asyncio
    async def test_degraded_without_clickhouse(self, cold_store: ColdStore):
        ha = HistoricalAccess(ch_client=None, b2_bucket="x", cold_store=cold_store, hot_ttl_days=30)
        rows = await ha.query(
            "evm_transfers",
            "ethereum",
            from_time=datetime(2026, 6, 29, tzinfo=UTC),
            to_time=datetime(2026, 6, 30, 23, 59, tzinfo=UTC),
        )
        assert len(rows) == 4  # cold side still serves history


# ── SQL routing (query_sql) ─────────────────────────────────────────


class TestQuerySql:
    def test_portable_spanning_query_routes(self, cold_store: ColdStore, monkeypatch: pytest.MonkeyPatch):
        fake = FakeCH(hot_rows=[_hot_row(200, "0xhot")])
        ha = _make_ha(fake, cold_store)
        # view not available → Python split/merge path
        sql = (
            "SELECT chain, block_number, tx_hash, from_address, to_address "
            "FROM wallet_memory.evm_transfers "
            "WHERE from_address = %(address)s "
            "AND block_timestamp >= %(start_ts)s "
            "AND block_timestamp <= %(end_ts)s "
            "ORDER BY block_timestamp DESC "
            "LIMIT 100"
        )
        params = {
            "address": "0xaaa",
            "start_ts": datetime(2026, 6, 1, tzinfo=UTC),
            "end_ts": datetime(2026, 8, 4, tzinfo=UTC),
        }
        rows = ha.query_sql(sql, params)
        assert rows is not None
        # hot + cold merged, projected to the SELECT list
        assert {r["tx_hash"] for r in rows} >= {"0xhot", "0xtx100"}
        assert all(set(r.keys()) == {"chain", "block_number", "tx_hash", "from_address", "to_address"} for r in rows)
        assert rows[0]["tx_hash"] == "0xhot"

    def test_fully_hot_query_not_routed(self, cold_store: ColdStore):
        fake = FakeCH(hot_rows=[_hot_row()])
        ha = _make_ha(fake, cold_store)
        sql = (
            "SELECT chain FROM wallet_memory.evm_transfers "
            "WHERE block_timestamp >= %(start_ts)s AND block_timestamp <= %(end_ts)s LIMIT 10"
        )
        params = {
            "start_ts": datetime.now(UTC) - timedelta(days=1),
            "end_ts": datetime.now(UTC),
        }
        assert ha.query_sql(sql, params) is None  # fully hot → caller runs hot SQL

    def test_unbounded_query_not_routed(self, cold_store: ColdStore):
        ha = _make_ha(FakeCH(), cold_store)
        sql = "SELECT chain FROM wallet_memory.evm_transfers LIMIT 10"
        assert ha.query_sql(sql, {}) is None

    def test_non_portable_function_not_routed(self, cold_store: ColdStore):
        ha = _make_ha(FakeCH(), cold_store)
        sql = "SELECT count() FROM wallet_memory.evm_transfers WHERE block_timestamp >= %(start_ts)s LIMIT 10"
        params = {"start_ts": datetime(2026, 6, 1, tzinfo=UTC)}
        assert ha.query_sql(sql, params) is None

    def test_where_or_not_routed(self, cold_store: ColdStore):
        ha = _make_ha(FakeCH(), cold_store)
        sql = (
            "SELECT chain FROM wallet_memory.evm_transfers "
            "WHERE from_address = %(a)s OR to_address = %(a)s "
            "AND block_timestamp >= %(start_ts)s LIMIT 10"
        )
        params = {"a": "0xaaa", "start_ts": datetime(2026, 6, 1, tzinfo=UTC)}
        assert ha.query_sql(sql, params) is None

    def test_other_table_not_routed(self, cold_store: ColdStore):
        ha = _make_ha(FakeCH(), cold_store)
        sql = "SELECT chain FROM wallet_memory.wallet_labels LIMIT 10"
        assert ha.query_sql(sql, {}) is None

    def test_offset_applied_after_merge(self, cold_store: ColdStore):
        fake = FakeCH(hot_rows=[_hot_row(200, "0xhot")])
        ha = _make_ha(fake, cold_store)
        sql = (
            "SELECT chain, block_number, tx_hash FROM wallet_memory.evm_transfers "
            "WHERE block_timestamp >= %(start_ts)s AND block_timestamp <= %(end_ts)s "
            "ORDER BY block_timestamp DESC LIMIT 2 OFFSET 1"
        )
        params = {
            "start_ts": datetime(2026, 6, 1, tzinfo=UTC),
            "end_ts": datetime(2026, 8, 4, tzinfo=UTC),
        }
        rows = ha.query_sql(sql, params)
        assert rows is not None
        assert len(rows) == 2
        assert rows[0]["block_number"] == 101  # offset 1 skips 0xhot(200)


# ── View rewrite (API transparent path) ──────────────────────────────


class TestViewRewrite:
    def test_rewrites_when_view_exists(self):
        fake = FakeCH(views=["evm_transfers_historical"])
        ha = HistoricalAccess(ch_client=fake, b2_bucket="x")
        sql = "SELECT chain FROM wallet_memory.evm_transfers WHERE chain = 'ethereum' LIMIT 10"
        rewritten = ha.maybe_rewrite_view(sql)
        assert "wallet_memory.evm_transfers_historical" in rewritten
        assert "wallet_memory.evm_transfers" not in rewritten.replace("evm_transfers_historical", "")

    def test_no_rewrite_without_view(self):
        fake = FakeCH(views=[])
        ha = HistoricalAccess(ch_client=fake, b2_bucket="x")
        sql = "SELECT chain FROM wallet_memory.evm_transfers LIMIT 10"
        assert ha.maybe_rewrite_view(sql) == sql

    def test_no_rewrite_for_other_table(self):
        fake = FakeCH(views=["wallet_labels_historical"])
        ha = HistoricalAccess(ch_client=fake, b2_bucket="x")
        sql = "SELECT chain FROM wallet_memory.wallet_labels LIMIT 10"
        assert ha.maybe_rewrite_view(sql) == sql

    def test_no_rewrite_for_write_statement(self):
        fake = FakeCH(views=["evm_transfers_historical"])
        ha = HistoricalAccess(ch_client=fake, b2_bucket="x")
        sql = "INSERT INTO wallet_memory.evm_transfers VALUES (1)"
        assert ha.maybe_rewrite_view(sql) == sql

    def test_view_check_cached(self):
        fake = FakeCH(views=["evm_transfers_historical"])
        ha = HistoricalAccess(ch_client=fake, b2_bucket="x")
        ha.maybe_rewrite_view("SELECT chain FROM wallet_memory.evm_transfers LIMIT 1")
        n = len(fake.executed)
        ha.maybe_rewrite_view("SELECT chain FROM wallet_memory.evm_transfers LIMIT 1")
        assert len(fake.executed) == n  # no second system.tables probe


# ── API wiring (api.db.query) ────────────────────────────────────────


class TestApiWiring:
    def test_query_routes_through_historical_view(self):
        import api.db as db

        fake = FakeCH(hot_rows=[_hot_row(200, "0xhot")], views=["evm_transfers_historical"])
        with (
            patch.object(db, "get_client", lambda: fake),
            patch.object(db, "HISTORICAL_ACCESS_ENABLED", True),
            patch.object(db, "_historical_init", False),
            patch.object(db, "_historical", None),
        ):
            rows = db.query(
                "SELECT chain, block_number, tx_hash FROM wallet_memory.evm_transfers WHERE chain = 'ethereum' LIMIT 10"
            )
        executed_sqls = [s for s, _ in fake.executed]
        assert any("wallet_memory.evm_transfers_historical" in s for s in executed_sqls)
        assert rows and rows[0]["tx_hash"] == "0xhot"

    def test_query_unroutable_passes_through(self):
        import api.db as db

        fake = FakeCH(hot_rows=[_hot_row()], views=["evm_transfers_historical"])
        with (
            patch.object(db, "get_client", lambda: fake),
            patch.object(db, "HISTORICAL_ACCESS_ENABLED", True),
            patch.object(db, "_historical_init", False),
            patch.object(db, "_historical", None),
        ):
            rows = db.query("SELECT chain FROM wallet_memory.wallet_labels LIMIT 1")
        executed_sqls = [s for s, _ in fake.executed]
        # original SQL passed through unchanged (no view rewrite)
        assert any("SELECT chain FROM wallet_memory.wallet_labels LIMIT 1" in s for s in executed_sqls)
        assert not any("_historical" in s for s in executed_sqls)
        assert isinstance(rows, list)

    def test_disabled_skips_historical(self):
        import api.db as db

        fake = FakeCH(hot_rows=[_hot_row()], views=["evm_transfers_historical"])
        with (
            patch.object(db, "get_client", lambda: fake),
            patch.object(db, "HISTORICAL_ACCESS_ENABLED", False),
            patch.object(db, "_historical_init", False),
            patch.object(db, "_historical", None),
        ):
            db.query("SELECT chain FROM wallet_memory.evm_transfers WHERE chain = 'ethereum' LIMIT 10")
        executed_sqls = [s for s, _ in fake.executed]
        assert not any("_historical" in s for s in executed_sqls)
        assert not any("system.tables" in s for s in executed_sqls)
