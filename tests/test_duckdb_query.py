"""
Tests for the DuckDB parquet query layer.

Writes tiny parquet fixtures into a temp dir that mirrors the B2 layout,
then exercises ``query_parquet`` against them (mount_dir mode, no network).
"""

from __future__ import annotations

import subprocess
from datetime import date
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")


@pytest.fixture()
def parquet_tree(tmp_path: Path) -> Path:
    """Create a fake mounted parquet tree: ethereum/2026/07/31/12/*.parquet.

    Hour-granular layout mirrors the live export:
    {chain}/{YYYY}/{MM}/{DD}/{HH}/{chain}_{YYYY-MM-DD}.parquet
    """
    con = duckdb.connect()

    def write_parquet(rel: Path, sql: str) -> None:
        rel.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({sql}) TO '{rel}' (FORMAT PARQUET)")

    base = tmp_path / "parquet"
    ethereum_dir = base / "ethereum" / "2026" / "07" / "31" / "12"
    write_parquet(
        ethereum_dir / "ethereum_2026-07-31.parquet",
        """
        SELECT '0xaaa' AS from_address, '0xbbb' AS to_address,
               '0xccc' AS token_address, 100::BIGINT AS value,
               to_timestamp(1785456000) AS block_timestamp,
               'ethereum' AS chain
        UNION ALL
        SELECT '0xaaa', '0xddd', '0xccc', 200::BIGINT,
               to_timestamp(1785456060), 'ethereum'
        """,
    )
    write_parquet(
        ethereum_dir / "ethereum_dex_2026-07-31.parquet",
        """
        SELECT 'uniswap_v2' AS dex, 'swap' AS type, '0xpair' AS contract_address,
               to_timestamp(1785456000) AS block_timestamp, 'ethereum' AS chain
        """,
    )

    # Solana tree — spl balance changes live in the chain dir with no dex file.
    solana_dir = base / "solana" / "2026" / "07" / "31" / "12"
    write_parquet(
        solana_dir / "solana_2026-07-31.parquet",
        """
        SELECT 'mintAAA' AS mint, '0xowner' AS token_account,
               to_timestamp(1785456000) AS block_timestamp
        """,
    )
    return base


class TestQueryParquet:
    def test_count_transfers(self, parquet_tree: Path):
        from lite_indexer.storage.duckdb_query import query_parquet

        rows = query_parquet(
            "SELECT count(*) AS n FROM transfers",
            chain="ethereum",
            start="2026-07-31",
            end="2026-07-31",
            mount_dir=parquet_tree,
        )
        assert rows == [{"n": 2}]

    def test_dex_view(self, parquet_tree: Path):
        from lite_indexer.storage.duckdb_query import query_parquet

        rows = query_parquet(
            "SELECT dex, count(*) AS swaps FROM dex GROUP BY dex",
            chain="ethereum",
            start=date(2026, 7, 31),
            end=date(2026, 7, 31),
            mount_dir=parquet_tree,
        )
        assert rows == [{"dex": "uniswap_v2", "swaps": 1}]

    def test_solana_view(self, parquet_tree: Path):
        from lite_indexer.storage.duckdb_query import query_parquet

        rows = query_parquet(
            "SELECT mint, count(*) AS changes FROM spl_balance_changes GROUP BY mint",
            chain="solana",
            start="2026-07-31",
            end="2026-07-31",
            mount_dir=parquet_tree,
        )
        assert rows == [{"mint": "mintAAA", "changes": 1}]

    def test_date_range_collects_two_days(self, tmp_path: Path):
        from lite_indexer.storage.duckdb_query import query_parquet

        con = duckdb.connect()
        for day in ("30", "31"):
            rel = tmp_path / "parquet" / "base" / "2026" / "07" / day / "12"
            rel.mkdir(parents=True, exist_ok=True)
            con.execute(
                f"COPY (SELECT {day}::BIGINT AS value, 'base' AS chain) "
                f"TO '{rel / ('base_2026-07-' + day + '.parquet')}' (FORMAT PARQUET)"
            )
        rows = query_parquet(
            "SELECT sum(value) AS total FROM transfers",
            chain="base",
            start="2026-07-30",
            end="2026-07-31",
            mount_dir=tmp_path / "parquet",
        )
        assert rows == [{"total": 61}]

    def test_missing_chain_returns_empty(self, parquet_tree: Path):
        from lite_indexer.storage.duckdb_query import query_parquet

        rows = query_parquet(
            "SELECT count(*) AS n FROM transfers",
            chain="nonexistent",
            start="2026-07-31",
            end="2026-07-31",
            mount_dir=parquet_tree,
        )
        assert rows == [{"n": 0}]

    def test_fetch_mode_uses_rclone_cat(self, parquet_tree: Path, tmp_path: Path, monkeypatch):
        from lite_indexer.storage.duckdb_query import query_parquet

        # Fake rclone: `rclone cat <path>` writes the mirrored local file to
        # stdout; missing hour dirs return nonzero (like the real binary).
        def fake_rclone(cmd, **kwargs):
            # cmd == ["rclone", "cat", "b2-crypt-v2:chain-data/parquet/ethereum/2026/07/31/12/..."]
            remote = cmd[2]
            rel = remote.removeprefix("b2-crypt-v2:chain-data/parquet/")
            src = parquet_tree / rel
            out = kwargs["stdout"]
            if not src.is_file():
                return subprocess.CompletedProcess(cmd, 1)
            out.write(src.read_bytes())
            out.flush()
            return subprocess.CompletedProcess(cmd, 0)

        monkeypatch.setattr("subprocess.run", fake_rclone)
        rows = query_parquet(
            "SELECT count(*) AS n FROM transfers",
            chain="ethereum",
            start="2026-07-31",
            end="2026-07-31",
            fetch=True,
            cache_dir=tmp_path / "cache",
        )
        assert rows == [{"n": 2}]

    def test_example_queries_are_sql(self):
        from lite_indexer.storage.duckdb_query import example_queries

        for name, sql in example_queries().items():
            assert sql.strip().upper().startswith("SELECT"), name
