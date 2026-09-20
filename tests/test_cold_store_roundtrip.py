"""Cold-leg round-trip against the canonical parquet producer's layout.

Producer = ``scripts/lite-indexer-parquet-daily.sh``: ClickHouse
``FORMAT Parquet`` writes per hour, verbatim table columns, no pyarrow::

    {chain}/{YYYY}/{MM}/{DD}/{HH}/{chain}_{YYYY-MM-DD}.parquet        (transfers)
    {chain}/{YYYY}/{MM}/{DD}/{HH}/{chain}_dex_{YYYY-MM-DD}.parquet    (dex)

Addresses are ``String`` -> VARCHAR, ``DateTime`` -> TIMESTAMP[ms, tz=UTC],
``Nullable(String)`` -> VARCHAR NULL, plus columns the cold leg drops
(``raw_payload``, ``tx_log_key``). Solana's export has no ``chain`` column.
Fixtures reproduce that schema (captured from an archived B2 object) so an
archived date round-trips through ``ColdStore.query`` with no network.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from lite_indexer.storage.cold_parquet import (  # noqa: E402
    LEGACY_REMOTE,
    PRODUCER_REMOTE,
    align_remote,
)
from lite_indexer.storage.cold_store import ColdStore  # noqa: E402

DAY = datetime(2026, 9, 20, 15, 30, tzinfo=UTC)
FROM_A = "0x055b2d815c31477cc0eaf8edd41092414cb14ea5"
_TYPES = {
    "str": pa.string(),
    "u64": pa.uint64(),
    "u32": pa.uint32(),
    "u8": pa.uint8(),
    "f64": pa.float64(),
    "ts": pa.timestamp("ms", tz="UTC"),
    "b20": pa.binary(20),
}
_DEFAULTS = {"str": "", "u64": 1, "u32": 0, "u8": 0, "f64": 0.0, "ts": DAY, "b20": b"\x00" * 20}

TRANSFER = (
    "chain:str,block_number:u64,tx_hash:str,from_address:str,to_address:str,token_address:str,"
    "value:str,block_timestamp:ts,log_index:u32,token_symbol:str,token_name:str,ingested_at:ts,"
    "source:str,raw_payload:str,tx_log_key:str"
)
DEX = (
    "type:str,chain:str,block_number:u64,block_timestamp:ts,tx_hash:str,log_index:u32,"
    "contract_address:str,dex:str,version:str,token0:str,token1:str,sender:str,recipient:str,"
    "amount0_in:str,amount1_in:str,amount0_out:str,amount1_out:str,amount0:str,amount1:str,"
    "whale:u8,price_usd:f64,volume_usd:f64,ingested_at:ts,source:str,raw_payload:str"
)
SOLANA = (
    "slot:u64,block_timestamp:ts,tx_hash:str,token_account:str,mint:str,owner:str,pre_amount:str,"
    "post_amount:str,delta:str,decimals:u8,token_symbol:str,token_name:str,ingested_at:ts"
)
BINARY = (
    "chain:str,block_number:u64,tx_hash:str,from_address:b20,to_address:str,token_address:str,"
    "value:str,block_timestamp:ts,log_index:u32,ingested_at:ts"
)


def _schema(spec: str) -> Any:
    """Build the producer's parquet schema from a ``name:type`` shorthand."""
    return pa.schema([(n, _TYPES[t]) for n, t in (f.split(":") for f in spec.split(","))])


def _row(spec: str, **over: Any) -> dict[str, Any]:
    """One producer row: type defaults overridden by ``over``."""
    row = {f.split(":")[0]: _DEFAULTS[f.split(":")[1]] for f in spec.split(",")}
    row.update(over)
    return row


def _write(root: Path, chain: str, day: str, hour: str, spec: str, rows: list[dict[str, Any]], kind: str = "") -> Path:
    """Write one producer object: ``{chain}/{Y}/{M}/{D}/{H}/{chain}[_kind]_{day}.parquet``."""
    y, m, d = day.split("-")
    path = root / chain / y / m / d / hour / f"{chain}_{kind + '_' if kind else ''}{day}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=_schema(spec)), path)
    return path


@pytest.fixture()
def producer_tree(tmp_path: Path) -> Path:
    """A parquet tree in the exact producer layout + ClickHouse encoding."""
    root = tmp_path / "parquet"
    _write(
        root,
        "ethereum",
        "2026-09-20",
        "15",
        TRANSFER,
        [
            _row(TRANSFER, chain="ethereum", tx_hash="0xaaa", from_address=FROM_A, value="234000000", token_name=None),
            _row(TRANSFER, chain="ethereum", tx_hash="0xbbb", from_address="0xdead", log_index=1, token_name=None),
        ],
    )
    _write(
        root,
        "ethereum",
        "2026-09-20",
        "15",
        DEX,
        [_row(DEX, chain="ethereum", type="swap", dex="uniswap_v2", price_usd=1.5, volume_usd=12.0)],
        kind="dex",
    )
    _write(
        root,
        "solana",
        "2026-09-20",
        "15",
        SOLANA,
        [_row(SOLANA, slot=436228313, mint="7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYm", decimals=9, delta="233294135")],
    )
    return root


def _store(root: Path) -> ColdStore:
    return ColdStore(remote=PRODUCER_REMOTE, mount_dir=root)


def test_evm_transfers_roundtrip(producer_tree: Path) -> None:
    """An archived EVM hour reads back through the cold leg, decoded."""
    rows = _store(producer_tree).query(
        "evm_transfers", "ethereum", from_time=DAY, to_time=DAY, where=f"from_address = '{FROM_A}'"
    )
    assert len(rows) == 1
    row = rows[0]
    # Canonical shape: extras dropped, every canonical column present.
    assert set(row) == {
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
    assert row["chain"] == "ethereum" and row["from_address"] == FROM_A
    assert row["value"] == "234000000" and row["block_number"] == 1
    assert "raw_payload" not in row and "tx_log_key" not in row


def test_dex_roundtrip(producer_tree: Path) -> None:
    rows = _store(producer_tree).query("dex_events", "ethereum", from_time=DAY, to_time=DAY)
    assert len(rows) == 1
    assert rows[0]["type"] == "swap" and rows[0]["dex"] == "uniswap_v2"
    assert rows[0]["price_usd"] == 1.5 and "raw_payload" not in rows[0]


def test_solana_roundtrip_injects_chain(producer_tree: Path) -> None:
    """Solana's export has no chain column — chain is injected, not filtered."""
    rows = _store(producer_tree).query("solana_transfers", "solana", from_time=DAY, to_time=DAY)
    assert len(rows) == 1
    assert rows[0]["chain"] == "solana" and rows[0]["slot"] == 436228313
    assert rows[0]["mint"] == "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYm"
    assert rows[0]["delta"] == "233294135"


def test_binary_fixedstring_addresses_decoded(tmp_path: Path) -> None:
    """A legacy FixedString-era object hex-decodes to the 0x String shape."""
    root = tmp_path / "parquet"
    day = datetime(2026, 9, 19, 15, 30, tzinfo=UTC)
    _write(
        root,
        "ethereum",
        "2026-09-19",
        "15",
        BINARY,
        [_row(BINARY, chain="ethereum", from_address=bytes.fromhex(FROM_A[2:]), block_timestamp=day, ingested_at=day)],
    )

    # 09-19 is outside the (09-20) window: proves file selection is date-driven.
    assert _store(root).query("evm_transfers", "ethereum", from_time=DAY, to_time=DAY) == []

    rows = _store(root).query("evm_transfers", "ethereum", from_time=day, to_time=day)
    assert len(rows) == 1 and rows[0]["from_address"] == FROM_A
    assert isinstance(rows[0]["from_address"], str)


def test_transfers_never_union_solana_files(producer_tree: Path) -> None:
    """Chain-agnostic EVM reads never pull Solana files (no chain column)."""
    rows = _store(producer_tree).query("evm_transfers", "", from_time=DAY, to_time=DAY)
    assert len(rows) == 2 and all(r["chain"] == "ethereum" for r in rows)


def test_producer_remote_alignment() -> None:
    assert align_remote(LEGACY_REMOTE) == PRODUCER_REMOTE
    assert align_remote(PRODUCER_REMOTE) == PRODUCER_REMOTE
    assert align_remote("other:prefix") == "other:prefix"
