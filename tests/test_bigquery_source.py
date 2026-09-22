"""Tests for BigQuerySource — the BigQuery public ``crypto_ethereum`` tier.

No network: the ``bq`` CLI is replaced by an injected fake runner
(``(sql, params) -> rows``). No monkeypatching — pure DI.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from lite_indexer.lakes.registry import _default_registry
from lite_indexer.lakes.router import LakeProviderRouter
from lite_indexer.query.bigquery_source import BigQuerySource
from lite_indexer.query.bigquery_spec import BigQuerySpec

PADDED = "0x" + "0" * 24 + "a" * 40  # 32-byte left-padded address


class FakeRunner:
    """Records ``(sql, params)`` calls and returns canned rows."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.calls: list[tuple[str, list[tuple[str, str, Any]]]] = []

    def __call__(self, sql: str, params: list[tuple[str, str, Any]]) -> list[dict[str, Any]]:
        self.calls.append((sql, params))
        return self.rows


def _creds(tmp_path: Any) -> str:
    path = tmp_path / "sa.json"
    path.write_text("{}")
    return str(path)


def _source(tmp_path: Any, runner: FakeRunner, **kwargs: Any) -> BigQuerySource:
    router = LakeProviderRouter(usage_path=tmp_path / "lake_usage.json")
    return BigQuerySource(credentials_path=_creds(tmp_path), runner=runner, router=router, **kwargs)


def _params(params: list[tuple[str, str, Any]]) -> dict[str, Any]:
    return {name: value for name, _type, value in params}


# ── Availability gate ────────────────────────────────────────────────


def test_unavailable_without_credentials(tmp_path: Any) -> None:
    runner = FakeRunner()
    src = BigQuerySource(credentials_path=str(tmp_path / "missing.json"), runner=runner)
    assert src.available() is False
    assert src.query(BigQuerySpec("transactions")) == []
    assert runner.calls == []


def test_enabled_flag_disables_even_with_credentials(tmp_path: Any) -> None:
    src = BigQuerySource(credentials_path=_creds(tmp_path), runner=FakeRunner(), enabled=False)
    assert src.available() is False


def test_default_enabled_when_credentials_exist(tmp_path: Any) -> None:
    src = BigQuerySource(credentials_path=_creds(tmp_path), runner=FakeRunner())
    assert src.available() is True


def test_default_disabled_when_credentials_absent(tmp_path: Any) -> None:
    src = BigQuerySource(credentials_path=str(tmp_path / "nope.json"), runner=FakeRunner())
    assert src.available() is False


# ── SQL construction ─────────────────────────────────────────────────


def test_date_range_sql(tmp_path: Any) -> None:
    runner = FakeRunner()
    _source(tmp_path, runner).query(BigQuerySpec("transactions", dates=["2026-09-12", "2026-09-13"]))
    sql, params = runner.calls[-1]
    assert "DATE(block_timestamp) IN UNNEST(@dates)" in sql
    assert _params(params)["dates"] == ["2026-09-12", "2026-09-13"]
    assert "`bigquery-public-data.crypto_ethereum.transactions`" in sql


def test_block_range_sql(tmp_path: Any) -> None:
    runner = FakeRunner()
    _source(tmp_path, runner).query(BigQuerySpec("transactions", from_block=100, to_block=200))
    sql, params = runner.calls[-1]
    assert "block_number >= @from_block" in sql and "block_number <= @to_block" in sql
    assert _params(params)["from_block"] == 100
    assert _params(params)["to_block"] == 200


def test_address_filter_sql_uses_padded_tail(tmp_path: Any) -> None:
    runner = FakeRunner()
    _source(tmp_path, runner).query(BigQuerySpec("transactions", address=PADDED))
    sql, params = runner.calls[-1]
    assert "LOWER(from_address) = @address" in sql
    assert "LOWER(to_address) = @address" in sql
    assert _params(params)["address"] == "a" * 40


def test_limit_is_parameterized_and_clamped(tmp_path: Any) -> None:
    runner = FakeRunner()
    _source(tmp_path, runner).query(BigQuerySpec("transactions", limit=99999))
    assert _params(runner.calls[-1][1])["limit"] == 1000


def test_invalid_date_rejected(tmp_path: Any) -> None:
    with pytest.raises(ValueError):
        _source(tmp_path, FakeRunner()).query(BigQuerySpec("transactions", dates=["2026-09-12; DROP TABLE x"]))


def test_unsupported_table_and_chain_return_empty(tmp_path: Any) -> None:
    runner = FakeRunner()
    src = _source(tmp_path, runner)
    assert src.query(BigQuerySpec("dex_events")) == []
    assert src.query(BigQuerySpec("transactions", chain="base")) == []
    assert runner.calls == []


def test_address_filter_unsupported_table_returns_empty(tmp_path: Any) -> None:
    runner = FakeRunner()
    assert _source(tmp_path, runner).query(BigQuerySpec("blocks", address=PADDED)) == []
    assert runner.calls == []


# ── Row mapping ──────────────────────────────────────────────────────


def test_row_mapping_padded_addresses_and_ms_timestamp(tmp_path: Any) -> None:
    row = {
        "block_number": 25963314,
        "block_timestamp": 1757703947000,  # epoch ms
        "tx_hash": "0xHASH",
        "from_address": PADDED,
        "to_address": "0x" + "0" * 24 + "b" * 40,
        "value": 61.0,
        "log_index": 382,
    }
    out = _source(tmp_path, FakeRunner([row])).query(BigQuerySpec("transactions"))
    assert len(out) == 1
    r = out[0]
    assert r["chain"] == "eth"
    assert r["block_number"] == 25963314
    assert r["from_address"] == "0x" + "a" * 40
    assert r["to_address"] == "0x" + "b" * 40
    assert r["value"] == "61"
    assert r["log_index"] == 382
    assert r["block_timestamp"] == datetime.fromtimestamp(1757703947, tz=UTC)
    assert "ingested_at" in r


def test_missing_columns_map_to_defaults(tmp_path: Any) -> None:
    row = {"block_number": 1, "block_timestamp": None, "hash": "0xH"}
    out = _source(tmp_path, FakeRunner([row])).query(BigQuerySpec("transactions"))
    r = out[0]
    assert r["block_timestamp"] is None
    assert r["from_address"] == ""
    assert r["value"] == "0"


# ── Registry quota fields ────────────────────────────────────────────


def test_registry_bigquery_quota_fields() -> None:
    prov = _default_registry()["bigquery"]
    assert prov.name == "bigquery"
    assert prov.kind == "gcp/bigquery"
    assert prov.rps == 1.0
    assert prov.daily_requests == 1000
    assert prov.daily_bytes == 1099511627776  # 1 TiB/month allowance
    assert ("eth", "token_transfers") in prov.serves
# ── Solana mapping + query builder ───────────────────────────────────


def test_solana_token_transfers_sql(tmp_path: Any) -> None:
    runner = FakeRunner()
    _source(tmp_path, runner).query(BigQuerySpec("token_transfers", chain="solana", dates=["2026-09-20"]))
    sql, params = runner.calls[-1]
    assert "`bigquery-public-data.crypto_solana_mainnet_us.Token Transfers`" in sql
    assert "DATE(block_timestamp) IN UNNEST(@dates)" in sql
    assert _params(params)["dates"] == ["2026-09-20"]
    assert "block_slot >= @from_slot" not in sql


def test_solana_instructions_sql_address_in_accounts(tmp_path: Any) -> None:
    runner = FakeRunner()
    _source(tmp_path, runner).query(BigQuerySpec(
        "instructions", chain="solana", address="Base58Addr", dates=["2026-09-20"]
    ))
    sql, params = runner.calls[-1]
    assert "`bigquery-public-data.crypto_solana_mainnet_us.Instructions`" in sql
    assert "@address IN UNNEST(accounts)" in sql
    assert _params(params)["address"] == "Base58Addr"


def test_solana_slot_bounds_sql(tmp_path: Any) -> None:
    runner = FakeRunner()
    _source(tmp_path, runner).query(BigQuerySpec("token_transfers", chain="solana", from_block=100, to_block=200))
    sql, params = runner.calls[-1]
    assert "block_slot >= @from_slot" in sql and "block_slot <= @to_slot" in sql
    assert _params(params)["from_slot"] == 100


def test_solana_unsupported_table_returns_empty(tmp_path: Any) -> None:
    runner = FakeRunner()
    assert _source(tmp_path, runner).query(BigQuerySpec("transactions", chain="solana")) == []
    assert runner.calls == []


def test_solana_invalid_date_rejected(tmp_path: Any) -> None:
    with pytest.raises(ValueError):
        _source(tmp_path, FakeRunner()).query(BigQuerySpec("token_transfers", chain="solana", dates=["bad"]))


def test_solana_token_transfer_row_mapping(tmp_path: Any) -> None:
    row = {
        "slot": 300_000_001,
        "block_timestamp": 1758300000000,
        "tx_hash": "5SIG",
        "mint": "MINTADDR",
        "owner": "AUTHORITY",
        "value": "123.5",
        "decimals": "6",
        "source": "SRC",
        "destination": "DST",
        "transfer_type": "1",
    }
    out = _source(tmp_path, FakeRunner([row])).query(BigQuerySpec("token_transfers", chain="solana"))
    r = out[0]
    assert r["chain"] == "solana"
    assert r["slot"] == 300_000_001
    assert r["tx_hash"] == "5SIG"
    assert r["mint"] == "MINTADDR"
    assert r["owner"] == "AUTHORITY"
    assert r["pre_amount"] == "0" and r["post_amount"] == "123.5" and r["delta"] == "123.5"
    assert r["decimals"] == 6
    assert r["source"] == "SRC" and r["destination"] == "DST"
    assert r["block_timestamp"] == datetime.fromtimestamp(1758300000, tz=UTC)
    assert "ingested_at" in r


def test_solana_instruction_row_mapping(tmp_path: Any) -> None:
    row = {
        "slot": 300_000_002,
        "block_timestamp": 1758300001000,
        "tx_hash": "SIG2",
        "transaction_index": 3,
        "program_id": "PROG",
        "accounts": ["A1", "A2"],
        "data": "0xdeadbeef",
    }
    out = _source(tmp_path, FakeRunner([row])).query(BigQuerySpec("instructions", chain="solana"))
    r = out[0]
    assert r["chain"] == "solana"
    assert r["program_id"] == "PROG"
    assert r["accounts"] == ["A1", "A2"]
    assert r["account_count"] == 2
    assert r["transaction_index"] == 3
    assert r["data"] == "0xdeadbeef"


def test_solana_empty_result(tmp_path: Any) -> None:
    runner = FakeRunner([])
    assert _source(tmp_path, runner).query(BigQuerySpec("token_transfers", chain="solana", dates=["2026-09-20"])) == []


# ── Eth path unchanged ───────────────────────────────────────────────


def test_eth_path_unchanged_by_solana_addition(tmp_path: Any) -> None:
    runner = FakeRunner()
    _source(tmp_path, runner).query(BigQuerySpec("transactions", dates=["2026-09-12"]))
    sql, params = runner.calls[-1]
    assert "`bigquery-public-data.crypto_ethereum.transactions`" in sql
    assert "crypto_solana" not in sql
    assert _params(params)["dates"] == ["2026-09-12"]


def test_registry_bigquery_serves_solana() -> None:
    prov = _default_registry()["bigquery"]
    assert ("solana", "token_transfers") in prov.serves
    assert ("solana", "instructions") in prov.serves
