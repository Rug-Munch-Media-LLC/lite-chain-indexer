"""
Cross-chain address_activity tests (EVM + TRON).

Mirrors tests/test_address_activity.py (the Solana suite):
  1. parse_evm_address_activity / parse_tron_address_activity — one row
     per address touched, chain tagging, contract (program_ids) capture,
     signer semantics, block-number/timestamp coercion.
  2. ClickHouse storage — the chain column is written and a legacy row
     without it defaults to 'solana'; ensure() applies the pre-015
     chain-column upgrade ALTERs.
  3. EVMChainStrategy / TronStrategy integration — events are buffered
     and drained into address_activity via write_clickhouse.

Fixtures mirror real parsed eth_getLogs transfer/swap events and TronGrid
TRC20 events (see lite_indexer/dex/transfers.py,
lite_indexer/dex/v2.py, lite_indexer/strategies/tron/parsing.py).
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from lite_indexer.storage.address_activity import (
    CH_ADDRESS_ACTIVITY_TABLE,
    ensure_address_activity_chain_column,
    ensure_address_activity_table,
    parse_address_activity,
    parse_evm_address_activity,
    parse_tron_address_activity,
    write_address_activity,
)
from lite_indexer.strategies import EVMChainStrategy, TronStrategy

# ── EVM fixtures ─────────────────────────────────────────────────────

WALLET = "0x1111111111111111111111111111111111111111"
RECIPIENT = "0x2222222222222222222222222222222222222222"
USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
PAIR = "0xcccccccccccccccccccccccccccccccccccccccc"
TOKEN0 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
TOKEN1 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
ROUTER = "0x3333333333333333333333333333333333333333"

EVM_TX = "0x" + "ab" * 32
EVM_SWAP_TX = "0x" + "cd" * 32
EVM_BLOCK = 21_000_000
EVM_TS = datetime(2026, 8, 4, 0, 1, 39, tzinfo=UTC)

TRON_FROM = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRON_TO = "TXYZopYRdj2D9XRtbG411XZZ3kM5VkAeBf"
TRON_CONTRACT = "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8"
TRON_TX = "0x" + "ef" * 32
TRON_BLOCK = 70_000_000
TRON_TS_MS = 1_785_801_699_000


def _evm_transfer() -> dict:
    """A parsed ERC-20 Transfer event (lite_indexer.dex.transfers shape)."""
    return {
        "type": "transfer",
        "chain": "ethereum",
        "blockNumber": EVM_BLOCK,
        "blockTimestamp": "2026-08-04T00:01:39Z",
        "txHash": EVM_TX,
        "logIndex": 12,
        "from": WALLET,
        "to": RECIPIENT,
        "tokenAddress": USDC,
        "value": "1000000",
    }


def _evm_swap() -> dict:
    """A parsed Uniswap V2 Swap event (lite_indexer.dex.v2 shape)."""
    return {
        "type": "swap",
        "dex_type": "v2",
        "dex": "uniswap_v2",
        "chain": "ethereum",
        "blockNumber": EVM_BLOCK,
        "blockTimestamp": "",
        "transactionHash": EVM_SWAP_TX,
        "logIndex": 3,
        "sender": ROUTER,
        "recipient": WALLET,
        "token0": TOKEN0,
        "token1": TOKEN1,
        "pairAddress": PAIR,
        "value": "0",
    }


def _tron_parsed() -> dict:
    """A parsed TRC20 event (parse_tron_transfer_event output shape)."""
    return {
        "type": "transfer",
        "chain": "tron",
        "blockNumber": TRON_BLOCK,
        "blockTimestamp": "2026-08-04T00:01:39Z",
        "txHash": TRON_TX,
        "txID": TRON_TX,
        "logIndex": 0,
        "from": TRON_FROM,
        "to": TRON_TO,
        "tokenAddress": TRON_CONTRACT,
        "value": "1000000",
    }


def _tron_raw() -> dict:
    """A raw TronGrid TRC20 event (snake_case, ms timestamp)."""
    return {
        "block_number": str(TRON_BLOCK),
        "transaction_id": TRON_TX,
        "event_index": 0,
        "from_address": TRON_FROM,
        "to_address": TRON_TO,
        "contract_address": TRON_CONTRACT,
        "block_timestamp": TRON_TS_MS,
    }


# ═══════════════════════════════════════════════════════════════════════
# parse_evm_address_activity
# ═══════════════════════════════════════════════════════════════════════


class TestParseEvmAddressActivity:
    def test_transfer_one_row_per_address(self):
        rows = parse_evm_address_activity(_evm_transfer())
        assert [r["address"] for r in rows] == [WALLET, RECIPIENT, USDC]

    def test_transfer_common_fields(self):
        rows = parse_evm_address_activity(_evm_transfer())
        for row in rows:
            assert row["chain"] == "ethereum"
            assert row["tx_signature"] == EVM_TX
            assert row["slot"] == EVM_BLOCK
            assert row["block_time"] == EVM_TS
            assert row["involved_accounts"] == [WALLET, RECIPIENT, USDC]
            # Contract set is the EVM analogue of Solana program_ids.
            assert row["program_ids"] == [USDC]

    def test_transfer_signer_not_guessed(self):
        # ERC-20 logs do not carry the tx sender; without an explicit
        # signer every row is is_signer=False (from may be a contract).
        rows = parse_evm_address_activity(_evm_transfer())
        assert all(r["is_signer"] is False for r in rows)

    def test_explicit_signer_marks_only_signer(self):
        event = _evm_transfer()
        event["signer"] = WALLET
        rows = parse_evm_address_activity(event)
        by_address = {r["address"]: r["is_signer"] for r in rows}
        assert by_address[WALLET] is True
        assert by_address[RECIPIENT] is False
        assert by_address[USDC] is False

    def test_swap_collects_sender_recipient_pair_and_tokens(self):
        rows = parse_evm_address_activity(_evm_swap())
        assert [r["address"] for r in rows] == [ROUTER, WALLET, PAIR, TOKEN0, TOKEN1]
        row = rows[0]
        assert row["tx_signature"] == EVM_SWAP_TX
        # Empty blockTimestamp falls back to a datetime, not a crash.
        assert isinstance(row["block_time"], datetime)
        assert row["program_ids"] == [PAIR, TOKEN0, TOKEN1]

    def test_hex_block_number_coerced(self):
        event = _evm_transfer()
        event["blockNumber"] = hex(EVM_BLOCK)
        assert parse_evm_address_activity(event)[0]["slot"] == EVM_BLOCK

    def test_extra_addresses_deduped(self):
        event = _evm_transfer()
        event["involvedAccounts"] = [WALLET, "0x" + "99" * 20, RECIPIENT]
        rows = parse_evm_address_activity(event)
        assert [r["address"] for r in rows] == [WALLET, RECIPIENT, USDC, "0x" + "99" * 20]

    def test_missing_signature_or_addresses_returns_empty(self):
        assert parse_evm_address_activity({}) == []
        assert parse_evm_address_activity("not-a-dict") == []
        no_addr = {"txHash": EVM_TX, "blockNumber": 1}
        assert parse_evm_address_activity(no_addr) == []


# ═══════════════════════════════════════════════════════════════════════
# parse_tron_address_activity
# ═══════════════════════════════════════════════════════════════════════


class TestParseTronAddressActivity:
    def test_parsed_event_one_row_per_address(self):
        rows = parse_tron_address_activity(_tron_parsed())
        assert [r["address"] for r in rows] == [TRON_FROM, TRON_TO, TRON_CONTRACT]
        for row in rows:
            assert row["chain"] == "tron"
            assert row["tx_signature"] == TRON_TX
            assert row["slot"] == TRON_BLOCK
            assert row["block_time"] == EVM_TS
            assert row["program_ids"] == [TRON_CONTRACT]

    def test_from_address_is_the_signer(self):
        rows = parse_tron_address_activity(_tron_parsed())
        by_address = {r["address"]: r["is_signer"] for r in rows}
        assert by_address[TRON_FROM] is True
        assert by_address[TRON_TO] is False
        assert by_address[TRON_CONTRACT] is False

    def test_raw_trongrid_event(self):
        rows = parse_tron_address_activity(_tron_raw())
        assert [r["address"] for r in rows] == [TRON_FROM, TRON_TO, TRON_CONTRACT]
        # block_timestamp is milliseconds → coerced to seconds.
        assert rows[0]["block_time"] == EVM_TS
        assert rows[0]["tx_signature"] == TRON_TX

    def test_missing_id_or_addresses_returns_empty(self):
        assert parse_tron_address_activity({}) == []
        assert parse_tron_address_activity({"transaction_id": TRON_TX}) == []


# ═══════════════════════════════════════════════════════════════════════
# ClickHouse storage — chain column + pre-015 upgrade
# ═══════════════════════════════════════════════════════════════════════


class TestCrossChainStore:
    def test_ensure_creates_canonical_table(self):
        client = MagicMock()
        assert ensure_address_activity_table(client) is True
        ddl = client.execute.call_args.args[0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "chain String DEFAULT 'solana'" in ddl
        assert "ORDER BY (address, tx_signature, chain)" in ddl

    def test_ensure_chain_column_upgrades_pre_015_table(self):
        client = MagicMock()
        ensure_address_activity_chain_column(client)
        statements = [c.args[0] for c in client.execute.call_args_list]
        assert any("ADD COLUMN IF NOT EXISTS chain" in s for s in statements)
        assert any("MODIFY ORDER BY (address, tx_signature, chain)" in s for s in statements)

    def test_write_inserts_chain_column(self):
        client = MagicMock()
        rows = parse_evm_address_activity(_evm_transfer())
        assert write_address_activity(client, rows) == 3
        sql, params = client.execute.call_args.args[0], client.execute.call_args.args[1]
        assert f"INSERT INTO {CH_ADDRESS_ACTIVITY_TABLE}" in sql
        assert sql.rstrip().endswith("chain) VALUES")
        assert params[0][-1] == "ethereum"
        assert params[0][0] == WALLET

    def test_legacy_row_without_chain_defaults_to_solana(self):
        client = MagicMock()
        # A row produced before the chain column existed.
        legacy = [{"address": "solAddr", "tx_signature": "sig", "slot": 1, "block_time": EVM_TS}]
        assert write_address_activity(client, legacy) == 1
        assert client.execute.call_args.args[1][0][-1] == "solana"

    def test_solana_parser_tags_chain(self):
        # parse_address_activity keeps emitting Solana rows with a chain tag.
        rows = parse_address_activity(
            {
                "slot": 1,
                "blockTime": 1_785_801_699,
                "meta": {"err": None, "innerInstructions": []},
                "transaction": {
                    "signatures": ["sig"],
                    "message": {
                        "header": {"numRequiredSignatures": 1},
                        "accountKeys": [{"pubkey": "addr1", "signer": True}],
                        "instructions": [],
                    },
                },
            }
        )
        assert rows and rows[0]["chain"] == "solana"


# ═══════════════════════════════════════════════════════════════════════
# Strategy integration — buffer → drain into address_activity
# ═══════════════════════════════════════════════════════════════════════


class TestEvmAddressActivityIngestion:
    def test_ingest_buffers_transfer_and_swap_rows(self):
        strategy = EVMChainStrategy("ethereum")
        buffered = strategy._ingest_address_activity_events([_evm_transfer(), _evm_swap()])
        assert buffered == 8  # 3 transfer addresses + 5 swap addresses
        assert strategy._address_activity_rows[0]["address"] == WALLET

    def test_write_clickhouse_drains_address_rows(self):
        strategy = EVMChainStrategy("ethereum")
        ch_writer = MagicMock()
        ch_writer.write_transfers.return_value = 3
        ch_writer._connect.return_value = MagicMock()
        written = strategy.write_clickhouse([_evm_transfer()], ch_writer)
        assert written == 3  # transfer write result is preserved
        assert strategy._address_activity_rows == []  # drained
        insert_sql = ch_writer._connect.return_value.execute.call_args_list[-1].args[0]
        assert f"INSERT INTO {CH_ADDRESS_ACTIVITY_TABLE}" in insert_sql

    def test_drain_keeps_rows_on_failure(self):
        strategy = EVMChainStrategy("ethereum")
        strategy._ingest_address_activity_events([_evm_transfer()])
        client = MagicMock()
        client.execute.side_effect = RuntimeError("connection refused")
        strategy._drain_address_activity_rows(client)
        assert len(strategy._address_activity_rows) == 3  # requeued


class TestTronAddressActivityIngestion:
    def test_ingest_buffers_rows(self):
        strategy = TronStrategy()
        assert strategy._ingest_address_activity_events([_tron_parsed()]) == 3
        assert strategy._address_activity_rows[0]["address"] == TRON_FROM

    def test_write_clickhouse_drains_address_rows(self):
        strategy = TronStrategy()
        ch_writer = MagicMock()
        ch_writer.write_transfers.return_value = 1
        ch_writer._connect.return_value = MagicMock()
        written = strategy.write_clickhouse([_tron_parsed()], ch_writer)
        assert written == 1
        assert strategy._address_activity_rows == []
        insert_sql = ch_writer._connect.return_value.execute.call_args_list[-1].args[0]
        assert f"INSERT INTO {CH_ADDRESS_ACTIVITY_TABLE}" in insert_sql
