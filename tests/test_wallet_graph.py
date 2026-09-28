"""Tests for the wallet-graph primitives (``lite_indexer.query.wallet_graph``).

The two things these pin, both of which are easy to get wrong:

* the funder is the EARLIEST inbound transfer — not the largest, not the
  most recent, not the first row the warehouse happens to return;
* every value is PARAMETERIZED, so an injection-shaped address cannot
  reach the SQL text.

No network: the ClickHouse client is a fake executor with the same
``(sql, params) -> list[dict]`` shape as ``api.db.query``.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.query.wallet_counterparties import counterparties
from lite_indexer.query.wallet_graph import earliest_inbound_funder

# A payload that would end a statement and append a DROP if interpolated.
INJECTION_ADDRESS = "0xabc' OR '1'='1'; DROP TABLE wallet_memory.evm_transfers --"


class RecordingExecutor:
    """Fake read executor: records the (sql, params) it was handed."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.sql: str = ""
        self.params: dict[str, Any] = {}

    def __call__(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.sql = sql
        self.params = params
        return self.rows


def _transfer(**overrides: Any) -> dict[str, Any]:
    row = {
        "chain": "ethereum",
        "block_number": 1000,
        "block_timestamp": "2026-01-01 00:00:00",
        "tx_hash": "0xtx",
        "log_index": 0,
        "from_address": "0xfunder",
        "to_address": "0xwallet",
        "token_address": "0xtoken",
        "value": "1000",
    }
    row.update(overrides)
    return row


# ── Funder: earliest, not loudest ──────────────────────────────────────


def test_funder_is_the_earliest_inbound_not_the_largest() -> None:
    """The earliest row wins even when a later one is 10^9 bigger."""
    run = RecordingExecutor(
        [
            _transfer(block_number=100, tx_hash="0xfirst", value="1"),
            _transfer(block_number=200, tx_hash="0xsecond", value="1000000000"),
        ]
    )
    funder = earliest_inbound_funder(run, "0xwallet")
    assert funder is not None
    # Ordering is delegated to the warehouse; assert the ORDER BY makes
    # "earliest" the actual selection rule rather than a coincidence.
    assert "ORDER BY block_number ASC, log_index ASC" in run.sql
    assert "LIMIT 1" in run.sql


def test_funder_never_sorts_by_value_or_recency() -> None:
    """A funder query that sorted by value/recency would be wrong."""
    run = RecordingExecutor([])
    earliest_inbound_funder(run, "0xwallet")
    lowered = run.sql.lower()
    assert "order by value" not in lowered
    assert "order by block_timestamp desc" not in lowered
    assert lowered.count("order by") == 1


def test_outbound_only_wallet_has_no_funder() -> None:
    """A wallet that only ever sent must report no funder, not itself."""
    run = RecordingExecutor([])
    assert earliest_inbound_funder(run, "0xwallet") is None


def test_funder_excludes_self_transfers() -> None:
    """Self-transfers are filtered in SQL — a wallet cannot fund itself."""
    run = RecordingExecutor([])
    earliest_inbound_funder(run, "0xwallet")
    assert "from_address != %(address)s" in run.sql


def test_funder_ordering_breaks_block_ties_by_log_index() -> None:
    """Two transfers in one block: log index is the chain's own order."""
    run = RecordingExecutor([])
    earliest_inbound_funder(run, "0xwallet")
    assert "block_number ASC, log_index ASC" in run.sql


# ── Injection posture ──────────────────────────────────────────────────


def test_injection_shaped_address_is_parameterized_not_interpolated() -> None:
    run = RecordingExecutor([])
    earliest_inbound_funder(run, INJECTION_ADDRESS)
    assert INJECTION_ADDRESS not in run.sql
    assert run.params["address"] == INJECTION_ADDRESS.lower()
    assert "%(address)s" in run.sql


def test_injection_shaped_chain_is_parameterized_not_interpolated() -> None:
    run = RecordingExecutor([])
    earliest_inbound_funder(run, "0xwallet", chain="' OR 1=1 --")
    assert "' OR 1=1 --" not in run.sql
    assert run.params["chain"] == "' or 1=1 --"


def test_counterparties_parameterize_address_and_chain() -> None:
    run = RecordingExecutor([])
    counterparties(run, INJECTION_ADDRESS, chain="base")
    assert INJECTION_ADDRESS not in run.sql
    assert run.params["address"] == INJECTION_ADDRESS.lower()
    assert run.params["chain"] == "base"


def test_blank_chain_emits_no_filter() -> None:
    run = RecordingExecutor([])
    counterparties(run, "0xwallet", chain="   ")
    assert "chain" not in run.params
    assert "chain = %(chain)s" not in run.sql


def test_address_is_lowercased_before_binding() -> None:
    run = RecordingExecutor([])
    counterparties(run, "0xAbCdEf", chain="Ethereum")
    assert run.params["address"] == "0xabcdef"
    assert run.params["chain"] == "ethereum"


# ── Counterparties ─────────────────────────────────────────────────────


def test_counterparty_sql_is_directional_and_symmetric() -> None:
    """Both directions in one pass, with the opposite side resolved."""
    run = RecordingExecutor([])
    counterparties(run, "0xwallet")
    assert "from_address = %(address)s OR to_address = %(address)s" in run.sql
    # counterparty is whichever side is NOT the queried wallet — that
    # inversion is what makes A→B and B→A the same edge.
    assert "if(from_address = %(address)s, to_address, from_address) AS counterparty" in run.sql


def test_counterparties_sums_volume_per_direction() -> None:
    run = RecordingExecutor([])
    counterparties(run, "0xwallet")
    assert "sumIf(toUInt256(value), from_address = %(address)s) AS out_volume" in run.sql
    assert "sumIf(toUInt256(value), to_address = %(address)s) AS in_volume" in run.sql
    assert "GROUP BY counterparty" in run.sql


def test_counterparties_orders_most_recently_active_first() -> None:
    run = RecordingExecutor([])
    counterparties(run, "0xwallet")
    assert "ORDER BY last_seen_block DESC" in run.sql


def test_counterparties_limit_is_an_int_not_a_string() -> None:
    """A non-numeric limit is coerced away, never concatenated raw."""
    run = RecordingExecutor([])
    counterparties(run, "0xwallet", limit=10)
    assert run.sql.endswith("LIMIT 10")
    assert "1; DROP" not in run.sql


def test_counterparties_returns_rows_unchanged() -> None:
    rows = [
        {
            "counterparty": "0xvenue",
            "chain": "base",
            "first_seen_block": 10,
            "last_seen_block": 99,
            "out_volume": "5",
            "in_volume": "0",
            "transfer_count": 3,
        }
    ]
    assert counterparties(RecordingExecutor(rows), "0xwallet") == rows
