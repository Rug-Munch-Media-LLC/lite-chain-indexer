"""Tests for the on-demand Solana address backfill.

Covers the two load-bearing behaviours:

1. **Never-re-fetch guard** — an address already recorded in
   ``address_backfill_state`` (or one attempted within the cooldown) is
   never dialled again; an empty history is a completed backfill.
2. **Multi-provider fallback** — public RPC first, then Alchemy, then
   Helius; a provider error falls through, and the Helius-only
   ``getTransactionsForAddress`` recovers a closed account.

RPC is driven by a recording fake transport (not monkeypatch), per the
fleet AI-test-oracle rules.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

from lite_indexer.query.address_backfill import AddressBackfill, default_solana_providers

SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
FEE_PAYER = "9A8ePyR48XkK8tVc4bjW9RQBq4WFc3jHs7GRoUjVU79C"
TRADER = "yyYx8k8a7Mz5zjZyvttXR984L86eG1KajSkBESzf7c1"
SIG = "5mLtwuNxKqdJvs8g3PDDjhLkCLhWNrPixfj7Ry2GXh7qmP9i1n3q8hBTmT6ZC89VRMzLpS9bwnB5N9yT2DiQaGAc"
SLOT = 437_068_921


class RecordingTransport:
    """Injectable RPC transport that records every call.

    ``handlers`` maps ``(method, endpoint_suffix) -> response``; a callable
    response receives the params list and returns the response (or None to
    simulate a provider failure).
    """

    def __init__(self, handlers: dict[tuple[str, str], Any] | None = None) -> None:
        self.handlers = handlers or {}
        self.calls: list[dict[str, Any]] = []

    async def call(self, endpoint: str, method: str, params: list[Any]) -> dict[str, Any] | None:
        self.calls.append({"endpoint": endpoint, "method": method, "params": params})
        for (handler_method, suffix), response in self.handlers.items():
            if handler_method == method and endpoint.endswith(suffix):
                return response(params) if callable(response) else response
        return None

    def methods(self) -> list[str]:
        return [c["method"] for c in self.calls]

    def endpoints_for(self, method: str) -> list[str]:
        return [c["endpoint"] for c in self.calls if c["method"] == method]


def _sample_tx() -> dict[str, Any]:
    """Minimal getTransaction (jsonParsed) result with two signers + programs."""
    return {
        "slot": SLOT,
        "blockTime": 1_785_801_699,
        "meta": {"err": None, "innerInstructions": []},
        "transaction": {
            "signatures": [SIG],
            "message": {
                "header": {"numRequiredSignatures": 2},
                "accountKeys": [
                    {"pubkey": FEE_PAYER, "signer": True, "writable": True},
                    {"pubkey": TRADER, "signer": True, "writable": True},
                    {"pubkey": TOKEN_PROGRAM, "signer": False, "writable": False},
                    {"pubkey": SYSTEM_PROGRAM, "signer": False, "writable": False},
                ],
                "instructions": [{"programId": TOKEN_PROGRAM}],
            },
        },
    }


def _backfill(transport: RecordingTransport, *, providers: list[tuple[str, str]] | None = None) -> AddressBackfill:
    """Build an AddressBackfill with a MagicMock CH client and the fake transport."""
    return AddressBackfill(
        ch_client=MagicMock(),
        transport=transport,
        providers=providers or [("public", "http://public"), ("alchemy", "http://alchemy")],
    )


def _state_insert_calls(client: MagicMock) -> list[Any]:
    return [
        call
        for call in client.execute.call_args_list
        if "address_backfill_state" in str(call.args[0]) and "INSERT" in str(call.args[0])
    ]


# ═══════════════════════════════════════════════════════════════════════
# Provider list
# ═══════════════════════════════════════════════════════════════════════


class TestProviderList:
    def test_public_first_and_no_unresolved_placeholders(self):
        providers = default_solana_providers()
        assert providers[0][0] == "public"
        assert providers[0][1].startswith("http")
        assert all("${" not in url for _name, url in providers)

    def test_public_fallback_second(self):
        providers = default_solana_providers()
        assert [name for name, _ in providers][:2] == ["public", "public-fallback"]


# ═══════════════════════════════════════════════════════════════════════
# Never-re-fetch guard
# ═══════════════════════════════════════════════════════════════════════


class TestNeverRefetchGuard:
    async def test_already_indexed_skips_all_rpc(self):
        transport = RecordingTransport()
        backfill = _backfill(transport)
        backfill.ch_client.execute.return_value = [(1,)]  # guard hit

        result = await backfill.backfill_address(FEE_PAYER)

        assert result.skipped is True
        assert result.reason == "already_indexed"
        assert transport.calls == []

    async def test_recent_attempt_blocks_repeat_within_cooldown(self):
        transport = RecordingTransport()  # every provider fails
        backfill = _backfill(transport)
        backfill.ch_client.execute.return_value = [(0,)]  # guard miss

        first = await backfill.backfill_address(FEE_PAYER)
        assert first.skipped is False
        assert first.reason == "all_providers_failed"
        calls_after_first = len(transport.calls)
        assert calls_after_first > 0

        second = await backfill.backfill_address(FEE_PAYER)
        assert second.skipped is True
        assert second.reason == "recent_attempt"
        assert len(transport.calls) == calls_after_first  # no new RPC

    async def test_empty_history_marks_complete(self):
        transport = RecordingTransport(
            {("getSignaturesForAddress", "public"): {"result": []}},
        )
        backfill = _backfill(transport)
        backfill.ch_client.execute.return_value = [(0,)]

        result = await backfill.backfill_address(FEE_PAYER)

        assert result.skipped is False
        assert result.signatures == 0
        inserts = _state_insert_calls(backfill.ch_client)
        assert inserts, "guard table must record the completed backfill"
        assert inserts[0].args[1][0][2] == "complete"


# ═══════════════════════════════════════════════════════════════════════
# Multi-provider fallback
# ═══════════════════════════════════════════════════════════════════════


class TestMultiProviderFallback:
    async def test_public_failure_falls_through_to_alchemy(self):
        transport = RecordingTransport(
            {
                # public errors (no handler → None); alchemy answers
                ("getSignaturesForAddress", "alchemy"): {"result": [{"signature": SIG, "slot": SLOT}]},
                ("getTransaction", "alchemy"): {"result": _sample_tx()},
            },
        )
        backfill = _backfill(transport)
        backfill.ch_client.execute.return_value = [(0,)]

        result = await backfill.backfill_address(FEE_PAYER)

        assert result.provider == "alchemy"
        assert result.signatures == 1
        assert result.rows_written == 4  # 4 account keys in the sample tx
        assert transport.endpoints_for("getSignaturesForAddress") == ["http://public", "http://alchemy"]

    async def test_helius_enhanced_recovers_closed_account(self):
        transport = RecordingTransport(
            {
                # getSignaturesForAddress is empty on every provider
                ("getSignaturesForAddress", "public"): {"result": []},
                ("getTransactionsForAddress", "helius"): {
                    "result": {"data": [{"signature": SIG, "slot": SLOT}]},
                },
                ("getTransaction", "helius"): {"result": _sample_tx()},
            },
        )
        backfill = _backfill(
            transport,
            providers=[("public", "http://public"), ("helius", "http://helius")],
        )
        backfill.ch_client.execute.return_value = [(0,)]

        result = await backfill.backfill_address(FEE_PAYER)

        assert result.provider == "helius-enhanced"
        assert result.signatures == 1
        assert result.rows_written == 4
        assert "getTransactionsForAddress" in transport.methods()

    async def test_all_providers_failed_does_not_mark_complete(self):
        transport = RecordingTransport()
        backfill = _backfill(transport)
        backfill.ch_client.execute.return_value = [(0,)]

        result = await backfill.backfill_address(FEE_PAYER)

        assert result.skipped is False
        assert result.reason == "all_providers_failed"
        assert _state_insert_calls(backfill.ch_client) == []

    async def test_unsupported_chain_short_circuits(self):
        transport = RecordingTransport()
        backfill = _backfill(transport)

        result = await backfill.backfill_address(FEE_PAYER, chain="ethereum")

        assert result.skipped is True
        assert result.reason == "unsupported_chain"
        assert transport.calls == []

    async def test_force_bypasses_guard(self):
        transport = RecordingTransport(
            {("getSignaturesForAddress", "public"): {"result": []}},
        )
        backfill = _backfill(transport)
        backfill.ch_client.execute.return_value = [(1,)]  # guard would hit

        result = await backfill.backfill_address(FEE_PAYER, force=True)

        assert result.skipped is False
        assert transport.endpoints_for("getSignaturesForAddress") == ["http://public"]


# ═══════════════════════════════════════════════════════════════════════
# Result shape
# ═══════════════════════════════════════════════════════════════════════


class TestBackfillResult:
    async def test_empty_address_is_skipped(self):
        backfill = _backfill(RecordingTransport())
        result = await backfill.backfill_address("   ")
        assert result.skipped is True
        assert result.reason == "empty_address"

    async def test_result_carries_parsed_block_time(self):
        transport = RecordingTransport(
            {
                ("getSignaturesForAddress", "public"): {"result": [{"signature": SIG, "slot": SLOT}]},
                ("getTransaction", "public"): {"result": _sample_tx()},
            },
        )
        backfill = _backfill(transport)
        backfill.ch_client.execute.return_value = [(0,)]

        await backfill.backfill_address(FEE_PAYER)

        insert = next(
            call
            for call in backfill.ch_client.execute.call_args_list
            if "INSERT" in str(call.args[0]) and "address_activity" in str(call.args[0])
        )
        first_row = insert.args[1][0]
        assert first_row[3] == datetime(2026, 8, 4, 0, 1, 39, tzinfo=UTC)


# ═══════════════════════════════════════════════════════════════════════
# Query-layer lazy backfill (QueryRouter wiring)
# ═══════════════════════════════════════════════════════════════════════


class FakeClickHouse:
    """Recording ClickHouse double for the guard + keystone tables."""

    _ACTIVITY_COLS = [
        ("address", "String"),
        ("tx_signature", "String"),
        ("slot", "UInt64"),
        ("block_time", "DateTime"),
        ("involved_accounts", "Array(String)"),
        ("program_ids", "Array(String)"),
        ("is_signer", "Bool"),
    ]

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.activity_rows: list[tuple[Any, ...]] = []
        self.state: set[tuple[str, str]] = set()

    def execute(self, sql: str, params: Any = None, **kwargs: Any) -> Any:
        text = str(sql)
        self.calls.append(text)
        if "address_backfill_state" in text and "count()" in text:
            key = (params["chain"], params["address"])
            return [(1 if key in self.state else 0,)]
        if "address_backfill_state" in text and "INSERT" in text:
            for row in params:
                self.state.add((row[1], row[0]))
            return []
        if "address_activity" in text and "INSERT" in text:
            self.activity_rows.extend(params)
            return []
        if "address_activity" in text and "SELECT" in text:
            rows = [r for r in self.activity_rows if r[0] == params["address"]]
            return rows, self._ACTIVITY_COLS
        return []


class TestQueryRouterLazyBackfill:
    async def _router(self, transport: RecordingTransport) -> Any:
        from lite_indexer.query.router import QueryRouter

        ch = FakeClickHouse()
        return (
            QueryRouter(
                ch_client=ch,
                backfill=AddressBackfill(
                    ch_client=ch,
                    transport=transport,
                    providers=[("public", "http://public")],
                ),
            ),
            ch,
        )

    async def test_first_touch_backfills_then_serves_from_clickhouse(self):
        transport = RecordingTransport(
            {
                ("getSignaturesForAddress", "public"): {"result": [{"signature": SIG, "slot": SLOT}]},
                ("getTransaction", "public"): {"result": _sample_tx()},
            },
        )
        router, ch = await self._router(transport)

        rows = await router.get_address_activity("solana", FEE_PAYER, limit=10)

        assert len(rows) == 1  # one keystone row for the requested address
        assert len(ch.activity_rows) == 4  # all 4 account keys backfilled
        assert "getSignaturesForAddress" in transport.methods()
        assert ("solana", FEE_PAYER) in ch.state  # guard recorded

    async def test_second_touch_uses_guard_and_makes_no_rpc(self):
        transport = RecordingTransport(
            {
                ("getSignaturesForAddress", "public"): {"result": [{"signature": SIG, "slot": SLOT}]},
                ("getTransaction", "public"): {"result": _sample_tx()},
            },
        )
        router, ch = await self._router(transport)
        await router.get_address_activity("solana", FEE_PAYER, limit=10)
        calls_after_first = len(transport.calls)

        rows = await router.get_address_activity("solana", FEE_PAYER, limit=10)

        assert len(rows) == 1
        assert len(transport.calls) == calls_after_first  # never re-fetched
