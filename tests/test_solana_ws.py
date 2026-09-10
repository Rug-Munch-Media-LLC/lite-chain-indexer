"""Tests for the free Solana websocket ingestor.

Covers the subscription request shapes (``logsSubscribe`` per program,
``accountSubscribe`` per watchlist account), the notification → transaction
→ ``address_activity`` path, failed-tx skipping, the account callback, and
the reconnect loop — all driven by a fake websocket (no network, no
monkeypatch).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

from lite_indexer.streaming.solana_ws import SolanaWebsocketConfig, SolanaWebsocketIngestor

PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
WATCHED_ACCOUNT = "9A8ePyR48XkK8tVc4bjW9RQBq4WFc3jHs7GRoUjVU79C"
TRADER = "yyYx8k8a7Mz5zjZyvttXR984L86eG1KajSkBESzf7c1"
SYSTEM_PROGRAM = "11111111111111111111111111111111"
SIG = "5mLtwuNxKqdJvs8g3PDDjhLkCLhWNrPixfj7Ry2GXh7qmP9i1n3q8hBTmT6ZC89VRMzLpS9bwnB5N9yT2DiQaGAc"
SLOT = 437_068_921


def _sample_tx() -> dict[str, Any]:
    return {
        "slot": SLOT,
        "blockTime": 1_785_801_699,
        "meta": {"err": None, "innerInstructions": []},
        "transaction": {
            "signatures": [SIG],
            "message": {
                "header": {"numRequiredSignatures": 2},
                "accountKeys": [
                    {"pubkey": WATCHED_ACCOUNT, "signer": True, "writable": True},
                    {"pubkey": TRADER, "signer": True, "writable": True},
                    {"pubkey": PROGRAM, "signer": False, "writable": False},
                    {"pubkey": SYSTEM_PROGRAM, "signer": False, "writable": False},
                ],
                "instructions": [{"programId": PROGRAM}],
            },
        },
    }


def _logs_notification(*, err: Any = None) -> str:
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "logsNotification",
            "params": {
                "result": {"context": {"slot": SLOT}, "value": {"signature": SIG, "err": err, "logs": []}},
                "subscription": 1,
            },
        }
    )


def _account_notification() -> str:
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "accountNotification",
            "params": {
                "result": {
                    "context": {"slot": SLOT},
                    "value": {"lamports": 1, "owner": PROGRAM, "data": ["", "base64"]},
                },
                "subscription": 2,
            },
        }
    )


class FakeWebsocket:
    """Minimal websocket double: records sends, yields scripted frames."""

    def __init__(self, frames: list[str]) -> None:
        self.frames = list(frames)
        self.sent: list[dict[str, Any]] = []
        self.closed = False

    async def send(self, data: str) -> None:
        self.sent.append(json.loads(data))

    def __aiter__(self) -> FakeWebsocket:
        return self

    async def __anext__(self) -> str:
        if not self.frames:
            raise StopAsyncIteration
        return self.frames.pop(0)


class FakeConnect:
    """Async context manager returned by the injected ``ws_connect``."""

    def __init__(self, ws: FakeWebsocket) -> None:
        self.ws = ws

    async def __aenter__(self) -> FakeWebsocket:
        return self.ws

    async def __aexit__(self, *args: Any) -> bool:
        self.ws.closed = True
        return False


# ═══════════════════════════════════════════════════════════════════════
# Subscription requests
# ═══════════════════════════════════════════════════════════════════════


class TestSubscriptionRequests:
    def test_logs_subscribe_per_program_with_mentions(self):
        ingestor = SolanaWebsocketIngestor(
            config=SolanaWebsocketConfig(programs=(PROGRAM,), accounts=()),
        )
        requests = ingestor.subscription_requests()
        assert len(requests) == 1
        assert requests[0]["method"] == "logsSubscribe"
        assert requests[0]["params"][0] == {"mentions": [PROGRAM]}
        assert requests[0]["params"][1]["commitment"] == "finalized"

    def test_account_subscribe_per_watchlist_account(self):
        ingestor = SolanaWebsocketIngestor(
            config=SolanaWebsocketConfig(programs=(), accounts=(WATCHED_ACCOUNT,)),
        )
        requests = ingestor.subscription_requests()
        assert len(requests) == 1
        assert requests[0]["method"] == "accountSubscribe"
        assert requests[0]["params"][0] == WATCHED_ACCOUNT
        assert requests[0]["params"][1]["encoding"] == "jsonParsed"

    def test_ids_are_unique_across_requests(self):
        ingestor = SolanaWebsocketIngestor(
            config=SolanaWebsocketConfig(programs=(PROGRAM,), accounts=(WATCHED_ACCOUNT,)),
        )
        ids = [r["id"] for r in ingestor.subscription_requests()]
        assert len(ids) == len(set(ids)) == 2


# ═══════════════════════════════════════════════════════════════════════
# Notification → address_activity
# ═══════════════════════════════════════════════════════════════════════


class TestNotificationHandling:
    async def test_logs_notification_indexes_transaction(self):
        client = MagicMock()
        ingestor = SolanaWebsocketIngestor(
            ch_client=client,
            config=SolanaWebsocketConfig(programs=(PROGRAM,), accounts=()),
            fetch_transaction=_stub_fetch,
        )
        written = await ingestor._handle_raw(_logs_notification())
        assert written == 4  # 4 account keys
        assert ingestor.events_indexed == 1
        insert = next(
            call
            for call in client.execute.call_args_list
            if "INSERT" in str(call.args[0]) and "address_activity" in str(call.args[0])
        )
        assert len(insert.args[1]) == 4

    async def test_failed_transaction_is_skipped(self):
        client = MagicMock()
        ingestor = SolanaWebsocketIngestor(
            ch_client=client,
            config=SolanaWebsocketConfig(programs=(PROGRAM,), accounts=()),
            fetch_transaction=_stub_fetch,
        )
        written = await ingestor._handle_raw(_logs_notification(err={"InstructionError": [0, "custom"]}))
        assert written == 0
        assert client.execute.call_args_list == []

    async def test_account_notification_invokes_callback(self):
        seen: list[dict[str, Any]] = []
        ingestor = SolanaWebsocketIngestor(
            config=SolanaWebsocketConfig(programs=(), accounts=(WATCHED_ACCOUNT,)),
            on_account_update=seen.append,
        )
        written = await ingestor._handle_raw(_account_notification())
        assert written == 0
        assert seen and seen[0]["owner"] == PROGRAM

    async def test_bad_frame_is_ignored(self):
        ingestor = SolanaWebsocketIngestor(config=SolanaWebsocketConfig(programs=(PROGRAM,), accounts=()))
        assert await ingestor._handle_raw("not-json") == 0

    async def test_on_transaction_hook_runs(self):
        hooks: list[dict[str, Any]] = []
        ingestor = SolanaWebsocketIngestor(
            ch_client=MagicMock(),
            config=SolanaWebsocketConfig(programs=(PROGRAM,), accounts=()),
            fetch_transaction=_stub_fetch,
            on_transaction=hooks.append,
        )
        await ingestor._handle_raw(_logs_notification())
        assert hooks and hooks[0]["slot"] == SLOT


# ═══════════════════════════════════════════════════════════════════════
# Connection loop
# ═══════════════════════════════════════════════════════════════════════


class TestRunLoop:
    async def test_run_subscribes_and_processes_then_stops(self):
        ws = FakeWebsocket([_logs_notification()])
        ingestor: SolanaWebsocketIngestor

        def _connect(_url: str) -> FakeConnect:
            return FakeConnect(ws)

        ingestor = SolanaWebsocketIngestor(
            ch_client=MagicMock(),
            config=SolanaWebsocketConfig(programs=(PROGRAM,), accounts=(), max_reconnects=1),
            fetch_transaction=_stub_fetch,
            on_transaction=lambda _tx: ingestor.stop(),
            ws_connect=_connect,
        )
        await ingestor.run()

        assert [s["method"] for s in ws.sent] == ["logsSubscribe"]
        assert ingestor.events_indexed == 1
        assert ws.closed is True


async def _stub_fetch(_signature: str) -> dict[str, Any]:
    return _sample_tx()
