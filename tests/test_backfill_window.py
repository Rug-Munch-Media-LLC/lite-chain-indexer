"""Quota-window-aware backfill scheduling tests.

Contracts:

* A chunk whose freemium account is near its window cap is DEFERRED (left
  queued), not dispatched into a 429.
* Just after the window resets the budget is full → dispatch is allowed.
* Providers with no ledger window are NEVER deferred (no starvation).
* The free/keyless pool is untouched: only the selected freemium endpoint is
  consulted, and a keyless endpoint never seeds/consumes window state.
* Post-reset jitter is deterministic and bounded.

Hermetic — no network, no ClickHouse.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

from lite_indexer.config import BACKFILL_WINDOW_JITTER_S
from lite_indexer.indexer.backfill import (
    BackfillChunk,
    BackfillQueue,
    WindowDecision,
    _chain_jitter,
    drain_backfill_queue,
    window_decision,
)
from lite_indexer.rpc.ledger import ProviderLedger, key_id_for_endpoint, provider_id_for_endpoint

# A keyed endpoint (account budget is what the window gate consults).
KEYED = "https://rpc.window-test.example/v2/abcdef0123456789"
FREE_KEYLESS = "https://free.window-test.example"


def _seed(
    ledger: ProviderLedger,
    endpoint: str,
    *,
    consumed_units: float,
    budget_units: int,
    reset_at: float,
) -> None:
    provider = provider_id_for_endpoint(endpoint)
    key = key_id_for_endpoint(endpoint)
    budget = ledger._ensure_budget(provider, key)  # noqa: SLF001 — test seam
    budget.budget_units = budget_units
    budget.consumed_units = consumed_units
    budget.reset_at = reset_at
    budget.updated_at = time.time()


# ── window_decision ─────────────────────────────────────────────────────


def test_defers_when_near_cap(tmp_path: Path) -> None:
    ledger = ProviderLedger(path=tmp_path / "ledger.json")
    now = 1_000_000.0
    _seed(ledger, KEYED, consumed_units=90, budget_units=100, reset_at=now + 120)

    decision = window_decision("ethereum", KEYED, now=now, ledger=ledger)
    assert decision.allowed is False
    assert decision.reason == "near_cap"
    assert decision.wait_seconds > 0


def test_fires_just_after_reset(tmp_path: Path) -> None:
    ledger = ProviderLedger(path=tmp_path / "ledger.json")
    now = 1_000_000.0
    # Window reset already passed → state_for rolls consumed back to 0.
    _seed(ledger, KEYED, consumed_units=95, budget_units=100, reset_at=now - 1)

    decision = window_decision("ethereum", KEYED, now=now, ledger=ledger)
    assert decision.allowed is True
    # state_for rolls the window over, so the budget is full → spend now.
    assert decision.reason in {"fresh_window", "budget_ok"}


def test_allows_when_budget_healthy(tmp_path: Path) -> None:
    ledger = ProviderLedger(path=tmp_path / "ledger.json")
    now = 1_000_000.0
    _seed(ledger, KEYED, consumed_units=10, budget_units=100, reset_at=now + 120)

    decision = window_decision("ethereum", KEYED, now=now, ledger=ledger)
    assert decision.allowed is True
    assert decision.reason == "budget_ok"


def test_unknown_provider_is_never_deferred(tmp_path: Path) -> None:
    ledger = ProviderLedger(path=tmp_path / "ledger.json")
    decision = window_decision("ethereum", KEYED, now=1_000_000.0, ledger=ledger)
    assert decision.allowed is True
    assert decision.reason == "unknown"


def test_keyless_endpoint_is_untouched(tmp_path: Path) -> None:
    ledger = ProviderLedger(path=tmp_path / "ledger.json")
    assert key_id_for_endpoint(FREE_KEYLESS) == ""
    decision = window_decision("ethereum", FREE_KEYLESS, now=1_000_000.0, ledger=ledger)
    assert decision.allowed is True
    assert decision.reason == "unknown"
    assert ledger.snapshot() == {}  # no state created for the free pool


class _StubLedger:
    """Duck-typed ledger returning a fixed state (no auto-reset)."""

    def __init__(self, state: dict[str, object]) -> None:
        self._state = state

    def state_for(
        self,
        provider: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> dict[str, object]:
        return self._state


def test_infers_short_window_when_reset_unknown() -> None:
    now = 1_000_000.5
    stub = _StubLedger({"budget_units": 100, "consumed_pct": 90.0, "reset_at": 0.0})
    decision = window_decision("ethereum", KEYED, now=now, ledger=stub, infer_window_s=60.0)  # type: ignore[arg-type]
    assert decision.allowed is False
    assert decision.reason == "near_cap"
    assert 0 < decision.wait_seconds <= 60.0 + BACKFILL_WINDOW_JITTER_S


def test_jitter_is_bounded_and_deterministic() -> None:
    vals = {_chain_jitter(f"chain-{i}", 30.0) for i in range(50)}
    assert len(vals) > 1  # spreads chains
    assert all(0.0 <= v < 30.0 for v in vals)
    assert _chain_jitter("ethereum", 30.0) == _chain_jitter("ethereum", 30.0)
    assert _chain_jitter("ethereum", 0.0) == 0.0


# ── drain_backfill_queue integration ────────────────────────────────────


async def _run_drain(tmp_path: Path, gate, calls: list[tuple[str, str]]):
    queue = BackfillQueue("ethereum", state_dir=tmp_path)
    queue.enqueue([BackfillChunk("ethereum", 0, 99)])

    async def _exec(chunk: BackfillChunk, endpoint: str) -> bool:
        calls.append((chunk.chain, endpoint))
        return True

    return await drain_backfill_queue(
        queue,
        _exec,
        max_chunks=2,
        max_concurrency=1,
        endpoint_selector=lambda _chain: KEYED,
        window_gate=gate,
    )


def test_drain_defers_near_cap_then_fires_after_reset(tmp_path: Path) -> None:
    ledger = ProviderLedger(path=tmp_path / "ledger.json")
    now = 1_000_000.0
    _seed(ledger, KEYED, consumed_units=90, budget_units=100, reset_at=now + 120)

    def gate(chain: str, endpoint: str) -> WindowDecision:
        return window_decision(chain, endpoint, now=now, ledger=ledger)

    calls: list[tuple[str, str]] = []
    deferred = asyncio.run(_run_drain(tmp_path, gate, calls))
    assert deferred["window_deferred"] == 1
    assert deferred["dispatched"] == 0
    assert calls == []

    # Window resets → the same chunk (still queued) now dispatches.
    ledger.reset(provider_id_for_endpoint(KEYED), key_id_for_endpoint(KEYED), now=now + 121)

    def gate_open(chain: str, endpoint: str) -> WindowDecision:
        return window_decision(chain, endpoint, now=now + 121, ledger=ledger)

    fired = asyncio.run(_run_drain(tmp_path, gate_open, calls))
    assert fired["window_deferred"] == 0
    assert fired["dispatched"] == 1
    assert calls == [("ethereum", KEYED)]


def test_drain_without_gate_still_dispatches(tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []
    stats = asyncio.run(_run_drain(tmp_path, None, calls))
    assert stats["dispatched"] == 1
    assert calls == [("ethereum", KEYED)]
