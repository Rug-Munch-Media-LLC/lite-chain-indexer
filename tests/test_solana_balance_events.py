"""Unit tests for the blockwise balance-delta helper (timestamp contract)."""
from typing import Any

from lite_indexer.strategies.solana.strategy import SolanaStrategy

MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
TS = "2026-09-21T08:58:30Z"


def _tx(pre: float | None, post: float | None, mint: str = MINT) -> dict[str, Any]:
    return {
        "transaction": {"signatures": ["sig1"]},
        "meta": {
            "preTokenBalances": [
                {"mint": mint, "owner": "own", "accountIndex": 3, "uiTokenAmount": {"uiAmount": pre}}
            ],
            "postTokenBalances": [
                {
                    "mint": mint,
                    "owner": "own",
                    "accountIndex": 3,
                    "address": "TokenAcct111",
                    "uiTokenAmount": {"uiAmount": post},
                }
            ],
        },
    }


def _strategy() -> SolanaStrategy:
    return SolanaStrategy(tracked_tokens=[{"mint": MINT, "symbol": "USDC", "decimals": 6, "name": "USD Coin"}])


def test_balance_events_emits_row_with_passed_block_time() -> None:
    s = _strategy()
    tracked = {t["mint"]: t for t in s.tracked_tokens}
    out = s._balance_events(_tx(1.0, 2.5), 449008180, tracked, TS)
    assert len(out) == 1
    row = out[0]
    assert row["slot"] == 449008180
    assert row["mint"] == MINT
    assert row["amount_pre"] == "1.0"
    assert row["amount_post"] == "2.5"
    assert row["change"] == "1.5"
    assert row["blockTimestamp"] == TS  # the real block time, never ingest time


def test_balance_events_zero_delta_skipped() -> None:
    s = _strategy()
    tracked = {t["mint"]: t for t in s.tracked_tokens}
    assert s._balance_events(_tx(2.5, 2.5), 1, tracked, TS) == []


def test_balance_events_untracked_mint_skipped() -> None:
    s = _strategy()
    tracked = {t["mint"]: t for t in s.tracked_tokens}
    other = "So11111111111111111111111111111111111111112"
    assert s._balance_events(_tx(0.0, 5.0, mint=other), 1, tracked, TS) == []


def test_balance_events_tolerates_missing_block_time() -> None:
    s = _strategy()
    tracked = {t["mint"]: t for t in s.tracked_tokens}
    out = s._balance_events(_tx(0.0, 1.0), 1, tracked, None)
    assert len(out) == 1 and out[0]["blockTimestamp"] is None
