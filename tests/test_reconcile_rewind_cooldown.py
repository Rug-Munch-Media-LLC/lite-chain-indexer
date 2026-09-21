"""Cooldown gate for reconciliation rewinds (2026-09-21 incident follow-up)."""

from lite_indexer.indexer._maintenance import rewind_cooldown_ok


def test_rewind_cooldown_fresh_then_throttled(tmp_path) -> None:
    assert rewind_cooldown_ok("robinhood", now=1_000.0, base_dir=tmp_path) is True
    # within window -> throttled
    assert rewind_cooldown_ok("robinhood", now=1_060.0, base_dir=tmp_path) is False
    # after the window -> allowed again
    assert rewind_cooldown_ok("robinhood", now=1_000.0 + 21_601, base_dir=tmp_path) is True


def test_rewind_cooldown_is_per_chain(tmp_path) -> None:
    assert rewind_cooldown_ok("arbitrum", now=2_000.0, base_dir=tmp_path) is True
    assert rewind_cooldown_ok("hyperliquid", now=2_000.0, base_dir=tmp_path) is True
