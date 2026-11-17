"""pm-producer exit code: a partial cycle is a success, a total failure is not.

INCIDENT 2026-11-17
    Polymarket RETIRED data-api.polymarket.com -- HTTP 410 Gone from Cloudflare
    on every path, while gamma-api returns 200 from the same IP (so it is not an
    IP block). trades.py and holders.py call DATA_API; markets.py calls GAMMA_API.

    The CLI returned 1 if ANY dataset errored, so one retired upstream failed the
    whole unit forever: markets wrote 1,004 rows every cycle while pm-producer sat
    in `failed` state from 2026-11-02, and pm_trades froze there.

A health signal that reports failure while data IS landing is worse than no
signal: it hides the datasets that work. The per-dataset results already carry
which failed and why, so the exit code only needs to answer "did this cycle
produce anything at all".
"""

from __future__ import annotations

import importlib
from unittest.mock import patch

import pytest

RUN = importlib.import_module("lite_indexer.producers.polymarket.run")


def _main_with(datasets: dict) -> int:
    """Run the CLI with ``run()`` stubbed to return ``datasets``."""
    with patch.object(RUN, "run", lambda *a, **k: {"datasets": datasets}):
        return RUN.main(["--json"])


def test_partial_cycle_succeeds(capsys):
    """markets ok + retired trades/holders endpoints must NOT fail the unit."""
    rc = _main_with(
        {
            "markets": {"status": "ok", "rows": 1004},
            "trades": {"status": "error", "error": "410 Gone"},
            "holders": {"status": "error", "error": "410 Gone"},
        }
    )

    assert rc == 0, "one retired upstream must not fail a cycle that landed rows"
    out = capsys.readouterr().out
    assert '"markets"' in out and '"rows": 1004' in out


def test_all_datasets_ok_succeeds():
    rc = _main_with({k: {"status": "ok"} for k in ("markets", "trades", "holders")})
    assert rc == 0


def test_total_failure_still_fails():
    """If NOTHING landed, the unit must report failure -- the original intent."""
    rc = _main_with({k: {"status": "error"} for k in ("markets", "trades", "holders")})
    assert rc == 1


@pytest.mark.parametrize("failed", [["markets"], ["trades"], ["holders"]])
def test_any_single_success_keeps_rc_zero(failed):
    """Symmetric check: no one dataset can veto the others."""
    datasets = {
        k: ({"status": "error"} if k in failed else {"status": "ok"})
        for k in ("markets", "trades", "holders")
    }
    assert _main_with(datasets) == 0


def test_all_skipped_is_not_a_failure():
    """A cycle where nothing was due has not failed."""
    rc = _main_with({k: {"status": "skipped"} for k in ("markets", "trades", "holders")})
    assert rc == 0
