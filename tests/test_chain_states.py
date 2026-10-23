"""Tests for the parked-chain declaration contract (chain-states.yml)."""

from __future__ import annotations

import pytest

from lite_indexer.chain_states import ChainStatesError, load_parked


def _write(tmp_path, body: str):
    path = tmp_path / "chain-states.yml"
    path.write_text(body)
    return str(path)


def test_missing_file_means_nothing_parked(tmp_path):
    """No declarations file is a valid 'every chain must meet the cadence'."""
    assert load_parked(str(tmp_path / "absent.yml")) == {}


def test_empty_parked_map_is_valid(tmp_path):
    """The shipped file parks no chain, so it must load as an empty map."""
    assert load_parked(_write(tmp_path, "parked: {}\n")) == {}


def test_declaration_round_trips(tmp_path):
    path = _write(
        tmp_path,
        "parked:\n"
        "  bsc:\n"
        "    reason: 'transfers demand-gated; dex and deployer streams only'\n"
        "    since: 2026-10-31\n"
        "    review_by: 2026-11-30\n",
    )
    decl = load_parked(path, known={"bsc"})["bsc"]
    assert decl.reason.startswith("transfers demand-gated")
    assert decl.review_by == "2026-11-30"


@pytest.mark.parametrize("reason", ["n/a", "tbd", "todo", "too short", ""])
def test_shrug_reason_is_rejected(tmp_path, reason):
    """A reason must explain, never shrug: that is a disabled chain."""
    path = _write(tmp_path, f"parked:\n  bsc:\n    reason: '{reason}'\n")
    with pytest.raises(ChainStatesError, match="reason"):
        load_parked(path)


def test_unknown_chain_is_rejected(tmp_path):
    """A typo must not silence a chain that is actually running."""
    path = _write(tmp_path, "parked:\n  notachain:\n    reason: 'this chain does not exist here'\n")
    with pytest.raises(ChainStatesError, match="not run by this daemon"):
        load_parked(path, known={"bsc", "base"})


def test_unknown_key_is_rejected(tmp_path):
    path = _write(
        tmp_path,
        "parked:\n  bsc:\n    reason: 'transfers demand-gated; dex and deployer only'\n    because: shrug\n",
    )
    with pytest.raises(ChainStatesError, match="unknown key"):
        load_parked(path)
