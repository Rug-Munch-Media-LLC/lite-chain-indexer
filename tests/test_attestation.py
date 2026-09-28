"""On-chain-verifiable index attestation tests (lite_indexer/attestation.py).

Contracts:

* append is deterministic (same leaves → same root) and idempotent for an
  unchanged range;
* a Merkle inclusion path verifies for every committed range;
* tampering with the rows digest, a block hash, or a proof hash fails;
* state persists across instances (restart);
* memory stays bounded (frontier O(log n));
* disabled → write-side no-op; verify_chunk matches/mismatches;
* the ``attest`` CLI verifies a manifest and shows accumulator state.

Hermetic — no network, no ClickHouse, tmp dirs only.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pytest

from lite_indexer import attestation
from lite_indexer.attestation import (
    RangeAttestor,
    collect_block_hashes,
    hash_rows,
    record_range_attestation,
    verify_chunk,
    verify_manifest,
)
from lite_indexer.attestation_record import AttestOptions  # noqa: F401
from lite_indexer.cli._attest import run_attest_cli


def _rows(n: int, base: int = 0) -> list[dict[str, object]]:
    return [
        {
            "blockNumber": base + i,
            "txHash": "0x" + f"{i:064x}",
            "from": "0x" + "aa" * 20,
            "to": "0x" + "bb" * 20,
            "value": str(i),
        }
        for i in range(n)
    ]


def _append(attestor: RangeAttestor, start: int, count: int = 1) -> str:
    return attestor.append_range(start, start + count - 1, [f"0x{start:064x}"], hash_rows(_rows(count, start)))


# ── append + root stability ─────────────────────────────────────────────


def test_root_is_deterministic_across_instances(tmp_path: Path) -> None:
    a = RangeAttestor("ethereum", state_dir=tmp_path)
    b = RangeAttestor("ethereum", state_dir=tmp_path / "other")
    for start in (100, 200, 300):
        assert _append(a, start) == _append(b, start)
    assert a.root == b.root
    assert a.leaf_count == b.leaf_count == 3


def test_append_is_idempotent_for_unchanged_range(tmp_path: Path) -> None:
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    root = _append(attestor, 100)
    assert _append(attestor, 100) == root
    assert attestor.leaf_count == 1


def test_changed_leaf_appends_a_new_leaf(tmp_path: Path) -> None:
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    _append(attestor, 100)
    first_root = attestor.root
    # Same range, different rows digest (e.g. a reorg backfill) → new leaf.
    attestor.append_range(100, 100, ["0x" + "11" * 32], hash_rows(_rows(1, 100)))
    assert attestor.leaf_count == 2
    assert attestor.root != first_root


# ── inclusion proofs ────────────────────────────────────────────────────


def test_inclusion_proofs_verify_for_every_leaf(tmp_path: Path) -> None:
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    for start in range(0, 64, 10):
        _append(attestor, start, count=5)
    for start in range(0, 64, 10):
        manifest = attestor.manifest_for(start, start + 4)
        assert manifest is not None
        assert verify_manifest(manifest) is True
        assert verify_manifest(manifest, expected_root=attestor.root) is True


def test_manifest_missing_range_returns_none(tmp_path: Path) -> None:
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    _append(attestor, 100)
    assert attestor.manifest_for(999, 1000) is None


# ── tamper detection ────────────────────────────────────────────────────


def test_mutated_rows_digest_fails(tmp_path: Path) -> None:
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    _append(attestor, 100)
    manifest = attestor.manifest_for(100, 100)
    assert manifest is not None
    manifest["leaf"]["rows_digest"] = "0" * 64
    assert verify_manifest(manifest) is False


def test_mutated_block_hash_binding_fails(tmp_path: Path) -> None:
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    _append(attestor, 100)
    manifest = attestor.manifest_for(100, 100)
    assert manifest is not None
    on_chain = manifest["leaf"]["block_hashes"][0]
    assert verify_manifest(manifest, expected_block_hash=on_chain) is True
    assert verify_manifest(manifest, expected_block_hash="0x" + "de" * 32) is False


def test_mutated_proof_fails(tmp_path: Path) -> None:
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    for start in range(0, 40, 10):
        _append(attestor, start, count=5)
    manifest = attestor.manifest_for(20, 24)
    assert manifest is not None
    manifest["proof"][0] = "0" * 64
    assert verify_manifest(manifest) is False


def test_wrong_expected_root_fails(tmp_path: Path) -> None:
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    _append(attestor, 100)
    manifest = attestor.manifest_for(100, 100)
    assert manifest is not None
    assert verify_manifest(manifest, expected_root="ab" * 32) is False


# ── persistence + bounded memory ────────────────────────────────────────


def test_state_persists_across_instances(tmp_path: Path) -> None:
    first = RangeAttestor("ethereum", state_dir=tmp_path)
    for start in (100, 200, 300):
        _append(first, start)
    root = first.root

    reloaded = RangeAttestor("ethereum", state_dir=tmp_path)
    assert reloaded.leaf_count == 3
    assert reloaded.root == root
    assert reloaded.ranges() == ["100:100", "200:200", "300:300"]
    manifest = reloaded.manifest_for(200, 200)
    assert manifest is not None
    assert verify_manifest(manifest, expected_root=root) is True


def test_frontier_is_bounded_logarithmic(tmp_path: Path) -> None:
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    for start in range(0, 1000):
        _append(attestor, start)
    assert attestor.leaf_count == 1000
    # Peaks ≤ floor(log2(n)) + 1.
    assert attestor.frontier_size <= (1000).bit_length()
    assert len(attestor.frontier) == attestor.frontier_size


# ── verify_chunk (trustless flat-file check) ────────────────────────────


def test_verify_chunk_matches_and_mismatches(tmp_path: Path) -> None:
    rows = _rows(10, 500)
    root = record_range_attestation(
        "ethereum",
        500,
        509,
        rows,
        options=AttestOptions(state_dir=tmp_path, block_hashes=["0x" + "ab" * 32]),
    )
    assert root is not None
    manifest = json.loads((tmp_path / "ethereum-500-509.json").read_text())

    assert verify_chunk(rows, manifest) is True
    # Reordered rows are still the same chunk (order-independent digest).
    assert verify_chunk(list(reversed(rows)), manifest) is True
    # Mutated content fails.
    tampered = [dict(r) for r in rows]
    tampered[0]["value"] = "999999"
    assert verify_chunk(tampered, manifest) is False


# ── disabled no-op ──────────────────────────────────────────────────────


def test_disabled_recording_is_noop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys.modules["lite_indexer.attestation_record"], "ATTESTATION_ENABLED", False)
    assert record_range_attestation("ethereum", 1, 2, _rows(1), options=AttestOptions(state_dir=tmp_path)) is None
    assert not list(tmp_path.glob("*.json"))


# ── collect_block_hashes ────────────────────────────────────────────────


def test_collect_block_hashes_dedupes_and_lowercases() -> None:
    events = [{"blockHash": "0xAA"}, {"blockHash": "0xaa"}, {"blockHash": ""}]
    others = [{"blockHash": "0xbb"}]
    assert collect_block_hashes(events, others) == ["0xaa", "0xbb"]
    assert collect_block_hashes([]) == []


# ── CLI ─────────────────────────────────────────────────────────────────


def test_cli_verify_and_show(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(attestation, "ATTESTATION_DIR", tmp_path)
    record_range_attestation("ethereum", 100, 109, _rows(10, 100), options=AttestOptions(state_dir=tmp_path))

    manifest_path = tmp_path / "ethereum-100-109.json"
    assert (
        run_attest_cli(argparse.Namespace(attest_action="verify", manifest=str(manifest_path), expected_root=None)) == 0
    )

    bad = json.loads(manifest_path.read_text())
    bad["root"] = "cd" * 32
    bad_path = tmp_path / "bad.json"
    bad_path.write_text(json.dumps(bad))
    assert run_attest_cli(argparse.Namespace(attest_action="verify", manifest=str(bad_path), expected_root=None)) == 2

    assert run_attest_cli(argparse.Namespace(attest_action="show", chain="ethereum")) == 0


async def test_attest_range_passes_block_hashes_through_options() -> None:
    """attest_range must bind block_hashes to AttestOptions, never positionally.

    Round 4 changed record_range_attestation's 5th positional from
    block_hashes to options. A positional call then binds the hash list to
    options and opts.block_hashes is empty, so attestation records a range
    with NO block hashes — silently. mypy caught it; 2,398 tests did not,
    because every existing test called record_range_attestation directly and
    none exercised the production path. This exercises attest_range, which is
    what the sync body calls.
    """
    from lite_indexer.indexer import _attest

    seen: dict[str, object] = {}

    def _capture(chain, from_block, to_block, rows, options):
        seen["options"] = options
        seen["rows"] = list(rows)
        return "ab" * 32

    monkey = pytest.MonkeyPatch()
    try:
        monkey.setattr(_attest, "ATTESTATION_ENABLED", True)
        monkey.setattr(_attest, "record_range_attestation", _capture)
        events = [{"tx_hash": "0xaaa", "blockNumber": 1, "logIndex": 0}]
        root = await _attest.attest_range("ethereum", 1, 2, events, [])
    finally:
        monkey.undo()

    assert root == "ab" * 32
    # The hashes must be ON the options object, not a stray positional.
    assert seen["options"] is not None
    assert getattr(seen["options"], "block_hashes", None) is not None, (
        "block_hashes must be bound via AttestOptions; a positional binding "
        "silently records a range with no block hashes"
    )


def test_sync_loop_delegates_attestation_to_the_attest_module() -> None:
    """The sync body must call attest_range, not inline the write path."""
    from lite_indexer.indexer import _sync_loop

    src = Path(_sync_loop.__file__).read_text()
    assert "await attest_range(" in src
    # The positional-misbind shape must not reappear.
    assert "                    block_hashes,\n" not in src
