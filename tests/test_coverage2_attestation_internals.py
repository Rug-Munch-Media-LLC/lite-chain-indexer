"""Coverage tests for ``lite_indexer.attestation`` Merkle internals.

Hermetic — pure hashing + a tmp-dir state file, no network. Complements
``tests/test_attestation.py`` (which drives the public RangeAttestor /
verify_manifest surface) by pinning the internal accumulator primitives
directly: domain-separated hashing, the frontier push/root fold, the
inclusion-proof builder/verifier, and the corrupt-state load guard.
"""

from __future__ import annotations

import json
from pathlib import Path

from lite_indexer.attestation import (
    EMPTY_ROOT,
    RangeAttestor,
    _frontier_push,
    _frontier_root,
    _inclusion_proof,
    _range_key,
    _verify_proof,
    canonical_leaf,
    hash_rows,
    leaf_bytes,
    leaf_hash,
    node_hash,
)

# ── canonical encoding + hashing ────────────────────────────────────────


def test_canonical_leaf_lowercases_block_hashes() -> None:
    leaf = canonical_leaf("ethereum", 1, 2, ["0xABC"], "digest")
    assert leaf["block_hashes"] == ["0xabc"]
    assert leaf["chain"] == "ethereum"
    assert leaf["from_block"] == 1
    assert leaf["to_block"] == 2


def test_leaf_bytes_is_deterministic_sorted_json() -> None:
    leaf = canonical_leaf("ethereum", 1, 2, ["0xaa"], "d")
    assert leaf_bytes(leaf) == leaf_bytes(dict(reversed(list(leaf.items()))))


def test_leaf_hash_is_domain_separated_from_node_hash() -> None:
    leaf = canonical_leaf("ethereum", 1, 2, [], "d")
    assert leaf_hash(leaf) != node_hash(leaf_hash(leaf), leaf_hash(leaf))


def test_node_hash_is_order_sensitive() -> None:
    a = b"\x01" * 32
    b = b"\x02" * 32
    assert node_hash(a, b) != node_hash(b, a)


def test_hash_rows_is_order_independent() -> None:
    rows = [{"a": 1}, {"b": 2}]
    assert hash_rows(rows) == hash_rows(list(reversed(rows)))


def test_hash_rows_changes_with_content() -> None:
    assert hash_rows([{"a": 1}]) != hash_rows([{"a": 2}])


def test_hash_rows_empty_is_stable() -> None:
    assert hash_rows([]) == hash_rows([])


# ── frontier internals ──────────────────────────────────────────────────


def test_frontier_root_of_empty_is_empty_root() -> None:
    assert _frontier_root([]) == EMPTY_ROOT


def test_frontier_push_merges_complete_subtrees() -> None:
    frontier: list[tuple[int, bytes]] = []
    for i in range(4):
        _frontier_push(frontier, bytes([i]) * 32)
    # 4 leaves → a single height-2 peak.
    assert len(frontier) == 1
    assert frontier[0][0] == 2


def test_frontier_size_is_logarithmic() -> None:
    frontier: list[tuple[int, bytes]] = []
    for i in range(8):
        _frontier_push(frontier, bytes([i]) * 32)
    assert len(frontier) == 1  # 8 = 2^3 → one peak


def test_frontier_root_matches_manual_fold() -> None:
    frontier: list[tuple[int, bytes]] = []
    leaves = [bytes([i]) * 32 for i in range(3)]
    for value in leaves:
        _frontier_push(frontier, value)
    # 3 leaves → peaks [h0, h1]; root = node_hash(peak0, peak1).
    assert _frontier_root(frontier) == node_hash(frontier[0][1], frontier[1][1])


# ── inclusion proof ─────────────────────────────────────────────────────


def test_inclusion_proof_verifies_for_single_leaf() -> None:
    leaves = [leaf_hash(canonical_leaf("ethereum", 1, 1, [], "d"))]
    proof = _inclusion_proof(leaves, 0)
    assert proof == []
    assert _verify_proof(leaves[0], 0, 1, proof, leaves[0]) is True


def test_inclusion_proof_verifies_for_every_leaf() -> None:
    leaves = [leaf_hash(canonical_leaf("ethereum", i, i, [], f"d{i}")) for i in range(5)]
    root = _frontier_root(_build_frontier(leaves))
    for index, leaf in enumerate(leaves):
        proof = _inclusion_proof(leaves, index)
        assert _verify_proof(leaf, index, len(leaves), proof, root) is True


def test_verify_proof_rejects_wrong_root() -> None:
    leaves = [leaf_hash(canonical_leaf("ethereum", i, i, [], f"d{i}")) for i in range(4)]
    proof = _inclusion_proof(leaves, 0)
    assert _verify_proof(leaves[0], 0, len(leaves), proof, b"\x00" * 32) is False


def test_verify_proof_rejects_truncated_proof() -> None:
    leaves = [leaf_hash(canonical_leaf("ethereum", i, i, [], f"d{i}")) for i in range(4)]
    root = _frontier_root(_build_frontier(leaves))
    proof = _inclusion_proof(leaves, 0)
    assert _verify_proof(leaves[0], 0, len(leaves), proof[:-1], root) is False


def _build_frontier(leaves: list[bytes]) -> list[tuple[int, bytes]]:
    frontier: list[tuple[int, bytes]] = []
    for value in leaves:
        _frontier_push(frontier, value)
    return frontier


# ── _range_key + corrupt-state load ─────────────────────────────────────


def test_range_key_format() -> None:
    assert _range_key(10, 20) == "10:20"


def test_load_ignores_corrupt_state_file(tmp_path: Path) -> None:
    state = tmp_path / "ethereum-attestation.json"
    state.write_text("{ not valid json")
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    assert attestor.leaf_count == 0
    assert attestor.root == EMPTY_ROOT.hex()


def test_load_skips_out_of_range_index(tmp_path: Path) -> None:
    state = tmp_path / "ethereum-attestation.json"
    state.write_text(
        json.dumps(
            {
                "version": 1,
                "chain": "ethereum",
                "leaves": [],
                "ranges": {"1:2": {"index": 5, "leaf": {"chain": "ethereum"}}},
            }
        )
    )
    attestor = RangeAttestor("ethereum", state_dir=tmp_path)
    assert attestor.ranges() == []
