"""On-chain-verifiable index attestations.

Prove that an indexed block range corresponds to the real chain — without
trusting the producer. A pre-indexed dataset (ClickHouse rows / Parquet
chunk / NDJSON flat file) is committed as a leaf of an **incremental
append-only SHA-256 Merkle accumulator** whose root is a compact commitment
over every committed range. A verifier needs only:

* the leaf (the range, the on-chain block hashes it covers, and a digest of
  the normalized indexed rows),
* an O(log n) Merkle inclusion path,
* the accumulator root (publish it anywhere — it is 32 bytes),

and can independently (a) recompute the leaf digest from the dataset,
(b) prove the leaf is in the committed root, and (c) check the leaf's block
hash against a block hash obtained from the chain itself. No provider, no
producer, and no trust in this indexer is required.

Design:

* Leaf canonical encoding is deterministic JSON
  ``{block_hashes, chain, from_block, rows_digest, to_block}`` with sorted
  keys; ``rows_digest`` is SHA-256 over the normalized rows sorted by their
  canonical encoding, so row order in a batch does not change the digest.
* ``leaf_hash = SHA-256(0x00 || leaf_bytes)`` and
  ``node_hash = SHA-256(0x01 || left || right)`` (domain separated).
* The accumulator keeps only a **frontier of complete-subtree peaks**
  (O(log n) memory); appending is incremental and the root is the peaks
  folded right-to-left. All leaf hashes are persisted (32 bytes each) so an
  inclusion path for any committed range can be regenerated; the bounded
  structure is the frontier, exposed as :attr:`RangeAttestor.frontier_size`.
* State is JSON under ``ATTESTATION_DIR`` written with the shared atomic
  write helper, so it survives restarts.

The generator is write-only/observe by default (``ATTESTATION_ENABLED``) and
must never block or abort the sync path: :func:`record_range_attestation`
logs and returns ``None`` on any failure.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from lite_indexer.config import ATTESTATION_DIR, ATTESTATION_ENABLED
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.state import atomic_write_json

logger = get_logger(__name__)

VERSION = 1

#: Hash of an empty accumulator (no leaves appended yet).
EMPTY_ROOT: bytes = hashlib.sha256(b"").digest()


# ── Canonical encoding + hashing ───────────────────────────────────────


def canonical_leaf(
    chain: str,
    from_block: int,
    to_block: int,
    block_hashes: Sequence[str],
    rows_digest: str,
) -> dict[str, Any]:
    """Canonical leaf dict for one committed range (deterministic JSON)."""
    return {
        "block_hashes": [str(h).lower() for h in block_hashes],
        "chain": str(chain),
        "from_block": int(from_block),
        "rows_digest": str(rows_digest),
        "to_block": int(to_block),
    }


def leaf_bytes(leaf: Mapping[str, Any]) -> bytes:
    """Deterministic byte encoding of a leaf (sorted keys, compact)."""
    return json.dumps(dict(leaf), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def leaf_hash(leaf: Mapping[str, Any]) -> bytes:
    """Domain-separated leaf hash (``0x00`` prefix)."""
    return hashlib.sha256(b"\x00" + leaf_bytes(leaf)).digest()


def node_hash(left: bytes, right: bytes) -> bytes:
    """Domain-separated internal node hash (``0x01`` prefix)."""
    return hashlib.sha256(b"\x01" + left + right).digest()


def hash_rows(rows: Iterable[Mapping[str, Any]]) -> str:
    """Stable digest of normalized indexed rows (order-independent).

    Each row is canonicalized to sorted-key compact JSON; the encodings are
    sorted before hashing, so a batch's row order (or dict key order) never
    changes the digest — only the row content does.
    """
    encoded = sorted(
        json.dumps(dict(row), sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False) for row in rows
    )
    digest = hashlib.sha256()
    for line in encoded:
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def collect_block_hashes(*event_lists: Iterable[Mapping[str, Any]]) -> list[str]:
    """Distinct, lowercased on-chain ``blockHash`` values across event lists."""
    found: set[str] = set()
    for events in event_lists:
        for event in events:
            value = event.get("blockHash")
            if isinstance(value, str) and value:
                found.add(value.lower())
    return sorted(found)


# ── Incremental accumulator internals ──────────────────────────────────


def _frontier_push(frontier: list[tuple[int, bytes]], value: bytes) -> None:
    """Append one leaf hash, merging complete subtrees into peaks."""
    height = 0
    carry = value
    while frontier and frontier[-1][0] == height:
        left = frontier.pop()[1]
        carry = node_hash(left, carry)
        height += 1
    frontier.append((height, carry))


def _frontier_root(frontier: list[tuple[int, bytes]]) -> bytes:
    """Fold the peaks (right-to-left) into the accumulator root."""
    if not frontier:
        return EMPTY_ROOT
    root = frontier[-1][1]
    for _, value in reversed(frontier[:-1]):
        root = node_hash(value, root)
    return root


def _inclusion_proof(leaves: list[bytes], index: int) -> list[bytes]:
    """Merkle inclusion path for ``index`` (promote-on-odd sibling scheme)."""
    proof: list[bytes] = []
    level = leaves
    idx = index
    while len(level) > 1:
        sibling = idx ^ 1
        if sibling < len(level):
            proof.append(level[sibling])
        nxt: list[bytes] = []
        for j in range(0, len(level), 2):
            nxt.append(node_hash(level[j], level[j + 1]) if j + 1 < len(level) else level[j])
        level = nxt
        idx //= 2
    return proof


def _verify_proof(leaf: bytes, index: int, leaf_count: int, proof: Sequence[bytes], root: bytes) -> bool:
    """Recompute the root from ``leaf`` + ``proof`` and compare to ``root``."""
    node = leaf
    idx = index
    level_size = leaf_count
    position = 0
    while level_size > 1:
        sibling = idx ^ 1
        if sibling < level_size:
            if position >= len(proof):
                return False
            node = node_hash(node, proof[position]) if idx % 2 == 0 else node_hash(proof[position], node)
            position += 1
        # Odd node promoted with no sibling: node unchanged.
        idx //= 2
        level_size = (level_size + 1) // 2
    return position == len(proof) and node == root


def _range_key(from_block: int, to_block: int) -> str:
    return f"{int(from_block)}:{int(to_block)}"


# ── RangeAttestor ──────────────────────────────────────────────────────


class RangeAttestor:
    """Persisted incremental Merkle accumulator over committed ranges."""

    def __init__(self, chain: str, state_dir: Path | str | None = None) -> None:
        self.chain = chain
        self._dir = Path(state_dir) if state_dir is not None else ATTESTATION_DIR
        self._state_path = self._dir / f"{chain}-attestation.json"
        self._leaves: list[bytes] = []
        self._frontier: list[tuple[int, bytes]] = []
        # range key -> {"index": int, "leaf": <canonical leaf dict>}
        self._ranges: dict[str, dict[str, Any]] = {}
        self._load()

    # -- read-only views ------------------------------------------------

    @property
    def leaf_count(self) -> int:
        """Number of committed leaves (ranges) in the accumulator."""
        return len(self._leaves)

    @property
    def frontier_size(self) -> int:
        """Number of peaks in the frontier — O(log leaf_count)."""
        return len(self._frontier)

    @property
    def frontier(self) -> list[tuple[int, bytes]]:
        """Copy of the (height, hash) frontier peaks (bounded memory)."""
        return list(self._frontier)

    @property
    def root(self) -> str:
        """Current accumulator root as a hex string."""
        return _frontier_root(self._frontier).hex()

    def ranges(self) -> list[str]:
        """Sorted range keys currently committed."""
        return sorted(self._ranges)

    def summary(self) -> dict[str, Any]:
        """Compact view for logging / the ``attest show`` CLI."""
        return {
            "chain": self.chain,
            "root": self.root,
            "leaf_count": self.leaf_count,
            "frontier_size": self.frontier_size,
            "ranges": self.ranges(),
        }

    # -- mutation -------------------------------------------------------

    def append_range(
        self,
        from_block: int,
        to_block: int,
        block_hashes: Sequence[str],
        rows_digest: str,
        *,
        persist: bool = True,
    ) -> str:
        """Append (or refresh) one committed range; return the new root.

        Idempotent for an unchanged range: re-appending the same
        ``(chain, from, to, block_hashes, rows_digest)`` leaf does not grow
        the tree (it returns the current root). A changed leaf for an
        already-committed range (e.g. after a reorg backfill) is appended as
        a new leaf so history is preserved.
        """
        leaf = canonical_leaf(self.chain, from_block, to_block, block_hashes, rows_digest)
        lh = leaf_hash(leaf)
        key = _range_key(from_block, to_block)
        existing = self._ranges.get(key)
        if existing is not None and self._leaves[int(existing["index"])] == lh:
            return self.root

        self._leaves.append(lh)
        _frontier_push(self._frontier, lh)
        self._ranges[key] = {"index": len(self._leaves) - 1, "leaf": leaf}
        if persist:
            self._save()
        return self.root

    def manifest_for(self, from_block: int, to_block: int) -> dict[str, Any] | None:
        """Manifest (leaf + inclusion path + root) for a committed range."""
        entry = self._ranges.get(_range_key(from_block, to_block))
        if entry is None:
            return None
        index = int(entry["index"])
        leaf = entry["leaf"]
        proof = _inclusion_proof(self._leaves, index)
        return {
            "version": VERSION,
            "chain": self.chain,
            "from_block": int(from_block),
            "to_block": int(to_block),
            "leaf": leaf,
            "leaf_hash": self._leaves[index].hex(),
            "index": index,
            "leaf_count": len(self._leaves),
            "proof": [p.hex() for p in proof],
            "root": self.root,
        }

    def write_manifest(
        self,
        from_block: int,
        to_block: int,
        path: Path | str | None = None,
    ) -> Path | None:
        """Write the manifest for a committed range to disk (atomic)."""
        manifest = self.manifest_for(from_block, to_block)
        if manifest is None:
            return None
        target = Path(path) if path is not None else self._dir / f"{self.chain}-{from_block}-{to_block}.json"
        atomic_write_json(target, manifest)
        return target

    # -- persistence ----------------------------------------------------

    def _load(self) -> None:
        if not self._state_path.exists():
            return
        try:
            data = json.loads(self._state_path.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("attestation_state_load_failed", chain=self.chain, error=str(exc)[:100])
            return
        try:
            for raw in data.get("leaves", []):
                self._leaves.append(bytes.fromhex(str(raw)))
            for key, entry in data.get("ranges", {}).items():
                leaf = entry.get("leaf") if isinstance(entry, dict) else None
                index = int(entry.get("index", 0)) if isinstance(entry, dict) else 0
                if leaf is not None and 0 <= index < len(self._leaves):
                    self._ranges[str(key)] = {"index": index, "leaf": leaf}
            self._frontier = []
            for value in self._leaves:
                _frontier_push(self._frontier, value)
            logger.info(
                "attestation_state_loaded",
                chain=self.chain,
                leaves=len(self._leaves),
                root=self.root[:16],
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("attestation_state_corrupt", chain=self.chain, error=str(exc)[:100])

    def _save(self) -> None:
        payload = {
            "version": VERSION,
            "chain": self.chain,
            "leaves": [value.hex() for value in self._leaves],
            "ranges": self._ranges,
        }
        atomic_write_json(self._state_path, payload)


# ── Verification ───────────────────────────────────────────────────────


def verify_manifest(
    manifest: Mapping[str, Any],
    expected_root: str | None = None,
    expected_block_hash: str | None = None,
) -> bool:
    """Verify a manifest's inclusion proof and optional on-chain binding.

    Args:
        manifest: A manifest produced by :meth:`RangeAttestor.manifest_for`.
        expected_root: Optional trusted accumulator root (hex). When given it
            must equal both the manifest root and the recomputed root.
        expected_block_hash: Optional on-chain block hash (hex) that must be
            one of the leaf's committed block hashes.

    Returns:
        True when the proof is valid and all supplied bindings match.
    """
    try:
        leaf = manifest["leaf"]
        index = int(manifest["index"])
        leaf_count = int(manifest["leaf_count"])
        recomputed_leaf = leaf_hash(leaf)
        if recomputed_leaf.hex() != str(manifest["leaf_hash"]):
            return False
        proof = [bytes.fromhex(str(h)) for h in manifest["proof"]]
        root = bytes.fromhex(str(manifest["root"]))
    except (KeyError, TypeError, ValueError):
        return False

    if not _verify_proof(recomputed_leaf, index, leaf_count, proof, root):
        return False
    if expected_root is not None and expected_root.lower() != root.hex():
        return False
    if expected_block_hash is not None:
        hashes = {str(h).lower() for h in leaf.get("block_hashes", [])}
        if expected_block_hash.lower() not in hashes:
            return False
    return True


def verify_chunk(rows: Iterable[Mapping[str, Any]], manifest: Mapping[str, Any]) -> bool:
    """Trustless flat-file check: does this chunk match the manifest digest?"""
    try:
        expected = str(manifest["leaf"]["rows_digest"])
    except (KeyError, TypeError):
        return False
    return hash_rows(rows) == expected


# ── Sync-path integration helper ───────────────────────────────────────


def record_range_attestation(
    chain: str,
    from_block: int,
    to_block: int,
    rows: Iterable[Mapping[str, Any]],
    block_hashes: Sequence[str] | None = None,
    *,
    state_dir: Path | str | None = None,
    write_manifest_file: bool = True,
) -> str | None:
    """Append a committed range to the chain's attestation accumulator.

    Write-only/observe: a no-op when ``ATTESTATION_ENABLED`` is off, and any
    failure is logged and swallowed (the sync path must never be blocked or
    aborted by attestation). Returns the new root hex, or ``None``.
    """
    if not ATTESTATION_ENABLED:
        return None
    try:
        digest = hash_rows(rows)
        attestor = RangeAttestor(chain, state_dir=state_dir)
        root = attestor.append_range(from_block, to_block, list(block_hashes or []), digest)
        if write_manifest_file:
            attestor.write_manifest(from_block, to_block)
        metrics_incr("attestations_appended")
        logger.info(
            "attestation_appended",
            chain=chain,
            from_block=from_block,
            to_block=to_block,
            root=root[:16],
            leaf_count=attestor.leaf_count,
        )
        return root
    except Exception as exc:  # noqa: BLE001 — attestation is best-effort
        logger.warning("attestation_failed", chain=chain, error=str(exc)[:120])
        return None


__all__ = [
    "EMPTY_ROOT",
    "RangeAttestor",
    "canonical_leaf",
    "collect_block_hashes",
    "hash_rows",
    "leaf_bytes",
    "leaf_hash",
    "node_hash",
    "record_range_attestation",
    "verify_chunk",
    "verify_manifest",
]
