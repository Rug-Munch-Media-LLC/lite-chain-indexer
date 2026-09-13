# On-chain-verifiable index attestations

A pre-indexed dataset (ClickHouse rows, a Parquet chunk, an NDJSON flat
file) can be verified against the chain **without trusting the producer**.
The indexer commits every successfully written block range as a leaf of an
incremental append-only SHA-256 Merkle accumulator and publishes the 32-byte
root. A verifier needs only the root, an O(log n) inclusion path, and a block
hash obtained from the chain itself.

## What a leaf commits to

```
leaf = {
  "chain":        "ethereum",
  "from_block":   20000000,
  "to_block":     20000049,
  "block_hashes": ["0x…", "0x…", …],   # on-chain hashes covered by the range
  "rows_digest":  "…"                   # SHA-256 of the normalized indexed rows
}
```

* `rows_digest` is SHA-256 over each row canonicalized to sorted-key compact
  JSON, with the encodings sorted before hashing — so batch row order (or
  dict key order) never changes the digest, only the content does.
* `leaf_hash = SHA-256(0x00 || canonical_leaf_json)` and
  `node_hash = SHA-256(0x01 || left || right)` (domain separated, so leaves
  and internal nodes can never be confused).

## Accumulator

The tree is append-only and incremental. Only a **frontier of complete
subtree peaks** is held in memory — `O(log n)` — and it is persisted (with
the leaf hashes) as JSON under `ATTESTATION_DIR`, written with the shared
atomic-write helper so it survives restarts. The root is the peaks folded
right-to-left. Inclusion paths for any committed range are regenerated from
the persisted leaf hashes.

## Verify a dataset

```bash
# 1. Publish/inspect the current root for a chain
lite-indexer attest show --chain ethereum

# 2. Verify a manifest (leaf + inclusion path + root) offline
lite-indexer attest verify /var/data/chain-indexer/state/attestations/ethereum-20000000-20000049.json
# exit 0 = valid, exit 2 = invalid

# 3. Pin a trusted root (e.g. one published in a signed release)
lite-indexer attest verify manifest.json --expected-root <hex-root>
```

From code, the trustless flat-file check is one call:

```python
from lite_indexer.attestation import verify_chunk, verify_manifest

assert verify_manifest(manifest, expected_root=TRUSTED_ROOT)
assert verify_chunk(parquet_rows, manifest)          # does this chunk match?
assert verify_manifest(manifest, expected_block_hash=rpc_block_hash)
```

The block-hash binding is what ties the dataset to the real chain: a verifier
fetches a block hash from any RPC (or a trusted header source) and checks it
is one of the leaf's committed hashes. Nothing about the producer is trusted —
only SHA-256 and the chain's own hashes.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `ATTESTATION_ENABLED` | `1` | Write attestations on each committed range. `0` disables (no-op). |
| `ATTESTATION_DIR` | `$DATA_DIR/state/attestations` | Accumulator state + manifest directory. |

Attestation is **write-only/observe**: it runs off the event loop (bounded
thread), never blocks or aborts the sync path, and any failure is logged and
swallowed. It does not change what is written to ClickHouse.
