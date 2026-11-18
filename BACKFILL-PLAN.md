# BACKFILL-PLAN.md — 90 days to B2, before the indexer runs

**Status:** plan, blocking · owner: Rug Munch Media LLC Engineering · 2026-11-18
**Operator sequence (fixed):**
1. Backfill 90 days per chain **directly to B2** — no local accumulation, no
   ClickHouse dependency.
2. Only once the archive is complete and verified, run lite-chain-indexer for
   live tip.
3. Then fix the remaining alerts.

---

## What already exists (verified by reading the code)

| piece | state |
|---|---|
| `backfill-evm-hours.py` | **works, but sources ClickHouse** — which holds ~30 days, so it cannot produce 90 |
| `biglake_source.py --to-b2` | writes **direct to B2**, no local retention — but **ethereum only** |
| SQD portal | "~13 EVM chains with complete history (blocks from 2021+ verified)" — the right source, but exposed only as `evm_traces()` with `limit=200`, query-shaped |
| Envio HyperSync | 19 chains via JSON-RPC; no B2 writer |
| Local staging | `backfill-evm-hours.py` uses `TemporaryDirectory` and auto-deletes — **already the bounded-footprint behaviour we want** |

So the disk contract is satisfied in the one tool that exists. The missing piece
is a **source-to-B2 writer for SQD/Envio**, not a disk-management fix.

---

## The one blocking decision

There is no generic "stream from source → write parquet → verify → upload to B2"
engine. Each source has its own reader with a different shape:

    biglake   query(table, chain, dates)   -> rows, truncating at LAKE_MAX_ROWS
    sqd       stream(dataset, from, to)    -> blocks, limit=200 per call
    envio     JSON-RPC over HyperSync      -> per-range

Building one writer per source is three implementations of the same verify+upload
contract. Building one **generic unit writer** that each source feeds is one
implementation and three adapters.

**Recommendation: generic writer + per-source adapters.**

    unit = (chain, table, day)
    adapter.fetch(chain, table, day) -> iterator of row dicts (bounded memory)
    writer.write_unit(rows) ->
        1. parquet to a TemporaryDirectory      (bounded local, auto-deleted)
        2. count rows
        3. upload to b2:chain-data/parquet/<chain>/<table>/<date>/
        4. re-read object size from B2 and assert it exists
        5. assert local count == manifest count
        6. return the verified unit record; temp dir dies with the context

This is exactly what `backfill-evm-hours.py` already does for ClickHouse. The
work is separating its verify/upload half from its ClickHouse-specific SQL half,
then writing three `adapter.fetch` implementations.

---

## Order of work

### Phase 1 — the generic writer (no source-specific code)
- [ ] Extract the verify+upload half of `backfill-evm-hours.py` into
      `scripts/b2_unit_writer.py`:
      `write_unit(chain, table, day, row_iter) -> {rows, bytes, key, verified}`.
- [ ] Keep `TemporaryDirectory`; never write outside it.
- [ ] Gate on B2 object existence AND row-count equality. Both, or refuse.
- [ ] Test with an already-archived day: it must report `skip` via `exists(key)`
      and touch nothing.

### Phase 2 — source adapters, widest coverage first
- [ ] **SQD adapter** — 13 EVM chains, history to 2021. Active the moment we have
      a block range to parquet row mapping. Today only `evm_traces` exists; a
      `token_transfers` mapping is required for `evm_transfers`.
- [ ] **Envio adapter** — 19 chains, covers bsc/fantom/tron/hyperliquid that SQD
      does not.
- [ ] **BigQuery adapter** — solana (SQD and Envio do not cover it).
- [ ] Skip BigLake as a backfill source: it is eth/btc only and eth is already
      covered by SQD/Envio/BigQuery.

### Phase 3 — run it, per chain, measured
- [ ] `--dry-run` each chain: report the days the source actually offers.
      **Do not assume 90.** SQD claims 2021+, but the dataset's coverage for
      *that table on that chain* is the only number that matters.
- [ ] Run to completion, recording per chain: days uploaded, rows, bytes, failures.
- [ ] DuckDB proof per chain:
      `SELECT count(*) FROM read_parquet('.../<chain>/<table>/*/*/*.parquet')`
      must equal the sum of verified unit counts.

### Phase 4 — only then, live indexing
- [ ] Start the indexer against a complete archive.
- [ ] Then the three stalls: bsc (10-block provider cap vs slice 200), ethereum
      (`-32005` size), linea (`1rpc.io` upsell in pool).

---

## Hard constraints (measured, not assumed)

- **B2 is the destination and the only retention.** Local holds one unit in a
  `TemporaryDirectory`.
- **ClickHouse is not a backfill source** — ~30 days, measured.
- **`aws-public-blockchain` v1.0/ contains exactly `btc/` and `eth/`.**
- **BigLake is eth-only**; the all-chain sources are SQD and Envio.
- **`backfill-evm-hours.py` cannot reach 90 days** while it reads ClickHouse, no
  matter what `--days` says.

## Honest status

Not started. Phases 1-4 are work, not configuration. The existing tools cover
the disk-safety half (transient staging, verify-before-upload) and one eth-only
source. The three adapters and the generic writer do not exist yet, and I am not
going to claim a backfill has run before it has.
