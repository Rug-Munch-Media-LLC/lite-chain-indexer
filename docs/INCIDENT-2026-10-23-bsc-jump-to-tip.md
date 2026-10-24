# bsc: jump to tip with a documented hole (2026-10-23)

**Decision:** bsc resumes at the chain tip. The range below is **skipped, not
indexed**, and is recorded here permanently. It is not queued for backfill and
must never be treated as indexed data.

## Skipped range (the hole)

| field | value |
| --- | --- |
| chain | `bsc` |
| skipped from | **120,079,835** (first block after the last durable cursor) |
| skipped to | **129,305,840** |
| blocks skipped | **9,226,006** (≈ 48 days) |
| tip at decision time | 129,305,845 (bsc-dataseed, 2026-10-23T10:27Z) |
| resume cursor | 129,305,840 (tip − 5 reorg buffer) |
| state backup | `/var/data/chain-indexer/state/lite_bsc.json.pre-jump-120079834` |

`evm_transfers` for bsc therefore has **no rows** between those blocks. Queries
that need continuous bsc history over 2026-09-05 → 2026-10-23 must treat the
range as unknown, not as "no transfers".

## Why (evidence)

* The cursor had not moved since **2026-10-22T07:12:54Z** (highwater
  `120,079,834`), 9.21M blocks behind tip.
* Blocker: `sync_aborted_fetch_failed` — a 200-block `eth_getLogs` window blew
  the 300 s phase budget on a pool where the only logs-capable endpoint is
  a keyed provider, whose `getlogs_max_range` is **10 blocks** (probe, 10:28:20Z).
* Backfilling 9.2M blocks at ≤20 blocks/call ≈ **460k `eth_getLogs` calls**,
  producing ~**4.4B rows** (15× the table) over an estimated 30 h on a host at
  load 39 with 494 GB free. That cost is not payable for stale history whose
  consumers (scanner, wallet history) only need recent data.

## What was NOT touched

* No rows were deleted or truncated.
* `block_checkpoints` for bsc was **not** used and **not** rebuilt: it runs
  68,651 blocks *ahead* of the load cursor and is not a source of truth for it.
* `highwater_bsc.json` was left at its forward-only value (`120,079,834`); the
  jump moves the load cursor, and the high-water mark advances on the first
  successful sync after it.

## How to close the hole later (optional, paid)

If continuous bsc history is ever required, backfill
`120,079,835 … 129,305,840` from an archive provider that serves wide
`eth_getLogs` ranges, into a staging table, then `INSERT INTO evm_transfers
SELECT …`. Do not rewrite the state file to rewind the cursor: a rewind
re-fetches from the RPC, which is exactly the cost this decision refuses.
