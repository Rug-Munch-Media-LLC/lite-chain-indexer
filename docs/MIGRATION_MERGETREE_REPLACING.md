# Migrating MergeTree → ReplacingMergeTree + PARTITION BY (wallet_memory)

**Status:** runbook · owner=Data Platform · last_updated=2026-09-14
**Tooling:** `scripts/migrate-ch-merge-to-replacing.sh`
**Target tables:** `evm_transfers`, `internal_transfers`, `solana_transfers`
**Server:** ClickHouse **26.7.1** (atomic `wallet_memory` database)

Conversion is **staged, idempotent, resumable and reversible**. The script has
no `DROP` statement: the original data is retained as `<table>_old_<timestamp>`
and can be swapped back.

---

## 1. Why

| Need | What the migration changes |
|---|---|
| Dedup on re-index (rewind backfill) | Engine `MergeTree` → `ReplacingMergeTree()` — collapses duplicate rows on the ORDER BY key during merges / `FINAL`. Lets `BACKFILL_INCLUDE_REWINDS=1` run without row explosion. |
| Partition-level TTL + cheaper merges at 2B rows | Adds `PARTITION BY toYYYYMM(block_timestamp)` (no partition key today). Whole months can be dropped by TTL and merged independently. |

### Hard constraint (verified)

In-place engine changes are impossible on 26.7.1 — both forms are syntax errors:

```
ALTER TABLE wallet_memory.internal_transfers__migtest ENGINE = MergeTree()
  Code: 62 ... failed at position 55 (ENGINE): ... (SYNTAX_ERROR)

ALTER TABLE wallet_memory.internal_transfers__migtest MODIFY ENGINE = MergeTree()
  Code: 62 ... failed at position 62 (ENGINE): ... (SYNTAX_ERROR)
```

Therefore: **create-new → copy → swap**.

---

## 2. Table inventory (live, 2026-09-14)

| Table | Rows | On disk | Engine | Partition | ORDER BY | Table TTL (live) |
|---|---:|---:|---|---|---|---|
| `evm_transfers` | 2 019 904 327 | 397 GB | MergeTree | — | `(chain, block_number, log_index)` | `ingested_at + 90d` |
| `internal_transfers` | 1 353 615 | 119 MB | MergeTree | — | `(chain, block_number, tx_hash, trace_address)` | `block_timestamp + 90d` |
| `solana_transfers` | 4 397 | 370 KB | MergeTree | — | `(signature, instruction_path)` | `ingested_at + 30d` |

`evm_transfers` also carries three **projections** (`evm_transfers_summary`,
`wallet_out_proj`, `wallet_in_proj`) and two column-level TTLs
(`token_symbol`/`token_name`, 30d).

`CREATE TABLE new AS old ENGINE = ...` preserves columns, codecs, column TTLs
and projections, but **drops the table-level TTL** — the script re-states it
explicitly.

---

## 3. PARTITION BY recommendation: **month**, not chain

**Use `toYYYYMM(block_timestamp)`.**

* **Cardinality / part sizing.** `evm_transfers` holds 17 chains. Partitioning by
  `chain` yields 17 partitions, the largest ~457M rows (~90 GB) — every merge,
  TTL rewrite and mutation would rewrite a single huge part, and backfills land
  entirely in one partition. Monthly partitions keep parts an order of magnitude
  smaller and let independent months merge/expire in parallel.
* **TTL.** Retention is defined in days on a timestamp. A month partition is the
  natural TTL drop unit (`DETACH/DROP PARTITION`), whereas a chain partition can
  only expire via row-level TTL rewrites that touch live data.
* **Data distribution.** The table spans 2015→2026 (historical backfill gaps),
  with the bulk in recent months (2026-08 alone = 1.23B rows). Monthly
  partitions separate the small historical tail from the hot current month,
  which is exactly what partition pruning needs.
* **Partition count.** ~77 distinct months, well under the ~200–1000 practical
  ceiling; each is small except the current months.

Timestamp column verified in the live schema: **all three tables use
`block_timestamp`** (there is no `block_time` column; `solana_transfers` uses
`block_timestamp DateTime64(3)`).

> **TTL alignment note.** For a month partition to be *dropped whole* by TTL, the
> TTL column must be the partition column. Today `evm_transfers` and
> `solana_transfers` TTL on `ingested_at`, while the partition is on
> `block_timestamp`. The script preserves the **live** TTL by default so this
> migration does not silently change retention. See §9 for the follow-up.

---

## 4. Copy-unit choice (important at 2B rows)

The source has **no partition key**, so "copy partition-by-partition" via
`WHERE toYYYYMM(block_timestamp)=…` is a **full scan per month** (77 scans of
400 GB for `evm_transfers`). The script therefore defaults `evm_transfers` and
`internal_transfers` to **chain units** — `WHERE chain='<c>'` is a primary-key
prefix range scan (17 and N units), cheap and resumable. `solana_transfers` uses
month units. Override with `--unit chain|partition`; a warning is printed when a
month unit would full-scan an unpartitioned source.

---

## 5. Pre-flight (do all of it)

1. **Disk headroom — currently FAILING for `evm_transfers`.**
   `/data/clickhouse` has **289 GB free**, but a copy of the 397 GB table needs
   **~497 GB** (table × 1.25 merge headroom). The script aborts (non-dry-run)
   when headroom is short. Before migrating you must either:
   * grow the volume to ≥ ~520 GB free, **or**
   * shrink `evm_transfers` first (let the export job prune cold days, or drop
     the small historical tail you don't need), then re-run pre-flight.

   `internal_transfers` (119 MB) and `solana_transfers` (370 KB) can migrate
   immediately.

2. **Backups.** Confirm the latest snapshot is good:
   * big store: `scripts/clickhouse-snapshot.sh` (check its manifest/checksum)
   * small state: `backup-lite-indexer-state.sh`
   Do **not** start the copy without a verified snapshot.

3. **Freeze writers.** Stop ingestion for the window (`systemctl stop` the
   indexer units / pause producers). Writes racing the swap would land on the
   new table and be lost on rollback. Confirm no active inserts:
   `SELECT query FROM system.processes WHERE query LIKE 'INSERT%'`.

4. **Validate the approach on clones** (no production data copied):
   `scripts/migrate-ch-merge-to-replacing.sh --validate-clone`
   → creates `<table>__migtest`, inserts ≤10 000 rows, verifies engine /
   partition / sorting key / TTL / raw count, drops the clone. Evidence in §7.

5. **Dry-run** to print the exact DDL, copy units and swap/rollback statements:
   `scripts/migrate-ch-merge-to-replacing.sh --dry-run`

---

## 6. Maintenance-window procedure

```bash
cd /path/to/lite-chain-indexer

# 0) verified snapshot exists + writers stopped (see §5)
scripts/migrate-ch-merge-to-replacing.sh --dry-run

# 1) create the empty <table>_new (merges frozen on it) — cheap, do first
scripts/migrate-ch-merge-to-replacing.sh --phase create

# 2) resumable copy + per-unit and total row-count parity
#    (re-run after any interruption; completed units are skipped)
scripts/migrate-ch-merge-to-replacing.sh --phase copy

# 3) swap only after parity passes: EXCHANGE TABLES + rename old aside
scripts/migrate-ch-merge-to-replacing.sh --phase swap
```

(`--phase all` = create+copy+swap in one run.) The copy sets
`optimize_on_insert=0` and issues `SYSTEM STOP MERGES <table>_new` for the whole
window; merges resume automatically after the swap. This is required because
**ReplacingMergeTree dedups during merges** — without freezing, raw row counts
would drift below the source and parity would fail.

Per-table:
`--table evm_transfers|internal_transfers|solana_transfers`.
Resume state lives in `MIGRATION_STATE_DIR` (default
`/var/lib/lite-indexer/migration-merge-to-replacing`), fingerprinted to the
target DDL; a changed DDL refuses to resume.

### Expected duration / risk

| Table | Copy volume | Risk |
|---|---:|---|
| `evm_transfers` | ~397 GB read + write (17 chain units) | **High** — long window, disk-bound, blocking rollback loses post-swap writes |
| `internal_transfers` | ~119 MB | Low — seconds |
| `solana_transfers` | ~370 KB | Negligible |

Budget the `evm_transfers` window generously (hours, IO-bound) and monitor
`system.merges`, `system.parts`, and free space throughout. A failed/interrupted
copy is safe: re-run `--phase copy`; the target is untouched until swap.

### Parity checks performed

* per copy unit: `count(old WHERE unit) == count(new WHERE unit)`
* total: `count(old) == count(new)`
* any mismatch **aborts before the swap**.

`FINAL` counts are *not* used for parity (dedup is intentional and lazy); the
drop in `FINAL` vs raw is the dedup win (e.g. evm sample: 9 184 raw → 4 800
distinct keys).

---

## 7. Validation evidence (clone, 2026-09-14)

`--validate-clone` output (production tables untouched; clones dropped after):

```
--- clone report: wallet_memory.evm_transfers__migtest ---
engine:        ReplacingMergeTree
partition_key: toYYYYMM(block_timestamp)
sorting_key:   chain, block_number, log_index
engine_full:   ReplacingMergeTree PARTITION BY toYYYYMM(block_timestamp)
               ORDER BY (chain, block_number, log_index)
               TTL ingested_at + toIntervalDay(90)
               SETTINGS index_granularity = 8192,
                        deduplicate_merge_projection_mode = 'rebuild'
expected_raw=10000 inserted_raw=10000 deduped_final=4800

--- clone report: wallet_memory.internal_transfers__migtest ---
engine:        ReplacingMergeTree
partition_key: toYYYYMM(block_timestamp)
sorting_key:   chain, block_number, tx_hash, trace_address
expected_raw=10000 inserted_raw=10000 deduped_final=10000

--- clone report: wallet_memory.solana_transfers__migtest ---
engine:        ReplacingMergeTree
partition_key: toYYYYMM(block_timestamp)
sorting_key:   signature, instruction_path
expected_raw=4397 inserted_raw=4397 deduped_final=4245
```

Notes proven by the clone run:

* `evm_transfers` has projections → ReplacingMergeTree needs
  `deduplicate_merge_projection_mode = 'rebuild'` (default `'throw'` errors
  with `SUPPORT_IS_DISABLED`).
* Explicit `TTL` is required — `CREATE … AS … ENGINE=` drops the table TTL.
* `EXCHANGE TABLES` works across differing engines/structures in the atomic DB.
* Syntax/static checks: `bash -n` OK, `shellcheck -S warning` clean.

---

## 8. Rollback

The old data is retained (never dropped) as
`wallet_memory.<table>_old_<timestamp>`. The swap printed the exact rollback
command at run time; it is:

```sql
EXCHANGE TABLES wallet_memory.<table> AND wallet_memory.<table>_old_<timestamp>;
```

Run it with writers frozen. This restores the original MergeTree table
instantly (metadata swap). Any writes accepted by the new table after the swap
are not present in the old data and must be replayed from the source/RPC if the
rollback is not immediate.

Full disaster path: restore from `scripts/clickhouse-snapshot.sh` snapshot.

---

## 9. Follow-ups (after a successful swap)

1. **Enable rewind dedup:** set `BACKFILL_INCLUDE_REWINDS=1`
   (`lite_indexer/config/indexing.py`, consumed by `_sidecars.py`) so rewind
   ranges are re-indexed; `ReplacingMergeTree` now absorbs the duplicates.
2. **Align TTL with the partition key** so month partitions can be dropped
   whole. Live TTLs are `ingested_at` for `evm_transfers`/`solana_transfers`;
   `migrations/012_ttl_retention.sql` declares the canonical policy as
   `block_timestamp` (evm 180d, solana 90d). Decide with Data Platform and set
   the new TTL **at migration time** (edit `TTL_EXPR` in the script) or via a
   new `migrations/017_*.sql`. [REVIEW — live schema drifted from mig 012:
   evm shows `ingested_at+90d` (mig 012 says `block_timestamp+180d`); solana
   shows `ingested_at+30d` (docs say 90d).]
3. **Re-verify query plans** on the hot wallet lookups (`wallet_in_proj` /
   `wallet_out_proj`) — projections are preserved, but confirm with
   `EXPLAIN`/`system.query_log`.
4. **Reclaim space** only after soak: `DROP TABLE wallet_memory.<table>_old_<ts>`
   once rollback is no longer needed (manual, explicit, not automated).
5. **Add a schema migration** (`migrations/017_merge_to_replacing.sql`) that is
   a no-op if the tables are already ReplacingMergeTree, so fresh deployments
   converge on the same engine without running the copy path.

---

## 10. Verification queries

```sql
-- engines / partitioning after swap
SELECT name, engine, partition_key, sorting_key
FROM system.tables
WHERE database='wallet_memory'
  AND name IN ('evm_transfers','internal_transfers','solana_transfers');

-- raw vs deduplicated (dedup is lazy; FINAL forces it at query time)
SELECT count() AS raw, count() FROM ... ; -- no-op form, run per table
SELECT count() FROM wallet_memory.evm_transfers FINAL;  -- expensive at 2B

-- part hygiene + disk
SELECT table, count() parts, formatReadableSize(sum(bytes_on_disk)) sz
FROM system.parts WHERE database='wallet_memory' AND active
  AND table LIKE '%transfers%' GROUP BY table;
```
