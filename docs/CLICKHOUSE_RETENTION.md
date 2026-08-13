# ClickHouse Retention Policy (wallet_memory)

**Status:** canonical · owner=Data Platform · last_updated=2026-08-13

Follow-up to the **2026-08-13 disk-full outage** (`/data/clickhouse` hit
607GB; `evm_transfers` alone was 173GB). This document defines the hot-store
retention contract: which tables are exported to B2 parquet vs. which are
TTL-only, the TTL windows, how to apply the migration, and how to verify the
disk stops growing.

---

## 1. Export vs. TTL-only

The daily cold-storage job (`scripts/lite-indexer-parquet-daily.sh` →
`lite_indexer.cli export-parquet --delete-after-upload`) exports **only**
`evm_transfers` and `dex_events`, and **only** for the 8 EVM chains
(`ethereum bsc polygon arbitrum optimism base avalanche gnosis`).
`lite_indexer/storage/parquet.py:export_chain` fetches exactly
`CH_TABLE` (evm_transfers) + `CH_DEX_TABLE` (dex_events) per day, deletes
them from ClickHouse after the B2 upload verifies.

Everything else is **TTL-only** — it accumulates forever unless a table TTL
drops it.

| Table | Exported to B2? | Engine | TTL time column | TTL window |
|---|---|---|---|---|
| `evm_transfers` | ✅ (8 EVM chains) | MergeTree | `block_timestamp` | **180d** (mig 012) |
| `dex_events` | ✅ (8 EVM chains) | MergeTree | `block_timestamp` | **180d** (mig 012) |
| `deployer_events` | ❌ | MergeTree | `block_timestamp` | **90d** (mig 012) |
| `solana_transfers` | ❌ | MergeTree | `block_timestamp` | 90d |
| `solana_spl_balance_changes` | ❌ | MergeTree | `block_timestamp` | 60d |
| `internal_transfers` | ❌ | MergeTree | `block_timestamp` | 90d |
| `evm_launches` | ❌ | MergeTree | `block_timestamp` | 365d |
| `solana_launches` | ❌ | MergeTree | `block_timestamp` | 365d (mig 007) |
| `tron_launches` | ❌ | MergeTree | `block_timestamp` | 365d (mig 008) |
| `solana_bundles` | ❌ | MergeTree | `block_timestamp` | 90d (mig 009) |
| `token_transfers` | ❌ | MergeTree | `block_timestamp` | 90d (empty) |
| `bridge_mappings` | ❌ | MergeTree | `bridged_at` | 180d |
| `dex_ohlcv_hourly` | ❌ | ReplacingMergeTree | `timestamp` | 90d |
| `wallet_labels` | ❌ | MergeTree | `loaded_at` | 6 months |
| `scam_addresses` | ❌ | MergeTree | `first_seen_at` | 6 months |
| `deployer_history` | ❌ | MergeTree | `deployed_at` | 90d |
| `holder_balances` | ❌ | ReplacingMergeTree(version) | `last_updated` | **90d** (mig 012) |
| `token_meta_ref` | ❌ | ReplacingMergeTree | `last_updated` | **365d** (mig 012) |
| `wallet_profiles` | ❌ | ReplacingMergeTree(updated_at) | `last_seen_at` | **180d** (mig 012) |
| `block_checkpoints` | ❌ | ReplacingMergeTree | `checked_at` | **180d** (mig 012) |
| `token_prices` | ❌ | ReplacingMergeTree | `updated_at` | **90d** (mig 012) |
| `chain_indexer_progress` | ❌ | MergeTree | `last_updated` | 30d |
| `parquet_export_state` | ❌ | ReplacingMergeTree | — | **NEVER** (see §3) |

Bold rows are changed by `migrations/012_ttl_retention.sql`.

### Rationale

* **Exported tables (180d):** export normally prunes at `PARQUET_COLD_DAYS=7`.
  The 180-day TTL is a pure safety net so a stalled export job can catch up
  without losing un-archived data. TTL tracks `block_timestamp` (event time),
  not `ingested_at`, so backfilled old blocks do not extend retention.
* **Un-exported event tables (60–90d):** Solana/Tron/launch/bundle data is
  small relative to EVM transfers; 90d of forensic depth with no cold copy.
* **Launch tables (365d):** rare, high-value signals; keep a full year.
* **State/cache tables:** TTL anchors on the natural staleness column
  (`last_updated`, `last_seen_at`, `checked_at`) so inactive entries age out.

---

## 2. Apply the migration

Apply-once, against a **provisioned** deployment (the runtime `ensure_*`
DDLs must have run so the tables exist — several tables are created by the
indexer at startup, not by migrations 001–011):

```bash
docker exec -i rmi-clickhouse clickhouse-client \
  --multiquery < migrations/012_ttl_retention.sql
```

**Idempotency caveat:** `MODIFY TTL` is a metadata rewrite, not
`CREATE ... IF NOT EXISTS`. Re-running the same file with the *same*
expressions is a no-op; re-running with *different* intervals silently
overwrites. Do not hand-edit the intervals — ship a new migration instead.

Optional immediate enforcement (heavy; only if you want TTL to fire before
natural merges):

```bash
docker exec rmi-clickhouse clickhouse-client \
  --query "OPTIMIZE TABLE wallet_memory.evm_transfers FINAL"
```

Normal operation needs no `OPTIMIZE`: TTL cleanup runs lazily on background
part merges.

---

## 3. `parquet_export_state` — NEVER TTL

This table records which `(chain, date)` pairs have been exported + deleted.
Dropping a row makes the exporter treat the day as un-exported: it would
re-export (empty) and then `ALTER TABLE ... DELETE` those rows **after the
parquet was already archived** — and the exported parquet would be
overwritten by the empty re-export. Cold-tier data loss. Do not add a TTL.

---

## 4. Verify the disk stops growing

Snapshot disk usage per table over time (run daily; watch the trend):

```sql
-- Per-table hot-store footprint (top 15).
SELECT table,
       formatReadableSize(sum(bytes_on_disk)) AS size,
       sum(rows) AS rows,
       min(min_date) AS oldest,
       max(max_date) AS newest
FROM system.parts
WHERE database = 'wallet_memory' AND active = 1
GROUP BY table
ORDER BY sum(bytes_on_disk) DESC
LIMIT 15;

-- Whole-database footprint trend (record this daily, e.g. via cron + TSV).
SELECT formatReadableSize(sum(bytes_on_disk)) AS total_size,
       count() AS parts
FROM system.parts
WHERE database = 'wallet_memory' AND active = 1;
```

Pass criteria after applying 012:

1. `evm_transfers` + `dex_events` size is bounded by export cadence (~7 days
   when healthy; ≤180 days worst case while the TTL safety net holds).
2. `holder_balances` stops growing past ~90 days of active-holder entries
   (largest un-exported table, was 1.7GB and monotonic).
3. `system.parts` shows old `min_date` parts disappearing for the TTL-only
   tables as merges run.
4. `SELECT count() FROM system.parts WHERE database='wallet_memory' AND
   active=1 AND min_date < today() - 200` trends toward 0 (no part older
   than the 180-day ceiling survives).

To keep the older-than-TTL backlog from blocking merges on the biggest
tables after a long export outage, run the `OPTIMIZE ... FINAL` from §2 once
per big table.

---

## 5. Current live state (2026-08-13, post-outage)

`evm_transfers` 951.9M rows / 186GB · `dex_events` 260.8M rows / 22GB ·
`holder_balances` 36.9M rows / 1.7GB · `wallet_labels` 39.5M rows / 1.7GB ·
everything else < 260MB. All numbers from `system.parts`/`system.tables`.
