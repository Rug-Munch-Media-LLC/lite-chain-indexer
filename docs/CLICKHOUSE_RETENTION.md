# ClickHouse Retention Policy (`wallet_memory`)

**Status:** canonical · owner=Data Platform · last_updated=2026-09-14

Hot/cold model for the indexer's ClickHouse store. The **single source of
truth** is `RETENTION_POLICY` in `lite_indexer/config/storage.py`, mirrored by
`migrations/013_retention_classes.sql` and enforced by the runtime `ensure_*`
DDL. `python3 -m lite_indexer.retention` fails if the live TTLs drift.

---

## 1. Hot/cold model

* **Hot window (ClickHouse):** a lean, per-class window (30/90/180/365 days)
  served at full query speed. The transfer/dex tables are
  `ReplacingMergeTree` + `PARTITION BY toYYYYMM(<event time>)`, so TTL drops
  whole partitions cheaply instead of rewriting parts.
* **Permanence (B2 Parquet):** everything older is archived to B2 as Parquet;
  B2 is the permanence layer, not the ClickHouse hot copy.
* **Older data is queried in place** via ClickHouse `s3()` over the B2
  Parquet — no re-import required.
* **Archive-before-drop is mandatory:** TTL deletion is lazy (part merges) and
  must never outrun the archive pipeline. A hot-window drop is only safe once
  the rows exist in the cold tier. A stalled archive job is a reason to pause
  the TTL, never to lose data.
* **Event-time anchor:** TTLs use `block_timestamp` (event time) wherever the
  table has it, so backfilling old blocks does not extend retention.

---

## 2. Retention classes

`engine` reflects the post-migration live schema (the transfer/label tables
were converted to `ReplacingMergeTree` + `PARTITION BY` by
`scripts/migrate-ch-merge-to-replacing.sh`, which is intentionally unchanged).

| Table | Time column | Hot window | Engine / partition | Archived to B2 |
|---|---|---|---|---|
| `evm_transfers` | `block_timestamp` | **30d** | ReplacingMergeTree · PARTITION BY toYYYYMM | ✅ |
| `dex_events` | `block_timestamp` | **90d** | ReplacingMergeTree · PARTITION BY toYYYYMM | ✅ |
| `internal_transfers` | `block_timestamp` | **90d** | ReplacingMergeTree · PARTITION BY toYYYYMM | later |
| `solana_transfers` | `block_timestamp` | **90d** | ReplacingMergeTree · PARTITION BY toYYYYMM | later |
| `scam_addresses` | `first_seen_at` | **180d** | MergeTree | later |
| `wallet_labels` | `loaded_at` | **180d** | ReplacingMergeTree · PARTITION BY toYYYYMM | later |
| `deployer_history` | `deployed_at` | **180d** | MergeTree (partitioned) | later |
| `evm_launches` | `block_timestamp` | **365d** | MergeTree | later |
| `solana_launches` | `block_timestamp` | **365d** | MergeTree | later |
| `tron_launches` | `block_timestamp` | **365d** | MergeTree | later |

All other tables keep their existing TTLs (migrations 004–012) — this policy
only tightens the classes above. `parquet_export_state` is **never** TTL'd
(§5).

### Why these windows

* **`evm_transfers` (30d)** — the volume hog (~6–7.6 GiB/day). It is the
  primary B2-archived table, so the hot copy only needs the recent, actively
  queried month. Anchoring on `block_timestamp` also fixes the live drift
  (`ingested_at + 90d`) where backfilled old blocks stayed hot far too long.
* **`dex_events` / `internal_transfers` / `solana_transfers` (90d)** — smaller
  event streams needing more forensic depth in the hot tier.
* **Labels/deployer history (180d)** — low-volume, high-value wallet intel;
  180d balances freshness against disk.
* **Launch tables (365d)** — rare, small, high-value signals.

---

## 3. Applying the policy

Migration `migrations/013_retention_classes.sql` is **apply-once**, against a
**provisioned** deployment (the runtime `ensure_*` DDL must have created the
tables — several are not covered by migrations 001–012):

```bash
clickhouse-client --multiquery < migrations/013_retention_classes.sql
```

`MODIFY TTL` is a metadata rewrite, not `CREATE ... IF NOT EXISTS`. Re-running
the same file with the same expressions is a no-op; a different interval
silently overwrites. Do not hand-edit intervals — ship a new migration.

Fresh deployments do not need the migration: the runtime `ensure_*` DDL reads
`RETENTION_POLICY` directly, and `SchemaMixin.ensure_ttl_policies` applies the
same table-level TTLs on the first write of each cycle (gated by
`CH_TTL_ENABLED`).

Normal operation needs no `OPTIMIZE`: TTL cleanup runs lazily on background
part merges. To force it early on a small table:

```sql
OPTIMIZE TABLE wallet_memory.evm_transfers FINAL;
```

---

## 4. Drift checker

```bash
python3 -m lite_indexer.retention         # or: bash scripts/check-retention-drift.sh
```

Reads `system.tables.engine_full` (fallback `create_table_query`) for every
policy table, parses the table-level TTL, and prints a comparison:

```
TABLE                  STATUS   EXPECTED                           ACTUAL
dex_events             ok       block_timestamp + 90d              block_timestamp + 90d
evm_transfers          drift    block_timestamp + 30d              ingested_at + 90d
scam_addresses         absent   first_seen_at + 180d               -
```

* **exit 0** — no drift, or ClickHouse is unreachable (prints
  `CH unavailable … skipped (exit 0)`; a gate that cannot reach CH must not
  red the build).
* **exit 1** — a provisioned table diverges from the policy, or an existing
  policy table has no TTL. Apply migration 013.

Absent tables are reported but are not drift — a fresh deployment legitimately
lacks them.

---

## 5. `parquet_export_state` — NEVER TTL

This table records which `(chain, date)` pairs have been archived. Dropping a
row makes the exporter treat the day as un-archived: it would re-export
(empty) and then `ALTER TABLE ... DELETE` rows **after** the parquet was
already archived, overwriting the real cold copy with an empty one. Cold-tier
data loss. Do not add a TTL.

---

## 6. Verifying the disk stays bounded

```sql
-- Per-table hot-store footprint (top 15).
SELECT table,
       formatReadableSize(sum(bytes_on_disk)) AS size,
       sum(rows) AS rows
FROM system.parts
WHERE database = 'wallet_memory' AND active = 1
GROUP BY table
ORDER BY sum(bytes_on_disk) DESC
LIMIT 15;

-- Parts older than the largest hot window (should trend to 0).
SELECT count()
FROM system.parts
WHERE database = 'wallet_memory' AND active = 1
  AND min_date < today() - 365;
```

Pass criteria after applying 013 and the runtime ensure:

1. `evm_transfers` hot size reflects the ~30-day window (plus merge lag);
   `dex_events` the ~90-day window.
2. `min_date` for the 180/365d classes ages out as merges run.
3. The archived parquet in B2 remains complete — archive-before-drop holds.
