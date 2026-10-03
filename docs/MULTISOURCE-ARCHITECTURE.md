# MULTISOURCE-ARCHITECTURE.md — Target architecture for the multi-source lite chain indexer

**Status:** DRAFT for operator review · 2026-10-03 · author: opencode research pass
**Scope:** research + design only. No implementation. Do not push.
**Verification basis:** every file path, table name, and row count below was read live from
Talos (repo `/srv/work/repos/lite-chain-indexer/`, ClickHouse 26.7.1.1315 `wallet_memory`,
B2 remote state) on 2026-10-03. Claims that could NOT be verified are marked **UNVERIFIED**.

---

## 0. Corrections to the brief (verified against live systems)

The vision statement contains three assumptions that are factually wrong on the
current fleet. The design below accounts for all three.

1. **ClickHouse has no native `bigquery()` table function on our build.**
   `system.table_functions` on 26.7.1.1315 contains `s3`, `s3Cluster`, `paimonS3`,
   `icebergS3`, `deltaLakeS3`, and a `GCS` **table engine** — no `bigquery` function,
   no `BigQuery` engine. Live BigQuery federation therefore stays where it already
   works: the Python query layer (`lite_indexer/query/bigquery_source.py`,
   `bigquery_rest.py`, `bigquery_tables.py`, `bigquery_accounts.py`). ClickHouse can
   still read BigQuery *exports* landed on GCS via the `GCS` engine / `s3()`
   interoperability mode, but that is an export pipeline, not live federation.

2. **The existing B2 cold tier is rclone-crypt encrypted and is unreadable by the
   b2-storage MCP.** The remote `b2-crypt-v2` is an rclone `crypt` overlay on a
   B2 bucket with `filename_encryption = standard` and
   `directory_name_encryption = true`. Neither the B2 Native API nor the S3-compatible
   API (which is what the b2-storage MCP speaks) can list or read those objects — the
   names and directory structure themselves are ciphertext. This is also why
   `lite_indexer/storage/cold_store.py` reads cold Parquet through `rclone mount` /
   `rclone cat` instead of DuckDB httpfs ("rclone-crypt encrypted and cannot use
   httpfs"). **Consequence:** "B2 via b2-storage MCP, rclone retired" requires a
   **new, non-crypt bucket** for all new writes; the legacy bucket stays readable
   only through rclone until it ages out or is migrated (§5, §7).

3. **B2 is currently a permanence layer, not a 90-day rolling tier.** The canonical
   policy (`RETENTION_POLICY` in `lite_indexer/config/storage.py`,
   `migrations/021_retention_classes.sql`, `docs/CLICKHOUSE_RETENTION.md`) says
   "B2 is the permanence layer, not the ClickHouse hot copy", and
   `parquet_export_state` is never TTL'd. The 90-day rolling cold tier is a **new
   product decision** this document designs for — it works precisely *because*
   deep history is federated to the public datasets instead of stored (§2, §5).

Also noted, minor: B2 `chain-data/parquet/` currently holds **21** chain
directories (includes `zksync`); `wallet_memory.wallet_labels` holds **39,967,628**
label rows (tier A 3,743,671 / B 29,821,305 / C 6,402,652) — the "~295K curated"
figure refers to the high-precision curated subset, not the table size.

---

## 1. Current-state audit

### 1.1 What exists (verified live)

**Engine** (`lite_indexer/`):
- Realtime RPC ingest: `lite_indexer/indexer/_sync_loop.py` +
  `lite_indexer/strategies/{evm,solana,tron}/`, append-only writers in
  `lite_indexer/storage/ch_*.py`, stall watchdog (`lite-indexer-stall-check.timer`,
  5-min cadence), memory-recycle ceiling, reorg protection, header-aware
  throttling, circuit breakers.
- Source order for RPC: `lite_indexer/data_sources.py` enforces keyless-first,
  keyed-last over `lite_indexer/chains/endpoints.py` + `non_evm.py` + private
  overlay (`LITE_INDEXER_ENDPOINT_OVERLAY`). Provider budgets:
  `config/production.example.toml` shape (rps / max_concurrency / daily_budget,
  100k req/day default per account).
- QueryRouter: `lite_indexer/query/router.py` — a facade over legs:
  `legs_clickhouse.py` (hot) → `legs_cold.py` (B2 Parquet via `ColdStore`) →
  free lake legs (`legs_lake.py` BigLake/AWS, `legs_portals.py` SQD,
  `envio_source.py` HyperRPC, `indexed_source.py`) → `legs_rpc.py` (paid leg,
  gated by `ON_DEMAND_RPC_ENABLED`). BigQuery legs:
  `bigquery_source.py` / `bigquery_rest.py` with 4-account free-tier rotation
  (`bigquery_accounts.py`, 1 TiB/month scanned each, exhaust → exit 3).
- Lake provider registry + quota router: `lite_indexer/lakes/registry.py`
  (`aws-eth`, `aws-sonarx-base`, `aws-sonarx-arbitrum`, `sqd`, `bigquery` with
  rps/daily/byte quotas) and `lite_indexer/lakes/router.py`
  (`LakeProviderRouter`, persisted usage ledger at `{DATA_DIR}/lake_usage.json`).
- Backfill CLIs: `lite_indexer/ingest/lake_backfill.py`, `lake_sonarx.py`,
  `lake_sqd.py`, `lake_bigquery_solana.py`, `lake_gaps.py`; snapshot replay:
  `lite_indexer/bootstrap.py` (B2 snapshot → `INSERT INTO ... FROM file('*.parquet')`).
- Parquet export: `lite_indexer/storage/parquet/{writer,export_flow,export_day,export_state}.py`.
  Exports **exactly two tables** (`_KIND_TABLES` in `export_day.py`:
  `evm_transfers` + `dex_events`), upload via rclone subprocess
  (`PARQUET_RCLONE_BIN`, `PARQUET_REMOTE` default `b2-crypt-v2:chain-data/parquet`,
  `PARQUET_COLD_DAYS=7`). Archive-before-drop enforced through
  `wallet_memory.parquet_export_state` (never TTL'd).
- Read surfaces: `api/` (chain-query-api.service), `lite_indexer/mcp_server.py`
  (`lci_chain_head`, `lci_rpc_health`, `lci_status`, `lci_recent_transfers`,
  `lci_attest_verify`), DuckDB cold reads (`storage/duckdb_query.py`).

**ClickHouse `wallet_memory` (hot store, live sizes):**

| table | engine | size | rows | role |
|---|---|---|---|---|
| `address_activity` | ReplacingMergeTree | 80.1 GiB | 332.5M | per-address tx index (Solana-first, `chain` column for cross-chain) |
| `evm_transfers` | ReplacingMergeTree | 68.7 GiB | 329.2M | token transfers, 20 chains incl. tron; hot 30d |
| `internal_transfers` | ReplacingMergeTree | 18.9 GiB | 626.6M | trace_* / debug_traceBlockByNumber depth |
| `solana_spl_balance_changes` | MergeTree | 7.4 GiB | 128.5M | Solana balance deltas |
| `dex_events` | ReplacingMergeTree | 2.1 GiB | 14.3M | DEX swaps/pools, 20 chains incl. solana |
| `wallet_labels` | ReplacingMergeTree | 1.6 GiB | 39.97M | label firehose, tiered A/B/C |
| `holder_balances(+_mv)` | RMT + MV | 16.9 GiB | 340M | pre-aggregated balances |
| `entity_labels` | ReplacingMergeTree | 11 MiB | 204K | curated entity attribution |
| `wallet_entities` | ReplacingMergeTree | 16 KiB | 223 | entity↔wallet links (embryonic) |
| `wallet_profiles` | ReplacingMergeTree | 2 KiB | **2** | profile rollup (**effectively empty**) |
| `scam_addresses` | MergeTree | 7 MiB | 450K | scam list |
| `deployer_events`/`deployer_history` | MergeTree | 29 MiB | 279K | deployer intel |
| `evm_launches`/`solana_launches`/`tron_launches` | MergeTree | ~16 MiB | ~110K | launch detection |
| `token_meta_ref` | ReplacingMergeTree | 47 MiB | 1.05M | token metadata (write-once reference) |
| `parquet_export_state` | ReplacingMergeTree | 35 KiB | 8,084 | archive ledger (never TTL) |

Plus Neo4j (169K+ graph labels) and the rmi-backend scanners, which consume
this data read-only via `_indexer_port` (one-way flow; see `ARCHITECTURE.md`
data-flow boundary).

**B2 cold tier (live layout, verified):**
`b2-crypt-v2:chain-data/parquet/<chain>/<yyyy-mm-dd>/` — day-partitioned
directories for 21 chains (arbitrum, avalanche, base, blast, bsc, celo,
ethereum, fantom, fraxtal, gnosis, hyperliquid, linea, mantle, metis,
optimism, polygon, robinhood, scroll, solana, tron, zksync). Bootstrap
snapshots live separately under `b2-crypt:chain-data/snapshots`.

### 1.2 Where the mess is

1. **The cold tier is welded to rclone-crypt.** Writes go through rclone
   subprocess, reads through rclone mount/cat; DuckDB httpfs and CH `s3()`
   are both unusable against it. Any move to b2-storage MCP is a bucket
   migration, not a client swap.
2. **Two competing "entity" stores at different maturities:** `wallet_labels`
   (39.97M rows, noisy, tiered) vs `entity_labels` (204K, curated) vs
   `wallet_entities` (223 rows) vs `wallet_profiles` (2 rows). The freemium
   product needs exactly one canonical read path (§4).
3. **`address_activity` (80 GiB) has no retention class** — it is absent from
   `RETENTION_POLICY` while being the largest table.
4. **Legacy/empty tables pollute the catalog** (§6): `default.blockchain_transactions`,
   `default.dex_trades` (both 0 rows), `default.wallet_labels` (132,945-row
   legacy duplicate of the `wallet_memory` table), `wallet_memory.token_transfers`
   (0 rows), ten `_*probe*`/`_cmp*`/`_lane2_*` test tables.
5. **`dex_events.source` still says `'subsquid'`** as its default — a fossil
   from the decommissioned engine era. Harmless, confusing.
6. **Retention drift is a known open item** (TODO.md): live `evm_transfers`
   TTL anchors on `block_timestamp + 30d`, migration DDL says
   `ingested_at + 90d`; `dex_events` mixes column- and table-level TTLs.
   `scripts/check-retention-drift.sh` / `python3 -m lite_indexer.retention`
   is the gate that must stay green through any change.
7. **Solana truth is split three ways**: `solana_spl_balance_changes` (128M,
   main), `solana_transfers` (14,583 rows, nearly empty), and `dex_events`
   rows with `chain='solana'` (327K). The multi-source design must declare
   one canonical Solana transfer table.

---

## 2. The federation design — source-routing table

**Doctrine (unchanged from `docs/DATA-SOURCES.md`):** no single source is ever
load-bearing; bulk history must always be obtainable without Google; every
chain keeps ≥2 independent paths. Federation is a *read-routing* problem —
the engine only ever **owns** what the realtime RPC ingest and the 90-day
rolling window produce; everything deeper is answered by routing, not storing.

Routing is implemented where it already lives: `QueryRouter` legs +
`LakeProviderRouter` quotas. New sources (CryptoHouse, HyperSync-bulk,
BigLake) register as legs/providers in `lite_indexer/lakes/registry.py` —
data-driven, env-tunable, no chain-specific code forks.

### 2.1 The routing table (core deliverable)

| Chain class | Data type | Realtime (fresh) | 0–90d backfill | >90d deep history | Trace depth | Cost basis |
|---|---|---|---|---|---|---|
| **EVM — all 18** (ethereum, base, bsc, polygon, arbitrum, optimism, avalanche, fantom, linea, scroll, zksync, mantle, gnosis, celo, robinhood, blast, fraxtal, metis, hyperliquid) | token transfers + DEX events | **Own RPC engine** (free pool, keyless-first via `data_sources.py`) | **HyperSync** (80+ EVM chains, ~2000× RPC; free-tier token — quota **UNVERIFIED**, see §7 Q1) → fallback SQD portal → AWS sonarx (base/arbitrum/ethereum only) | **Never stored.** Federated on demand: BigQuery `crypto_ethereum` (ethereum only; 4×1 TiB/mo rotation) → SQD portal → AWS sonarx → HyperSync metered | **SQD portal** (full-history EVM traces → `internal_transfers` shape) → BigQuery `traces` (eth) → own `trace_*` RPC for the realtime tail only | $0 at steady state; BigQuery within free tier; paid RPC leg stays gated by `ON_DEMAND_RPC_ENABLED=0` default |
| **Solana** | SPL transfers / balance changes / DEX | **Own RPC engine** (`strategies/solana/`) | **CryptoHouse** (crypto.clickhouse.com — reachable 2026-10-03, HTTP 200; Solana blocks/transactions/token_transfers/accounts/tokens) → SQD portal (`sqd_solana.py` exists) | **CryptoHouse** primary → BigQuery `crypto_solana_mainnet_us` ("Token Transfers"/"Instructions", day-partitioned, verified schema in `bigquery_tables.py`) → Old Faithful CAR archives (last resort; AGPL, raw ledger needs decode — see TODO) | BigQuery `Instructions` (program_id-clustered) → CryptoHouse transactions | $0; CryptoHouse fair-use terms **UNVERIFIED** (§7 Q4) |
| **Tron** | TRC-20 transfers | **Own RPC engine** (`strategies/tron/`; **blockTime is MILLISECONDS — convert at parse boundary, never store raw**) | SQD portal (`sqd_tron.py` exists; portal is eval-quota — treat as best-effort) | Own archive from the 90d window + SQD while quota lasts; no free deep public dataset — accepted gap | own RPC | $0 |
| **Any chain** | entity labels / profiles | **Our entity layer** (`wallet_labels` + `entity_labels` + Neo4j) — this is the product, never outsourced | same | same | — | $0 |
| **Any chain** | token metadata | `token_meta_ref` (write-once reference, already the pattern) | backfill from transfers | — | — | $0 |

**Why this split:**
- *Freshness* is the moat — only our engine delivers second-resolution,
  entity-enriched, 20-chain realtime for $0. Never delegate the head of the chain.
- *90d backfill* goes to whoever is fastest and free: HyperSync for EVM
  (purpose-built bulk API), CryptoHouse/SQD for Solana. Own RPC is deliberately
  NOT the backfill path — free public RPCs make backfill the slowest, most
  fragile leg (TODO: "your backfill is RPC-bound").
- *>90d depth is never owned.** Storing it costs money forever; BigQuery/
  CryptoHouse/SQD already store it for free. The QueryRouter already proves
  the pattern (BigLake/SQD/Envio/indexed.xyz legs exist today).
- *Trace depth* is the freemium differentiator and the most expensive to
  self-produce (CU-heavy `trace_*`); federate bulk, keep only the realtime
  tail in-house.

### 2.2 New legs to register (implementation later; design only)

| Provider | Kind | Serves | Quota registration point |
|---|---|---|---|
| `hypersync` | http/api (token) | `(*, transfers)`, `(*, logs)` EVM | `lakes/registry.py` `_default_registry()` + `LAKE_HYPERSYNC_*` envs |
| `cryptohouse` | ch/http | `(solana, token_transfers|transactions|accounts|tokens)`, `(ethereum, *)` | same |
| `biglake` | gcp/biglake | read path to our own exported Parquet on GCS-adjacent storage (optional; `query/biglake_source.py` already exists for AWS) | same |

---

## 3. The data flow

```
                     ┌─────────────────────── SOURCES ───────────────────────┐
                     │ own RPC pool (free) │ HyperSync │ SQD │ CryptoHouse   │
                     │ BigQuery (4-acct)   │ AWS sonarx │ indexed.xyz/Envio  │
                     └─────────┬───────────────────────────────┬─────────────┘
                               │ backfill/bulk (scheduled,      │ on-demand
                               │  quota-metered fills)          │ query legs
                               ▼                               │
   ┌────────────────────────────────────────────────────┐     │
   │ ENGINE (lite_indexer/)                              │     │
   │  strategies/{evm,solana,tron} → normalize →         │     │
   │  append-only writers (storage/ch_*.py)              │     │
   └──────────────┬─────────────────────────────────────┘     │
                  ▼                                            │
   ┌──────────────────────────────────────────┐               │
   │ ClickHouse HOT — wallet_memory            │◄──────────────┘
   │  evm_transfers 30d │ dex_events 90d │     │   QueryRouter:
   │  internal_transfers 90d │ labels 180d │   │   hot → cold(B2) →
   │  address_activity (needs class §5)        │   free lakes → paid RPC (gated)
   └──────┬───────────────────────▲───────────┘
          │ daily export          │ bootstrap replay (bootstrap.py)
          │ (parquet/export_*)    │
          ▼                       │
   ┌──────────────────────────────────────────┐
   │ B2 COLD — NEW bucket (SSE-B2, NO rclone-  │
   │  crypt) written via b2-storage MCP flow:  │
   │  s3_create_multipart_upload / presigned   │
   │  PUT URLs; 90-day rolling lifecycle       │
   └──────────────────────────────────────────┘
```

Rules that survive from the current system unchanged:
- **Append-only, never mutate indexed rows; corrections are new rows.**
- **Archive-before-drop is mandatory** — a hot TTL drop is only safe after
  the rows exist in the cold tier; `parquet_export_state` (never TTL'd) is the
  ledger that proves it; a stalled exporter pauses TTL, never loses data.
- **Local disk is a cache, never the record** — `{DATA_DIR}/parquet` staging
  is deleted after verified upload.
- **Daily integrity check (06:15 UTC) stays green** — the exporter, the drift
  checker (`scripts/check-retention-drift.sh`), and row-count parity are the
  three legs; any federation change must keep all three passing.
- **One chain = one config entry, zero chain-specific code forks** — new
  sources plug into the registry/legs, not into strategy code.
- Tron `blockTime` milliseconds → converted at the parse boundary (existing
  behavior in `strategies/tron/`), never stored raw.

---

## 4. The wallet-entity layer (the freemium product)

### 4.1 Canonical read model

One canonical enrichment path, read-side only (append-only doctrine forbids
mutating transfer rows):

```
transfer rows (evm_transfers / solana_*) 
   ⋈ address → wallet_labels (39.97M rows; tier filter: A > B > C)
   ⋈ address → entity_labels (204K curated; entity_type/label_type/name/provenance/confidence)
   ⋈ address → scam_addresses (450K; is_sanctioned)
   ⋈ entity_id → wallet_entities → Neo4j cluster expansion (SAME_WALLET)
```

- `wallet_labels` is the coverage layer (noisy, tiered — free queries get
  tier-A only, which is the precision tier at 3.74M rows).
- `entity_labels` is the curated attribution layer (the freemium asset).
- `wallet_profiles` (2 rows today) is the intended rollup surface —
  **build-out is a prerequisite for the freemium tier** (§7 Q6): per-wallet
  totals, counterparties, first/last seen, risk_score/risk_level.
- Enrichment happens in the QueryRouter/API read path as a join, so label
  reloads never touch indexed history.

### 4.2 Free vs freemium boundary

| Capability | Free | Freemium |
|---|---|---|
| Realtime transfer feed (all 20 chains, second-resolution) | ✅ | ✅ |
| Wallet transfer history within the owned 90d window (hot CH + cold B2) | ✅ rate-limited | ✅ higher limits |
| Token metadata (`token_meta_ref`), chain head/status, MCP read tools | ✅ | ✅ |
| Tier-A label lookup ("is this address labeled?") | ✅ | ✅ |
| **Entity attribution on transfers** (join to `entity_labels`, entity names/categories/confidence) | ❌ | ✅ |
| **Trace depth** (`internal_transfers`, call trees, federated BigQuery traces) | ❌ | ✅ |
| **Full label tiers** (B/C tiers, provenance, sanction flags joined in bulk) | ❌ | ✅ |
| **Multi-chain joins** (one query across chains via `address_activity.chain` / unified transfer shape) | ❌ | ✅ |
| **>90d federated history** (BigQuery/CryptoHouse/SQD pass-through, cost-metered per account) | ❌ | ✅ with quota |
| **Bulk export** (signed Parquet URLs from B2 via b2-storage MCP presign) | ❌ | ✅ |
| Neo4j cluster expansion (entity graph neighborhood) | ❌ | ✅ |

**Enforcement point (design, not decided):** one API-key check at the
`chain-query-api.service` / MCP boundary, where a `tier` claim gates the
join legs and the federation legs. Federation legs are *cost-metered* per
key because BigQuery bytes scanned are real money once the free tier is
exhausted (§7 Q5).

---

## 5. Retention and Parquet layout

### 5.1 The three-tier window

| Tier | Store | Window | Notes |
|---|---|---|---|
| Hot | ClickHouse `wallet_memory` | per class (`RETENTION_POLICY`): `evm_transfers` **30d**; `dex_events`, `internal_transfers`, `solana_transfers` **90d**; labels/scam/deployer **180d**; launches **365d** | ReplacingMergeTree + `PARTITION BY toYYYYMM(block_timestamp)`; TTL drops whole partitions; event-time anchor so backfills don't extend retention |
| Cold (owned) | **NEW B2 bucket**, SSE-B2, no rclone-crypt | **90-day rolling** — B2 lifecycle rule `daysFromUploadingToHiding: 90` + `daysFromHidingToDeleting: 7` (or straight delete variant); enforced server-side, plus `parquet_export_state` reconciliation | Total *owned* window = 90d. This is the operator's new product decision, enabled by federating >90d (§2) |
| Deep (federated) | BigQuery / CryptoHouse / SQD / HyperSync / AWS sonarx | genesis → now | Never stored. Routed reads with byte/day quotas (`LakeProviderRouter`) |

Gap to close: **`address_activity` (80 GiB, the largest table) has no
retention class** — add it to `RETENTION_POLICY` (suggest 90d on
`block_time`) before the multisource rollout, or it silently defeats the
90-day story.

### 5.2 Parquet layout (new bucket)

Keep the proven day-partition grain; adopt hive-style key prefixes so
ClickHouse `s3()` with `use_hive_partitioning=1` (available on 26.7) can read
partition columns directly:

```
s3://<new-bucket>/chain-data/parquet/chain=<chain>/table=<evm_transfers|dex_events>/dt=<yyyy-mm-dd>/<part-uuid>.parquet
```

- One file set per (chain, table, day); zstd compression; schema = the CH
  `SELECT *` shape (so `bootstrap.py`'s `INSERT INTO ... FROM file(...)`
  keeps working verbatim).
- `manifest.json` per (chain, dt) stays — row counts + block range; the
  daily integrity check and bootstrap both consume it.
- **Writes via b2-storage MCP mechanics** (S3-compatible API):
  `s3_create_multipart_upload` → presigned part PUTs →
  `s3_complete_multipart_upload`; small manifests via `s3_put_object`.
  The rclone subprocess path (`PARQUET_RCLONE_BIN`) is retired for new writes.
- **Reads:** CH `s3()` for ad-hoc SQL, DuckDB httpfs for local/agent reads —
  both become possible the moment rclone-crypt is out of the path, which
  also deletes `ColdStore`'s `rclone mount`/`rclone cat` modes.
- **Legacy archive** (`b2-crypt-v2`): freeze writes, keep the rclone read
  path as a legacy leg until the last day ages past the federated-query
  comfort window, then decommission (§7 Q2/Q3).

ClickHouse migration gotcha to respect when altering these tables: Code 36 —
`MODIFY ORDER BY` cannot append an existing column; new columns go in via
MATERIALIZED or DROP-then-ADD (playbook rule, unchanged).

---

## 6. What to torch

Deletions (all verified live, 2026-10-03; drop after a final row-count check):

| Object | Evidence | Action |
|---|---|---|
| `default.blockchain_transactions` | 0 rows, MergeTree, legacy skeleton schema | DROP |
| `default.dex_trades` | 0 rows, MergeTree | DROP |
| `default.wallet_labels` | 132,945 rows, legacy duplicate of `wallet_memory.wallet_labels` (39.97M) | verify no reader, then DROP |
| `wallet_memory.token_transfers` | 0 rows | DROP |
| `wallet_memory.bridge_mappings` | 0 rows, no writer found in repo | DROP |
| `wallet_memory.chain_indexer_progress` | 0 rows | DROP |
| `wallet_memory._nottl`, `_cmp`, `_cmp2`, `_rmt`, `_rtest`, `_perm_probe`, `_tsprobe`, `_lane2_base`, `_lane2_priv_probe`, `_lane2_size_probe` | probe/test residue | DROP |
| `wallet_memory.internal_transfers_rebuild` | 1,687 rows — leftover rebuild staging | verify then DROP |
| `dex_events.source DEFAULT 'subsquid'` | fossil default from the decommissioned engine | re-default to `'lite_indexer'` on the next schema touch |
| ColdStore `rclone mount` / `rclone cat` read modes | only exist because of rclone-crypt | retire once the new bucket serves cold reads via httpfs/`s3()` |
| rclone write path in `storage/parquet/writer.py` (`PARQUET_RCLONE_BIN` subprocess) | replaced by b2-storage MCP upload flow | retire for new writes (keep the code path gated until legacy bucket ages out) |
| Duplicate Solana truth: `wallet_memory.solana_transfers` (14,583 rows) vs `solana_spl_balance_changes` (128.5M rows) | two tables claiming Solana transfers | declare `solana_spl_balance_changes` canonical (or fold), stop writing the near-empty one |

Do **not** torch: `parquet_export_state` (archive ledger, never TTL),
bootstrap snapshots (`b2-crypt:chain-data/snapshots`) until the snapshot
product is re-homed, and the `LakeProviderRouter` usage ledger.

---

## 7. Open questions for the operator

1. **HyperSync free-tier quota is unverified** (flagged in TODO.md since
   2026-09-14). Before it becomes the canonical EVM backfill leg someone must
   mint a token and run a measured benchmark (rows/sec, daily ceiling, what
   happens at the cap). If the free tier is eval-sized, the fallback chain
   becomes SQD → AWS sonarx → BigQuery, and HyperSync drops to "accelerator".
2. **B2 encryption decision.** New bucket unencrypted, or SSE-B2 (AES256,
   bucket default via `b2_create_bucket`/`b2_update_bucket`)? And the legacy
   rclone-crypt archive: one-time decrypt-and-reupload migration (costs
   egress + rewrite of ~2 months of Parquet), or freeze-and-age-out? Note:
   B2 application keys are bucket-scoped, not prefix-scoped — the indexer
   daemon's key must be a scoped key on the new bucket with only
   `writeFiles`/`listFiles`, never the master key.
3. **Do bootstrap snapshots stay permanent?** The 90-day rolling rule covers
   the cold tier, but `snapshots/` is the "genesis shortcut" product
   (`bootstrap.py`). Recommend: snapshots live in the new bucket under a
   `snapshots/` prefix **exempt from the 90-day lifecycle** (separate
   lifecycle rule by prefix). Confirm.
4. **CryptoHouse terms.** crypto.clickhouse.com is reachable and free, but
   its fair-use/SLA terms are unverified. If it rate-limits hard, Solana
   deep history falls back to BigQuery (1 TiB/mo × 4 accounts) and Old
   Faithful (AGPL — license-clean for internal use, never republish).
5. **Where does freemium enforcement live?** Options: (a) key check in
   `chain-query-api.service` with a `tier` claim gating router legs;
   (b) MCP-level gating (`mcp_server.py`); (c) x402 pay-per-call on the
   premium legs only (fits the fleet's x402 stack: `x402-gateway`,
   `rmi-mcp-x402`). (c) makes ">90d federated history" self-funding because
   BigQuery bytes are metered per call.
6. **Who builds `wallet_profiles`?** It has 2 rows today. The freemium
   boundary above assumes a rollup pipeline populating it
   (totals/counterparties/risk). Is that pipeline in scope for this repo
   (a new writer) or does rmi-backend own profile rollups? This is the one
   genuinely blocking product question.
7. **Canonical Solana transfer table** — confirm
   `solana_spl_balance_changes` is the one true table and
   `solana_transfers` (14.5K rows) is deprecated, before federation legs
   start routing to either.

---

## Appendix A — Verified facts used in this document

- Repo state read 2026-10-03: `/srv/work/repos/lite-chain-indexer/`
  (`ARCHITECTURE.md`, `README.md`, `TODO.md`, `docs/DATA-SOURCES.md`,
  `docs/CLICKHOUSE_RETENTION.md`, `docs/COMPARISON.md`,
  `config/production.example.toml`, `lite_indexer/{data_sources.py,
  lakes/registry.py, lakes/router.py, query/router.py, query/legs_*.py,
  query/bigquery_tables.py, query/envio_source.py, storage/parquet/*.py,
  storage/cold_store.py, config/storage.py, config/chains.py, bootstrap.py,
  strategies/__init__.py, mcp_server.py}`, `migrations/` 001–023).
- ClickHouse 26.7.1.1315 live: table list, sizes, row counts, schemas,
  `system.table_functions` / `system.table_engines` (no `bigquery`).
- B2 live: `b2-crypt-v2` is an rclone `crypt` remote (filename + directory
  name encryption); `chain-data/parquet/` contains 21 chain directories with
  `<yyyy-mm-dd>` day partitions.
- External reachability checked 2026-10-03: `crypto.clickhouse.com` HTTP 200;
  `play.clickhouse.com` answers queries; HyperSync docs confirm 80+ EVM
  chains + Fuel, token-based access, ~2000× RPC benchmark claims.
- Not verified (marked UNVERIFIED inline): HyperSync free-tier quota,
  CryptoHouse fair-use terms, SQD portal production-quota status.
