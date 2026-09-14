# Lite Chain Indexer — TODO & Roadmap

## Status: v1.5.0 — production, 21 chains

**Chains:** 20 (+ Hyperliquid EVM, chainId 999).
**Tests:** ~1490 passing, 5 skipped (ruff, mypy strict, public-leak gate all green).
**Footprint:** free-RPC-first; public repo ships keyless RPCs only, the curated
keyed stack lives in the private runtime overlay (`LITE_INDEXER_ENDPOINT_OVERLAY`).

---

## Recently completed

- [x] **MCP server** (`lite_indexer/mcp_server.py`) — read-only indexer tools for agents
- [x] **Hyperliquid EVM** as chain 21 (official + Pocket keyless; keyed overlay endpoints)
- [x] **Reorg handling fails closed** on unknown status (was treating lookup errors as "no reorg")
- [x] **Prometheus `/metrics` exporter + scrape job** (the `LiteIndexer*` alerts were structurally dead)
- [x] **State / quota-ledger / attestation backups** (daily, local + B2)
- [x] **systemd sandbox** (`NoNewPrivileges`, `ProtectSystem=strict`, scoped `ReadWritePaths`, …)
- [x] **HTTPS clock sync** (this network filters all outbound UDP/123)
- [x] **Lite-by-default**: vector ingestion opt-in; embeddings default to the local lane (no silent paid DeepInfra)
- [x] **Per-account quota tracking** for URL-keyed endpoints (userinfo + mid-path + `name=value`)
- [x] Free-provider coverage: Blockscout (8), Tenderly gateway (13), Pocket HL, OnFinality metered
- [x] **Disk root-cause fixed** — 397 GB stale native CH backup dir reclaimed; snapshot job now purges before its free-space preflight

---

## Recently completed (2026-09-14)

- [x] **MergeTree → partitioned ReplacingMergeTree migration** — `evm_transfers` (2.02 B rows),
      `internal_transfers`, `solana_transfers` swapped to `ReplacingMergeTree` +
      `PARTITION BY toYYYYMM(block_timestamp)`; row parity verified, 64.4 M exact-duplicate rows
      deduped, rollback copies dropped. TTL now burns at partition level.
- [x] **Memory-wedge fix** — continuous `memory-watchdog` thread recycles the daemon at
      `MEMORY_RECYCLE_MB`, independent of the sync loop (a post-cycle-only check could not fire
      once a mid-cycle cgroup `MemoryHigh` throttle wedged the daemon; it ran 5 h 45 m, 0 writes).
- [x] **Stall-check watchdog re-installed durably** (it was symlinked into volatile `/tmp`).
- [x] **`BACKFILL_INCLUDE_REWINDS=1`** enabled (safe now that the transfer tables dedup).
- [x] **Reclaimed the `*_old_<ts>` rollback copies.**

---

## Deferred backlog (additive — no re-architecture required)

These are seams we can layer on later; none blocks current operation.

- [ ] **Dynamic / factory contract discovery** (Ponder-style): discover pools/vaults/factories
      from a seed contract + event, then index their children. Strategy-level, additive.
- [ ] **Second migration pass** — per-table verdicts (2026-09-14 schema review):
      - `dex_events` (28.76 GiB): `ORDER BY (chain, block_number, log_index)` — unique, safe to
        dedup. Needs `ddl_create` to re-state the **column-level** TTLs (`block_timestamp + 90d`)
        as well as the table-level `ingested_at + 90d`, and to carry the `dex_events_summary`
        projection. MV `dex_ohlcv_hourly_mv` reads it (name-bound → survives the swap).
      - `wallet_labels` (1.56 GiB): `PARTITION BY toYYYYMM(loaded_at)`, `ORDER BY (address,
        label_name, source)`, `TTL loaded_at + toIntervalMonth(6)`.
      - ⚠️ `solana_spl_balance_changes` — **do NOT migrate as-is**: `ORDER BY (mint, slot,
        token_account)` is **not unique** (a token account can change several times per slot), so
        RMT would silently drop legitimate rows. Needs a richer key (e.g. `+ tx_hash`) first.
      Same tooling; do NOT edit `TABLES` mid-run.
- [ ] **Reconcile TTL drift** — live `evm_transfers` TTL (`block_timestamp + 30d`) vs the migration
      DDL's `ingested_at + 90d`; and `dex_events` uses both column-level (`block_timestamp + 90d`)
      and table-level (`ingested_at + 90d`) TTLs. Decide the retention window and align the
      migration script + `migrations/` TTL files.
- [ ] **GraphQL / subgraph-style schema layer** over ClickHouse (query surface, additive).
- [ ] **Expand transaction tracing** (internal transfers) from 3 chains to all where our free
      tiers serve `trace_block` (per-chain mapping lives in the private runtime overlay;
      see LITE_INDEXER_ENDPOINT_OVERLAY). Wire per-chain trace endpoints in `traces/endpoints.py`
      (add to the private overlay for keyed ones). Also probe `debug_traceBlockByNumber` for chains
      where `trace_block` is unavailable
      (arbitrum, polygon, avalanche, linea, scroll, zksync, mantle, celo, blast, metis).
      Trace methods are CU-heavy (~3.0 CU) — the quota ledger already meters them.
- [ ] **Free external backfill ingest** (research: `/root/.knowledge/FREE-BACKFILL-DATASETS-2026-09-14.md`).
      No single free source covers all 21 chains; **Envio HyperSync ∪ Subsquid SQD = all 21**.
      Plan: prototype **HyperSync `collect_parquet`** (MPL-2.0; ~20/21 + Solana via a separate client;
      free-token quota UNVERIFIED — verify first) → reshape → B2 → `bootstrap`. **SQD Portal** for
      metis/Tron/Hyperliquid (AGPL-3.0 code; public portal is *eval-only*, not production).
      **Solana = Old Faithful** free CAR archives (AGPL-3.0, raw ledger → needs decode). Tron = free
      node-DB snapshots. Firehose is OSS (Apache-2.0) but hosted = paid; The Graph Market free tier is
      a trial. **Licensing: never republish these as our own OSS.** Reuse `ingest/flatfile.py`
      (`s3://` B2 + manifest/glob) but each source needs reshape to our schema.
      Trap: Flipside appears defunct — don't build on it.
- [ ] **Browser UI / query console**.
- [ ] More non-EVM drivers (Aptos, Sui, Cosmos) — registry presets exist; drivers missing.
- [ ] Promote or drop stale keyed provider endpoints (see private overlay inventory).

---

## Known issues / notes

- `evm_transfers` TTL is `ingested_at + 90d`; historical backfill rows (2022–2023) therefore never
  expire until the table is partitioned — resolved by the in-flight migration.
- Some overlay endpoints are free-tier-blocked for `eth_getLogs` (tip reads only). Noted, not wired for logs.
- Public git history still contains the pre-overlay provider methodology (no keys; gitleaks clean).
  HEAD is clean and guarded by the CI leak gate.
