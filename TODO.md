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

## In flight (2026-09-14)

- [ ] **MergeTree → partitioned ReplacingMergeTree migration** (evm_transfers 2.0 B rows,
      internal_transfers, solana_transfers). Enables TTL to actually **burn** (partition-level
      drops) and unblocks safe rewind-backfill. Tooling: `scripts/migrate-ch-merge-to-replacing.sh`,
      runbook `docs/MIGRATION_MERGETREE_REPLACING.md`.

---

## Deferred backlog (additive — no re-architecture required)

These are seams we can layer on later; none blocks current operation.

- [ ] **Dynamic / factory contract discovery** (Ponder-style): discover pools/vaults/factories
      from a seed contract + event, then index their children. Strategy-level, additive.
- [ ] **Second migration pass**: `dex_events` (28.75 GiB, unpartitioned → TTL can't burn) plus the
      small Solana/Tron tables (`solana_launches`, `tron_launches`, `evm_launches`, …).
      Same tooling; run after the in-flight migration completes. Do NOT edit `TABLES` mid-run.
- [ ] **Reconcile evm_transfers TTL drift** (live `block_timestamp + 30d` vs the migration DDL's
      `ingested_at + 90d`) — decide the correct retention window post-migration and align the
      migration script + `migrations/` TTL files.
- [ ] **Enable `BACKFILL_INCLUDE_REWINDS=1`** once the transfer tables are `ReplacingMergeTree`.
- [ ] **Reclaim the `*_old_<ts>` table** after migration verification.
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
