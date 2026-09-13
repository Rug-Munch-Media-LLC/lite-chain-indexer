# Lite Chain Indexer — TODO & Roadmap

## Status: v1.5.0 — production, 21 chains

**Chains:** 20 (+ Hyperliquid EVM, chainId 999).
**Tests:** ~1490 passing, 5 skipped (ruff, mypy strict, public-leak gate all green).
**Footprint:** free-RPC-first; public repo ships keyless RPCs only, the curated
keyed stack lives in the private runtime overlay (`LITE_INDEXER_ENDPOINT_OVERLAY`).

---

## Recently completed

- [x] **MCP server** (`lite_indexer/mcp_server.py`) — read-only indexer tools for agents
- [x] **Hyperliquid EVM** as chain 21 (official + Pocket keyless; dRPC/thirdweb/Dwellir overlay)
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
- [ ] **Enable `BACKFILL_INCLUDE_REWINDS=1`** once the transfer tables are `ReplacingMergeTree`.
- [ ] **Reclaim the `*_old_<ts>` table** after migration verification.
- [ ] **GraphQL / subgraph-style schema layer** over ClickHouse (query surface, additive).
- [ ] **Firehose / Portal flat-file ingest** as an alternate ingest source (builds on `ingest/flatfile.py`).
- [ ] **Browser UI / query console**.
- [ ] More non-EVM drivers (Aptos, Sui, Cosmos) — registry presets exist; drivers missing.
- [ ] Promote **stale provider keys** (QuickNode/Moralis/CoinGecko Pro/Birdeye/Solscan) or drop them.

---

## Known issues / notes

- `evm_transfers` TTL is `ingested_at + 90d`; historical backfill rows (2022–2023) therefore never
  expire until the table is partitioned — resolved by the in-flight migration.
- Dwellir free tier blocks `eth_getLogs` (tip reads only); Alchemy's Hyperliquid endpoint is
  free-tier-blocked. Both noted, not wired for logs.
- Public git history still contains the pre-overlay provider methodology (no keys; gitleaks clean).
  HEAD is clean and guarded by the CI leak gate.
