# Free Data Sources — Redundancy Matrix

Independent external sources the query layer relies on (verified 2026-09-16).
Goal: no single provider failure interrupts wallet history, tracing, or backfills.

| Source | Chains | Data | Access | Status |
|---|---|---|---|---|
| AWS public blockchain ("BigLake") | Ethereum, Bitcoin | blocks, txs, logs, token_transfers, **traces**, contracts (full history) | anonymous S3 via DuckDB | ✅ integrated (`biglake_source`) |
| Envio HyperRPC / HyperSync | 79+ EVM + Solana (early) | logs, txs, receipts; `trace_block` on eth/base `-traces` subdomains | free API token in URL | ✅ integrated (`envio_source`) |
| SQD Portal | Solana, Tron, Bitcoin, Hyperliquid, EVM | Solana instructions/token balances; TRC-20 + native internals; HL fills | HTTP POST JSONL (public endpoints) | ✅ integrated (`sqd_source`) |
| **Indexed.xyz** | ethereum, base, gnosis, linea, optimism, zksync, zora, sui, pgn, arweave | raw blocks/txs/logs + **decoded** logs, CC0 | public R2 credentials (below), S3 API / DuckDB | ✅ **INTEGRATED** (f660fcc — router third path, minutes-fresh) |
| solarchive.org | Solana | txs/accounts/tokens Parquet (CC-BY) | public HTTPS — data host currently 404 | 🟡 monitor |
| **CryptoHouse** (crypto.clickhouse.com) | Solana (LIVE), Ethereum/Base (snapshot-era ≈24.99M blocks), Polymarket | blocks, transactions, token_transfers, **instructions**, accounts, rewards, **traces/logs** (eth/base), + MVs | ✅ **HTTP direct access verified** — `https://crypto-clickhouse.clickhouse.com`, user `crypto`, empty password (native 9440 TLS flaky from Talos) | ✅ verified 2026-09-16 |
| **Bitquery cloud dumps** | Solana, Tron, BTC, Hyperliquid, Robinhood, BSC, XRP, EVM, Polymarket, PumpFun | Parquet by slot ranges `<chain>/<topic>/<start>_<end>.parquet` | **samples free** on `bitquery-blockchain-dataset.s3.us-east-1.amazonaws.com` (verified 513k-row read); full dataset licensed (sales@bitquery.io) | 🟡 samples only |
| Goldsky explore (app.goldsky.com/explore) | Ethereum, Base, + | decoded ERC-20/721/1155 transfers, stablecoin transfers, beacon data (same ecosystem as Indexed.xyz) | `goldsky indexed` CLI / Indexed.xyz bucket | ✅ via indexed.xyz |
| Pinax | Substreams SVM/Tron, token/perp APIs | streaming + APIs | api.pinax.network | 🟡 not integrated |
| Blockchair dumps | many | TSV dumps | now requires auth (401) | 🔴 dropped |
| Google BigQuery public | ethereum, bitcoin | SQL datasets | GCP account, 1 TB/mo free queries | ⚪ untested |

## Access recipes

### Indexed.xyz (Cloudflare R2 — public read)

```bash
export RCLONE_CONFIG_IX_TYPE=s3 RCLONE_CONFIG_IX_PROVIDER=Cloudflare \
  RCLONE_CONFIG_IX_ACCESS_KEY_ID=43c31ff797ec2387177cabab6d18f15a \
  RCLONE_CONFIG_IX_SECRET_ACCESS_KEY=afb354f05026f2512557922974e9dd2fdb21e5c2f5cbf929b35f0645fb284cf7 \
  RCLONE_CONFIG_IX_ENDPOINT=https://ed5d915e0259fcddb2ab1ce5592040c3.r2.cloudflarestorage.com \
  RCLONE_CONFIG_IX_REGION=auto
rclone lsf ix:indexed-xyz-wnam/ethereum/raw/blocks/v2.0.0/ | tail -3
```

DuckDB (path layout `s3://indexed-xyz-wnam/{chain}/raw/{table}/v2.0.0/dt=YYYY-MM-DD/*.parquet`):

```sql
SET s3_endpoint='ed5d915e0259fcddb2ab1ce5592040c3.r2.cloudflarestorage.com';
SET s3_region='auto';
SET s3_access_key_id='...'; SET s3_secret_access_key='...';
SELECT count(*) FROM read_parquet('s3://indexed-xyz-wnam/ethereum/raw/blocks/v2.0.0/dt=2026-09-15/*.parquet');
```

### CryptoHouse (ClickHouse HTTP — the free Solana firehose)

```bash
curl -s -u 'crypto:' https://crypto-clickhouse.clickhouse.com \
  --data-binary "SELECT count() FROM solana.token_transfers WHERE block_timestamp >= now() - INTERVAL 10 MINUTE"
```

**Query rules (learned the hard way):** the sort key is ``(block_timestamp, block_slot, id)`` —
filter on **`block_timestamp` first** or you'll trip the 10-billion-row scan limit.
Limits: 60 queries/hour/user · 60s timeout · 10B rows scanned. Solana is near-realtime
(verified minutes-fresh); Ethereum/Base are snapshot-era (good depth, incl. traces).

### Bitquery cloud samples (public S3, no key)

```sql
SELECT * FROM read_parquet(
  'https://bitquery-blockchain-dataset.s3.us-east-1.amazonaws.com/solana/balance_updates/390740000_390740049.parquet'
) LIMIT 10;
```

Full datasets are licensed; the public layout (`<chain>/<topic>/<startSlot>_<endSlot>.parquet`) doubles as schema reference.

## Redundancy per dependency

| Need | Independent paths |
|---|---|
| ETH wallet history | BigLake · Indexed.xyz · Envio · SQD |
| ETH traces | BigLake · Envio `-traces` · CryptoHouse (snapshot-era traces) |
| Multi-chain EVM (base/opt/gnosis/linea/zksync) | Indexed.xyz · Envio · own indexer (hot+B2) |
| Solana history | SQD · Envio (beta) · **CryptoHouse (live SQL)** · Bitquery (licensed) · own indexer |
| Tron | SQD · own indexer |
| Hyperliquid fills | SQD (single source — watch) |
| BTC | SQD · BigLake |
