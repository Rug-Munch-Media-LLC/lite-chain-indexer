# Free Data Sources — Redundancy Matrix

Independent external sources the query layer relies on (verified 2026-09-16).
Goal: no single provider failure interrupts wallet history, tracing, or backfills.

| Source | Chains | Data | Access | Status |
|---|---|---|---|---|
| AWS public blockchain ("BigLake") | Ethereum, Bitcoin | blocks, txs, logs, token_transfers, **traces**, contracts (full history) | anonymous S3 via DuckDB | ✅ integrated (`biglake_source`) |
| **AWS v1.1 (SonarX + chains)** | arbitrum, base, aptos, provenance, xrp (sonarx); bnb, cronos, stellar, ton | **traces** + transfers + logs + receipts + txs (+approvals); bnb: blocks/txs; ~1-week lag on sonarx, bnb fresh to yesterday | public S3 `s3://aws-public-blockchain/v1.1/…`, anonymous, DuckDB | ✅ verified 2026-09-16 (base 251.7M traces/day!) |
| Envio HyperRPC / HyperSync | 79+ EVM + Solana (early) | logs, txs, receipts; `trace_block` on eth/base `-traces` subdomains | free API token in URL | ✅ integrated (`envio_source`) |
| SQD Portal | Solana, Tron, Bitcoin, Hyperliquid, **EVM (13 chains)** | Solana instructions/token balances; TRC-20 + native internals; HL fills; **EVM execution traces — complete history, callFrom/callTo filters** | HTTP POST JSONL (public endpoints) | ✅ integrated (`sqd_source`) |
| **Indexed.xyz** | ethereum, base, gnosis, linea, optimism, zksync, zora, sui, pgn, arweave | raw blocks/txs/logs + **decoded** logs, CC0 | public R2 credentials (below), S3 API / DuckDB | ✅ **INTEGRATED** (f660fcc — router third path, minutes-fresh) |
| solarchive.org | Solana | txs/accounts/tokens Parquet (CC-BY) | public HTTPS — data host currently 404 | 🟡 monitor |
| **CryptoHouse** (crypto.clickhouse.com) | Solana (LIVE), Ethereum/Base (snapshot-era ≈24.99M blocks), Polymarket | blocks, transactions, token_transfers, **instructions**, accounts, rewards, **traces/logs** (eth/base), + MVs | ✅ **HTTP direct access verified** — `https://crypto-clickhouse.clickhouse.com`, user `crypto`, empty password (native 9440 TLS flaky from Talos) | ✅ verified 2026-09-16 |
| **Bitquery cloud dumps** | Solana, Tron, BTC, Hyperliquid, Robinhood, BSC, XRP, EVM, Polymarket, PumpFun | Parquet by slot ranges `<chain>/<topic>/<start>_<end>.parquet` | **samples free** on `bitquery-blockchain-dataset.s3.us-east-1.amazonaws.com` (verified 513k-row read); full dataset licensed (sales@bitquery.io) | 🟡 samples only |
| **Bitquery API (account exists!)** | V2: eth/bsc/base/arbitrum/optimism/matic/robinhood/solana/tron/hyperliquid + Trading. V1: BTC family, etc. | GraphQL: transfers, DEX trades, balances, holders, **traces + Solana instructions (3-mo window on any plan)**, events/calls. Coinpath money-flow = AI-Investigation MCP or Enterprise | `https://streaming.bitquery.io/graphql` (V2) — keys authenticate (**use `X-API-KEY:` header**; fresh Bearer tokens use `Authorization: Bearer ory_at_…`). Blocker: `No active billing period` → activate trial/plan at account.bitquery.io | 🔑 keys live in fleet, billing inactive |
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

**Validated recipes (2026-09-16):**

```sql
-- Solana: every transaction touching a wallet's token balances (last 30 min)
SELECT signature, block_timestamp, err FROM solana.transactions
WHERE block_timestamp >= now() - INTERVAL 30 MINUTE
  AND arrayExists(x -> x.owner = '<WALLET>', post_token_balances) LIMIT 10;

-- Ethereum: internal traces for an address (parity-style, snapshot ≤ 2026-04-30)
SELECT block_timestamp, from_address, to_address, value, call_type
FROM ethereum.traces WHERE to_address = '<ADDR>' LIMIT 10;

-- Ethereum: decoded event logs (event_signature/event_params populate on newer rows)
SELECT block_number, event_signature, event_params FROM ethereum.logs
WHERE address = '<CONTRACT>' ORDER BY block_number DESC LIMIT 10;
```

**Data boundaries:** Solana = live (verified minutes-fresh). Ethereum & Base = snapshot through
**2026-04-30** (traces/logs included). Bonus: `polymarket` db (markets, positions, PnL).
`solana.transactions` also carries `pre/post_token_balances`, `log_messages`, `balance_changes`
— the richest wallet-delta source of any free provider.

**Query rules (learned the hard way):** the sort key is ``(block_timestamp, block_slot, id)`` —
filter on **`block_timestamp` first** or you'll trip the 10-billion-row scan limit.
Limits: 60 queries/hour/user · 60s timeout · 10B rows scanned. Solana is near-realtime
(verified minutes-fresh); Ethereum/Base are snapshot-era (good depth, incl. traces).

### AWS v1.1 — SonarX bulk lake (arbitrum/base traces + transfers)

```sql
SELECT count(*) FROM read_parquet(
  's3://aws-public-blockchain/v1.1/sonarx/base/traces/date=2026-09-09/*.parquet');  -- 251.7M rows
```

Layout: `v1.1/sonarx/{chain}/{table}/date=YYYY-MM-DD/`. Tables: blocks, transactions, transactions_failed,
logs, receipts, transfers, approvals, traces. Chains: arbitrum, base, aptos, provenance, xrp.
Also `v1.1/bnb/{blocks,transactions}` (fresh), `cronos/`, `stellar/`, `ton/`. Schemas: UPPERCASE columns
for sonarx (BLOCK_NUMBER, FROM_ADDRESS, PATH, TRANSACTION_HASH…). Ideal for bulk backfill/analytics;
SQD remains the live/full-history on-demand tier.

### SQD EVM traces (free, complete history)

```bash
python3 -m lite_indexer.query.sqd_source evm-traces polygon --from-block N --to-block M --address 0xWALLET
```

13 EVM chains, blocks from 2021+ verified, ``callFrom``/``callTo`` filters (two queries per wallet).
Wired into the router as the ``internal_transfers`` leg — failures fall through to RPC.

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
| Traces (internal transfers, EVM) | **SQD Portal — FREE, complete history, 13 chains** (ethereum, arbitrum, arbitrum-nova, base, polygon, avalanche, optimism, gnosis, celo, zksync, scroll, linea, mantle) · BigLake (eth, full history) · **AWS v1.1 SonarX (arbitrum/base, bulk, ~1wk lag)** · Envio `-traces` (eth/base/arbitrum/gnosis live) · CryptoHouse (eth/base snapshots) · Bitquery (paid — no longer needed) |
| Multi-chain EVM (base/opt/gnosis/linea/zksync) | Indexed.xyz · Envio · own indexer (hot+B2) |
| Solana history | SQD · Envio (beta) · **CryptoHouse (live SQL)** · Bitquery (licensed) · own indexer |
| Tron | SQD · own indexer |
| Hyperliquid fills | SQD (single source — watch) |
| BTC | SQD · BigLake |
