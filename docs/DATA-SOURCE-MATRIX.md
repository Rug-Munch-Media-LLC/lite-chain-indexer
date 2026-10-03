# DATA-SOURCE-MATRIX.md — which source serves which chain × data-type, and is it free?

> Validated 2026-10-03 with live queries (BigQuery MCP, CryptoHouse HTTPS endpoint,
> AWS S3 bucket listing, Envio docs/pricing). "VALIDATED" = queried today, not assumed.
> Doctrine unchanged: no single source is load-bearing; BigQuery is a rotating fallback
> (see DATA-SOURCES.md). This doc is the coverage truth table behind the QueryRouter.

## TL;DR

- **Only 2 of our 20 chains have live BigQuery datasets**: ethereum (two datasets, incl. traces)
  and polygon (Google-managed, NO traces). `crypto_polygon` is frozen at 2024-09-01 — do not use.
- **CryptoHouse is the best truly-free traces source**: ethereum + base traces AND the only
  free Solana source with inner instructions (= traces). $0, no key, HTTPS endpoint.
- **HyperSync free tier is the broadest single source**: 18/20 chains (all but solana + metis),
  blocks/txs/logs + traces — but eth/base traces are a PAID add-on.
- **AWS S3 parquet**: free full-history for ethereum (incl. traces), arbitrum (incl. traces),
  base (incl. traces); bnb = blocks+txs ONLY.
- **metis has NO third-party free source** — our own RPC indexer is the only validated coverage.
- **Labels and decoded calls are not freely available anywhere** except decoded_events on the two
  Google-managed datasets (eth + polygon). Labels stay in-house (ClickHouse ~295k).

---

## 1. The Matrix (rows = chains, cols = data types; cell = winning FREE source)

Legend: **BQ** = BigQuery public dataset · **CH** = CryptoHouse · **HS** = HyperSync (free tier)
· **AWS** = AWS S3 parquet · **SQD** = Subsquid portal · **OWN** = our lite-chain-indexer RPC pool
· **—** = no free source found. `(p)` = paid add-on / metered. Token transfers on EVM are
derivable from logs where no dedicated table exists.

| Chain | blocks/txs | logs | **traces** | token_transfers | token metadata | decoded calls |
|---|---|---|---|---|---|---|
| ethereum | BQ·CH·AWS·HS | BQ·CH·AWS·HS | **BQ crypto_ethereum.traces** (LIVE) · CH · AWS · HS(p) | BQ·AWS | BQ (tokens/contracts) | BQ goog decoded_events |
| base | CH·AWS·HS | CH·AWS·HS | **CH base.traces** · AWS sonarx · HS(p) | AWS (transfers) | — | — |
| arbitrum | AWS·HS | AWS·HS | **AWS sonarx traces** · HS | AWS (transfers) | — | — |
| bsc | AWS(bnb)·HS | HS | **HS** | HS | — | — |
| polygon | BQ goog·HS | BQ goog·HS | **HS** | HS | — | BQ goog decoded_events |
| optimism | HS | HS | **HS** | HS | — | — |
| avalanche | HS | HS | **HS** | HS | — | — |
| fantom | HS | HS | **HS** | HS | — | — |
| gnosis | HS | HS | **HS (on request)** | HS | — | — |
| celo | HS | HS | **HS** | HS | — | — |
| linea | HS | HS | **HS** | HS | — | — |
| scroll | HS | HS | **HS** | HS | — | — |
| mantle | HS | HS | **HS** | HS | — | — |
| blast | HS | HS | **HS** | HS | — | — |
| fraxtal | HS | HS | **HS** | HS | — | — |
| robinhood | HS | HS | **HS** | HS | — | — |
| hyperliquid | HS | HS | **HS** | HS | — | — |
| tron | HS·SQD | HS·SQD | **OWN** (internal transfers) | OWN | — | — |
| solana | **CH** | n/a (logs in tx) | **CH instructions** (= inner ix) | CH token_transfers | CH tokens | CH instructions (parsed col) |
| metis | **OWN** | **OWN** | **OWN** | **OWN** | — | — |

OWN (lite-chain-indexer) covers all 20 chains for transfers/swaps/deployers/internal-traces
via keyless RPC (see §2.6) — it is the universal fallback, not listed in every cell for readability.

**FULLY free-covered chains** (every core data type incl. traces has a $0 path):
ethereum, base, arbitrum, solana. Plus effectively-covered via HS free fair-use: bsc, polygon,
optimism, avalanche, fantom, celo, linea, scroll, mantle, blast, fraxtal, robinhood, hyperliquid.

**Gap chains**: metis (OWN only), tron (traces OWN only), gnosis traces (HS on-request → OWN).

---

## 2. Source details

### 2.1 BigQuery public datasets — VALIDATED 2026-10-03

Project `bigquery-public-data`. Free tier 1 TiB scanned/month per billing account
(we rotate 4 accounts — see DATA-SOURCES.md). Always day-partition-filter.

| Dataset | Status | Tables | Notes |
|---|---|---|---|
| `crypto_ethereum` | **LIVE** (max block 2026-10-03) | blocks, transactions, logs, **traces**, token_transfers, tokens, contracts, balances (VIEWs) | Best free eth traces + token metadata |
| `goog_blockchain_ethereum_mainnet_us` | **LIVE** (2026-10-03) | blocks, transactions, logs, **traces**, token_transfers, receipts, **decoded_events**, accounts* | Google-managed; richer than crypto_ethereum |
| `goog_blockchain_polygon_mainnet_us` | **LIVE** (2026-10-03) | blocks, transactions, logs, receipts, decoded_events | **NO traces, NO token_transfers** (derive from logs) |
| `crypto_polygon` | **FROZEN 2024-09-01** | full EVM set incl. traces | DO NOT USE for live — historical only |
| `crypto_ethereum_classic` | FROZEN 2020-11-22 | full EVM set | dead |
| `crypto_bitcoin` (+ bitcoin_cash, dash, dogecoin, litecoin, zcash) | LIVE | blocks, transactions, inputs, outputs | UTXO schema; none are target chains |
| `crypto_{arbitrum,avalanche,fantom,optimism,tron,cronos,near,base,bsc,solana,celo,gnosis,blast,linea,mantle,scroll,metis,fraxtal}` | **DO NOT EXIST** | — | Access-denied/nonexistent confirmed individually 2026-10-03. The old ethereum-etl community datasets are gone from bigquery-public-data. |

### 2.2 CryptoHouse — VALIDATED live 2026-10-03

Free ClickHouse+Goldsky public service. No key, no account, no meter.
- Endpoint: `https://crypto-clickhouse.clickhouse.com/?user=crypto&password=` (HTTPS POST SQL)
  or `clickhouse client --host crypto-clickhouse.clickhouse.com --secure --user crypto --password ''`
- Databases (live-verified): `ethereum`, `base`, `solana`, `polymarket`
- `ethereum`/`base` tables: blocks, logs, **traces**, transactions (+ first_time_for_address MV)
- `solana` tables: blocks, transactions, **instructions** (inner-ix = trace parity),
  token_transfers, tokens, accounts, block_rewards + ~20 daily aggregate MVs
- Caveats: ReplacingMergeTree (dedupe on read), at-least-once delivery, date-filter big tables.
- **Best for**: eth+base traces at $0, and ALL Solana depth at $0. The single most valuable
  $0-unmetered source we validated.

### 2.3 HyperSync (Envio) — docs.envio.dev, VALIDATED against supported-networks page

- 18/20 target chains: ethereum(1), base(8453), bsc(56), polygon(137), arbitrum(42161),
  optimism(10), avalanche(43114), fantom(250), gnosis(100), celo(42220), linea(59144),
  scroll(534352), mantle(5000), blast(81457), fraxtal(252), robinhood(4663),
  hyperliquid(999), tron(728126428). **Missing: solana (EVM+Fuel only), metis.**
- Data: blocks, transactions, logs, traces (field-selected, Parquet/Arrow streaming).
- **Traces caveat**: eth + base traces are a PAID add-on ("On Demand" pricing); gnosis traces
  access-on-request; all other chains include traces in the standard stream.
- Pricing (validated pricing page): **Free $0 fair-use rate-limited** (API token required) ·
  Starter $70/mo 100rpm · Pro $480/mo 1000rpm. Rate limits cap speed, not volume.
- Endpoint pattern: `https://<chain>.hypersync.xyz` or `https://<chain-id>.hypersync.xyz`.

### 2.4 Subsquid SQD Network

- 100–200+ chains: EVM (incl. long-tail), Substrate, Starknet, Fuel, Tron; Solana in beta.
- Data: logs, receipts, transactions, **traces**, state diffs per tx — good trace depth.
- Cost: portal free quota for dev volumes (our sqd-fill path relies on it); post-mainnet
  extraction metered ≈ $1–5 per TB. Near-free, not guaranteed-free at scale.
- Not yet chain-verified per-target here; treat as secondary EVM bulk path (already wired
  into `lite_indexer lake sqd-fill`).

### 2.5 AWS S3 public blockchain datasets — VALIDATED via bucket manifest 2026-10-03

`s3://aws-public-blockchain/` (us-east-2, `--no-sign-request`, parquet, date-partitioned).
HTTP listable: `https://aws-public-blockchain.s3.us-east-2.amazonaws.com/manifest.json`.

| Path | Tables | Target chain? |
|---|---|---|
| `v1.0/eth/` | blocks, contracts, logs, token_transfers, **traces**, transactions (daily, 0-day lag) | ethereum ✅ FULL |
| `v1.1/sonarx/arbitrum/` | approvals, blocks, logs, receipts, **traces**, transactions, transfers, transactions_failed | arbitrum ✅ FULL |
| `v1.1/sonarx/base/` | same full set | base ✅ FULL |
| `v1.1/bnb/` | **blocks, transactions ONLY** (prefix-verified) | bsc ⚠️ partial |
| `v1.1/cronos/` | blocks, decoded-events, logs, receipts, transactions | not a target |
| others: btc, aptos, provenance, xrp, stellar, ton | various | not targets |

Query path: DuckDB/ClickHouse `s3()` direct on parquet — already our `lake backfill` route.

### 2.6 Our own RPC indexer (lite-chain-indexer) — VALIDATED from source

- Chains: **all 20 targets** (`lite_indexer/chains/chain_ids.py`: 19 EVM incl. blast, fraxtal,
  metis, robinhood, hyperliquid + solana + tron; zksync extra). 123 keyless endpoints.
- Data: transfers (ERC-20/TRC-20/SPL), DEX swaps (V2/V3 forks), deployer events,
  **internal-transfer traces**, pair/pool creation. ClickHouse hot + B2 parquet cold.
  359M+ rows in production. $0 marginal cost; per-IP rate limits are the constraint.
- This is the ONLY coverage for **metis**, and the only free traces path for **tron** and
  (unless HS grants it) **gnosis**.

---

## 3. Coverage gaps (no free third-party source)

| chain × data-type | status | mitigation |
|---|---|---|
| metis × everything | **OWN only** (no HS, no BQ, no AWS, no CH) | own RPC indexer |
| tron × traces | HS has no trace stream for tron; OWN only | own indexer internal transfers |
| gnosis × traces | HS access-on-request | ask Envio, else OWN |
| solana × (BQ/HS) | non-EVM gap | CryptoHouse covers fully |
| polygon × traces | BQ crypto_polygon frozen; goog dataset lacks traces | HS free tier / OWN |
| eth/base × traces on HS | paid add-on | use BQ/CH/AWS instead |
| **token metadata** (all chains except eth via BQ, sol via CH, eth via AWS contracts) | no free source | OWN + RPC + databus providers |
| **decoded contract calls** | only goog decoded_events (eth, polygon) | decode from logs ourselves |
| **labels** | no free source anywhere | in-house ClickHouse label sets |

## 4. ClickHouse `bigquery()` federation note

ClickHouse can query BigQuery live via the `bigQuery` table function
(`SELECT ... FROM bigQuery('<project>', 'bigquery-public-data', 'crypto_ethereum.traces')`
with GCP credentials configured server-side), letting rmi-clickhouse federate the two live
eth datasets and the live goog polygon dataset without an ETL copy. Caveats: every federated
query scans BQ bytes against the 1 TiB/month free quota (partition filters mandatory, matching
the QueryRouter day-partition guard), latency is seconds-not-ms, and BQ stays a rotating
fallback per doctrine — never the only path.

## 5. Surprises found during validation

1. **BigQuery lost the community EVM datasets** — crypto_arbitrum/optimism/avalanche/fantom/
   bsc/tron/near are all gone; only ethereum is live. `crypto_polygon` silently froze 2024-09-01.
2. **`goog_blockchain_polygon_mainnet_us` exists and is live** (decoded_events!) but has no
   traces and no token_transfers — better and worse than expected at once.
3. **CryptoHouse is unmetered-$0 with eth+base TRACES and full Solana inner instructions** —
   the best free traces source we found, plus a bonus `polymarket` database.
4. **HyperSync covers Tron, Robinhood and HyperEVM** — far broader than "EVM L2s" implies;
   but eth/base traces moved behind a paid add-on.
5. **AWS bnb is blocks+transactions only** — no logs, no traces; BSC needs HS or OWN.
6. **metis is a genuine desert** — absent from HyperSync's 80+ network list; OWN is the
   only validated source.
