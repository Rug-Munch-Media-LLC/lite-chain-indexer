# INTELLIGENCE-DESIGN.md — what we are building, and why it beats the incumbents

**Status:** canonical direction · owner: Rug Munch Media LLC Engineering
**Opened:** 2026-11-18 · measured facts dated inline

This document states the **design intent** of the intelligence platform: how
data is gathered, how it is routed, how wallet entities grow, and what our
advantage actually is. If a component disagrees with this file, one of them is
wrong and both must be fixed in the same commit.

---

## 1. The thesis, in one paragraph

Incumbents (Helius, Arkham, Nansen) sell **computed labels** and **query
access**. We cannot out-label them on day one. What we can own is the
**archive plus the reasoning over it**: permanent 90-day+ history in B2 we
control, one entity model that every product writes into, and an AI layer that
turns a question into the right tier call automatically. The moat is not the
label count — it is that **our entity graph compounds every time any product
touches a wallet**, and nobody else has our corpus, our scanners, or our Rag.

---

## 2. What we actually have (measured 2026-11-18, not estimated)

### 2a. Wallet labels — 39.5M distinct addresses, but read the breakdown

| chain | distinct wallets | note |
|---|---|---|
| **btc** | **39,269,486** | 99.4% of the total. Wilder dataset. |
| ethereum | 158,471 | real EVM coverage |
| solana | 103,322 | |
| base | 5,988 | thin |
| bsc | 5,520 | thin |
| avalanche | 4,942 | thin |
| worldchain | 4,902 | |
| ton | 3,901 | |
| arbitrum | 3,152 | thin |
| optimism | 2,107 | thin |

**The honest read: our EVM and Solana coverage is ~280k wallets, not 39.5M.**
The 37.4M wilder BTC addresses are classification-only and add no tracing
ability. Reporting 39.5M as our label count would be the kind of number that
looks good and answers nothing.

### 2b. The wilder BTC set is not worthless — most of it is

The 37.4M addresses break down as exchange (15.1M), p2p_service (13.3M),
gambling (3.2M), individual (2.6M), darknet (2.4M). Volume of addresses is not
value.

**`default.realcats_btc` is the valuable part**: 90,597 BTC wallets with actual
`balance`, `total_received`, `total_sent` **and USD figures**, labelled by threat
type. This is where BTC value lives:

| label | wallets | USD received |
|---|---|---|
| Fake Returns Scam | 709 | 606B |
| Blackmail Scam | 27,141 | 112B |
| Sextortion Scam | 18,620 | 105B |
| Bitcoin Tumbler | 2,714 | 70B |
| **Terrorism** | 85 | 46B |
| Investment Scam | 3,336 | 34B |
| **Ransomware** | 20,548 | 26B |
| Darknet Market | 1,405 | 7B |

**Decision: keep `realcats_btc` (90,597 wallets, real flows, threat-labelled).
Treat the 37.4M wilder classifications as a reference index, not as coverage.**
They are useful for "is this BTC address known-bad" lookup; they are not a
tracing corpus.

### 2c. Entities — 223 rows. This is the gap.

`wallet_entities` holds **223 rows, all `same_deployer`**. Everything else —
shared funding source, bridge use, CEX deposit clustering, behavioural
fingerprint, AMM pool ownership — does not exist yet.

This is the single most important number in this document. Labels are lookup;
**entities are intelligence.** Arkham's product is entity clustering. 223 is
where we start, not where we are.

### 2d. BTC tracing is claimed but not built

We hold 39.3M BTC labels and **zero BTC transaction tables**. `biglake_source.py`
says it plainly: *"`btc/` exists in the lake but is not mapped yet."*

AWS Open Data carries `v1.0/btc/blocks/` and `v1.0/btc/transactions/`. So BTC
tracing is **one adapter away** — we have the labels and the source and no
bridge between them. Until that lands, BTC is a label lookup, not tracing.

---

## 3. The data tiers and when each is used

| tier | holds | retention | answers |
|---|---|---|---|
| ClickHouse `wallet_memory` | hot index | ~30 days | live scans, alerting, dashboards |
| **B2 parquet** | permanent archive | **90 days+** | history, DuckDB, backtest, entity batch |
| Envio HyperSync | 19 chains | upstream | gap ranges, traces on eth+base |
| Subsquid | 13 EVM + Tron, 2021+ | upstream | deep backfill |
| BigQuery public | eth, solana | full | Solana/ETH deep history |
| BigLake AWS | **eth, btc** | full | ETH+BTC history, **BTC is unmapped today** |
| RPC pool | tip | n/a | what is true now |

**Rule: a routine read comes from tier 1 or 2.** Reaching an external provider
for data we already archive is paying twice.

---

## 4. Intelligent routing — the part that must not be hand-tuned

The router's job: **given (chain, table, time range, address filter), pick the
cheapest tier that can answer it, and never hammer one provider.**

Selection order, and why:

1. **ClickHouse** if the range is inside ~30 days and the table is hot.
   Zero external cost.
2. **B2 parquet via DuckDB** if the range is older but archived. Zero external
   cost. This is the tier that makes us different — the incumbents charge for it.
3. **Local index gap** → fill from the widest external source that covers it:
   Subsquid (13 chains, 2021+), Envio (19 chains), BigQuery (sol/eth),
   BigLake (eth/btc).
4. **RPC** only for tip/receipt/state that no archive holds.

**Anti-hammering, all four levers together:**

- **Span gating.** Measured `eth_getLogs` span ranges 0–2000 blocks across tiers.
  A slice is 200 blocks chunked at 50, so an endpoint below 50 can never complete
  a DEX chunk. Retrying it burns the slice budget until `slices_done=0` while the
  pool looks healthy — that froze 8 of 21 chains on 2026-11-18.
- **rps below advertised.** rps=10 soft-banned thirdweb within minutes (62 of 81
  softban events). The advertised tier number is where throttling *starts*.
- **Per-KEY pacing, per-ACCOUNT quota.** N keys give N rate buckets; they do not
  multiply the daily budget, so rotation cannot silently spend.
- **Free-first ordering.** Keyless endpoints lead; keyed capacity is touched only
  when free capacity is exhausted. That is what keeps us off ban lists.

### 4a. The routing brain already exists — do not rebuild it

`lite_indexer/query/router.py` binds six legs and orders them
**free-first**:

    ClickHouse  ->  B2 cold      ->  BigLake/S3  ->  Envio
                ->  indexed     ->  SQD         ->  RPC (last, paid)

`lake_preference.should_try_lake()` is the gate that decides whether the free
external lake is consulted at all. Its docstring records the bug it exists to
prevent: gating the lake on "does the paid RPC leg need to run" made the system
skip the one tier that holds wide history **and costs nothing per call**,
precisely when that tier had something to offer.

**Design rule: a new source is a new LEG, not a new router.** The ordering,
gating and row-shape normalisation stay in one place; a provider is added by
implementing a leg and registering it.

### 4b. Measured chain coverage per tier (2026-11-18)

| tier | chains | chains covered |
|---|---|---|
| **Subsquid** | **14** | arbitrum, arbitrum-nova, avalanche, base, celo, ethereum, gnosis, linea, mantle, metis, optimism, polygon, scroll, zksync |
| **Envio HyperSync** | **18** | the same set **plus** blast, bsc, fantom, fraxtal, hyperliquid, **tron** |
| SQD ∪ Envio | **19** | every chain we actively index |
| BigQuery public | eth, solana (+19 more datasets, see 4c) | deep history where the others are thin |
| BigLake AWS | eth, btc | full history, **BTC unmapped today** |

SQD and Envio overlap on 13 chains and diverge usefully: SQD does **metis**,
Envio does **blast/fantom/fraxtal/hyperliquid/tron**. A chain in neither set has
to come from BigQuery or RPC.

### 4c. BigQuery has 20 crypto datasets — we wired 2

`bigquery-public-data` carries, verified by `bq ls`:

    crypto_ethereum        crypto_ethereum_classic   crypto_polygon
    crypto_solana_mainnet_us  crypto_bitcoin         crypto_bitcoin_cash
    crypto_litecoin        crypto_dogecoin            crypto_dash
    crypto_polkadot        crypto_kusama              crypto_near_mainnet_us
    crypto_aptos_mainnet_us  crypto_sui_mainnet_us    crypto_iotex
    crypto_multiversx_mainnet_eu   bitcoin_blockchain
    blockchain_analytics_ethereum_mainnet_us

`bigquery_tables.py` currently maps **eth and solana only**. Every dataset above
is a free, full-history table set: `crypto_polygon` alone exposes blocks,
contracts, logs, token_transfers, tokens, traces and transactions, day-
partitioned on `block_timestamp` — i.e. the same shape we already read.

**This is the widest free win available and it is config, not architecture**: a
new dataset is a table map, because the SQL builder, quota rotation and
`BigQuerySpec` already exist.

**Provider names, keys and caps live in the private overlay**
(`LITE_INDEXER_ENDPOINT_OVERLAY`), never in git — `check-public-no-freemium.py`
enforces it.

---

## 5. The entity system — how it grows, and why that is our advantage

`wallet_entities` is the compounding asset. The rule: **every product that
touches a wallet writes the relationship it discovered back**, so the graph grows
from real usage rather than a periodic scrape.

Heuristic types to build, in order of signal-per-effort:

| heuristic | method | chains | exists? |
|---|---|---|---|
| `same_deployer` | identical contract deployer | EVM | **yes, 223** |
| `shared_funding` | first-inbound address clusters N wallets | EVM+SOL | no |
| `cex_deposit` | withdrawal address matches a known CEX hot wallet | EVM+SOL | no |
| `bridge_corridor` | same address pair across a bridge hop | multi | no |
| `behavioural` | timing + gas-price + nonce fingerprint | EVM | no |
| `amm_pool_owner` | LP token holder ↔ pool ↔ deployer | EVM | no |

**`cex_deposit` is the highest-value next step**: we already hold 15.7M
exchange-labelled addresses. Clustering wallets by shared withdrawal source is
the single heuristic that turns "anonymous wallet" into "an entity we can name".

**Entity confidence is a float, never a boolean.** A heuristic produces a
confidence and a source; a label with `confidence < threshold` is served as
"possible" and never used to accuse. This matters more than accuracy on the
happy path: a wrong attribution in a scanner is a false accusation with a
customer behind it.

---

## 6. How RugBrain benefits, and how the routing reaches it

RugBrain already reads `wallet_memory` (`scripts/ingest_wallet_labels.py`,
`seed_qdrant_collections.py`). The chain is:

    wallet_memory (ClickHouse)  ->  rugbrain ingest  ->  Qdrant
                                        |
                            entity + label context in every RAG answer

That means a scan or a chat answer can cite **our** entity graph rather than
restating a third-party API. The compounding effect: an entity discovered by the
scanner is immediately available to the bot's answers and to every later scan,
without a new integration.

**This is the parity argument with a twist.** Helius sells tracing; Arkham sells
entity pages; Nansen sells smart-money labels. We are building a system where
the **label, the entity, the archive and the AI answer share one store**, so the
marginal cost of the next question approaches zero. That is the "any dumbass can
use it" goal: the user does not need to know which provider has the data,
because the router does.

---

## 7. Webhooks — designed against the same model

A webhook is **a write into the same schema as a polled event**, so downstream
cannot tell them apart:

    provider webhook  ->  verify signature  ->  normalise to warehouse row
                      ->  ClickHouse (same table as polling)
                      ->  entity hook: does this tx touch a known wallet?

Non-negotiables:
- **Signature verification first.** An unverified webhook is an injection vector
  into our own intelligence.
- **Idempotency by (chain, tx_hash, log_index)**, so a replay is free.
- **The entity hook runs after the write**, not before: an entity that fails must
  never lose the raw event.
- **A webhook and a poll for the same range must converge on the same rows.**
  That is testable and it is the acceptance criterion.

---

## 8. Where we are honestly behind, and the order to close it

1. **BTC tracing does not exist.** 39.3M labels, no transaction table. One
   BigLake adapter away — highest-value gap relative to effort.
2. **Entity clustering is 223 rows.** `cex_deposit` is the next heuristic, and we
   already hold the exchange labels it needs.
3. **90-day backfill is not run.** The writer exists (`lite_indexer/archive/`);
   the per-source adapters (Subsquid, Envio, BigQuery) do not.
4. **EVM testnet-style thin chains** (base 5,988; bsc 5,520; arbitrum 3,152) are
   under-covered relative to their scan volume.

We are not at Helius or Arkham parity today. The path is the four items above,
in that order, and each one is measurable.

---

## 9. Verification — how to prove this document is true

    # label coverage by chain (never quote the union)
    docker exec rmi-clickhouse clickhouse-client --query \
      "SELECT chain_id, uniqExact(address) FROM wallet_memory.wallet_labels GROUP BY 1 ORDER BY 2 DESC"

    # entity count (the intelligence number, not the label number)
    docker exec rmi-clickhouse clickhouse-client --query \
      "SELECT heuristic_type, count() FROM wallet_memory.wallet_entities GROUP BY 1"

    # BTC value set (the part worth keeping)
    docker exec rmi-clickhouse clickhouse-client --query \
      "SELECT label, count(), round(sum(total_received_usd)) FROM default.realcats_btc GROUP BY 1 ORDER BY 3 DESC"

    # archive depth per chain
    rclone lsf -R --dirs-only rugmunch-b2:rmi-chain-data/parquet/<chain> | \
      grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | sort -u | wc -l

---

## 10. Known-false things this document exists to stop

- **"We have 39.5M labelled wallets."** We have ~280k EVM+Solana and 39.3M
  BTC classifications, most of which are classification-only.
- **"BTC tracing works."** It does not. There is no BTC transaction table.
- **"We have an entity system."** 223 rows, one heuristic.
- **"BigLake covers all chains."** AWS has exactly `eth/` and `btc/`.
- **"The archive is 90 days."** Measured: base 71, ethereum 67; the rest less.
- **"Alchemy gives us range because we have 3 keys."** Keys multiply rate, never
  span.
