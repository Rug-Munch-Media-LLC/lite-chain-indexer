# DATA-ARCHITECTURE.md — how the fleet gets on-chain data

**Status:** canonical draft · owner: Rug Munch Media LLC Engineering · 2026-11-18
**Scope:** every consumer — scanner, rugmunchbot, lite-chain-indexer, webhooks,
tracing. If a component disagrees with this file, the component is wrong or this
file is stale and must be fixed in the same commit.

---

## 1. The retrieval tiers, in order

Every on-chain question is answered from the cheapest tier that can answer it.

| # | tier | what it holds | retention | used for |
|---|---|---|---|---|
| 1 | **ClickHouse `wallet_memory`** | hot index built by lite-chain-indexer | **~30 days** (measured: 2026-10-19 → 2026-11-18) | live scans, dashboards, alerts |
| 2 | **B2 parquet** (`chain-data/parquet/<chain>/<table>/<date>/`) | permanent cold archive | **90 days and growing** | anything older than ClickHouse; DuckDB reads |
| 3 | **Envio HyperSync** | 19 chains, traces on eth+base | upstream, ~realtime | range the local index never covered |
| 4 | **Subsquid** | 13 EVM chains + Tron | upstream | full-range backfill, portal queries |
| 5 | **BigQuery GCP public** | `crypto_ethereum`, `crypto_solana_mainnet_us` | upstream, full history | Solana + Ethereum deep history |
| 6 | **BigLake / AWS Open Data** | `aws-public-blockchain/v1.0/{eth,btc}` | upstream, full history | ETH + BTC only — verified: that bucket has exactly `btc/` and `eth/` |
| 7 | **RPC pool** | live chain | n/a | tip state, receipts, anything not indexed |

**Tier 3–7 are for gaps, not routine reads.** A routine scan read must come from
tier 1 or 2 or it is paying an external provider for something we already own.

---

## 2. What each tier is authoritative for

- **Tier 1 (ClickHouse)** is the *hot* record. It is ~30 days by design; it is not
  a retention guarantee and never was.
- **Tier 2 (B2 parquet)** is the *permanent* record and the only 90-day source.
  DuckDB reads it without loading it into ClickHouse.
- **Tiers 3–6** are *backfill and gap* sources. They are not durable storage and
  we do not depend on their retention.
- **Tier 7 (RPC)** answers "what is true at the tip right now". It is never
  archived from — the indexer writes what it fetches.

---

## 3. The local-disk contract (bounded footprint)

The operator's rule: **gather → verify → upload → delete local**. Local disk must
never grow with retention.

    for each unit (one day, or one hour for high-volume chains):
      1. fetch from the chosen tier into staging/
      2. write parquet
      3. VERIFY   row count == source count
      4. upload   to b2:chain-data/parquet/<chain>/<table>/<date>/
      5. VERIFY   object exists in B2 at the expected size
      6. DELETE   the local parquet AND its staging dir
    B2 keeps 90 days permanently; local holds ~1 unit in flight.

**Step 6 does not exist yet.** `archive-daily.py` and `archive-export.py` upload
and keep. That is the single change that makes the footprint bounded.

**The delete must be gated on step 5, not on "upload returned 0".** An upload that
returned success without the object present is the exact failure that would
delete the only copy.

---

## 4. Provider rotation (measured, not assumed)

**Measured capability matrix, 2026-11-18** — largest `eth_getLogs` span that
returned a `result`, per provider per chain:

Provider names, per-account keys, tier lists and verified rate caps are **not
listed here on purpose**. The public tree ships free/keyless endpoints plus a
private overlay extension point (`LITE_INDEXER_ENDPOINT_OVERLAY`); the curated
freemium stack lives in the runtime overlay at
`/data/lite-indexer-endpoints.json` and never enters git.
`scripts/check-public-no-freemium.py` enforces that, and it is right to.

What matters for design, and is measurable without naming anyone:

- Measured `eth_getLogs` span ranged from **0 to 2000 blocks** across the
  curated tiers. One tier answered `eth_blockNumber` but rejected every
  `eth_getLogs` — it is a block/timestamp provider, not a log provider.
- A slice is 200 blocks chunked at 50, so **any endpoint below a 50-block span
  can never complete a DEX chunk**. Retrying it burns the slice budget until
  `slices_done=0` while the pool still looks healthy. That froze 8 of 21 chains.
- Three keys of one provider multiply **rate**, never **range**.
- Passing `--threads`/batch tuning, provider budgets and span per chain are all
  recorded in the private overlay and the generated
  `docs/RPC-CAPABILITY-MATRIX.md` (which names tiers by span, not by account).

**Rules that came from measurement:**
- **Never put a sub-chunk endpoint in a DEX leg list.** A slice is 200 blocks
  chunked at 50; an endpoint capped at 10 can never complete a chunk, and
  retrying it burns the slice budget until `slices_done=0` while the pool still
  looks healthy. That exact failure froze 8 of 21 chains.
- **rps=3, not the advertised tier.** rps=10 soft-banned a curated tier within minutes
  (62 of 81 softban events on `56.rpc.` and `100.rpc.a curated tier.com`; 439
  `all_endpoints_down`). The advertised number is where throttling *starts*.
- **Pacing is per KEY, quota is per ACCOUNT.** N keys of one provider give N rate
  buckets but do NOT multiply the daily budget. Rotation cannot silently spend.
- **Keyless endpoints lead.** The loader merges builtin (free) before overlay
  (keyed), so paid capacity is only touched when free capacity is exhausted —
  which is also what keeps us off ban lists.
- **`blockscout.com` is not an RPC.** It returns `<!DOCTYPE html>` for a
  JSON-RPC POST on every chain. Excluded via `logs_capable=False`.

---

## 5. What is genuinely shared, and what is not

**One RPC router, one vendored copy — currently DRIFTED.**

    canonical   rmi-backend/app/domains/scanner/engines/rpc_router/   (package, 1021 lines)
    vendored    rugmunchbot/src/rugmunchbot/_vendor/scanner_engines/rpc_router.py  (flattened, 784 lines)

The vendored copy is 237 lines behind and was flattened from a package into a
single file, so `refresh-vendors.sh` cannot reproduce it path-for-path. This is
the only real duplication in the RPC path and it must be reconciled (re-vendor
from the package, or retire the vendored copy and depend on the package).

**Not duplication** (verified by reading each file's purpose):
- `lite-chain-indexer/chains/endpoints.py` — the *public free pool + overlay hook*
  for the indexer. Different job: it is the endpoint catalogue, not a router.
- `rmi-backend/app/routers/v1/providers.py` — a *status endpoint* exposing
  provider health, not a client.

**The capability rules above belong to the shared layer, not to each consumer.**
Anything that dials a chain provider — scanner, bot, indexer, webhooks, traces —
must go through the one router so a ban, a span cap, or a rotation change is made
once.

---

## 6. Traces

Traces are the expensive path and are deliberately tiered:

    local ClickHouse internal_transfers   -> what we already indexed
    Envio HyperSync traces                -> ethereum, base only
    BigLake aws-public-blockchainv1.0/eth/traces -> ethereum, full history
    Subsquid                              -> available chains

Anything else needs the RPC `debug_trace*` family, which is the slowest and least
available tier — treat it as a fallback, not a default.

---

## 7. Parity with a curated tier / Arkham-class providers

Their advantage is breadth (every token, every wallet, precomputed) and a
retained history. Ours is that we own the data and the archive.

To reach functional parity on *calling ability*:
1. **90 days in B2 for every chain** — the archive is the thing they charge for.
2. **One router** so every consumer has the same provider breadth and rotation.
3. **Gap-filling from tiers 3–6** rather than claiming coverage we do not have.
4. **Webhooks** that write into the same ClickHouse schema, so a webhook event and
   a polled event are indistinguishable downstream.

We should not claim to match their computed-label depth. We should match, and
beat, their *retained history* and *query determinism*, because that is what our
archive gives us and what their API charges for.

---

## 8. Verification (how to prove this document is true)

    # tier 1 depth
    docker exec rmi-clickhouse clickhouse-client --query \
      "SELECT chain,min(toDate(block_timestamp)),max(toDate(block_timestamp)),count() \
       FROM wallet_memory.evm_transfers GROUP BY chain"

    # tier 2 depth per chain
    rclone lsf -R --dirs-only rugmunch-b2:rmi-chain-data/parquet/<chain> | \
      grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | sort -u | wc -l

    # tier 2 readability (must equal the verified unit sum)
    duckdb -c "SELECT count(*) FROM read_parquet('.../<chain>/<table>/*/*/*.parquet')"

    # provider span (the number that decides pool membership)
    python3 scripts/verify-rpc-pool.py <chain>

    # the LIVE prometheus rule, never the git copy
    curl -s localhost:9091/api/v1/rules | grep -A2 B2Retention

---

## 9. Known-false things this document exists to stop

- "BigLake covers all chains." It does not — `v1.0/` holds only `btc/` and `eth/`.
  The all-chain sources are **Subsquid** and **Envio**.
- "ClickHouse is our history." It is ~30 days. B2 is the history.
- "a curated tier gives us span because we have 3 keys." Keys multiply rate, not range.
- "The rule is deployed because it merged." `/etc/prometheus/rules/` is
  `chattr +i` and drifted 25 days; check `api/v1/rules`.
