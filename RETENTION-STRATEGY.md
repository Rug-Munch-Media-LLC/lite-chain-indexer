# RETENTION-STRATEGY.md — own the archive, borrow the history

**Status:** canonical · owner: Rug Munch Media LLC Engineering · 2026-11-18
**Question this answers:** for 90 days across all valuable chains, do we
(i) read it live from BigLake/BigQuery every time, (ii) ingest it daily into our
own archive, or (iii) some mix — and where is the line?

---

## 1. The answer, with the numbers that decide it

**Archive our own. Borrow history we have not yet produced. Never re-query
history we already own.**

Measured cost of our own 90-day archive (sizes from real B2 objects, dated):

| chain | MB/day | 90 days | $/mo at $6/TB |
|---|---|---|---|
| polygon | 750 | 65.9 GB | 0.39 |
| base | 176 | 15.5 GB | 0.09 |
| gnosis | 175 | 15.4 GB | 0.09 |
| ethereum | 147 | 12.9 GB | 0.08 |
| fantom | 77 | 6.8 GB | 0.04 |
| tron | 52 | 4.6 GB | 0.03 |
| optimism | 46 | 4.0 GB | 0.02 |
| bsc | 29 | 2.5 GB | 0.01 |
| 8 more chains | <10 each | ~2.6 GB | ~0.01 |
| **TOTAL (16 chains)** | | **130 GB** | **0.76** |

**$0.76/month for the entire 90-day, 16-chain archive.**

The alternative — re-reading 90 days from BigQuery on every query — costs
$6.25/TB scanned. One polygon-wide 90-day scan is ~70 GB = **$0.44 per query**.
Our whole archive costs less per month than **two** such queries.

The decision is not close. There is no configuration in which re-querying
history we could have archived is cheaper.

---

## 2. The three questions, separated

These are different problems and conflating them is how archives get built
twice or not at all.

### 2a. "Where does NEW data come from?" → live ingestion
Lite-chain-indexer polls the RPC pool and writes ClickHouse; `archive-export`
ships finished days to B2. This is the **producer**. It is what makes the data
*ours*: our row shape, our entity hooks, our labels.

### 2b. "Where does OLD data come from?" → BigLake / BigQuery / Subsquid / Envio
A chain we started indexing 3 weeks ago has no 3-month history of ours. That
history exists upstream and is free to read. Backfill it **once**, into our own
archive, and never read it upstream again.

### 2c. "How do we answer a question spanning both?" → the router
`router.py` already orders ClickHouse → B2 cold → BigLake → Envio → indexed →
SQD → RPC, free-first. A query older than our archive falls through to upstream
for **only the missing window**, and that window is also the thing the next
backfill job should pull.

---

## 3. Which of the "additional chains" are valuable enough

Not every chain earns its 90 days. Applying the filter honestly:

| chain | verdict | reason |
|---|---|---|
| ethereum, base, polygon, bsc, arbitrum, optimism | **archive** | scan volume + DEX activity; our products already serve them |
| gnosis, fantom, celo, mantle, linea, scroll, zksync, avalanche | **archive** | cheap (<16 GB each at 90d); thinning them saves cents and loses coverage |
| tron | **archive** | high USDT-transfer volume, and Envio covers it when our index lags |
| **btc** | **archive, via BigLake only** | we have 39M BTC labels and no BTC txs; AWS `v1.0/btc` is parquet UTXO and the cheapest path. Do NOT re-derive BTC from RPC. |
| metis, blast, fraxtal, hyperliquid, robinhood | **do not archive** | declared `dead` in chain-states.yml; stopped 2026-09-23. Archiving them would archive nothing and re-alert forever. |
| solana | **archive via BigQuery** | Envio/SQD do not cover it; `crypto_solana_mainnet_us` does |
| near, aptos, sui, polkadot, kusama, iotex, multiversx, litecoin, dogecoin, dash, bitcoin-cash | **do not archive yet** | datasets exist and are registered in `bigquery_datasets.py`, but we have no product surface asking for them. Registered ≠ ingested. |

**The rule: a chain gets an archive when a product answers questions about it.**
Registering a free dataset costs nothing; ingesting it costs disk and attention
and buys nothing until something asks.

---

## 4. Storage tier split — why B2 and not ClickHouse

| tier | holds | retention | why |
|---|---|---|---|
| **B2 parquet** | the permanent record | 90 days+ | $6/TB/mo, DuckDB reads it in place, no server cost |
| ClickHouse | hot working set | ~30 days | fast, but disk is local and grows with the server |

Keeping 90 days in ClickHouse would put ~130 GB of parquet-equivalent plus
indexes on the host — the disk pressure that took `/` to 86% on 2026-11-18.
**B2 is the retention tier; ClickHouse is the speed tier.** That split is the
whole reason the archive is affordable.

---

## 5. The bounded-disk contract (the operator's rule)

    gather → verify → upload → delete local

Local disk holds **one unit in flight** and never grows with retention.
`lite_indexer/archive/` enforces this: the parquet lives in a
`TemporaryDirectory`, the upload is verified by object-existence **and** row
count, and the local part dies with the context.

The delete step is gated on proof, not on an exit code. A measured rclone
behaviour makes this non-optional: `rclone lsjson --stat <file>` reports
`IsDir: true, Size: -1` for **both** an existing object and a missing one, so a
naive existence check always passes and would delete the only copy. The writer
lists the parent and matches the basename instead.

---

## 6. What "permanence of access" means here

Three separate guarantees, and we should not blur them:

1. **Our own data is permanent** — B2, versioned, 90 days and growing. We control
   it; no provider can revoke it or change its price.
2. **Upstream history is available, not owned** — BigLake (AWS Open Data,
   anonymous, free), BigQuery public (free within quota, then $6.25/TB), SQD and
   Envio (freemium quotas). These can change terms. That is precisely why the
   archive exists.
3. **Tracing works over both** — recent windows from our archive, older or
   pre-index windows from upstream, with the router choosing. A trace spanning
   our archive boundary reads our parquet for the covered part and upstream for
   the gap, in one query.

**The permanence we sell is (1).** (2) is what lets us backfill a chain we just
started. Confusing the two is how a platform promises history it cannot serve.

---

## 7. Anti-hammering, specific to a backfill run

A 90-day backfill for 16 chains is the single most likely action to get us
banned. The rules that apply to the live indexer apply harder here:

- **Free-first ordering**: keyless endpoints lead; keyed capacity is touched only
  when free capacity is exhausted.
- **rps below the advertised tier.** Measured: rps=10 soft-banned one provider
  within minutes (62 of 81 softban events). Advertised is where throttling
  *starts*.
- **Span gating.** Measured `eth_getLogs` span is 0–2000 blocks across tiers. A
  slice is 200 blocks chunked at 50, so an endpoint under 50 blocks cannot
  complete a chunk — retrying it stalls the chain rather than filling it.
- **Per-KEY pacing, per-ACCOUNT quota.** More keys buy rate, never budget.
- **Backfill is a batch job, not a live path.** It runs at low priority
  (`nice`/`ionice`), off-peak, with the live indexer's provider budget reserved
  first.
- **`skip_if_present`** in the unit writer makes a re-run free: an interrupted
  backfill resumes instead of re-fetching.

---

## 8. The order of operations

1. **Produce** — live indexer writes ClickHouse, `archive-export` ships days to
   B2. This is running; depth is the open item.
2. **Backfill what we missed** — for each chain, pull from the widest source
   that covers the gap (SQD 14 chains, Envio 18, BigQuery sol/eth/polygon,
   BigLake eth/btc) into our own archive via `lite_indexer/archive/`.
3. **Never re-read what we own** — the router prefers B2 for archived ranges.
4. **Grow coverage only when a product asks** — a registered dataset is not an
   ingested one.

---

## 9. Verification

    # archive depth per chain (the number that matters)
    rclone lsf -R --dirs-only rugmunch-b2:rmi-chain-data/parquet/<chain> | \
      grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | sort -u | wc -l

    # real size per chain (feeds the cost table above)
    rclone size rugmunch-b2:rmi-chain-data/parquet/<chain> --json

    # what the router would pick for a range
    python3 -m lite_indexer.query.router --help

---

## 10. Known-false things

- **"Read 90 days from BigLake each time, it's free."** BigLake AWS is free to
  read; BigQuery is $6.25/TB scanned, and one 90-day polygon scan is ~$0.44.
  Owning the archive costs $0.76/month total.
- **"Keep 90 days in ClickHouse."** That is 130 GB on the local disk that
  already hit 86% — B2 is the retention tier.
- **"Archive every chain we can see."** Five are declared `dead`; eleven others
  have datasets but no product asking. Register free; ingest on demand.
- **"Backfill is just a bigger query."** It is the most ban-prone action we take
  and needs free-first ordering, sub-advertised rps, span gating and
  `skip_if_present` resumability.
