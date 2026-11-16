# The Chain Data System — what we are building and why

**Status:** canonical · **owner:** Rug Munch Media LLC Engineering · **updated:** 2026-11-16
**Audience:** any agent or engineer who touches chain ingestion, tracing, rugbrain, or the scanner.

Read this before changing anything in `lite-chain-indexer`, the BigLake/`query/` legs, or the
ClickHouse chain schemas. It records **intent**, because the code shows what exists and
nothing shows why.

---

## 1. The one-sentence goal

> Hold ~90 days of chain data ourselves at zero marginal cost, serve anything older from
> free public lakes, and never be unable to answer a trace because a provider quota ran out.

Everything below is in service of that sentence.

---

## 2. The two-resource model (the thing most people get wrong)

There are **two separate resource classes**. They have different auth, different quotas,
different direction, and conflating them is the origin of most confusion here.

| | **RPC providers** | **Lake providers** |
|---|---|---|
| direction | write (into ClickHouse) | read only |
| job | fetch *fresh* blocks as they arrive | fill *historical* holes |
| auth | mostly keyless/freemium, some keys | 3 anonymous S3, 1 SQD key, 4 GCP service accounts |
| quota | per-endpoint rps + daily, enforced with bans/cooldowns | per-provider rps/daily/bytes, rotating |
| failure mode | provider bans us / rate-limits us | dataset does not cover the chain/window |
| lives in | `lite_indexer/rpc/` (76 modules) | `lite_indexer/lakes/` + `lite_indexer/query/` |

**RPC is the tap that keeps us current. Lakes are the hose that fills the past.**
A backfill that uses RPC is a backfill we paid for. A backfill that uses the lake is free.

---

## 3. The lake inventory (5 providers — only ONE is BigLake)

From `lite_indexer/lakes/registry.py`:

| provider | kind | serves | limits |
|---|---|---|---|
| `aws-eth` | S3 anonymous | eth `token_transfers`, `traces` | 10 rps |
| `aws-sonarx-base` | S3 anonymous | base `transfers`, `traces` | 10 rps |
| `aws-sonarx-arbitrum` | S3 anonymous | arbitrum `transfers`, `traces` | 10 rps |
| `sqd` | http/api | full-history EVM internals via SQD portal traces | 2 rps, 5000/day |
| `bigquery` | **gcp/bigquery** | eth transactions/token_transfers/blocks/contracts/logs + solana | 1 rps, 1000/day, 1 TiB |

**"4 BigLake providers" means 4 GCP service-account PROJECTS rotating against the single
`bigquery` provider** — `gen-lang-client-0708984505`, `cryptorugmunch`, `crm-check-492506`,
`friendly-maker-497404-u5`. That rotation is what turns a 1000-requests/day quota into ~4000.

**SQD is not a BigLake source.** It is its own leg with its own key (`SQD_API_KEY`). Asking
"is subsquid a biglake source" has a precise answer: no — they are adjacent rungs of the same
cascade, and SQD is used for internals/traces that BigQuery's public dataset does not carry.

---

## 4. The cascade — order is the design

`QueryRouter._route_query` tries, in order:

```
ClickHouse  →  cold/B2 (parquet)  →  biglake/S3  →  envio  →  indexed.xyz  →  sqd  →  RPC
   free            free                 free          free        free         free    PAID
```

Each rung is free until the last. A rung is only skipped when it provably cannot help.

### 4a. Two switches, deliberately separate

- `ON_DEMAND_RPC_ENABLED` — the **paid** RPC leg.
- `LAKE_READ_ENABLED` — every **free** leg.

These were once the same switch, so an operator cutting RPC spend **silently lost every free
historical read**. That is fixed (`leg_gates.py`): you can now disable paid spend and keep all
free history. Both resolve `True` at runtime, and `env_flag()` defaults ON unless the value is
exactly `"0"` — so an unset variable never silently disables a leg.

### 4b. A partial gap is still a gap (fixed 2026-11-16)

`should_try_lake` used to return `False` the moment **any** row was present — reading "not
empty" as "done". That is the commonest backfill shape: ClickHouse holds 6 days of an 84-day
range, so the range is neither empty nor complete. The old rule sent the entire remainder to
the **paid** RPC leg while the free lake held exactly that history.

Now the caller's `limit` defines "enough": fewer rows than requested is a gap the lake may
fill. With no stated appetite, the previous contract is preserved so no other caller changes.

---

## 5. The daily cycle (what "operational" looks like)

1. **RPC leg** ingests new blocks continuously, rotating endpoints so no single provider is
   hammered: per-endpoint budgets (`rps=5`, `daily=5000`, preemptive at 80%), capability
   probing, ban/cooldown state, 3-endpoint consensus check for correctness.
2. **Archive** writes hour-partitioned parquet to B2:
   `b2-crypt-v2:chain-data/parquet/<chain>/<YYYY>/<MM>/<DD>/<HH>/`.
   Hour granularity, not day, because *"the day-granularity archive could not keep up with a
   chain that writes ~1M rows/hour"*. An hour is considered written only when its parquet row
   count equals ClickHouse `count()` for that hour — never a partial upload.
3. **Retention** — ClickHouse TTLs drop hot partitions once the archive exists.
   The invariant is explicit in `config/storage.py`: **a TTL must never outrun the archive
   pipeline.** `check-retention-drift.sh` diffs live TTLs against `RETENTION_POLICY` and fails
   on drift (currently 10/10 matching).
4. **Beyond the window** — anything older than hot retention is served from the lake cascade,
   so a caller asking for a 2-year-old transaction still gets an answer.

### Retention windows today

| table | days | anchor |
|---|---|---|
| `evm_transfers` | 30 | block_timestamp |
| `dex_events`, `internal_transfers`, `solana_transfers` | 90 | block_timestamp |
| `scam_addresses`, `wallet_labels`, `deployer_history` | 180 | first_seen / loaded / deployed |
| `evm_launches`, `solana_launches`, `tron_launches` | 365 | block_timestamp |

`evm_transfers` at 30 days is **below** the 90-day product target — flagged, not yet changed.

---

## 6. Why this exists: the intelligence layer

The data is not the product. It feeds three consumers:

- **rugbrain** — entity labels, scam/rug signals, wallet behaviour.
- **the scanner** — token safety, holder structure, deployer history.
- **tracing** — wallet/transaction lineage that answers "where did this come from".

**Entity labelling compounds.** Every trace we serve and every launch we index is another
labelled edge. The label set only grows, and it grows for free as a side effect of data we
were passing through anyway. That is the moat: commercial-grade tracing built on
data we already hold, at ~zero marginal cost.

Two rules follow, and they are not negotiable:

1. **Never pay for data we can get free.** RPC for new blocks, lakes for old ones.
2. **Never let a quota make us unable to answer.** If we do not hold it, a lake does.

---

## 7. Operating rules (learned the hard way)

- **The indexer being "up" is not the same as the indexer ingesting.** On 2026-11-16 only the
  NATS sidecar was running; the indexer itself was down and ClickHouse had been frozen at
  2026-10-23 for **24 days** while 18 chains reported gaps. Check `max(block_timestamp)`, not
  `docker ps`.
- **A backfill queue file that has not been written is not a progressing backfill.** Compare
  file mtime and the newest ingested block before believing progress.
- **Provenance columns must name the real writer.** `source DEFAULT 'subsquid'` survived the
  Subsquid retirement and lied about where rows came from; it is now `lite_indexer`. Every live
  row already carried the real value, so this was a documentation bug with a security smell.
- **One name per provider.** A duplicate bucket or a second config for the same job is drift,
  and drift is what makes "is it running?" unanswerable.

---

## 8. Current state (2026-11-16), honestly

| item | state |
|---|---|
| Indexer running | **yes** — restarted this day; ingesting live (verified newest block 18:16) |
| Backfill queues | loaded and moving again (bsc 237, solana 256, arbitrum 67 pending ranges) |
| Hot data per chain | **~6 days** (2026-10-17 → 10-24), not 90 — the 84-day gap is the open work |
| Retention | armed, 10/10 tables match policy |
| Lake providers | 5 registered, all legs enabled, 4 GCP projects rotating |
| Lake-for-partial-gap | fixed this day (see §4b) |
| Duplicate bucket | `rmi-chain-lake-90d` referenced **nowhere** in code; `rmi-chain-data` is the live one — **unverified, do not delete without checking contents** |

**The open task** is the 84 missing days. It is free if it runs through the lakes and expensive
if it falls through to RPC — which is exactly why §4b was fixed first.
