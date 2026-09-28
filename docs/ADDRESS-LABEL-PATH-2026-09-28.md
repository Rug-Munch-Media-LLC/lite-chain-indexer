# Address-Label Path — Verified State & Design

> **Lane:** `address-labeler` · **Repo:** `/srv/work/repos/lite-chain-indexer`
> **Date:** 2026-09-28 · **Status:** design + verified measurements, no code changes.
>
> **Lane claim:** supermemory `Lr3VEcPeZ3LjFVHsZ6sqqD` (project scope).
> **Touched:** this file only. **Not touched:** `/srv/work/repos/rugbrain/**`,
> Qdrant, ClickHouse data, any service.
>
> Every count below was read from the live stores on 2026-09-28. Commands and
> result lines are in the evidence section at the bottom.

---

## 0. Three corrections to the brief, up front

The brief's framing is right about the *problem* but wrong about three
*facts*. Each correction changes the design, so they come first.

### 0.1 The `*_8b` collections already exist — at full population

The brief lists three generations and proposes "create a NEW collection for a
4096 lane". A 4096 lane already exists and is already populated:

| Collection | Points | Dim | Distance |
|---|---:|---:|---|
| `wallet_profiles` | 40,355 | 2560 | Cosine |
| `wallet_profiles_8b` | 40,355 | **4096** | Cosine |
| `wallet_v2` | 9,657 | 2560 | Cosine |
| `wallet_v2_8b` | 9,657 | **4096** | Cosine |
| `wallet_clusters` | 0 | 1024 | Cosine |

`wallet_profiles_8b` and `wallet_v2_8b` hold **exactly the same point counts**
as their 2560-dim siblings. This is a finished migration, not a missing one.

**Consequence for deliverable 2:** do *not* propose new names for
profiles/v2. Propose exactly one new name, for the one thing that has no
collection at all (§2).

### 0.2 Nothing writes these collections from this repo — and that is correct

`grep -rn '4096|_8b|8B'` across `lite-chain-indexer` returns **zero** hits for
any vector lane. Every `4096` in the tree is unrelated (block numbers, config
caps — e.g. `lite_indexer/config/indexing.py:112`).

The repo's vector lane is a **different system**: LanceDB + Qwen3-4B/2560.

- `lite_indexer/vectors/config.py:28` — `DEEPINFRA_EMBED_MODEL` default `Qwen/Qwen3-Embedding-4B`
- `lite_indexer/vectors/config.py:50` — same for the local llama.cpp model
- `lite_indexer/vectors/config.py:57` — `EMBEDDING_DIM` default `2560`
- `lite_indexer/vectors/collections.py:69-73` — `wallet_embeddings`, `contract_embeddings`, `transaction_embeddings`, `token_embeddings`

So there is a **fourth** generation: LanceDB `wallet_embeddings` at 2560-dim.
The retired Qdrant lanes were retired by a process this repo does not own.

**Consequence:** this repo should not be made to write Qdrant. Its correct
role is the ClickHouse/API owner (it already is, §1). The 4096 lane stays a
Concern A of whoever owns `wallet_profiles_8b`.

### 0.3 The label path is already built — the gap is *reachability*, not code

The brief's item 1 asks me to "propose the address-label path end to end".
`docs/LABEL-GRAPH.md` documents it and it is implemented:

| Concern | Implementation |
|---|---|
| Read / entity resolution | `api/routers/labels.py:104` `get_address_label` |
| Write / label growth | `api/routers/labels.py:164` `post_labels` |
| Row→model mapping | `api/label_rows.py:13` `to_label_out` |
| Write credentials | `api/label_store.py:33` `ENTITY_LABELS_DDL`, `:49` `get_write_client`, `:96` `insert_entity_labels` |
| DDL migration | `migrations/018_entity_labels.sql` |
| Read allowlist | `api/db.py:59`, `:60` `entity_labels` |
| Ownership rationale | `docs/LABEL-GRAPH.md:9-15` |

`docs/LABEL-GRAPH.md:3-5` states status **implemented** as of 2026-09-10.

**Consequence:** the deliverable is not "design the path". It is (a) state the
path that exists, (b) name the one real gap, and (c) fix the schema split (§1.2)
that makes the built path look empty.

---

## 1. The address-label path

### 1.1 The path that exists today

```
  source CSV / API
        │
        ▼
  label_importer (rugmunch-scanner)     ← 2601 lines, 13 modules
        │  DDL: label_importer/config.py
        │  URLs: source_urls.py, sources_public.py
        ▼
  ClickHouse wallet_memory.wallet_labels        40,006,518 rows
        │  (and, separately:)
        │  wallet_memory.entity_labels            204,373 rows
        ▼
  indexer API  ── read: chain_api (SELECT-only)  api/db.py
                ── write: CH_WRITE_* user        api/label_store.py:49
        │
        ▼
  GET  /api/v1/labels/{address}   → EntityResolutionOut   (labels.py:104)
  POST /api/v1/labels             → EntityLabelWriteResult (labels.py:164)
        │
        ▼
  consumer
```

Verified characteristics of the existing path:

- **Storage.** `ReplacingMergeTree(first_seen) ORDER BY (address, chain_id, source)`
  (`api/label_store.py:43-44`). Re-posting a label is an idempotent upsert, not
  a duplicate — the property that makes re-runs safe.
- **Reads use `FINAL`** (`api/routers/labels.py:132`) so dedup happens before
  ranking; rows are ranked `confidence DESC, first_seen DESC` (`:135`).
- **Unknown is a valid answer**, not a 404 (`:142`).
- **Writes are separated from reads**: the read user is SELECT-only
  (`api/db.py:5`, `:60-62`), writes use `CH_WRITE_*` (`api/config.py:41-47`).
- **Write auth is fail-closed**: an unset `LABEL_API_TOKEN` returns 503 naming
  the variable rather than serving unauthenticated
  (`api/routers/labels.py:81-90`), with an explicit dev escape checked at
  startup.

This is a well-built path. It should not be redesigned.

### 1.2 The one real defect: `wallet_labels` and `entity_labels` do not share a schema

This is the finding that explains "the built path looks empty".

| | `wallet_labels` | `entity_labels` |
|---|---|---|
| rows | 40,006,518 | 204,373 |
| distinct addresses | — | 176,459 |
| address column | `address` | `address` |
| chain | `chain_id` | `chain_id` |
| name | `label_name` | `name` |
| kind | `label_category` | `label_type` + `entity_type` |
| source | `source` | `source` + `provenance` |
| confidence | **absent** | `confidence` (Float32) |
| sanction flag | `is_sanctioned` | **absent** |
| extra | `label_subtype` | `first_seen` |

Only `address` and `chain_id` are shared. The API reads **only**
`entity_labels` (`api/routers/labels.py:130-137`).

**Correction — `wallet_labels` is NOT unreachable.** A repo-wide grep of `api/`
found the read surfaces I initially missed:

- `api/db.py:59`, `:60` — `wallet_labels` is in the `ALLOWED_TABLES` allowlist
- `api/db.py:90` — `TABLE_ALIASES["labels"] = "wallet_labels"`, so it resolves
  in `POST /api/v1/query` (`api/routers/query.py:38`) as `FROM labels`
- `api/routers/stats.py:83` and `api/graphql.py:202` — row counts only

So the 40M rows **are** queryable today through the generic validated-SQL
endpoint. The real gap is narrower and more specific than "not served":

1. **No typed, ranked, single-address contract.** `POST /api/v1/query` is
   generic SQL. Consumers get rows, not
   `EntityResolutionOut` with a primary label, confidence ordering, or
   `resolved=true|false`. The ergonomic contract exists for `entity_labels`
   only.
2. **Schema mismatch is a live trap.** A consumer who writes
   `SELECT confidence … FROM labels` gets an **unknown identifier error** —
   `wallet_labels` has no `confidence` column (§1.2). The failure surfaces at
   query time, not as a clear error at integration time.
3. **A different consumer class reaches it.** `wallet_entities` and
   `wallet_profiles` are also in the allowlist (`api/db.py:64-65`), so the
   generic surface is a general analytics escape hatch, not the label
   contract.

This strengthens §1.3 rather than weakening it: the ClickHouse lookup
conclusion is unchanged, but the recommended fix is a **projection over the
existing surface** (add `wallet_labels` to the typed endpoint, with an explicit
confidence default), not a new store and not a new access path.

**Measured consequence** — I tested whether activity addresses carry a label,
in both directions, because a ReplacingMergeTree `IN` subquery can lie:

```
labeled addresses that also appear in address_activity   17,093  (of 176,459)
```

**9.7% of labeled addresses are ever active on the chains we index.** The
other 90.3% are dormant, or belong to chains we do not index, or are
attacker-planted spam.

So for a random active address the realistic answer from
`GET /api/v1/labels/{address}` is `resolved=false` — even though 40M labels
exist. The 40M are in a table with no typed resolution endpoint and a schema
the ranked contract cannot consume directly.

### 1.3 The verdict: ClickHouse lookup, not a vector store

**Labels are fetched BY ADDRESS. Embedding buys nothing.** Stated plainly
because it is the load-bearing claim:

- `GET /api/v1/labels/{address}` takes an address. A vector index answers
  *similarity*. There is no query in the API — or in any consumer — that asks
  "labels like this one."
- 40M labels × 4096 dims × 4 bytes = **~655 GB** of pure vector payload, before
  payload, HNSW graph, or replication. ClickHouse already holds all 40M rows
  in 11.33 MiB-equivalent of `entity_labels` scale and ~1.4 GiB of
  `wallet_labels`, answering point lookups on `(address, chain_id, source)`.
- ClickHouse is the store that can even *hold* the row count. A 655 GB vector
  index for a workload that is 100% point-lookup is the definition of wrong.

**Recommendation, in order:**

1. **ClickHouse is the system of record and the only lookup surface.** Keep it.
   It is already provisioned, already migrated, already indexed by the
   `ORDER BY` key, and already served by `api/routers/labels.py`.
2. **Project the 40M rows onto the typed contract, not a new store.** The rows
   are already queryable via `POST /api/v1/query` (`api/db.py:90`); what is
   missing is `EntityResolutionOut`-shaped, confidence-ranked resolution. Add
   `wallet_labels` to the typed endpoint with an explicit confidence default,
   because that table has no `confidence` column.
3. **Postgres is not needed here.** A Postgres copy of 40M labels would be a
   second source of truth for a table that is already a `ReplacingMergeTree`
   with a correct sort key. That is `docs/LABEL-GRAPH.md`'s whole thesis
   inverted (`:9-15`).
4. **A vector slice is justified only for a small, high-value subset** — the
   ~204K `entity_labels` and ~176K distinct addresses, not 40M. See §2.

### 1.4 What to do about the RAG blindness

The brief notes zero `clickhouse` references in `src/rugbrain/search/` and
`src/rugbrain/rag/`, with only `alerts/`, `fear_index/`, `market_snapshot.py`
and `system_health.py` connecting.

The correct fix is **one HTTP call to an endpoint that already exists** —
`GET /api/v1/labels/{address}` — not a new ClickHouse driver inside rugbrain.
`docs/LABEL-GRAPH.md:22` already defines this contract, and `:87-92` documents
the intended scanner-side fallback discipline (if the indexer is reachable, an
empty answer is authoritative; only a total outage falls back).

That work belongs to whoever owns rugbrain's search layer. **I did not touch
it.** Flagged as a cross-lane dependency, not a blocker to this design.

---

## 2. The new 4096-dim collection

Per §0.1, profiles and v2 already have their 4096 lane. The only gap with no
collection at any dimension is **label/cluster semantics**.

### 2.1 Recommendation: exactly one new collection

**Name: `label_semantics_8b`**

Why a new name is required rather than reusing `wallet_clusters`:

- `wallet_clusters` is 1024-dim and 0 points. The operator constraint is
  *nothing is deleted, re-dimmed, re-created or purged*. Changing its dim to
  4096 would violate that literally.
- A distinct name is self-describing: `*_8b` encodes the 4096/Qwen3-8B lane
  (the established fleet convention, visible in 30+ collections), and
  `label_semantics` says what the domain is.
- The suffix is the convention that keeps retired lanes legible:
  `wallet_profiles` (2560) vs `wallet_profiles_8b` (4096).

**Do not create a second collection** for scam/entity joins. `scam_intel_v2_8b`,
`known_scams_8b` and `wallet_v2_8b` already exist in that space. One new
collection is the whole ask.

### 2.2 Size, and why it is small

Populate from `entity_labels` — **204,373 rows / 176,459 distinct addresses** —
not from `wallet_labels`. 204K × 4096 × 4 B ≈ **3.3 GB**. That fits, and it
fits deliberately: it is the labeled, deduplicated, confidence-ranked slice.

**Do not embed all 40M `wallet_labels` rows.** Argument, restated so it is not
re-litigated: address-keyed lookup, no similarity query exists, ~655 GB, and
the 40M rows are 90.3% dormant relative to the chains we actually index (§1.2).
If someone later wants "find labels resembling this scam pattern", that is a
`scam_intel_v2_8b` / `wallet_v2_8b` job over an already-existing collection —
not a 40M-row address index.

### 2.3 Payload

Embed the **label semantics**, not the address. An address is a random hex
string; it has no meaning to embed. The embedded text is the human-readable
label material, which is the only part with semantic content:

```
f"{entity_type}: {label_type} — {name} (source={source}, confidence={confidence})"
```

Payload fields, all already present in `entity_labels` (no new ClickHouse
columns required):

| Field | Type | Purpose |
|---|---|---|
| `address` | keyword | exact lookup / join back |
| `chain_id` | keyword | filter |
| `entity_type` | keyword | filter |
| `label_type` | keyword | filter |
| `source` | keyword | provenance filter |
| `confidence` | float | pre-filter before search |
| `first_seen` | datetime | recency |

Filtering happens on the payload index, then a distance search runs on the
survivors. That is the correct order for 204K points: exact keyword filters
first, vector distance only on the residual.

### 2.4 Deterministic IDs — idempotent re-runs

The requirement is that re-running the embedder never duplicates a point and
never needs a delete.

```
point_id = uuid5(NAMESPACE_LABEL_SEMANTICS, f"{chain_id}|{address}|{source}")
```

- **Why `source` is in the key:** it mirrors the table's own identity —
  `ReplacingMergeTree ... ORDER BY (address, chain_id, source)`
  (`api/label_store.py:44`). Qdrant upsert is last-write-wins, so re-embedding
  the same logical label overwrites in place. A confidence bump or a name
  correction is a correction, not a new point.
- **Why `uuid5` and not a hash of the vector:** the vector changes when the
  model or the label text changes; the identity must not. `uuid5` is
  deterministic across processes, machines and runs, with no state to carry.
- **Upsert, never recreate:** `PUT /collections/label_semantics_8b/points`.
  Re-running the backfill is a no-op on already-embedded labels.
- **Namespace:** a fixed, checked-in constant. A namespace derived at runtime
  silently changes every ID and orphans the whole collection.

**Caveat to state plainly:** the key includes `source` but not
`label_type`/`name`. A source that mutates its own `label_type` in place
overwrites the point — correct. A source that *retracts* a label leaves a stale
point until the next pass. Retraction handling needs a `last_seen` sweep and is
out of scope here; flagging rather than silently designing around it.

---

## 3. `wallet_clusterer.py` — verdict: **delete**

**File:** `/srv/work/repos/rugbrain/src/rugbrain/wallet_clusterer.py`, 261 lines.
**Owner:** another agent. **I did not modify it.** This is the verdict only,
as requested.

### 3.1 Verified evidence

```
$ grep -rn 'wallet_clusterer' /srv/work/repos --include='*.py' | grep -v 'wallet_clusterer.py:'
(no output — zero importers, fleet-wide)

$ grep -n 'qdrant\|Qdrant\|6334\|upsert\|wallet_clusters' src/rugbrain/wallet_clusterer.py
9:Indexed into Qdrant 'wallet_clusters' for search-time entity resolution.
```

Line 9 is the **only** match in the file. There is no Qdrant client, no
`upsert`, no port. The docstring is false, and it is the sole cause of the
empty `wallet_clusters` collection: nothing ever wrote to it, because this file
does not write to it.

It also hardcodes 5 Binance addresses as `KNOWN_ENTITIES` (`:24-40`), which is
the kind of constant that quietly becomes a fixture nobody dares remove.

### 3.2 Delete vs fix

**Delete. Do not fix.**

The repair cost is not the 261 lines — it is the 40M-row ClickHouse write path
that a real clusterer needs, the Neo4j edge layer that does not exist (§4), and
a confidence model for entity resolution. None of that is in this file. "Fixing"
it means writing a new subsystem and hanging the old docstring on it.

Delete is also the honest signal: an empty `wallet_clusters` collection with
zero writers is **not** a missing feature. It is a stub. Deleting the stub
removes a false claim from the codebase; the collection stays untouched, as the
operator requires.

### 3.3 What breaks either way

**Delete breaks: nothing.** Zero importers fleet-wide. No test references it
(no `test_wallet_cluster*` file exists). No import in `rugbrain/__init__.py`
path. Deletion is inert.

**Fix would break: the budget.** It would put a 40M-label write path plus a
Neo4j edge layer on the critical path, and the edge layer does not exist yet.

**Preserve one thing on delete:** the 5 `KNOWN_ENTITIES` addresses are real
labels with real provenance. Before the file goes, they should be
re-asserted as rows in `entity_labels` (`source='rugbrain_known_entities'`) so
no label is lost. That is a 5-row insert via the existing
`POST /api/v1/labels` path (`api/routers/labels.py:164`) — no new code.

**Recommended sequence:** (1) re-assert the 5 addresses as
`entity_labels` rows, (2) delete the file, (3) leave `wallet_clusters` at
0 points / 1024-dim, untouched.

---

## 4. The edge layer — `wallet_entities` at 223 rows

### 4.1 Verified state

```
SELECT count() FROM wallet_entities            → 223
columns: entity_id, wallet_address, chain_id, heuristic_type,
         confidence_score, entity_label, entity_category,
         first_linked_at, last_updated_at
```

Against 279,984,612 `address_activity` rows and 294,126,750 `evm_transfers`
rows. The ratio is roughly **1 : 1.3M**. The edge layer is not unbuilt — it is
**1,250 rows from empty**.

Critically, the schema already anticipates the graph: `entity_id` +
`wallet_address` means one entity spans many wallets, with
`confidence_score` and `heuristic_type` recording *why* they were linked. The
shape is right. The population job is missing.

### 4.2 The cheapest correct way to populate it

`internal_transfers` (626,551,001 rows) and `evm_transfers` (294,126,750) both
already carry a funding direction. A funder→fundee edge is a single SQL
projection — no graph engine, no traversal, no new pipeline.

The cheapest *correct* design is a **one-hop funding-derived seed**, not
multi-hop graph construction:

1. **Seed set = the labeled core.** 176,459 distinct `entity_labels` addresses
   (17,093 of them active). This is bounded and known — no discovery problem.
2. **One hop out.** For each seed, take its first-touch fundees from
   `evm_transfers`/`internal_transfers` where the sender is in the seed set.
   That yields candidate co-funded wallets.
3. **Score and store.** Insert into `wallet_entities` with
   `heuristic_type='co_funded'`, `confidence_score` derived from hop count and
   funding value, `entity_id` = the seed entity.
4. **One hop back.** Second pass: wallets that consistently receive from *and*
   pay to the same entity get promoted. This is where real entity resolution
   starts, and it is where a naive implementation over-merges. Cap confidence
   and keep `heuristic_type` distinct so a later pass can audit which links
   came from which rule.

This is a ClickHouse job. It reads two tables it already has, writes one it
already has, and needs no new infrastructure.

### 4.3 ClickHouse or Neo4j — ClickHouse, and not close

The fleet already runs Neo4j for label propagation, which makes Neo4tje the
tempting answer. It is the wrong one:

- **The data is already in ClickHouse.** 921M transfer rows across two tables.
  Deriving edges in Neo4j means *copying* 921M rows into a second store first —
  the single most expensive step available, and it buys nothing that the source
  data doesn't already support.
- **The graph is shallow.** Entity resolution here is 1-2 hops from a labeled
  seed. Neo4j's advantage is deep traversal (5+ hops), which we neither need
  nor can afford at this volume.
- **`wallet_entities` is already a ClickHouse table** with a ClickHouse-shaped
  schema. Populating it in ClickHouse is a `INSERT INTO SELECT`. Populating it
  from Neo4j is an export/import round-trip.
- **One hop is SQL.** `GROUP BY` + joins. A graph DB earns its cost when
  traversal depth is the bottleneck; here depth is not the bottleneck.

**Use Neo4j only when** multi-hop propagation (>3 hops) becomes a measured
requirement — e.g. once you want "who is connected to a sanctioned address
through 5 intermediaries", which is a genuine traversal and a genuine
Neo4j workload. Until then it is a second store holding a copy.

**Be honest about the ordering:** the 223-row `wallet_entities` table is not an
emergency. No consumer reads it at meaningful scale. **The 40M-label schema
split (§1.2) is worth far more than the edge layer**, because the schema split
gives existing, already-ingested data no ranked contract, while the one
endpoint built for it reads a different table. Fix the split first. The edge layer is the *next* project, not this one.

---

## 5. Graphiti: **not justified — do not resurrect it**

The prior finding is correct and my measurements confirm it.

**No bi-temporal fields exist.** Verified by inspecting the live columns of
every table in `wallet_memory`. The temporal surface is:

| Table | Temporal columns |
|---|---|
| `entity_labels` | `first_seen` — **one** column |
| `wallet_entities` | `first_linked_at`, `last_updated_at` — activity, not validity |
| `address_activity` | `block_time`, `ingested_at` — event time + ingest time |
| `evm_transfers` | block/ingest timestamps |
| `wallet_labels` | `loaded_at` — **ingest time only** |

`entity_labels` is a `ReplacingMergeTree(first_seen)`
(`api/label_store.py:43`) with one `DateTime` and no `valid_from`/`valid_to`.
There is no interval, no assertion, no retraction, no second temporal axis.

Graphiti's entire premise is a **bi-temporal** knowledge graph: it stores what
was believed, when it was believed, and what superseded it. We have a single
axis and no retraction semantics.

**And the graph is not a graph.** `wallet_entities` = 223 rows. A temporal
graph engine over 223 rows is overhead with no query to serve.

**A label edge case argues *against* Graphiti, not for it:** the `ReplacingMergeTree`
at `api/label_store.py:43-44` **silently overwrites** rows on
`(address, chain_id, source)`. A corrected label destroys the previous value
with no history. That is precisely the information loss a bi-temporal engine
exists to prevent — so the current design is what Graphiti would be *replacing*,
and the replacement has no data properties that demand it.

**Verdict: no.** Not now, not for 223 rows, not without `valid_from`/`valid_to`
plus a real retraction story. Re-open Graphiti only when **both** hold:
(1) multi-hop entity resolution is a measured requirement, and (2) the
ReplacingMergeTree overwrite is actually losing information someone has
complained about. Neither is true today.

---

## 6. Recommended order of work

1. **Give `wallet_labels` a typed, ranked contract** (§1.2). Highest value by a
   wide margin — 40M already-ingested rows are queryable through the generic SQL
   surface but have no ranked single-address API. This is a projection over an
   existing endpoint, not new infrastructure. Note the schema difference:
   `wallet_labels` has no `confidence`, so the API's
   `ORDER BY confidence DESC` (`api/routers/labels.py:135`) needs an explicit
   default for those rows.
2. **Create `label_semantics_8b`** (§2) over the 176,459 labeled addresses,
   only if a similarity query actually appears. If none does, skip it — the
   vector lane is the speculative part of this plan.
3. **Re-assert the 5 `KNOWN_ENTITIES` addresses, then delete
   `wallet_clusterer.py`** (§3.3). Cross-lane; owner of rugbrain executes.
4. **Edge layer last** (§4.2), and only after (1) lands. Not an emergency.

Explicitly **not** recommended: re-dimensioning or purging any existing
collection; embedding 40M labels; a Postgres mirror; a Neo4j copy of the
transfer tables; Graphiti.

---

## 7. Evidence

All commands run on `talos.rugmunch.io`, 2026-09-28, read-only. ClickHouse
credentials were read from the live `lite-indexer.service` process
environment; the password is never printed.

| # | Command | Result |
|---|---|---|
| 1 | `hostname` | `talos.rugmunch.io` |
| 2 | `curl http://127.0.0.1:6334/collections` | 96 collections incl. `wallet_profiles_8b`, `wallet_v2_8b` |
| 3 | `curl http://127.0.0.1:6334/collections/{wallet_profiles,wallet_profiles_8b,wallet_v2,wallet_v2_8b,wallet_clusters}` | 40355@2560, 40355@4096, 9657@2560, 9657@4096, 0@1024 — all Cosine, all `indexed_vectors_count == points_count` |
| 4 | `grep -rn '4096\|_8b\|8B' lite-chain-indexer` (excl. `.venv`, tests) | 0 vector-lane hits; all `4096` unrelated (config caps, block numbers) |
| 5 | `ls api/`, `wc -l api/routers/labels.py api/label_store.py api/db.py` | 188 / 145 / 305 lines; `migrations/018_entity_labels.sql` present |
| 6 | `system.tables` (live, `wallet_memory`) | `address_activity` 279,984,612 · `evm_transfers` 294,126,750 · `entity_labels` 204,373 · `wallet_labels` 40,006,518 · `scam_addresses` 450,250 · `wallet_entities` 223 · `internal_transfers` 626,551,001 |
| 7 | `system.columns` for `wallet_labels` / `entity_labels` / `address_activity` / `wallet_entities` | schemas as tabulated in §1.2 and §4.1 |
| 8 | `SELECT count(DISTINCT address) FROM entity_labels` | 176,459 |
| 9 | `SELECT source, count() … FROM wallet_labels GROUP BY source LIMIT 8` | `wilder_btc_classification` 37,460,735 · `mbal_ground_truth` 1,051,874 · `mbal_external` 614,352 · `github:uniswap.json` 331,342 · `mbal_heuristic` 142,378 · `eth_labels_accounts` 111,866 · `solana_cex` 100,000 · `github:etherscan_malicious_labels.csv` 79,849 |
| 10 | `SELECT label_category, count() … GROUP BY label_category LIMIT 8` | `exchange` 15,736,078 · `p2p_service` 13,261,749 · `gambling` 3,301,174 · `individual` 2,580,804 · `darknet_market` 2,400,310 · `sanctioned` 1,051,174 |
| 11 | `SELECT count() FROM (SELECT DISTINCT address FROM entity_labels) e WHERE e.address IN (SELECT address FROM address_activity)` | **17,093 of 176,459 (9.7%)** |
| 12 | `grep -rn 'wallet_clusterer' /srv/work/repos --include='*.py'` (excl. self) | **zero importers** |
| 13 | `grep -n 'qdrant\|Qdrant\|6334\|upsert\|wallet_clusters' src/rugbrain/wallet_clusterer.py` | **line 9 only** — the false docstring |
| 14 | `SELECT count() FROM wallet_entities` | 223 |

### Measurement note (why command 11 is the one that matters)

An earlier form of command 11 put `address_activity` on the *outer* side of
the `IN` and reported ~385,726 of 500,000 rows "labeled". That is wrong and I
discarded it. It is a known `ReplacingMergeTree` artifact: an
`IN (subquery)` over a table with duplicate sort keys inflates matches, and
the 500,000-row `LIMIT` slice is taken *before* dedup. The corrected form
DAGs the **small, pre-deduplicated** side (`entity_labels`, 176,459 distinct
addresses) as the outer relation, which is the only direction where a single
`IN` gives a trustworthy answer at this size.

**17,093 is the number to use.** It is the honest one, and it is the reason
§1.2 is the top recommendation: 90.3% of the addresses we have labels for are
never active on the chains we index, so label coverage over *real* activity is
thinner than "40M labels" suggests — and those 40M sit in a table with no typed
single-address contract, despite being reachable through the generic SQL
endpoint.

---

## 8. Cross-lane dependencies (not mine, flagged not touched)

| Item | Owner | Note |
|---|---|---|
| `src/rugbrain/wallet_clusterer.py` delete | rugbrain agent | Verdict in §3; needs 5-address re-assert first |
| RAG ↔ ClickHouse wiring | rugbrain agent | One HTTP call to `GET /api/v1/labels/{address}` already suffices; no new CH driver needed |
| `label_importer/` reuse | rugmunch-scanner | 2601 lines, 13 modules; the reference registry, not in this repo |

## 9. Open questions / [REVIEW]

- **[REVIEW]** RESOLVED during review: `wallet_labels` reachability. A grep of
  `api/` shows it is queryable today via `POST /api/v1/query` with the alias
  `FROM labels` (`api/db.py:59`, `:60`, `:90`) and is counted by
  `api/routers/stats.py:83` and `api/graphql.py:202`. The gap is the absence
  of a typed, ranked, single-address contract for it — not absence of access.
  See the correction box in §1.2.
- **[REVIEW]** `address_activity` has Solana-shaped columns (`slot`,
  `program_ids`, `is_signer`, `involved_accounts`) at 279,984,612 rows, while
  `evm_transfers` is separate at 294,126,750. The one-hop edge-layer sketch in
  §4.2 reads the EVM tables; the Solana path needs its own join. Worth
  confirming before implementing.
- **[REVIEW]** Retraction semantics for `label_semantics_8b` (§2.4) are
  unspecified by design. If a source deletes a label, the Qdrant point is
  stale until a sweep. No `last_seen` sweep is proposed.
