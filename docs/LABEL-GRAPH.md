# Label Graph — Ownership & Cutover

> Status: **implemented** (2026-09-10). Labels are DATA and live with the
> indexer that owns the raw on-chain warehouse. The scanner queries the
> indexer's label API; rugmunchbot stays a thin client.

## Why

The wallet label graph (`entity_labels`) and entity resolution previously
lived in `rugmunch-scanner/rugmunch_scanner/wallet_memory/`, which also
created the ClickHouse table. That coupled label **storage** (a data
concern) to the scanner's detection methodology and forced every consumer
to reach into a scanner-owned database. Consolidating the label graph onto
the indexer gives one owner for `wallet_memory.*` and one API for
consumers.

## What moved to the indexer

| Concern | Before | After |
|---|---|---|
| `entity_labels` table provisioning | scanner `label_importer` created it | indexer migration `016_entity_labels.sql` |
| Label read / entity resolution | scanner read ClickHouse directly | `GET /api/v1/labels/{address}` |
| Label write (label growth) | scanner wrote ClickHouse directly | `POST /api/v1/labels` |
| Label API surface | scanner `wallet_memory` internals | indexer `api/routers/labels.py` |

The indexer's `api/db.py` already reads `wallet_memory.*` with the
SELECT-only `chain_api` user; `entity_labels` was added to the read
allowlist. Writes use a separate credential (`CH_WRITE_*`) via
`api/label_store.py` — the read user stays SELECT-only.

## What stays in the scanner

Risk scoring, clustering, deployer intel, cross-chain linking heuristics
(EVM address portability, bridge mappings, same-deployer matching) and the
`/api/v1/wallet/*` product API all stay in the scanner. **Detection
methodology stays; label storage + entity resolution move.** The scanner's
`EntityResolver` keeps its cross-chain link logic and only the *label
enrichment* path is redirected to the indexer.

## Request flow

```
                    indexer ClickHouse (wallet_memory.entity_labels)
                       ▲ write (label growth)      │ read (resolve)
                       │                            ▼
scanner label_growth ──┴──> POST /api/v1/labels    GET /api/v1/labels/{address} <── scanner EntityResolver
                                                                                    │ (fallback only if down)
                                                                                    └─> local entity_labels read
rugmunchbot ──> scanner /api/v1/wallet/*  (thin client, owns no labels)
```

## API

### `GET /api/v1/labels/{address}?chain=&limit=`

Returns `EntityResolutionOut`:

```json
{
  "address": "0xabc",
  "chain": "ethereum",
  "resolved": true,
  "entity_type": "exchange",
  "label_type": "cex_hot_wallet",
  "name": "Binance 7",
  "primary_label": "Binance 7",
  "labels": [ { "address": "0xabc", "chain": "ethereum", "entity_type": "exchange",
                "label_type": "cex_hot_wallet", "name": "Binance 7", "source": "etherscan",
                "provenance": "etherscan-label-cloud", "confidence": 0.95, "first_seen": "..." } ]
}
```

Unknown address → HTTP 200 with `resolved=false`, `labels=[]` (unknown is a
valid answer, not a 404). Rows are `FINAL`-deduplicated and ranked by
confidence; the top row is the primary label.

### `POST /api/v1/labels`

Body `{"labels": [EntityLabelCreate, ...]}` (1–1000 rows). Idempotent upsert
on `(address, chain_id, source)` (ReplacingMergeTree). Returns `201` with
`{"inserted": N, "addresses": [...]}`. `503` when writes are disabled or the
store is unreachable. Guarded by an optional shared-secret bearer token
(`LABEL_API_TOKEN`).

## Scanner fallback behavior

`EntityResolver._labels_via_indexer` queries the indexer for every member
wallet. If **every** request fails (indexer unreachable), it returns `None`
and the resolver falls back to a local `entity_labels` read via
`storage.get_entity_labels`. If the indexer is reachable but has no label,
that empty answer is authoritative — **no** local fallback. A scan never
aborts because the indexer is down.

## Configuration

| Variable | Where | Purpose |
|---|---|---|
| `CH_WRITE_HOST` / `CH_WRITE_PORT` / `CH_WRITE_USER` / `CH_WRITE_PASSWORD` / `CH_WRITE_DATABASE` | indexer | label write credentials (fall back to `CH_*`) |
| `LABEL_API_TOKEN` | indexer | optional bearer token for `POST /api/v1/labels` |
| `LABEL_API_WRITE_ENABLED` | indexer | kill switch for the write endpoint (default `1`) |
| `INDEXER_API_URL` | scanner | indexer base URL (default `http://127.0.0.1:8100`) |
| `INDEXER_API_TIMEOUT` | scanner | per-request timeout seconds (default `5`) |
| `INDEXER_API_TOKEN` | scanner | bearer token matching `LABEL_API_TOKEN` |

## Migration note

Both services point at the same ClickHouse `wallet_memory` database, so
there is **no data copy** — migration `016_entity_labels.sql` only makes
the indexer the canonical provisioner (idempotent `CREATE TABLE IF NOT
EXISTS`) and grants `chain_api` SELECT + `rmi` SELECT/INSERT. Existing rows
are preserved and the scanner's fallback read still works during cutover.
