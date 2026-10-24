# Polymarket producer (`pm_trades`, `pm_markets`, `pm_holders`)

Status: live. Producer added 2026-10-25; tables created 2026-10-23.

## The honest answer on "90 days of Polymarket"

**We can have 90 days of Polymarket data starting today. The past is
unrecoverable.** The three tables existed with an archive spec and no writer:
one crawl ran on 2026-10-23 (191 trades, 100 markets, 930 holder rows, all
inside a single 2-hour window), and nothing has written them since. There is no
upstream backfill for that gap — Polymarket's public read API serves *current*
state (`/trades` is a newest-first feed, `/holders` is a live snapshot), so the
days between market creation and 2026-10-25 cannot be reconstructed from it.

Any table reading "90 days" of `pm_*` before 2026-10-25 is stating a plan, not a
fact. From 2026-10-25 onward the producer runs on a timer and the archive spec
(`archive_specs/_registry_polymarket.py`) ships each closed day to B2.

## What writes what

| Table | Cadence | Mode | Natural key (dedupe) | Version column |
|---|---|---|---|---|
| `pm_trades` | hourly | append | `(condition_id, ts, proxy_wallet, asset, size)` | `ingested_at` (default `now()`) |
| `pm_markets` | 15 min | upsert | `(condition_id, market_id)` | `asof` |
| `pm_holders` | hourly | snapshot | `(condition_id, token, proxy_wallet, asof)` | `asof` |

All three are `ReplacingMergeTree` in `wallet_memory`, so a replayed or
restarted cycle collapses onto the same logical rows — idempotency is the
table's, not a state file's. `pm_trades` alone has an arrival column
(`ingested_at`), which is why the archive spec uses `mode=cursor` for it and
`mode=snapshot` for the other two.

## Schedule

One systemd timer (`systemd/pm-producer.timer`, every 15 minutes) runs
`python -m lite_indexer.producers.polymarket --json`. Trading and holder
cadence is enforced inside the cycle from the last row actually written
(`ingested_at` / `asof`), so a missed tick self-heals on the next one and no
second scheduler is introduced.

## Failure behaviour

- One pooled `httpx` client, 4 req/s floor, 3 attempts with exponential backoff.
- A dataset that fails (`ProviderError`) is logged, counted
  (`polymarket_producer_errors`), and skipped — the other datasets still write.
- A single market that rejects `/holders` is skipped, the rest of the snapshot
  still lands.
- No credentials anywhere: both endpoints are keyless public reads.

## Operating it

```bash
cd /srv/work/repos/lite-chain-indexer
set -a; . /srv/secrets/lite-indexer.env; set +a
.venv/bin/python -m lite_indexer.producers.polymarket --json            # normal tick
.venv/bin/python -m lite_indexer.producers.polymarket --force --json    # replay window
```

Proof of no duplication: `count()` may exceed `uniqExact(<natural key>)` until
the merge runs; `count() FINAL` must equal `uniqExact(...)`.
