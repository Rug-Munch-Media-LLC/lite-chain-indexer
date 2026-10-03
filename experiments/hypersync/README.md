# HyperSync 90-day Backfill PoC (Base)

**Status: PoC / eval-only. Not production-integrated. Nothing pushed to git.**

Goal: can Envio **HyperSync** replace or accelerate 90-day historical backfill for
`lite-chain-indexer` on Base, on a free tier?

---

## TL;DR

| Question | Answer |
|---|---|
| Is HyperSync reachable from Talos? | **Yes** — `base.hypersync.xyz:443` OPEN |
| Does the **free** tier work? | **UNVERIFIED — blocked on a token I cannot mint.** No anonymous access exists any more; every request returns HTTP 401 without a dashboard-created token. |
| Rows/sec, MB/day, 90-day estimate | **NOT MEASURED.** All throughput numbers require a live token. |
| Does it belong in the production pipeline? | **Not yet.** Blocked on measurement, not on viability. |

The one thing this PoC proves for free: **transport, endpoint topology, chain
coverage, and the entire decode→partition→Parquet path.** What it cannot prove
without a token is rate and quota.

---

## Blocking prerequisite: a token

```
HTTP/1.1 401
{"error":"Your token is malformed. API Tokens can be created at
           https://app.envio.dev/api-tokens."}
```

HyperSync **requires** an API token for all requests. Tokens are created only
through the interactive Envio dashboard (GitHub sign-in). There is no anonymous
tier and no programmatic token minting, so an agent cannot self-provision one.

**To finish this PoC, a human must:**

1. Sign in at <https://envio.dev/app/api-tokens>
2. Expand the **Free package** row → enter a token name → **Create**
3. Copy it (free-package tokens can be re-revealed later)
4. `export ENVIO_API_TOKEN=<token>` on Talos
5. Store it in gopass — **never** in the repo or in git

Then run the commands below. The script needs no further changes.

### Pricing context (verified 2026-10-03 from envio.dev/pricing/hypersync)

| Plan | Price | Limit |
|---|---|---|
| **Free** | **$0** | **Fair-use rate limiting (no published rpm)** |
| Starter | $70/mo | 100 rpm, burst 250, pay per extra |
| Pro | $480/mo | 1,000 rpm, burst 2,500, pay per extra |

Notes that matter for a 90-day backfill:

- **No monthly data-volume cap.** "All plans include data for every supported
  chain. There are no strict data volume limits." The limit is *rate*, not volume.
- **Budget is per 60s window and shared across chains** — `eth` and `base`
  requests with one token draw from the same budget.
- **Budget is counted in units, not requests.** A single HyperSync request
  returns far more data than one RPC call, so "fair-use" is not comparable to an
  RPC rpm number.
- **Traces are a paid add-on** — confirming the design-lane note. Base/eth traces
  are unavailable on Free, so this PoC is logs-only by necessity.
- Free tokens get "a short burst of faster queries at the beginning".

---

## How to run

```bash
cd /srv/work/repos/lite-chain-indexer

# venv already built at .venv-hypersync-poc (hypersync 1.2.1, pyarrow 25.0.1)
export ENVIO_API_TOKEN=<token>

# Smoke test: cap at 50k rows to confirm the free tier lets you start at all
.venv-hypersync-poc/bin/python experiments/hypersync/backfill_base.py \
    --days 7 --max-rows 50000

# Full bounded slice (the real measurement)
.venv-hypersync-poc/bin/python experiments/hypersync/backfill_base.py --days 7

# Heavier runs belong in batch.slice (8 cores / IOReadBandwidthMax=400M)
systemd-run --slice=batch.slice --unit=hs-backfill-base --wait --collect \
    .venv-hypersync-poc/bin/python \
    experiments/hypersync/backfill_base.py --days 7
```

Useful flags: `--from-block N` (start anywhere, good for spot checks),
`--concurrency N` (default 8 — lower it if you hit 429),
`--batch-size N` (default 100k blocks), `--rows-per-file N` (default 250k).

Exit codes: `0` success · `1` stream error mid-run · `2` no token ·
`3` cannot reach / token rejected.

### Offline correctness self-test (no token needed)

```bash
.venv-hypersync-poc/bin/python experiments/hypersync/_selftest_mocked.py
```

Mocks the client and asserts the decode, day-bucketing, layout
`base/<YYYY-MM-DD>/<file>.parquet`, zstd round-trip, schema equality and
big-endian `value` decode. Currently **PASSES** — this is the part of the PoC
that is genuinely verified.

---

## What was measured (free, no token)

### Endpoint topology — the first real finding

The design lanes pointed at `apibase.hypersync.xyz`. **That host is dead.**
TCP-refused on 80/443/8443 from both Talos (netcup, 152.53.80.39) and Cinnabox
(residential, 103.197.243.4). Correct scheme is **per-chain subdomain**:

```
https://<chain>.hypersync.xyz      # base, bsc, polygon, ethereum, solana, ...
```

### Chain coverage from Talos — 16/16 non-Ethereum reachable

| Chain | IP | TCP 443 |
|---|---|---|
| base, bsc, polygon, optimism, avalanche, fantom, celo, linea, scroll, mantle, blast, fraxtal, robinhood, hyperliquid, tron, solana | 193.32.220.131–135 | **OPEN** (401) |
| **ethereum** | 213.190.30.131 | **REFUSED** |

Every `193.32.220.x` host answers the 401 token challenge, i.e. the service is
live and routing. **Only Ethereum mainnet is unreachable from this box.** Note the
whole legacy `213.190.30.0/24` range (AS48314, IP-Projects, Frankfurt) is refused
from both networks — that is Envio's old edge, not a netcup datacenter block.

This **revises** the "18/20 chains" premise in a useful way: 16 of our chains are
confirmed reachable *and* answer with the standard token error, so coverage is
good. Ethereum — a chain we do index — is the odd one out, and traces for
base/eth are a paid add-on regardless.

### Latency (unauthenticated path, base)

10 sequential POSTs: **0.379–0.515 s**, median ≈ **0.394 s** per round trip
from Talos. No local network or TLS-pathology problem.

### Parquet cost (measured on synthetic rows, same schema)

~**943 bytes/row** zstd-compressed. Extrapolating against Base's real ERC-20
Transfer volume is what the live run is for, but this sets the order of
magnitude: a 90-day Base backfill is plausibly **tens of GB**, not hundreds —
which B2 handles comfortably.

---

## Why there are no throughput numbers

Rows/sec, MB/day and the 90-day projection all come from `last_run_report.json`,
which is written **only on a successful authenticated stream**. Fabricating a
number from an unauthenticated 401 would be worse than reporting nothing, so
the report states plainly that this is unmeasured and why.

The projection logic is already implemented and will populate automatically:
`projected_90d_hours`, `projected_90d_rows`, `projected_90d_gb`, plus
`rate_limit_info` read via the client's `rate_limit_info()` / `x-ratelimit-*`
headers, which is how the real free-tier budget gets read back.

---

## How it would plug into the existing export pipeline

The engine is `lite_indexer/` — append-only writes, hourly Parquet rotation,
B2 upload as the permanent tier.

**What already lines up**

- Output layout is `<chain>/<date>/<file>.parquet`, matching the existing
  Parquet partition convention, so it drops into the same lake.
- Files are written with `compression="zstd"` and never rewrite an existing
  partition — append-only, consistent with the doctrine.
- Field selection is the natural fit: HyperSync returns only requested fields,
  which is exactly how you keep Parquet narrow.

**The integration seam**

HyperSync is a *backfill source*, not a live-ingest source. It does not replace
the RPC path that keeps the highwater advancing. The shape would be:

```
HyperSync ──(backfill, this PoC)──▶ Parquet partitions ──▶ B2 (permanent)
                                                             │
                                          ClickHouse ◀───────┘ (optional load)
```

Two viable placements:

1. **Fill the cold-history gap only.** Point it at block ranges the live indexer
   never covered, write straight to Parquet/B2, leave ClickHouse alone. Lowest
   blast radius and no interaction with the production write path.
2. **Replace the RPC backfill executor.** Bigger win — the playbook records Base
   `eth_getLogs` wedging on a 50-block range, and HyperSync bypasses JSON-RPC
   entirely, so that failure mode disappears. But it touches the backfill path
   that already has a Tron-specific history of non-idempotent rewinds.

**Recommendation: option 1.** Backfill is exactly where a
2000x-faster-scan claim pays off, and it is isolated from the append-only live
writer. Option 2 should wait until the free tier is measured and someone has
looked at the dedup story — the playbook's rewind lesson applies to anything
that writes into `evm_transfers`.

**Gotchas to carry over**

- Tron `blockTime` is **milliseconds**. HyperSync returns a normalised
  timestamp, so convert at the parse boundary and never store raw — this is the
  exact bug the playbook records for the Tron reconciler.
- Base learned-range: the PoC sidesteps the 50-block `eth_getLogs` cap entirely
  (no JSON-RPC involved). Do **not** port a learned-range workaround into this
  path; there is nothing to learn.
- Dedup key must stay `(chain, block_number, tx_hash, log_index)` — the schema
  carries all four.
- Backfill is heavy: run it in `batch.slice`, never in production units.

---

## Files

| Path | Role |
|---|---|
| `backfill_base.py` | The PoC. 247 lines, single primary function, stdlib + `hypersync` + `pyarrow`. |
| `_selftest_mocked.py` | Offline correctness harness — **passing**, no token needed. |
| `out/` | Output root. `<chain>/<date>/transfers-<ts>-<n>.parquet`. |
| `out/last_run_report.json` | Written on a successful run only. Holds every measured number. |

PoC venv lives at `/srv/work/repos/lite-chain-indexer/.venv-hypersync-poc`
(deliberately outside this directory so the repo's own tooling ignores it).

## Verdict

**Eval-only, pending measurement.** HyperSync is reachable, well-covered, fast on
the wire, and the integration seam is clean. The open question is purely
economic: whether the free tier's fair-use budget covers a 90-day Base backfill
before $70/mo Starter. That is a one-token experiment, and it is worth doing —
but the token has to come from a human.
