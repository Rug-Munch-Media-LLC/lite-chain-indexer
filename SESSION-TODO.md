> **Status:** active · owner: Rug Munch Media LLC Engineering · opened 2026-11-18
> **Operator intent:** 90 days of backfill per chain into B2 (permanent), with a
> bounded local footprint — gather a day, verify it, upload it, delete the local
> copy, repeat. Then live indexing. Then the remaining alerts.

# SESSION-TODO — lite-chain-indexer

## CORRECTION to my earlier reading

I said "BigLake is ethereum-only, so 90 days for all chains is impossible." That
was **wrong** — I read one of five tiers and generalised. The actual backfill
surface:

| tier | module | coverage |
|---|---|---|
| Subsquid (SQD) | `sqd_source.py` `EVM_DATASETS` | **13 EVM chains** + Tron |
| Envio HyperSync | `envio_source.py` `CHAIN_IDS` | **19 chains** (traces: eth, base) |
| BigQuery GCP public | `bigquery_tables.py` | eth (`crypto_ethereum`), solana (`crypto_solana_mainnet_us`) |
| BigLake AWS Open Data | `biglake_source.py` | **eth only** (`v1.0/eth/*` — the bucket has only `btc/` and `eth/`) |
| ClickHouse local | `legs_clickhouse.py` | whatever is hot — **~30 days** |

So the all-chain 90-day backfill is achievable. Subsquid is the widest EVM
source and needs no per-chain plumbing: `EVM_DATASETS[chain]` is the whole map.

**Keys present in the indexer:** `SQD_API_KEY`, `ENVIO_API_TOKEN`,
`ENVIO_RPC_TOKEN`, `GOLDSKY_API_KEY`, `BIGQUERY_ACCOUNTS` (4 GCP projects).

## The retention design the operator specified

    day N:  gather from source  ->  write parquet  ->  verify row count
            ->  upload to B2  ->  verify object  ->  DELETE local copy
            next day repeats; B2 keeps 90 days permanently

Local disk therefore holds at most ~1 day of one chain's work plus its in-flight
upload. B2 is the permanent record. This is the existing verify-then-upload
contract in `backfill-evm-hours.py` ("A unit is uploaded only when its parquet
row count equals ClickHouse `count()`") **plus a delete step that does not exist
yet.**

## Open — in order

### 1. Add the verify-then-delete step (the design gap)
- [ ] `scripts/archive-daily.py` and `scripts/archive-export.py` currently upload
      and keep the local part. Add: after a verified B2 object, delete the local
      parquet and its staging dir.
- [ ] Gate the delete on the SAME evidence the upload used (row count match +
      object present in B2), never on "upload returned 0".
- [ ] Bound local: reaper for any `staging/` older than 24h that has a verified
      B2 twin.
- [ ] Prove it: run one day, confirm local part gone AND `read_parquet` over the
      B2 path returns the expected count.

### 2. Backfill 90 days, widest source first
- [ ] Per chain, pick the tier by coverage (SQD 13, Envio 19, BQ eth/sol,
      BigLake eth). Cross-check two tiers on ethereum — cheap, and it is the one
      chain where overlap exists.
- [ ] `--dry-run` first per chain; the script reports which hours do not match.
- [ ] Upload to `b2-crypt-v2:chain-data/parquet/<chain>/<table>/<date>/`.
- [ ] Record depth per chain; **do not assume 90** — source retention governs it.

### 3. DuckDB accessibility proof
- [ ] `SELECT count(*) FROM read_parquet('.../parquet/<chain>/<table>/*/*/*.parquet')`
      per chain must equal the sum of verified units.
- [ ] Document the exact glob in the runbook so the MCP/query layer reads it the
      same way.

### 4. Then live indexing — 3 remaining stalls
- [ ] `bsc` — providers cap getLogs at 10 blocks; `SYNC_CHECKPOINT_SLICE=200`
      makes every slice arithmetically impossible. Needs per-chain slice sizing.
- [ ] `ethereum` — `-32005 log response size exceeded`; behind ~4h so slices are
      denser than the cap.
- [ ] `linea` — `1rpc.io` still in the merged pool (paid-subscription upsell).

### 5. Remaining criticals
- [ ] `mcp-stack-health` / "retrieval DEGRADED" — supermemory ingest backlog
      32→39 with memory-agent calls timing out at **300s**. Local 4B Q6_K is the
      bottleneck; `/srv/models/qwen3.5-2b-heretic` is a 28K stub that needs
      re-downloading for a smaller/faster lane.
- [ ] `check-lane-sync` (200/hr) — real twin divergence
      (`security_patterns` base=825 twin=15487)
- [ ] 10 × `FleetUnitFailedNew`
- [ ] `RepoWorkNotLanding` × 57 — genuine unlanded-work backlog

## Done (verified)

- [x] `archive-export` → `success 0` (was 220 fails/hr; `/data` mount ordering)
- [x] CH artifact check reads the B2 `.horizon` sidecar → `PASS rpo=23h/24h`
- [x] Deployed the chain-state join into the LIVE Prometheus. `/etc/prometheus/rules/`
      was a stale 24-Oct file with `chattr +i`; the git fix had never reached it.
      **B2 alerts 8 → 0.**
- [x] `b2-retention-status.sh` date matchers fixed (base 1→71 days)
- [x] RPC capability matrix measured; `gen-rpc-overlay.py`,
      `verify-rpc-pool.py`, `seed-capability-refusals.py` landed on main

## Do not re-derive

- **BigLake is eth-only.** The AWS bucket has exactly `v1.0/btc/` and `v1.0/eth/`.
  I got this wrong once by reading one module and generalising; the all-chain
  paths are **Subsquid** and **Envio**.
- **ClickHouse holds ~30 days.** Never a 90-day source.
- **`--max-concurrent` is a daemon ARGV flag** and beats `MAX_CONCURRENT_CHAINS`.
- **rps=10 soft-bans thirdweb in minutes.** rps=3 is the ceiling.
- **`/etc/prometheus/rules/` is immutable and drifted 25 days.** Check
  `api/v1/rules` for the live expression; git != running.
- **`blockscout.com` returns HTML**, not JSON-RPC, on every chain.
- **`backfill-evm-hours.py` sources from ClickHouse**, so it is a ~30-day tool,
  not a 90-day one.
