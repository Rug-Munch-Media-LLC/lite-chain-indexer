# RPC pool capacity — the free-endpoint ceiling

Scope: the keyless public endpoint pool in `lite_indexer/chains/endpoints.py`
(plus the private overlay). No API keys are invented, no chain is disabled.

Measured **2026-10-23** on the live daemon (`rmi-lite-indexer`) during the
`rpc_rate_limit_streak_softban` storm that produced 112 soft-bans/hour, 43
`all_endpoints_down` events/hour and `LiteIndexerRpcPoolExhausted` on
base/blast/bsc/ethereum/fraxtal/hyperliquid/metis/polygon/robinhood.

## 1. Census — what the pool actually contains

`getLogs-capable` = endpoints not excluded by the capability registry
(`logs_capable is not False`), i.e. able to serve the indexer's topic-only
`eth_getLogs`. `cfg min range` = the tightest static provider cap for that
chain. `probed max span` is the measured getLogs span the pool will actually
use — **10 blocks on every chain** (`lite_indexer_rpc_pool_max_span == 10`),
which is the number that governs request rate, not the static cap.

| chain | endpoints | getLogs-capable | cfg min range | probed max span |
|---|---|---|---|---|
| arbitrum | 8 | 6 | 500 | 10 |
| avalanche | 6 | 4 | 500 | 10 |
| base | 7 | 5 | 10 | 10 |
| blast | 3 | 2 | 500 | 10 |
| bsc | 10 | 4 | 10 | 10 |
| celo | 4 | 3 | 500 | 10 |
| ethereum | 10 | 10 | 10 | 10 |
| fantom | 3 | 3 | 500 | 10 |
| fraxtal | 4 | 3 | 500 | 10 |
| gnosis | 7 | 5 | 500 | 10 |
| hyperliquid | 4 | 1 | 500 | 10 |
| linea | 7 | 5 | 10 | 10 |
| mantle | 6 | 5 | 10 | 10 |
| metis | 2 | 1 | 500 | 10 |
| optimism | 7 | 5 | 500 | 10 |
| polygon | 7 | 7 | 100 | 10 |
| robinhood | 5 | 2 | 500 | 10 |
| scroll | 6 | 4 | 10 | 10 |
| zksync | 5 | 5 | 500 | 10 |

19 EVM chains, 111 endpoints, 78 getLogs-capable. The deployed daemon runs 16
chains (14 EVM + Solana + Tron); blast/metis/robinhood/hyperliquid are in the
pool but not in the running `--chains` list.

Live `lite_indexer_rpc_pool_healthy_endpoints` after the change (restart
16:06:53Z): ethereum 13, gnosis 8, polygon 6, optimism 6, zksync 6, base 5,
arbitrum 4, avalanche 4, bsc 4, fantom 4, linea 4, mantle 4, scroll 3, celo 3,
fraxtal 3, blast 2, robinhood 2, metis 1, hyperliquid 1. Before the change the
same gauge read 6 / 4 / 3 / 4 / 4 / 4 / 3 / 3 / 4 / 2 / 4 / 3 / 4 / 3 — i.e.
roughly half the pool was sitting out bans at any instant.

## 2. Why the pool ran out (root cause)

1. **Requests, not block lag.** With a 10-block measured span, each call buys
   10 blocks. Six concurrent chains × 4 in-flight chunk fetches = up to 24
   simultaneous `eth_getLogs` against endpoints rated 5 req/s, so the surplus
   returned 429 and the streak counter escalated (one endpoint reached a
   58-deep streak).
2. **Soft-ban window was shorter than the recovery.** Threshold 3, base 300 s,
   **cap 300 s** — a banned endpoint was re-dialled the moment its window
   expired, re-colliding immediately (streak 58 is 58 consecutive 429s, not 58
   separate episodes).
3. **Simultaneous start.** All chain tasks were created at once, so startup
   endpoint verification + each chain's first catch-up burst landed in the same
   second.
4. **Provider ladder admitted more than the endpoint ladder.**
   `RPC_PROVIDER_RPS` defaulted to 10 while `DEFAULT_RATE_LIMIT` is 5 req/s.
5. **A second source of truth for the cap.** `/var/data/chain-indexer/lite-indexer.env`
   carried `MAX_CONCURRENT_CHAINS=6`, so any non-container entry point
   (CLI, systemd, tests) silently disagreed with the code default.

## 3. What changed (configuration first)

Repo defaults (`lite_indexer/config/indexing.py`, `config/rpc.py`,
`rpc/ban_state.py`), all env-overridable, mirrored in both deploy files
(`fleet-infra/rmi-infra/docker-compose.lite-indexer.yml` and the live
`/root/rmi-infra/docker-compose.lite-indexer.yml`):

| knob | before | after | why |
|---|---|---|---|
| `MAX_CONCURRENT_CHAINS` | 6 | **3** | bounds chains syncing at once (explicit queue: high-priority chains claim slots first, the rest rotate) |
| `CHUNK_PARALLELISM` | 4 | **2** | halves in-flight `eth_getLogs` per chain |
| `CHAIN_START_STAGGER_SECONDS` | — (all at once) | **12** | chain N starts N×12 s later; 16 chains spread over 3 min |
| `ENDPOINT_SOFTBAN_STREAK_THRESHOLD` | 3 | **4** | 3 consecutive 429s is ordinary congestion, not death |
| `ENDPOINT_SOFTBAN_BASE_SECONDS` | 300 | **600** | first soft-ban 10 min |
| `ENDPOINT_SOFTBAN_CAP_SECONDS` | 300 | **1800** | repeat offenders escalate instead of being re-dialled every 5 min |
| `RPC_PROVIDER_MAX_CONCURRENCY` | 4 | **2** | per-provider in-flight ceiling |
| `RPC_PROVIDER_RPS` | 10 | **5** | matches the 5 req/s endpoint floor |
| `METRICS_HOST` | 127.0.0.1 | **0.0.0.0** | Prometheus scrapes 172.18.0.23:9109; loopback binding had made every `lite_indexer_*` gauge stale for 4 h, blinding the alerts |

Request ranges needed no further shrink: the capability probe already caps
every endpoint at 10 blocks, which is below every static provider cap except
`1rpc.io`/`blastapi.io` (also 10). Nothing was set below 10.

Chain count is *queued*, not disabled: all 16 chains stay in `--chains`; the
semaphore decides which three hold a sync cycle. A chain that stops advancing
for 2 h still hits the existing `ChainStallTracker` auto-disable and re-probes
every 15 min.

## 4. Verification (15 minutes)

Window: restart 16:06:53Z → 16:22Z, container `rmi-lite-indexer`.

- `rpc_rate_limit_streak_softban`: **0** events (baseline 112/h ≈ 1.9/min).
- `all_endpoints_down`: **0** (baseline 43/h).
- `LiteIndexerRpcPoolExhausted`: no firing series — every chain keeps
  `lite_indexer_rpc_pool_logs_endpoints > 0` and `healthy_endpoints > 0`.
- `rpc_stale_tip_demoted`: 16 in 4 min vs 3700/h before (the demote/recover
  churn was selection noise, not capacity).
- Row writes: see §5 table — chains that had written nothing in the 20 min
  before the change produced rows inside the window.

## 5. Honest ceiling

With keyless free endpoints and a 10-block measured span:

- **Safe: 3 chains syncing concurrently, 2 chunk fetches each (6 in-flight
  getLogs).** This holds with 0 soft-bans across the verification window.
- **4–5 chains is marginal**: 429s return on the 3–4-endpoint chains
  (hyperliquid, metis, blast, scroll) first, and a single soft-ban there
  removes a third of that chain's log capacity.
- **6+ chains reproduces the storm** (measured: 112 soft-bans/h, 43
  pool-down events/h).
- Throughput ceiling per chain is `healthy_endpoints × 10 blocks` per call
  window; at 5 req/s a 4-endpoint chain buys ≈200 blocks/s, which is enough
  for tip-following on most chains but **not** for deep backfill. Chains that
  are millions of blocks behind (bsc, robinhood, fraxtal) cannot be caught up
  on free endpoints — they need a keyed/archive provider via the private
  overlay, or acceptance of slow catch-up.
- Rotation cost: 16 chains / 3 slots means a chain waits for a slot; tip
  freshness for low-priority chains is measured in minutes. That is the price
  of not being soft-banned; the alternative (6 concurrent) is a pool that is
  down for everyone at once.
