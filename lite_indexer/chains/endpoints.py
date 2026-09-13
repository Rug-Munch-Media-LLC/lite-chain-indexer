"""
EVM chain RPC endpoints.

Each EVM chain has a list of eth_getLogs-compatible RPC endpoints in tiers:
  T1 (indices 0-1) — fastest, most reliable (drpc.org, tenderly, official RPCs)
  T2 (indices 2-4) — fallbacks (tatum, 48.club, blxrbdn, nodereal)
  T3 (indices 5+)  — last resort (range-limited publics)
  T4 (indices 5+)  — API-key free tiers (${ALCHEMY_KEY}, ${GETBLOCK_KEY}, etc.)

All endpoints were live-verified 2026-08-05 with an eth_blockNumber probe
plus a topic-only eth_getLogs probe (50-block range from tip) — the exact
request shape the indexer uses. Providers that answer eth_blockNumber but
reject topic-only eth_getLogs (publicnode → archive token required,
merkle → -32601 "Method not found") are kept as blockNumber/timestamp
fallbacks in lower tiers; the pool scorer's eth_getLogs verification
deprioritizes them for log queries (see lite_indexer/rpc/health.py).
meowrpc → getLogs unsupported, binance seeds → limit exceeded, ankr →
unauthorized except celo, 1rpc.io/eth → usage limit, llamarpc → HTTP 521,
tatum (except bsc/polygon), cloudflare-eth and polygon-rpc are confirmed
dead and removed.

2026-08-05: onfinality (eth.api.onfinality.io/public) re-added as Ethereum
T1 after 429-ban during 50-block catch-up; blastapi re-added as T5 —
both verified against 10-block eth_getLogs chunks from tip. Keep
Ethereum chunks small (ETH_BATCH_SIZE=10) so free-tier range caps
(Alchemy/blastapi = 10 blocks) and the ~20K result cap are respected.

2026-09-13: Uniblock (api.uniblock.dev) wired as an archive eth_getLogs
provider via the query-param template
``.../json-rpc?chainId=<id>&apiKey=${UNIBLOCK_KEY}``. Live-verified
topic-only eth_getLogs for robinhood/blast/fraxtal/metis + the major EVMs
and ``getHealth`` for solana. It is the only non-official archive source for
robinhood. The dead PublicNode hosts ``fantom-rpc.publicnode.com`` and
``zksync-era-rpc.publicnode.com`` (HTTP 404, re-probed 2026-09-13) were
removed from the candidate pool.

2026-09-13: Pocket Network (keyless) wired at T1 for 18 EVM chains via
``https://<slug>.api.pocket.network`` (2000 req/IP/min; live-verified
``eth_chainId`` for every slug, plus topic-only archive ``eth_getLogs`` for
fraxtal/bsc/ethereum; blast returns ``[]`` which is valid). Metis is the one
exception — Pocket has no archival-capable endpoint there ("no
archival-capable endpoints"), so its Pocket entry is a NON-archive fallback
and Uniblock remains metis's archive source. Infura keyed templates
(``https://<net>.infura.io/v3/${INFURA_KEY}``) added for its 13 verified
chains (T2/T3, archive-capable). Robinhood was NOT in Pocket's published
list but ``robinhood.api.pocket.network`` answered ``eth_chainId 0x1237``
here — added as a keyless fallback.

ANKR FREE-KEY LIMITATION (live-probed 2026-09-13): the provisioned
``${ANKR_KEY}`` works for ONLY eth, bsc, polygon, arbitrum, avalanche,
base, celo, gnosis. It returns HTTP 403 "API key is not allowed to access
blockchain" for optimism, fantom, linea, blast, fraxtal, metis — no Ankr
templates are registered for those six so the pool stays honest (a dead
keyed entry only produces verification-retry noise).

BLOCKPI PER-CHAIN KEYS (live-probed 2026-09-13): BlockPI keys are
per-chain, NOT a single ``BLOCKPI_KEY``. The keyless
``https://<chain>.blockpi.network/v1/rpc/public`` URLs were HTTP-521-flaky
(2026-08-07) and are replaced for the 9 provisioned chains
(ethereum/bsc/arbitrum/optimism/avalanche/linea/scroll/zksync/fantom) by
keyed archive templates ``https://<chain>.blockpi.network/v1/rpc/${BLOCKPI_<CHAIN>_KEY}``
(ETH/BSC/ARBITRUM/OPTIMISM/AVALANCHE/LINEA/SCROLL/ZKSYNC/FANTOM). Each was
live-verified here with ``eth_chainId`` HTTP 200 and an archive
``eth_getLogs`` (ethereum blocks 1..2 → ``[]``). Keys are read at runtime
from the environment; unset keys drop the endpoint from the active pool as
usual. The unindexed BlockPI chains (taiko/cronos/bitcoin) are documented
in the provider registry only — they are not added to the EVM pool.
Polygon has no provisioned key, so its ``/public`` entry stays a candidate
(flaky) rather than an active endpoint.

Some endpoints require an API key from a free-tier provider (Alchemy,
GetBlock, Chainstack, QuickNode, Infura). Set the corresponding ${ENV_VAR}
in your environment — the indexer resolves placeholders at import time via
os.path.expandvars (chains.endpoints._resolved_endpoints()). Endpoints
whose key is NOT set are excluded from the active pool (they can never be
dialed — a literal ``${...}`` in a URL only produces DNS/JSON verification
retries). Restart the service after setting a key to activate the endpoint.

``NODEREAL_KEY`` (NodeReal BSC T1) is the sole REQUIRED key — it does NOT
follow the optional-key rule. The endpoint URL is built from it, and a
missing key fails startup loudly (see :func:`require_nodereal_key`) so the
BSC T1 tier can never silently shrink.

Total: 74 endpoints across 15 chains (61 public + 13 API-key free tiers).
"""

from __future__ import annotations

import os

# Load the shared env file (ALCHEMY_KEY / INFURA_KEY / HELIUS_KEY ...)
# BEFORE the module-level placeholder resolution below, so every import
# path — daemon, health module, cron, manual — sees the same env as the
# daemon's systemd unit. Idempotent and non-destructive.
from lite_indexer.env import load_env_file
from lite_indexer.logging import get_logger

load_env_file()

# NODEREAL_KEY is a REQUIRED secret — the sole exception to the
# optional-key rule below. The NodeReal BSC endpoint previously shipped a
# public demo key baked into the URL (audit finding 2026-09-06); that key
# was rotated externally and every deployment now needs its own. Unlike the
# free-tier keys, an unset NODEREAL_KEY must NOT silently drop the endpoint
# from the active pool — that would shrink the BSC T1 tier without warning.
# Fail startup loudly instead (the systemd EnvironmentFile must provide it).


def require_nodereal_key() -> None:
    """Raise a startup error when the required ``NODEREAL_KEY`` is unset.

    ``NODEREAL_KEY`` is the one required secret (NodeReal BSC T1). The
    systemd EnvironmentFile must provide it; an unset key would silently
    shrink the BSC T1 tier, so startup fails loudly instead of degrading.

    Raises:
        RuntimeError: When ``NODEREAL_KEY`` is unset or empty.
    """
    if not os.environ.get("NODEREAL_KEY"):
        raise RuntimeError(
            "NODEREAL_KEY is required but is not set — add NODEREAL_KEY to the "
            "systemd EnvironmentFile for the indexer and restart. The NodeReal "
            "BSC T1 endpoint is built from it and must never silently drop out "
            "of the active pool."
        )


require_nodereal_key()

logger = get_logger(__name__)

GET_LOGS_ENDPOINTS: dict[str, list[str]] = {
    # ── Ethereum ──────────────────────────────────────────────────────
    # T1: onfinality public (verified 2026-08-05 — eth_getLogs OK at
    #     10-block chunks from tip; 429-banned on 50-block catch-up,
    #     keep chunks small via ETH_BATCH_SIZE=10)
    # T2: nodereal public demo key (works, but 429-bans under parallel load),
    #     nodies (eth-pokt.nodies.app — verified 2026-08-05; 50-block
    #     getLogs cap matches CHUNK_SIZE_EVM exactly; 20k result cap)
    # T3: drpc.org (degraded — ~75% 408 free-plan timeout, keep as fallback),
    #     flashbots (MEV-protected; ranged topic-only getLogs errors),
    #     mevblocker (verified 2026-08-05 — 10k-result getLogs cap, blocks
    #     MEV txs; fine for blockNumber + small getLogs ranges),
    #     publicnode (verified 2026-08-05 blockNumber; eth_getLogs needs an
    #     archive token — blockNumber/timestamp fallback only)
    # T4: alchemy, getblock, chainstack, quicknode (free API-key tiers)
    # T5: blastapi (10-block range cap — OK with ETH_BATCH_SIZE=10),
    #     merkle (verified 2026-08-05 blockNumber; eth_getLogs returns
    #     -32601 Method not found — tip/blockNumber fallback only)
    # NOTE: llamarpc (HTTP 521), ankr keyless (Unauthorized) and
    #       1rpc.io/eth (usage limit) were tested 2026-08-05 and failed
    #       the eth_blockNumber probe — not added. Keyed Ankr/dRPC-lb
    #       added 2026-08-23 (freemium depth — activate via env).
    "ethereum": [
        "https://eth.api.onfinality.io/public",
        "https://eth.api.pocket.network",  # T1 — Pocket Network (keyless, 2000 req/IP/min, archive-capable; live-verified eth_chainId + eth_getLogs 2026-09-13)
        "https://eth-pokt.nodies.app",
        "https://eth.drpc.org",
        "https://rpc.mevblocker.io",
        "https://rpc.flashbots.net",
        "https://virginia.rpc.blxrbdn.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://eth-mainnet.public.blastapi.io",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://eth.nownodes.io/${NOWNODES_API_KEY}",  # NOWNodes (verified 2026-08-23, 100k req/mo plan)
        "https://lb.drpc.org/ogrpc?network=ethereum&dkey=${DRPC_KEY}",  # dRPC load balancer
        "https://rpc.ankr.com/eth/${ANKR_KEY}",  # Ankr keyed — free key works ONLY for eth/bsc/polygon/arbitrum/avalanche/base/celo/gnosis (HTTP 403 elsewhere: optimism/fantom/linea/blast/fraxtal/metis)
        "https://mainnet.infura.io/v3/${INFURA_KEY}",  # Infura free tier
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=1&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://ethereum.blockpi.network/v1/rpc/${BLOCKPI_ETH_KEY}",  # T2 — BlockPI per-chain keyed (archive-capable; eth_chainId + archive eth_getLogs blocks 1..2 verified 2026-09-13)
    ],
    # ── Base (5 endpoints) ────────────────────────────────────────────
    # T1: tenderly, official mainnet.base.org (both verified eth_getLogs)
    # T2: drpc.org, 1rpc.io (50-block cap — matches EVM chunk size, verified 2026-08-03)
    # T4: alchemy
    # NOTE: publicnode (needs address filter) and blastapi (HTTP400) do not
    #       serve topic-only eth_getLogs on free tier and were removed.
    "base": [
        "https://base.gateway.tenderly.co",
        "https://base.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://mainnet.base.org",
        "https://base.drpc.org",
        "https://base-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://base-mainnet.public.blastapi.io",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://lb.drpc.org/ogrpc?network=base&dkey=${DRPC_KEY}",
        "https://rpc.ankr.com/base/${ANKR_KEY}",
        "https://base-mainnet.infura.io/v3/${INFURA_KEY}",  # T3 — Infura free (archive-capable)
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=8453&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
    ],
    # ── BSC (8 endpoints) ─────────────────────────────────────────────
    # T1: nodereal public, 48.club (live-verified 2026-08-04 with the
    #     exact indexer request shape: topic-only eth_getLogs over a
    #     100-block range from tip — both returned full result sets)
    # T2: blockrazor (25-block range cap — small-range fallback,
    #     registered in limits.py so the indexer caps chunks for it),
    #     official Binance public seeds (blockNumber verified 2026-08-07;
    #     bsc-dataseed1.defibit.io / ninicoin.io are mirrors)
    # T4: alchemy
    # NOTE: tatum (range-limit + 429), binance seeds (limit exceeded),
    #       bsc-rpc.publicnode.com (http_403 on ranged topic-only —
    #       4.7k rejections during 08/04 outage), merkle (HTTP400),
    #       ankr (unauthorized), 1rpc.io (usage limit) removed 2026-08-04.
    #       2026-08-07: blockpi.bsc (HTTP 521) and 1rpc.io/bnb (usage
    #       limit) probed and rejected — official seeds added instead.
    "bsc": [
        "https://bsc-mainnet.nodereal.io/v1/${NODEREAL_KEY}",  # key rotated externally
        "https://bsc.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId + eth_getLogs 2026-09-13)
        "https://rpc-bsc.48.club",  # T1 — 48.club (verified)
        "https://bsc.blockrazor.xyz",  # T2 — blockrazor (25-block range cap)
        "https://bsc-dataseed.binance.org",  # T2 — official Binance seed (blockNumber verified 2026-08-07)
        "https://bsc-dataseed1.binance.org",  # T2 — official Binance seed mirror
        "https://bsc-dataseed1.defibit.io",  # T2 — official Binance seed mirror
        "https://bsc-dataseed1.ninicoin.io",  # T2 — official Binance seed mirror
        "https://bsc-mainnet.public.blastapi.io",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://bsc-rpc.publicnode.com",  # T3 — re-added 2026-09-02 during rate-limit crisis (was http_403 on 2026-08-04 — re-probe)
        "https://bsc-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
        "https://lb.drpc.org/ogrpc?network=bsc&dkey=${DRPC_KEY}",
        "https://rpc.ankr.com/bsc/${ANKR_KEY}",
        "https://bsc-mainnet.infura.io/v3/${INFURA_KEY}",  # T3 — Infura free (archive-capable)
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=56&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://bsc.blockpi.network/v1/rpc/${BLOCKPI_BSC_KEY}",  # T2 — BlockPI per-chain keyed (archive-capable; eth_chainId + archive eth_getLogs live-verified 2026-09-13)
    ],
    # ── Polygon (6 endpoints) ─────────────────────────────────────────
    # T1: tenderly, drpc.org (verified)
    # T2: tatum (audit-verified on polygon), quiknode public
    #     (rpc-mainnet.matic.quiknode.pro — verified 2026-08-05:
    #     blockNumber + topic-only eth_getLogs from tip both OK)
    # T3: polygon-bor.publicnode.com (blockNumber verified 2026-08-07;
    #     topic-only getLogs needs an address filter — fallback only)
    # T4: alchemy
    # NOTE: polygon-rpc.blxrbdn.com removed 2026-08-05 — DNS failure
    #       (000) during the 08/05 outage; polygon-rpc.com returns 401
    #       (tenant disabled) and polygon-bor-rpc.publicnode.com requires
    #       an address filter for getLogs, so neither was added.
    #       2026-08-07: polygon.blockpi (HTTP 521), 1rpc.io/matic (usage
    #       limit), rpc.ankr.com/polygon (unauthorized) probed and
    #       rejected — publicnode added as T3 fallback instead.
    "polygon": [
        "https://polygon.gateway.tenderly.co",
        "https://poly.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://polygon.drpc.org",
        "https://polygon-mainnet.gateway.tatum.io",
        "https://polygon-bor-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://rpc-mainnet.matic.network",  # T3 — official Polygon public (re-added 2026-09-02 during rate-limit crisis)
        "https://polygon-rpc.com",  # T3 — re-added 2026-09-02 during rate-limit crisis (was 401 on 2026-08-05 — re-probe)
        "https://rpc-mainnet.matic.quiknode.pro",
        "https://lb.drpc.org/ogrpc?network=polygon&dkey=${DRPC_KEY}",
        "https://rpc.ankr.com/polygon/${ANKR_KEY}",
        "https://polygon-mainnet.infura.io/v3/${INFURA_KEY}",  # T3 — Infura free (archive-capable)
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=137&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
    ],
    # ── Arbitrum (6 endpoints) ────────────────────────────────────────
    # T1: tenderly, drpc.org (verified)
    # T2: official arb1.arbitrum.io (verified)
    # T3: arbitrum-one.publicnode.com (blockNumber verified 2026-08-07;
    #     getLogs needs archive token — fallback only), arbitrum.meowrpc
    #     (blockNumber verified 2026-08-07; meowrpc getLogs unsupported —
    #     blockNumber/timestamp fallback only)
    # T4: alchemy
    # NOTE: 2026-08-07: arbitrum.blockpi (HTTP 521), 1rpc.io/arb (usage
    #       limit), rpc.ankr.com/arbitrum (unauthorized) probed and
    #       rejected — publicnode + meowrpc added as T3 fallbacks.
    "arbitrum": [
        "https://arbitrum.gateway.tenderly.co",
        "https://arb-one.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://arbitrum.drpc.org",
        "https://arb1.arbitrum.io/rpc",
        "https://arbitrum-one-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://arbitrum.meowrpc.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://lb.drpc.org/ogrpc?network=arbitrum&dkey=${DRPC_KEY}",
        "https://rpc.ankr.com/arbitrum/${ANKR_KEY}",
        "https://arbitrum-mainnet.infura.io/v3/${INFURA_KEY}",  # T3 — Infura free (archive-capable)
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=42161&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://arbitrum.blockpi.network/v1/rpc/${BLOCKPI_ARBITRUM_KEY}",  # T2 — BlockPI per-chain keyed (archive-capable; eth_chainId + archive eth_getLogs live-verified 2026-09-13)
    ],
    # ── Optimism (4 endpoints) ────────────────────────────────────────
    # T1: tenderly (verified)
    # T2: official mainnet.optimism.io (verified, occasionally slow),
    #     drpc.org (verified 2026-08-03)
    # T4: alchemy
    "optimism": [
        "https://optimism.gateway.tenderly.co",
        "https://op.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://mainnet.optimism.io",
        "https://optimism.drpc.org",
        "https://optimism-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://lb.drpc.org/ogrpc?network=optimism&dkey=${DRPC_KEY}",
        "https://opt-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
        "https://optimism-mainnet.infura.io/v3/${INFURA_KEY}",  # T3 — Infura free (archive-capable)
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=10&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://optimism.blockpi.network/v1/rpc/${BLOCKPI_OPTIMISM_KEY}",  # T2 — BlockPI per-chain keyed (archive-capable; eth_chainId + archive eth_getLogs live-verified 2026-09-13)
    ],
    # ── Avalanche (4 endpoints) ───────────────────────────────────────
    # T1: official api.avax.network, drpc.org (verified)
    # T2: tenderly gateway (verified 2026-08-03)
    # T4: infura (free tier)
    "avalanche": [
        "https://api.avax.network/ext/bc/C/rpc",  # T1 — official (verified)
        "https://avax.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://avalanche.drpc.org",  # T1 — drpc.org (verified)
        "https://avalanche.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
        "https://avalanche-c-chain-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://avalanche-mainnet.infura.io/v3/${INFURA_KEY}",  # T4 — Infura free
        "https://lb.drpc.org/ogrpc?network=avalanche&dkey=${DRPC_KEY}",  # freemium
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=43114&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://avalanche.blockpi.network/v1/rpc/${BLOCKPI_AVALANCHE_KEY}",  # T2 — BlockPI per-chain keyed (archive-capable; eth_chainId + archive eth_getLogs live-verified 2026-09-13)
    ],
    # ── Fantom (4 endpoints) ──────────────────────────────────────────
    # T1: official rpc.fantom.network + rpcapi.fantom.network (verified)
    # T2: drpc.org (verified)
    # T4: alchemy
    "fantom": [
        "https://rpc.fantom.network",  # T1 — official (verified)
        "https://fantom.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://rpcapi.fantom.network",  # T1 — official-alt (verified)
        "https://fantom.drpc.org",  # T2 — drpc.org (verified)
        "https://fantom-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
        "https://lb.drpc.org/ogrpc?network=fantom&dkey=${DRPC_KEY}",  # freemium
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=250&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://fantom.blockpi.network/v1/rpc/${BLOCKPI_FANTOM_KEY}",  # T2 — BlockPI per-chain keyed (archive-capable; eth_chainId + archive eth_getLogs live-verified 2026-09-13)
    ],
    # ── Linea (5 endpoints) ───────────────────────────────────────────
    # T1: official rpc.linea.build, drpc.org (verified)
    # T2: tenderly gateway (verified 2026-08-03), 1rpc.io (50-block cap —
    #     matches EVM chunk size, verified 2026-08-03)
    # T4: infura (free tier)
    "linea": [
        "https://rpc.linea.build",  # T1 — official (verified)
        "https://linea.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://linea.drpc.org",  # T1 — drpc.org (verified)
        "https://linea.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
        "https://linea-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://1rpc.io/linea",  # T2 — 1rpc.io (50-block cap, verified 2026-08-03)
        "https://linea-mainnet.infura.io/v3/${INFURA_KEY}",  # T4 — Infura free
        "https://lb.drpc.org/ogrpc?network=linea&dkey=${DRPC_KEY}",  # freemium
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=59144&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://linea.blockpi.network/v1/rpc/${BLOCKPI_LINEA_KEY}",  # T2 — BlockPI per-chain keyed (archive-capable; eth_chainId + archive eth_getLogs live-verified 2026-09-13)
    ],
    # ── Scroll (4 endpoints) ──────────────────────────────────────────
    # T1: official rpc.scroll.io, drpc.org (verified)
    # T2: 1rpc.io (50-block cap — matches EVM chunk size, verified 2026-08-03)
    # T4: infura (free tier)
    "scroll": [
        "https://rpc.scroll.io",  # T1 — official (verified)
        "https://scroll.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://scroll.drpc.org",  # T1 — drpc.org (verified)
        "https://1rpc.io/scroll",  # T2 — 1rpc.io (50-block cap, verified 2026-08-03)
        "https://scroll-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://scroll-mainnet.infura.io/v3/${INFURA_KEY}",  # T4 — Infura free
        "https://lb.drpc.org/ogrpc?network=scroll&dkey=${DRPC_KEY}",  # freemium
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=534352&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://scroll.blockpi.network/v1/rpc/${BLOCKPI_SCROLL_KEY}",  # T2 — BlockPI per-chain keyed (archive-capable; eth_chainId + archive eth_getLogs live-verified 2026-09-13)
    ],
    # ── zkSync Era (4 endpoints) ──────────────────────────────────────
    # T1: official mainnet.era.zksync.io, drpc.org (verified)
    # T2: tenderly gateway (verified 2026-08-03)
    # T4: alchemy (zksync-era)
    "zksync": [
        "https://mainnet.era.zksync.io",  # T1 — official (verified)
        "https://zksync-era.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://zksync.drpc.org",  # T1 — drpc.org (verified)
        "https://zksync.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
        "https://zksync-era.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
        "https://zksync-mainnet.infura.io/v3/${INFURA_KEY}",  # T3 — Infura free (archive-capable)
        "https://lb.drpc.org/ogrpc?network=zksync&dkey=${DRPC_KEY}",  # freemium
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=324&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://zksync-era.blockpi.network/v1/rpc/${BLOCKPI_ZKSYNC_KEY}",  # T2 — BlockPI per-chain keyed (archive-capable; eth_chainId + archive eth_getLogs live-verified 2026-09-13)
    ],
    # ── Mantle (4 endpoints) ──────────────────────────────────────────
    # T1: official rpc.mantle.xyz (verified)
    # T2: drpc.org (verified; free-plan usage limit under burst),
    #     tenderly gateway (verified 2026-08-03), 1rpc.io (50-block cap,
    #     verified 2026-08-03)
    "mantle": [
        "https://rpc.mantle.xyz",  # T1 — official (verified)
        "https://mantle.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://mantle.drpc.org",  # T2 — drpc.org (verified)
        "https://mantle.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
        "https://1rpc.io/mantle",  # T2 — 1rpc.io (50-block cap, verified 2026-08-03)
        "https://mantle-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://mantle-mainnet.infura.io/v3/${INFURA_KEY}",  # T3 — Infura free (archive-capable)
        "https://lb.drpc.org/ogrpc?network=mantle&dkey=${DRPC_KEY}",  # freemium
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=5000&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
    ],
    # ── Gnosis (3 endpoints) ──────────────────────────────────────────
    # T1: official rpc.gnosischain.com, drpc.org (verified)
    # T2: tenderly gateway (verified 2026-08-03)
    "gnosis": [
        "https://rpc.gnosischain.com",
        "https://gnosis.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://gnosis.drpc.org",
        "https://gnosis.gateway.tenderly.co",
        "https://gnosis-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://lb.drpc.org/ogrpc?network=gnosis&dkey=${DRPC_KEY}",  # freemium
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=100&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
    ],
    # ── Celo (3 endpoints) ────────────────────────────────────────────
    # T1: official forno.celo.org, ankr.com/celo (verified — only working ankr)
    # T2: drpc.org (verified)
    "celo": [
        "https://forno.celo.org",  # T1 — official forno (verified)
        "https://celo.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://rpc.ankr.com/celo",  # T1 — ankr (verified — celo only)
        "https://celo-mainnet.infura.io/v3/${INFURA_KEY}",  # T3 — Infura free (archive-capable)
        "https://celo.drpc.org",  # T2 — drpc.org (verified)
        "https://lb.drpc.org/ogrpc?network=celo&dkey=${DRPC_KEY}",  # freemium
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=42220&apiKey=${UNIBLOCK_KEY}",  # T3 — Uniblock — live-verified archive eth_getLogs 2026-09-13
    ],
    # ── Blast (verified 2026-09-01) ──────────────────────────────────
    # L2, chainId 81457 (0x13e31). publicnode: chainId ✓, getLogs ✓.
    "blast": [
        "https://blast-rpc.publicnode.com",  # T1 — verified live (getLogs ✓)
        # T2: Uniblock — archive eth_getLogs (live-verified 2026-09-13);
        #     keyed fallback so the chain is no longer single-endpoint.
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=81457&apiKey=${UNIBLOCK_KEY}",  # T2 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://blast.api.pocket.network",  # T3 — Pocket Network (keyless; live-verified eth_chainId 2026-09-13; getLogs returns [] which is valid on this chain)
        "https://blast-mainnet.infura.io/v3/${INFURA_KEY}",  # T3 — Infura free (archive-capable)
    ],
    # ── Fraxtal (verified 2026-09-01) ────────────────────────────────
    # L2, chainId 252. publicnode: chainId ✓, getLogs ✓.
    "fraxtal": [
        "https://fraxtal-rpc.publicnode.com",  # T1 — verified live (getLogs ✓)
        # T2: Uniblock — archive eth_getLogs (live-verified 2026-09-13);
        #     keyed fallback so the chain is no longer single-endpoint.
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=252&apiKey=${UNIBLOCK_KEY}",  # T2 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://fraxtal.api.pocket.network",  # T3 — Pocket Network (keyless, archive-capable; live-verified eth_chainId + eth_getLogs 2026-09-13)
    ],
    # ── Metis (verified 2026-09-01) ──────────────────────────────────
    # L2, chainId 1088. publicnode: chainId ✓, getLogs ✓.
    "metis": [
        "https://metis-rpc.publicnode.com",  # T1 — verified live (getLogs ✓)
        # T2: Uniblock — archive eth_getLogs (live-verified 2026-09-13);
        #     keyed fallback so the chain is no longer single-endpoint.
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=1088&apiKey=${UNIBLOCK_KEY}",  # T2 — Uniblock — live-verified archive eth_getLogs 2026-09-13
        # T3: Pocket keyless fallback, NON-ARCHIVE for eth_getLogs ("no
        #     archival-capable endpoints" here) — Uniblock stays the archive
        #     source; Pocket is the non-archive tip/blockNumber fallback.
        "https://metis.api.pocket.network",  # T3 — Pocket Network (keyless NON-archive fallback; live-verified eth_chainId 2026-09-13)
    ],
    # ── Robinhood Chain (2 endpoints, verified 2026-09-01) ────────────
    # Arbitrum Orbit L2, chainId 4663 (0x1237), native ETH. The old spec
    # chainId 78600 was wrong — chainlist mapped it to "Vanguard" (VANRY).
    # LIVE-VERIFIED 2026-09-01:
    #   - robinhood-rpc.publicnode.com: eth_chainId ✓, eth_blockNumber ✓
    #     (block 52M, ~0.5s blocks), eth_getBlockByNumber ✓ — but getLogs
    #     is ARCHIVE-GATED on the free tier (allnodes.com personal token).
    #   - robinhood.drpc.org: eth_chainId ✓ but eth_getLogs and
    #     eth_getBlockByNumber are "method not available" — limited surface.
    # STATUS: registered but NOT indexed yet — the indexer needs eth_getLogs
    # for ERC-20 Transfer events, which neither free endpoint provides.
    # ACTION: add to the daemon's chain list when a getLogs-capable endpoint
    # launches (dRPC paid tier, Alchemy, or publicnode archive token).
    # ── Robinhood Chain (5 endpoints, verified 2026-09-01) ────────────
    # Arbitrum Orbit L2, chainId 4663 (0x1237), native ETH, ~0.5s blocks.
    # LIVE-VERIFIED 2026-09-01:
    #   - rpc.mainnet.chain.robinhood.com (OFFICIAL): chainId ✓,
    #     blockNumber ✓ (block 52M), getBlockByNumber ✓, getLogs ✓ —
    #     FULLY INDEXABLE.
    #   - robinhood-rpc.publicnode.com: chainId ✓, blockNumber ✓ —
    #     getLogs archive-gated on free tier.
    #   - rpc.nodeflare.app/robinhood/public: chainId ✓ (getLogs untested).
    #   - robinhood.rpc.blxrbdn.com: chainId ✓ but getLogs 403 Forbidden.
    #   - robinhood.drpc.org: chainId ✓ but getLogs/getBlockByNumber
    #     "method not available" — limited surface.
    "robinhood": [
        "https://rpc.mainnet.chain.robinhood.com",  # T1 — OFFICIAL, fully indexable
        # Uniblock (2026-09-13): archive-capable topic-only eth_getLogs — the
        # only non-official archive source for this chain; kept high-priority.
        "https://api.uniblock.dev/uni/v1/json-rpc?chainId=4663&apiKey=${UNIBLOCK_KEY}",  # T1-high — Uniblock — live-verified archive eth_getLogs 2026-09-13
        "https://robinhood.api.pocket.network",  # T3 — Pocket Network (keyless, probed live 2026-09-13: eth_chainId 0x1237 == 4663)
        "https://robinhood-rpc.publicnode.com",  # T2 — verified (getLogs archive-gated)
        "https://rpc.nodeflare.app/robinhood/public",  # T3 — verified chainId
        "https://robinhood.rpc.blxrbdn.com",  # T3 — chainId only (getLogs 403)
        "https://robinhood.drpc.org",  # T4 — limited surface
    ],
}

# Summary of EVM endpoints per chain:
#   ethereum: 13   base: 10     bsc: 15     polygon: 12
#   arbitrum: 10   optimism: 9  avalanche: 8 fantom: 7
#   linea: 9       scroll: 8    zksync: 8   mantle: 9
#   gnosis: 7      celo: 7      blast: 4    fraxtal: 3
#   metis: 3       robinhood: 7
# -------------------------------------------------------
#   Total source templates: 158 across 18 chains
#   (93 free/keyless + 65 API-key free tiers — keyed ones activate via env)
#
#   Non-EVM: solana (9), tron (4)
#   Grand total source templates: 162 across 18 EVM + 2 non-EVM chains
#
# 2026-09-13: +18 Uniblock archive templates (15 EVM chains — robinhood,
#   blast, fraxtal, metis + 14 T3 fallbacks) and +1 Solana template. Uniblock
#   is the only non-official archive eth_getLogs source for robinhood.
# 2026-09-13: +18 Pocket Network keyless T1 templates (metis tagged
#   non-archive — Uniblock stays its archive source), +9 Infura keyed archive
#   templates (13 verified chains total incl. the 4 pre-existing), +1 Pocket
#   solana and +1 Pocket tron fallback.
# 2026-09-13: +9 BlockPI per-chain keyed archive templates (ethereum/bsc/
#   arbitrum/optimism/avalanche/linea/scroll/zksync/fantom) replacing the
#   HTTP-521-flaky keyless /public candidates; polygon /public kept in the
#   candidate pool (no provisioned key).


# ── Candidate Endpoints (awaiting live verification) ────────────────
# Unverified additions from chainlist cross-pollination + operator tips.
# NOT part of the active pool — scripts/probe-rpc-endpoints.py probes
# these with eth_chainId + eth_blockNumber (+ optional topic-only
# eth_getLogs range check) and emits ready-to-paste promoted snippets.
# Promotion criteria: chainId matches, blockNumber OK, latency < 1500ms,
# and (for this indexer) topic-only eth_getLogs over a small tip range.
# Rejected candidates stay documented in the probe report, never silently.
CANDIDATE_ENDPOINTS: dict[str, list[str]] = {
    "ethereum": [
        "https://eth.llamarpc.com",  # re-probe: 521 on 2026-08-05, provider since stabilized?
        "https://1rpc.io/eth",  # re-probe: usage-limited 2026-08-05
        "https://eth.meowrpc.com",  # getLogs unsupported — blockNumber fallback only
        # ethereum.blockpi.network/v1/rpc/public removed 2026-09-13 — flaky
        # (HTTP 521) and superseded by the keyed ${BLOCKPI_ETH_KEY} T2 entry.
        "https://cloudflare-eth.com",  # reported dead 2026-08-05 — confirm before cull
    ],
    "base": [
        "https://base.llamarpc.com",
        "https://base-meow.rpc.thirdweb.com",
    ],
    "bsc": [
        "https://binance.llamarpc.com",
        "https://bsc.meowrpc.com",
        "https://bsc-blockchain.rpc.thirdweb.com",
        # bsc.blockpi.network/v1/rpc/public removed 2026-09-13 — flaky
        # (HTTP 521 on 2026-08-07) and superseded by the keyed ${BLOCKPI_BSC_KEY} T2 entry.
    ],
    "polygon": [
        "https://polygon.llamarpc.com",
        "https://polygon.meowrpc.com",
        "https://polygon-mainnet.public.blastapi.io",
        # polygon has no provisioned BlockPI key — keep /public but treat it
        # as flaky (HTTP 521 on 2026-08-07); do not promote to the active pool.
        "https://polygon.blockpi.network/v1/rpc/public",  # /public is flaky — archive-unverified
    ],
    "arbitrum": [
        "https://arbitrum.llamarpc.com",
        "https://arb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
        # arbitrum.blockpi.network/v1/rpc/public removed 2026-09-13 — flaky
        # (HTTP 521 on 2026-08-07) and superseded by the keyed ${BLOCKPI_ARBITRUM_KEY} T2 entry.
    ],
    "optimism": [
        "https://optimism.llamarpc.com",
        "https://optimism.meowrpc.com",
        "https://opt-mainnet.public.blastapi.io",
    ],
    "avalanche": [
        "https://avalanche.mainnet.nodies.app",
    ],
    "fantom": [
        "https://fantom-mainnet.public.blastapi.io",
    ],
    "linea": [
        # linea.blockpi.network/v1/rpc/public removed 2026-09-13 — flaky
        # (HTTP 521) and superseded by the keyed ${BLOCKPI_LINEA_KEY} T2 entry.
    ],
    "scroll": [
        "https://scroll-mainnet.public.blastapi.io",
    ],
    "zksync": [
        "https://mainnet.era.zksync.network",  # alternate official host
    ],
    "mantle": [],
    "gnosis": [
        "https://gnosis.blockscout.com",  # likely blockNumber-only
    ],
    "celo": [
        "https://celo-mainnet.public.blastapi.io",
    ],
}


# ── Endpoint URL template resolution ───────────────────────────────
# API-key free tiers use ${ENV_VAR} placeholders (${ALCHEMY_KEY},
# ${GETBLOCK_KEY}, ...). They are resolved at import time so the main
# RPC path never dials dead placeholder URLs. Endpoints whose env var is
# NOT set are excluded from the active pool entirely — a URL containing a
# literal ``${...}`` can never be dialed (DNS/JSON errors) and only
# produces verification retry noise (verify_endpoint_failed storms). Set
# the corresponding env var in the service environment and restart to
# activate those endpoints.


# ── Expected chainId map ────────────────────────────────────────────
# Canonical EIP-155 ids per chain name. The probe script
# (scripts/probe-rpc-endpoints.py) hard-rejects any endpoint answering
# with a different id — a wrong chainId means impostor/misconfigured
# endpoint regardless of latency. Solana/Tron use non-EVM probes.
EXPECTED_CHAIN_IDS: dict[str, str] = {
    "ethereum": "0x1",
    "base": "0x2105",
    "bsc": "0x38",
    "polygon": "0x89",
    "arbitrum": "0xa4b1",
    "optimism": "0xa",
    "avalanche": "0xa86a",
    "fantom": "0xfa",
    "linea": "0xe708",
    "scroll": "0x82750",
    "zksync": "0x144",
    "mantle": "0x1388",
    "gnosis": "0x64",
    "celo": "0xa4ec",
    "blast": "0x13e31",  # 81457 — verified 2026-09-01
    "fraxtal": "0xfc",  # 252 — verified 2026-09-01
    "metis": "0x440",  # 1088 — verified 2026-09-01
    "robinhood": "0x1237",  # 4663 — old spec chainId 78600 was Vanguard
}


def _resolve_endpoint_url(url: str) -> str:
    """Resolve ${ENV_VAR} placeholders in an endpoint URL.

    Uses :func:`os.path.expandvars` so both ``${VAR}`` and ``$VAR`` forms
    are handled. An unset variable leaves the placeholder in the returned
    URL so callers can detect and skip it.

    Args:
        url: Endpoint URL potentially containing ${ENV_VAR} templates.

    Returns:
        URL with env vars resolved (placeholder kept if unset).
    """
    return os.path.expandvars(url)


def _resolved_endpoints(urls: list[str]) -> list[str]:
    """Resolve env-var placeholders and drop endpoints that could not expand.

    Endpoints whose ``${ENV_VAR}`` placeholder is not set in the
    environment are excluded from the active pool: a URL containing a
    literal ``${...}`` can never be dialed (DNS/JSON errors) and only
    produces verification retry noise. Set the env var (and restart) to
    activate the endpoint.

    Args:
        urls: Raw endpoint URL templates.

    Returns:
        URLs with env vars expanded, minus unresolved placeholders.
    """
    resolved: list[str] = []
    for url in urls:
        expanded = os.path.expandvars(url)
        if "${" in expanded:
            logger.debug("endpoint_unresolved_env_skipped", url=url)
            continue
        resolved.append(expanded)
    return resolved


GET_LOGS_ENDPOINTS = {chain: _resolved_endpoints(urls) for chain, urls in GET_LOGS_ENDPOINTS.items()}

# ── Last-Resort Endpoints ───────────────────────────────────────────────
# Known-good endpoints tried when every endpoint in the active pool fails
# for a chain. These are the most battle-tested public providers from the
# live-verified sets above (official RPCs + drpc.org). They are a final
# fallback only — the regular pool's health scoring / bans / circuit
# breakers still apply; the last-resort list exists so a total pool wipe
# (e.g. an outage taking out every tier at once) still has a shot at
# serving the range instead of raising RpcFetchFailedError immediately.
LAST_RESORT_ENDPOINTS: dict[str, list[str]] = {
    "ethereum": [
        "https://eth.api.onfinality.io/public",
        "https://eth-pokt.nodies.app",
        "https://eth.drpc.org",
        "https://rpc.mevblocker.io",
        "https://rpc.flashbots.net",
    ],
    "base": [
        "https://base.gateway.tenderly.co",
        "https://mainnet.base.org",
        "https://base.drpc.org",
    ],
    "bsc": [
        "https://rpc-bsc.48.club",
        "https://bsc-dataseed.binance.org",
        "https://bsc-dataseed1.defibit.io",
    ],
    "polygon": [
        "https://polygon.gateway.tenderly.co",
        "https://polygon.drpc.org",
        "https://polygon-mainnet.gateway.tatum.io",
        "https://rpc-mainnet.matic.quiknode.pro",
    ],
    "arbitrum": [
        "https://arbitrum.gateway.tenderly.co",
        "https://arbitrum.drpc.org",
        "https://arb1.arbitrum.io/rpc",
    ],
    "optimism": [
        "https://optimism.gateway.tenderly.co",
        "https://mainnet.optimism.io",
        "https://optimism.drpc.org",
    ],
    "avalanche": ["https://api.avax.network/ext/bc/C/rpc"],
    "fantom": ["https://rpc.fantom.network"],
    "linea": ["https://rpc.linea.build", "https://linea.drpc.org"],
    "scroll": ["https://rpc.scroll.io", "https://scroll.drpc.org"],
    "zksync": ["https://mainnet.era.zksync.io"],
    "mantle": ["https://rpc.mantle.xyz"],
    "gnosis": [
        "https://rpc.gnosischain.com",
        "https://gnosis.drpc.org",
        "https://gnosis.gateway.tenderly.co",
    ],
    "celo": ["https://forno.celo.org"],
    "robinhood": ["https://rpc.mainnet.chain.robinhood.com"],
}


def last_resort_endpoints(chain: str) -> list[str]:
    """Return the known-good last-resort endpoints for a chain.

    Resolves ``${ENV_VAR}`` placeholders and drops unresolved ones, same
    as the main pool, so a last-resort URL that needs a key that isn't
    set is never dialed.

    Args:
        chain: Chain name.

    Returns:
        List of dialable fallback endpoint URLs (may be empty).
    """
    return _resolved_endpoints(LAST_RESORT_ENDPOINTS.get(chain, []))
