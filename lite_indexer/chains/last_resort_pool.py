"""Last-resort endpoint pool — the emergency tail of the RPC ladder.

Extracted from ``chains/endpoints.py`` (shrink-only size ratchet) as a pure
data table. ``chains.endpoints`` re-exports :data:`LAST_RESORT_ENDPOINTS` and
owns the ``last_resort_endpoints()`` accessor (callers monkeypatch that one, so
it must stay in ``endpoints``), and both stay re-exported from
``lite_indexer.chains``.
"""

from __future__ import annotations

# ── Last-Resort Endpoints ───────────────────────────────────────────────
# Known-good endpoints tried when every endpoint in the active pool fails
# for a chain. These are the most battle-tested public providers from the
# fallback only — the regular pool's health scoring / bans / circuit
# breakers still apply; the last-resort list exists so a total pool wipe
# (e.g. an outage taking out every tier at once) still has a shot at
# serving the range instead of raising RpcFetchFailedError immediately.
LAST_RESORT_ENDPOINTS: dict[str, list[str]] = {
    "ethereum": [
        "https://eth.api.onfinality.io/public",
        "https://eth-pokt.nodies.app",
        "https://rpc.mevblocker.io",
        "https://rpc.flashbots.net",
    ],
    "base": [
        "https://base.gateway.tenderly.co",
        "https://mainnet.base.org",
    ],
    "bsc": [
        "https://rpc-bsc.48.club",
        "https://bsc-dataseed.binance.org",
        "https://bsc-dataseed1.defibit.io",
    ],
    "polygon": [
        "https://polygon.gateway.tenderly.co",
        "https://polygon-mainnet.gateway.tatum.io",
        "https://rpc-mainnet.matic.quiknode.pro",
    ],
    "arbitrum": [
        "https://arbitrum.gateway.tenderly.co",
        "https://arb1.arbitrum.io/rpc",
    ],
    "optimism": [
        "https://optimism.gateway.tenderly.co",
        "https://mainnet.optimism.io",
    ],
    "avalanche": ["https://api.avax.network/ext/bc/C/rpc"],
    "fantom": ["https://rpc.fantom.network"],
    "zksync": ["https://mainnet.era.zksync.io"],
    "mantle": ["https://rpc.mantle.xyz"],
    "gnosis": [
        "https://rpc.gnosischain.com",
        "https://gnosis.gateway.tenderly.co",
    ],
    "celo": ["https://forno.celo.org"],
    "robinhood": ["https://rpc.mainnet.chain.robinhood.com"],
    # hyperliquid: publicnode FIRST — the official RPC is the wrong
    # last-resort here precisely because it can no longer serve a block
    # hash (daily archived-block budget exhausted, -32005), which is what
    # reorg_stage needs. Same rule as the main pool: the last-resort list
    # must contain an endpoint that can actually answer the reorg probe.
    "hyperliquid": [
        "https://hyperliquid-rpc.publicnode.com",
        "https://rpc.hyperliquid.xyz/evm",
    ],
}
