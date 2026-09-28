"""LP (Liquidity Provider) event topics for LiteChainIndexer.

Split out of :mod:`lite_indexer.config.chains` (2026-09-28) so that file
stays under the per-file size ratchet. Re-exported by
:mod:`lite_indexer.config`, so ``from lite_indexer.config import
MINT_V2_TOPIC`` keeps working unchanged.

LP add/remove on Uniswap V2 pairs and V3 pools is expressed as Mint and
Burn events on the pool contract. The scanner's ``liquidity_drain``
detector reads dex_events rows with type IN
('add_liquidity','remove_liquidity','lp_add','lp_remove'); we emit the
first two so no scanner-side edit is needed.

Hashes are keccak256 of the exact canonical signature — they are DERIVED,
not copied. ``tests/test_dex_lp_events.py`` recomputes each one from its
signature, so a typo in this file fails the suite instead of silently
ingesting zero logs (a wrong topic0 never errors, it just returns an
empty result set).
"""

from __future__ import annotations

# ── LP (Liquidity Provider) events ────────────────────────────────────
# LP add/remove on Uniswap V2 pairs and V3 pools is expressed as Mint and
# Burn events on the pool contract. The scanner's `liquidity_drain`
# detector reads dex_events rows with type IN
# ('add_liquidity','remove_liquidity','lp_add','lp_remove'); we emit the
# first two so no scanner-side edit is needed.
#
# Hashes are keccak256 of the exact canonical signature — they are
# DERIVED, not copied. tests/test_dex_lp_events.py recomputes each one
# from its signature, so a typo in this file fails the suite instead of
# silently ingesting zero logs (a wrong topic0 never errors, it just
# returns an empty result set).

# keccak256("Mint(address,uint256,uint256)")
# Uniswap V2 Pair: sender=topics[1]; data = amount0 + amount1 (2× uint256)
MINT_V2_TOPIC = "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f"

# keccak256("Burn(address,uint256,uint256,address)")
# Uniswap V2 Pair: sender=topics[1], to=topics[2];
# data = amount0 + amount1 (2× uint256)
BURN_V2_TOPIC = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"

# keccak256("Mint(address,address,int24,int24,uint128,uint256,uint256)")
# Uniswap V3 Pool: owner=topics[1], sender=topics[2];
# data = tickLower + tickUpper + amount + amount0 + amount1 + fee
MINT_V3_TOPIC = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"

# keccak256("Burn(address,address,int24,int24,uint128,uint256,uint256)")
# Uniswap V3 Pool: owner=topics[1], sender=topics[2];
# data = tickLower + tickUpper + amount + amount0 + amount1 + fee
BURN_V3_TOPIC = "0x7a9138e633647262793b76f357ef66928a1afc7d086587a1fbe87087e8ff4b02"

# The two dex_events `type` values derived from the topics above. These
# are the exact strings the scanner's liquidity_drain filter matches on.
LP_ADD_TYPE = "add_liquidity"
LP_REMOVE_TYPE = "remove_liquidity"

# topic0 -> (dex_events type, dex version, dex label). Single source of
# truth for the derivation rule; consumed by dex/lp.py and re-checked
# against the constant values by the test suite.
LP_TOPIC_TYPES: dict[str, tuple[str, str, str]] = {
    MINT_V2_TOPIC: (LP_ADD_TYPE, "v2", "uniswap_v2"),
    BURN_V2_TOPIC: (LP_REMOVE_TYPE, "v2", "uniswap_v2"),
    MINT_V3_TOPIC: (LP_ADD_TYPE, "v3", "uniswap_v3"),
    BURN_V3_TOPIC: (LP_REMOVE_TYPE, "v3", "uniswap_v3"),
}
