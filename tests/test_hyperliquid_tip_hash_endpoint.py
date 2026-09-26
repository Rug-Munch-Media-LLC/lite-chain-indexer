"""The Hyperliquid pool must hand reorg_stage an endpoint with a tip hash.

The 2026-09-26 forward stall: every sync cycle died in
``EVMChainStrategy.reorg_stage`` with ``reorg_status_unknown`` because the
endpoint ``get_best_endpoint`` selected — the OFFICIAL ``rpc.hyperliquid.xyz``
RPC — answers ``eth_blockNumber`` (so it passes the liveness probe and is never
quarantined) but refuses every ``eth_getBlockByNumber`` with ``-32005
"More than 3000 archived blocks queried in one day"``. ``reorg_stage`` asks that
single endpoint for a TIP BLOCK HASH, so it always got ``None``.

pocket was never the problem: it returns a tip hash. The fix promotes the
keyless ``hyperliquid-rpc.publicnode.com`` (same getLogs payload, same block
hashes, ~55ms vs pocket's ~490ms) to the head of the pool, so the endpoint
``reorg_stage`` is handed can serve the probe.

``test_selected_endpoint_returns_tip_block_hash`` is the regression guard and
dials the real endpoint (``smoke``-marked, skipped without egress).
"""

from __future__ import annotations

import pytest

from lite_indexer.chains.endpoints import BUILTIN_ENDPOINTS, GET_LOGS_ENDPOINTS
from lite_indexer.rpc.tip_capable import get_tip_hash_endpoint, tip_hash_capable_endpoints

CHAIN = "hyperliquid"

# The official RPC: liveness-clean but archive-budget-exhausted (no block hash).
OFFICIAL = "https://rpc.hyperliquid.xyz/evm"
# Promoted to the head of the pool 2026-09-26 — returns a tip block hash.
PUBLICNODE = "https://hyperliquid-rpc.publicnode.com"
POCKET = "https://hyperliquid.api.pocket.network"


def test_pool_head_returns_a_tip_block_hash_capable_endpoint() -> None:
    """The head of the public pool is a tip-hash-capable endpoint.

    Static guard: no network, runs in CI. It pins the ORDER, because order is
    the cheap half of the fix — ``get_best_endpoint`` still ends in a random
    P2C draw, which is why the selector test below is the real guard.
    Pocket is logs-capable and tip-capable but is range-limited, so it must
    not sit above the publicnode primary.
    """
    pool = BUILTIN_ENDPOINTS[CHAIN]
    assert pool[0] == PUBLICNODE
    assert OFFICIAL not in pool[:2], "tip-incapable official RPC must not lead the pool"
    assert POCKET in pool, "pocket stays as the range-limited second source"
    # Every entry is dialable (no unresolved ${ENV} placeholder) and unique.
    assert all("${" not in url for url in pool)
    assert len(pool) == len(set(pool))


def test_reorg_selector_never_returns_the_tip_incapable_official_rpc() -> None:
    """The reorg tip-hash selector never hands out the official RPC.

    This is the real regression guard. ``get_best_endpoint`` is NOT enough:
    its final step is a random two-draw over the healthy subset and it does
    not consult the logs-capability quarantine, so position in the pool does
    not decide what ``reorg_stage`` receives. Asserted over repeated draws
    so a lucky draw cannot pass the test.
    """
    pool = GET_LOGS_ENDPOINTS[CHAIN]
    assert pool, "hyperliquid pool must not be empty"

    # The ordering helper keeps the tip-incapable endpoint out of the head,
    # only as a last-resort tail when nothing capable exists.
    ordered = tip_hash_capable_endpoints(CHAIN, pool)
    assert ordered[0] == PUBLICNODE
    assert OFFICIAL in ordered, "tip-incapable endpoint stays as the last-resort tail"
    assert ordered.index(OFFICIAL) == len(ordered) - 1

    for _ in range(25):
        assert get_tip_hash_endpoint(CHAIN, pool) != OFFICIAL


def test_tip_incapable_chain_with_no_capable_endpoint_still_returns_one() -> None:
    """A pool of only tip-incapable endpoints still yields an endpoint.

    The selector must never hand ``reorg_stage`` an empty pool: it degrades
    to the least-bad endpoint rather than turning a stall into a crash.
    """
    assert get_tip_hash_endpoint(CHAIN, [OFFICIAL]) == OFFICIAL


@pytest.mark.smoke
@pytest.mark.skipif(not GET_LOGS_ENDPOINTS[CHAIN], reason="hyperliquid pool empty — overlay/config missing")
async def test_selected_endpoint_returns_tip_block_hash() -> None:
    """The endpoint the pool actually selects serves a tip block hash.

    This is the exact call ``reorg_stage`` makes
    (``rpc.eth_get_block_by_number`` → ``result["hash"]``); a ``None`` block
    there is what produced ``reorg_status_unknown`` on every cycle.
    """
    from lite_indexer.rpc.client import RpcClient

    endpoint = get_tip_hash_endpoint(CHAIN, GET_LOGS_ENDPOINTS[CHAIN])
    rpc = RpcClient()
    tip = await rpc.eth_block_number(endpoint)
    assert tip is not None, f"selected endpoint {endpoint} serves no tip height"

    block = await rpc.eth_get_block_by_number(endpoint, tip)
    assert block is not None, f"selected endpoint {endpoint} returned no block for tip {tip}"
    assert block.get("hash"), f"selected endpoint {endpoint} returned a block with no hash"
    assert isinstance(block["hash"], str) and block["hash"].startswith("0x")
