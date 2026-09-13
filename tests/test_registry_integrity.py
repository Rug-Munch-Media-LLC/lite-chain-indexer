"""Registry integrity tests — endpoint pools stay dialable and dupe-free.

Contracts added 2026-08-23 (freemium depth + candidate pool):

* every active endpoint is a full URL — no unresolved ``${...}`` templates
  leak into the pool (they are dropped at import by design)
* no duplicate URLs within a chain's active pool
* candidate pool never overlaps the active pool (probe skips dupes, but
  the invariant is enforced here so drift fails loudly)
* keyed freemium templates exist for every major chain (escalation depth)
* expected chainId map covers every chain with an active or candidate pool
"""

from __future__ import annotations

from lite_indexer.chains.endpoints import (
    CANDIDATE_ENDPOINTS,
    EXPECTED_CHAIN_IDS,
    GET_LOGS_ENDPOINTS,
    LAST_RESORT_ENDPOINTS,
)
from lite_indexer.chains.non_evm import SOLANA_ENDPOINTS, TRON_ENDPOINTS


def _no_unresolved(urls: list[str]) -> None:
    for url in urls:
        assert "${" not in url, f"unresolved template leaked into pool: {url}"


def test_active_pools_contain_only_resolved_urls() -> None:
    for _chain, urls in GET_LOGS_ENDPOINTS.items():
        _no_unresolved(urls)


def test_non_evm_pools_contain_only_resolved_urls() -> None:
    _no_unresolved(SOLANA_ENDPOINTS)
    _no_unresolved(TRON_ENDPOINTS)


def test_no_duplicates_within_chain_pool() -> None:
    for chain, urls in GET_LOGS_ENDPOINTS.items():
        assert len(urls) == len(set(urls)), f"duplicate endpoints in {chain}: {urls}"
    assert len(SOLANA_ENDPOINTS) == len(set(SOLANA_ENDPOINTS))
    assert len(TRON_ENDPOINTS) == len(set(TRON_ENDPOINTS))


def test_candidates_never_overlap_active_pool() -> None:
    for chain, urls in CANDIDATE_ENDPOINTS.items():
        active = set(GET_LOGS_ENDPOINTS.get(chain, []))
        overlap = set(urls) & active
        assert not overlap, f"candidates already active in {chain}: {overlap}"
        assert len(urls) == len(set(urls)), f"candidate duplicates in {chain}"


def test_public_pool_is_keyless_by_design() -> None:
    """The PUBLIC repo ships free/keyless endpoints only; curated/keyed
    providers are supplied by the private runtime overlay (see
    lite_indexer/chains/_overlay.py). A keyed template in the builtin pool
    would be a provider leak."""
    from lite_indexer.chains.endpoints import BUILTIN_ENDPOINTS

    for chain, urls in BUILTIN_ENDPOINTS.items():
        for url in urls:
            assert "${" not in url, f"{chain}: keyed template in public pool: {url}"


def test_expected_chain_ids_cover_all_pooled_chains() -> None:
    pooled = set(GET_LOGS_ENDPOINTS) | set(CANDIDATE_ENDPOINTS) | {"solana", "tron"}
    missing = {c for c in pooled if c not in EXPECTED_CHAIN_IDS}
    # solana/tron use non-EVM probes; every EVM chain must have its id.
    evm_missing = missing - {"solana", "tron"}
    assert not evm_missing, f"chainId map missing: {evm_missing}"


def test_last_resort_subset_of_active_templates() -> None:
    """Last-resort URLs must exist in the chain's active pool (post-
    resolution) — a rescue list pointing at non-pool endpoints is a lie."""
    for chain, urls in LAST_RESORT_ENDPOINTS.items():
        active = set(GET_LOGS_ENDPOINTS.get(chain, []))
        strays = [u for u in urls if u not in active]
        assert not strays, f"last-resort strays in {chain}: {strays}"


def test_free_first_ordering_active_pools() -> None:
    """Free-first product principle: within each active chain pool, every
    keyless public endpoint must precede all keyed (freemium) templates.

    Position feeds the pool scoring penalty (+i), so a keyed endpoint
    sitting before free publics would outrank them on ties — violating
    the 'free data first always' rule. Enforced structurally here.
    """
    for chain, urls in GET_LOGS_ENDPOINTS.items():
        seen_keyed = False
        for url in urls:
            if "${" in url:
                seen_keyed = True
            else:
                assert not seen_keyed, (
                    f"{chain}: free endpoint after keyed template violates free-first ordering: {url}"
                )
    for urls in (SOLANA_ENDPOINTS, TRON_ENDPOINTS):
        seen_keyed = False
        for url in urls:
            if "${" in url:
                seen_keyed = True
            else:
                assert not seen_keyed, f"non-EVM ordering violation: {url}"
