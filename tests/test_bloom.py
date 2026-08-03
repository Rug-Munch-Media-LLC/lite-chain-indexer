"""Tests for the EVM bloom filter pre-filtering.

Regression tests for the critical byte-order bug: Ethereum block-header
``logsBloom`` is big-endian — geth's ``bloom9.go`` stores bit ``p`` at
byte ``255 - p//8`` — but ``_test_bit`` used to read byte ``p//8``,
producing 100% false negatives (address queries returned empty for blocks
that DO contain the address).

Bloom membership is cross-validated against the ``eth_bloom`` reference
library (ethereum/eth-bloom, used by geth tooling) when installed;
otherwise the module's own Keccak-256 builds the same big-endian layout.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from lite_indexer.strategies.evm.bloom import (
    BloomCache,
    BloomFilter,
    _address_bytes,
    _bloom_bit_positions,
    _parse_bloom_hex,
    _topic_bytes,
    candidate_blocks_for_address,
    candidate_blocks_for_topic,
    fetch_block_blooms,
)

USDT = "0xdac17f958d2ee523a2206206994597c13d831ec7"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_ADDRESS = "0x" + "0" * 40


def _reference_logs_bloom(value: bytes) -> str:
    """Build a geth-conformant 256-byte ``logsBloom`` hex for ``value``.

    Uses eth_bloom (the reference ethereum/eth-bloom implementation) when
    installed: its integer bloom filter serialized big-endian yields
    exactly geth's byte layout. Falls back to the module's own Keccak-256
    with the same MSB-first bit placement.
    """
    try:
        from eth_bloom import BloomFilter as RefBloom

        ref = RefBloom()
        ref.add(value)
        return int(ref).to_bytes(256, "big").hex()
    except ImportError:
        bitmap = bytearray(256)
        for position in _bloom_bit_positions(value):
            bitmap[255 - (position >> 3)] |= 1 << (position & 0x07)
        return bitmap.hex()


class TestBloomByteOrder:
    """The logsBloom byte order must match geth (MSB-first)."""

    def test_bloom_contains_known_address(self):
        """Verify bloom matches against a geth-computed logsBloom."""
        # Reference logsBloom computed for the USDT contract address.
        bloom = BloomFilter("0x" + _reference_logs_bloom(_address_bytes(USDT)))
        assert bloom.contains_address(USDT)
        assert not bloom.contains_address(ZERO_ADDRESS)  # random address

    def test_bloom_bits_at_geth_positions(self):
        """Bits live at byte ``255 - p//8``, not at ``p//8`` (the old bug)."""
        bloom = BloomFilter("0x" + _reference_logs_bloom(_address_bytes(USDT)))
        positions = _bloom_bit_positions(_address_bytes(USDT))
        assert len(positions) == 3
        for position in positions:
            # geth bloom9.go: b[255 - p//8] |= 1 << (p % 8)
            assert bloom._bitmap[255 - (position >> 3)] & (1 << (position & 0x07))
            # The pre-fix implementation read byte p//8 — provably unset here.
            assert not bloom._bitmap[position >> 3] & (1 << (position & 0x07))

    def test_bloom_contains_known_topic(self):
        """Topics use the same big-endian layout as addresses."""
        bloom = BloomFilter("0x" + _reference_logs_bloom(_topic_bytes(TRANSFER_TOPIC)))
        assert bloom.contains_topic(TRANSFER_TOPIC)
        assert not bloom.contains_topic("0x" + "0" * 64)

    def test_parse_invalid_bloom_raises(self):
        """A malformed logsBloom must be rejected."""
        with pytest.raises(ValueError):
            BloomFilter("0x00")
        with pytest.raises(ValueError):
            BloomFilter("0x" + "zz" * 256)


class TestFetchBlockBloomsConcurrency:
    """fetch_block_blooms must bound concurrent bloom fetches."""

    def test_bounded_concurrency_and_results(self):
        """In-flight RPC requests never exceed the concurrency cap."""

        in_flight = 0
        max_in_flight = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(0.01)
            in_flight -= 1
            return httpx.Response(
                200,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "result": {"logsBloom": "0x" + "00" * 256},
                },
            )

        async def run() -> tuple[dict[int, BloomFilter], int]:
            transport = httpx.MockTransport(handler)
            async with httpx.AsyncClient(transport=transport) as client:
                cache = BloomCache()
                blooms = await fetch_block_blooms(
                    "test",
                    set(range(200)),
                    client=client,
                    endpoints=["http://rpc.test"],
                    cache=cache,
                    concurrency=16,
                )
                return blooms, max_in_flight

        blooms, max_in_flight = asyncio.run(run())
        assert len(blooms) == 200
        assert max_in_flight <= 16
        assert max_in_flight >= 1

    def test_empty_inputs_noop(self):
        """Empty block set / endpoints returns {} without RPC calls."""

        async def run() -> dict[int, BloomFilter]:
            async with httpx.AsyncClient() as client:
                cache = BloomCache()
                return await fetch_block_blooms(
                    "test",
                    set(),
                    client=client,
                    endpoints=["http://rpc.test"],
                    cache=cache,
                )

        assert asyncio.run(run()) == {}


class TestBloomConstruction:
    """BloomFilter construction and parsing edge cases."""

    def test_hex_property_roundtrip(self):
        bloom = BloomFilter("0x" + _reference_logs_bloom(_address_bytes(USDT)))
        # Re-parse the canonical hex; membership is stable.
        reparsed = BloomFilter(bloom.hex)
        assert reparsed.contains_address(USDT)
        assert reparsed.hex == bloom.hex

    def test_prefix_optional(self):
        raw = _reference_logs_bloom(_address_bytes(USDT))
        with_prefix = BloomFilter("0x" + raw)
        without_prefix = BloomFilter(raw)
        assert with_prefix.hex == without_prefix.hex

    def test_empty_bloom_matches_nothing(self):
        empty = BloomFilter("0x" + "00" * 256)
        assert not empty.contains_address(USDT)
        assert not empty.contains_topic(TRANSFER_TOPIC)

    def test_parse_bloom_hex_rejects_invalid(self):
        assert _parse_bloom_hex(None) is None
        assert _parse_bloom_hex(12345) is None
        assert _parse_bloom_hex("0x00") is None  # too short
        assert _parse_bloom_hex("0x" + "zz" * 256) is None  # not hex
        valid = _parse_bloom_hex("0x" + _reference_logs_bloom(_address_bytes(USDT)))
        assert isinstance(valid, BloomFilter)


class TestBloomCache:
    """BloomCache LRU + TTL semantics."""

    def _usdt_bloom_hex(self) -> str:
        return "0x" + _reference_logs_bloom(_address_bytes(USDT))

    async def _roundtrip(self) -> tuple[BloomCache, BloomFilter]:
        cache = BloomCache(max_size=8, ttl_seconds=60)
        await cache.set("ethereum", 1, self._usdt_bloom_hex())
        bloom = await cache.get("ethereum", 1)
        assert bloom is not None
        assert bloom.contains_address(USDT)
        return cache, bloom

    def test_set_get_roundtrip(self):
        cache, bloom = asyncio.run(self._roundtrip())
        assert cache.hits == 1  # the get() was a hit
        assert cache.misses == 0

    def test_miss_increments_counter(self):
        async def run() -> BloomCache:
            cache = BloomCache()
            bloom = await cache.get("ethereum", 999)
            assert bloom is None
            return cache

        cache = asyncio.run(run())
        assert cache.misses == 1

    def test_invalid_bloom_not_cached(self):
        async def run() -> BloomCache:
            cache = BloomCache()
            await cache.set("ethereum", 1, "0xnothex")
            assert cache.size == 0
            assert await cache.get("ethereum", 1) is None
            return cache

        asyncio.run(run())

    def test_ttl_expiry_evicts(self, monkeypatch):
        import lite_indexer.strategies.evm.bloom as bloom_mod

        now = 1000.0
        monkeypatch.setattr(bloom_mod.time, "monotonic", lambda: now)

        async def run() -> BloomCache:
            cache = BloomCache(max_size=8, ttl_seconds=60)
            await cache.set("ethereum", 1, self._usdt_bloom_hex())
            assert await cache.get("ethereum", 1) is not None
            return cache

        cache = asyncio.run(run())

        # Advance the clock past the TTL, then re-read: the entry must be
        # evicted (a get() after expiry returns None and drops the entry).
        now = 1100.0
        monkeypatch.setattr(bloom_mod.time, "monotonic", lambda: now)
        assert asyncio.run(cache.get("ethereum", 1)) is None
        assert cache.size == 0
        assert cache.misses >= 1

    def test_lru_eviction_when_over_max(self):
        async def run() -> BloomCache:
            cache = BloomCache(max_size=2)
            await cache.set("ethereum", 1, self._usdt_bloom_hex())
            await cache.set("ethereum", 2, self._usdt_bloom_hex())
            await cache.set("ethereum", 3, self._usdt_bloom_hex())
            assert cache.size == 2  # block 1 evicted (LRU)
            assert await cache.get("ethereum", 1) is None
            assert await cache.get("ethereum", 3) is not None
            return cache

        asyncio.run(run())

    def test_contains_address_returns_none_on_malformed_address(self):
        async def run() -> bool:
            cache = BloomCache()
            await cache.set("ethereum", 1, self._usdt_bloom_hex())
            return await cache.contains_address("ethereum", 1, "0x1234") is None

        assert asyncio.run(run())


class TestCandidateBlocks:
    """candidate_blocks_for_address/topic conservative policy."""

    def _seed_cache(self) -> BloomCache:
        cache = BloomCache()
        bloom_hex = "0x" + _reference_logs_bloom(_address_bytes(USDT))

        async def _seed() -> BloomCache:
            await cache.set("ethereum", 10, bloom_hex)  # contains USDT
            await cache.set("ethereum", 20, "0x" + "00" * 256)  # contains nothing
            return cache

        return asyncio.run(_seed())

    def test_address_keeps_match_and_unknown_drops_nonmatch(self):
        cache = self._seed_cache()
        blocks = {10, 20, 30}  # 30 has no cached bloom → conservative keep
        candidates = asyncio.run(candidate_blocks_for_address("ethereum", blocks, USDT, cache))
        assert candidates == {10, 30}
        assert 20 not in candidates  # bloom provably has no USDT bits

    def test_topic_unknown_bloom_conservative(self):
        cache = self._seed_cache()
        blocks = {10, 30}
        candidates = asyncio.run(candidate_blocks_for_topic("ethereum", blocks, TRANSFER_TOPIC, cache))
        # block 10's bloom has USDT address bits (not topic bits) → likely excluded;
        # block 30 (uncached) is always kept.
        assert 30 in candidates
        assert 20 not in candidates
