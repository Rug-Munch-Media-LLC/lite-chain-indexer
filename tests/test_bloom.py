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
    _topic_bytes,
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
