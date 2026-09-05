"""
Ethereum bloom filter pre-filtering (TrueBlocks approach).

Each EVM block header carries a 256-byte ``logsBloom`` — a 2048-bit bloom
filter over every log emitted in the block. Checking whether a queried
address or topic *can* appear in a block before issuing the expensive
``eth_getLogs`` call lets us skip blocks that provably contain no match
(bloom filters have no false negatives, only false positives).

Bit-position algorithm follows `ethereum/eth-bloom`::

    digest = keccak256(value)
    for chunk in (digest[0:2], digest[2:4], digest[4:6]):
        high, low = chunk
        bit = (low + (high << 8)) & 2047   # 16-bit LE word, masked to 11 bits

The filter stores blooms in an in-memory LRU cache, optionally mirrored
to Redis (``REDIS_URL`` env var) so multiple indexer processes share the
same cache. Keccak-256 uses whatever implementation is installed
(pycryptodome, pysha3, eth-hash) and falls back to a pure-Python
Keccak-f[1600] so the module works with zero optional dependencies.
"""

from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Any, cast

import httpx

from lite_indexer.config import (
    BLOOM_CACHE_MAX,
    BLOOM_CACHE_TTL,
    BLOOM_FETCH_CONCURRENCY,
    REDIS_URL,
    RPC_TIMEOUT,
)
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── Keccak-256 ─────────────────────────────────────────────────────────

_KeccakFunc = Callable[[bytes], bytes]

# Keccak-f[1600] round constants (24 rounds).
_KEK_RC: tuple[int, ...] = (
    0x0000000000000001,
    0x0000000000008082,
    0x800000000000808A,
    0x8000000080008000,
    0x000000000000808B,
    0x0000000080000001,
    0x8000000080008081,
    0x8000000000008009,
    0x000000000000008A,
    0x0000000000000088,
    0x0000000080008009,
    0x000000008000000A,
    0x000000008000808B,
    0x800000000000008B,
    0x8000000000008089,
    0x8000000000008003,
    0x8000000000008002,
    0x8000000000000080,
    0x000000000000800A,
    0x800000008000000A,
    0x8000000080008081,
    0x8000000000008080,
    0x0000000080000001,
    0x8000000080008008,
)

# Rotation offsets r[x][y] for the rho step (FIPS 202 Table 2).
# NOTE: the conventional "r[x][y]" rows are NOT what most quick references
# show; this verified layout matches the Keccak reference implementation.
_KEK_ROT: tuple[tuple[int, ...], ...] = (
    (0, 36, 3, 41, 18),
    (1, 44, 10, 45, 2),
    (62, 6, 43, 15, 61),
    (28, 55, 25, 21, 56),
    (27, 20, 39, 8, 14),
)

_MASK64 = (1 << 64) - 1


def _rol64(value: int, shift: int) -> int:
    """Rotate a 64-bit integer left by ``shift`` bits."""
    if shift == 0:
        return value
    return ((value << shift) | (value >> (64 - shift))) & _MASK64


def _keccak_f1600(state: list[list[int]]) -> list[list[int]]:
    """Apply one Keccak-f[1600] permutation to a 5x5 lane state."""
    for rc in _KEK_RC:
        # theta
        c: list[int] = [0] * 5
        for x in range(5):
            c[x] = state[x][0] ^ state[x][1] ^ state[x][2] ^ state[x][3] ^ state[x][4]
        d: list[int] = [0] * 5
        for x in range(5):
            d[x] = c[(x - 1) % 5] ^ _rol64(c[(x + 1) % 5], 1)
        for x in range(5):
            for y in range(5):
                state[x][y] ^= d[x]

        # rho + pi
        b: list[list[int]] = [[0] * 5 for _ in range(5)]
        for x in range(5):
            for y in range(5):
                b[y][(2 * x + 3 * y) % 5] = _rol64(state[x][y], _KEK_ROT[x][y])

        # chi
        for x in range(5):
            for y in range(5):
                state[x][y] = b[x][y] ^ ((~b[(x + 1) % 5][y]) & b[(x + 2) % 5][y])

        # iota
        state[0][0] ^= rc

    return state


def _keccak256_pure(data: bytes) -> bytes:
    """Pure-Python Keccak-256 (rate 1088 bits, capacity 512 bits).

    Fallback used only when no optimized keccak implementation is
    installed. Verified against pycryptodome's ``Crypto.Hash.keccak`` in
    the test suite.
    """
    rate = 136  # bytes per absorbed block for 256-bit output
    # Keccak multi-rate padding: 0x01 domain byte, zero-fill to a rate
    # boundary, then set the 0x80 terminator bit (merging into the 0x01
    # byte when the domain byte already ends the block).
    padded = bytearray(data)
    padded.append(0x01)
    while len(padded) % rate != 0:
        padded.append(0x00)
    padded[-1] ^= 0x80

    state: list[list[int]] = [[0] * 5 for _ in range(5)]
    for offset in range(0, len(padded), rate):
        block = padded[offset : offset + rate]
        for i in range(rate // 8):
            lane = int.from_bytes(block[i * 8 : (i + 1) * 8], "little")
            x, y = i % 5, i // 5
            state[x][y] ^= lane
        state = _keccak_f1600(state)

    out = bytearray()
    while len(out) < 32:
        for y in range(5):
            for x in range(5):
                out.extend(state[x][y].to_bytes(8, "little"))
        state = _keccak_f1600(state)
    return bytes(out[:32])


def _load_keccak() -> _KeccakFunc:
    """Return the best available keccak-256 implementation.

    Preference: pycryptodome → pysha3 → eth-hash → pure-Python fallback.
    """
    try:  # pycryptodome
        from Crypto.Hash import keccak

        def _via_crypto(data: bytes) -> bytes:
            h = keccak.new(digest_bits=256)
            h.update(data)
            return h.digest()

        return _via_crypto
    except ImportError as exc:
        logger.debug("bloom_keccak_pycryptodome_unavailable", error=str(exc)[:60])
    try:  # pysha3
        import sha3 as _sha3

        def _via_pysha3(data: bytes) -> bytes:
            return cast(bytes, _sha3.keccak_256(data).digest())

        return _via_pysha3
    except ImportError as exc:
        logger.debug("bloom_keccak_pysha3_unavailable", error=str(exc)[:60])
    try:  # eth-hash
        from eth_hash.auto import keccak as _eth_hash_keccak

        return _eth_hash_keccak
    except ImportError as exc:
        logger.debug("bloom_keccak_eth_hash_unavailable", error=str(exc)[:60])
    return _keccak256_pure


_keccak256: _KeccakFunc = _load_keccak()


# ── Bloom Filter ───────────────────────────────────────────────────────

_BLOOM_BITS = 2048
_BLOOM_BYTES = 256


def _strip_0x(value: str) -> str:
    """Remove a ``0x`` prefix from a hex string if present."""
    return value[2:] if value.startswith("0x") else value


def _bloom_bit_positions(value: bytes) -> tuple[int, int, int]:
    """Return the three 2048-bit bloom positions for ``value``.

    Mirrors `ethereum/eth-bloom`: keccak256 the value, take the first
    three 16-bit little-endian words, mask each to 11 bits.

    Args:
        value: Raw bytes to hash (20-byte address or 32-byte topic).

    Returns:
        Tuple of three bit positions in ``[0, 2048)``.
    """
    digest = _keccak256(value)
    positions: list[int] = []
    for i in range(0, 6, 2):
        high, low = digest[i], digest[i + 1]
        positions.append((low + (high << 8)) & 2047)
    return positions[0], positions[1], positions[2]


def _address_bytes(address: str) -> bytes:
    """Convert a 0x-prefixed 20-byte address to raw bytes.

    Raises ValueError for malformed addresses.

    Args:
        address: Hex address (0x-prefixed, 40 hex chars).

    Returns:
        20 raw bytes.
    """
    hex_str = _strip_0x(address)
    if len(hex_str) != 40:
        raise ValueError(f"invalid address length: {address}")
    return bytes.fromhex(hex_str)


def _topic_bytes(topic: str) -> bytes:
    """Convert a 0x-prefixed 32-byte topic hash to raw bytes.

    Args:
        topic: Hex topic (0x-prefixed, 64 hex chars).

    Returns:
        32 raw bytes.
    """
    hex_str = _strip_0x(topic)
    if len(hex_str) != 64:
        raise ValueError(f"invalid topic length: {topic}")
    return bytes.fromhex(hex_str)


class BloomFilter:
    """A parsed Ethereum block ``logsBloom`` (2048-bit filter).

    Construct from the hex string found in ``eth_getBlockByNumber``
    headers, then test membership of addresses/topics with
    :meth:`contains_address` / :meth:`contains_topic`.
    """

    __slots__ = ("_bitmap",)

    def __init__(self, logs_bloom_hex: str) -> None:
        """Parse a ``logsBloom`` hex string.

        Args:
            logs_bloom_hex: 256-byte (512 hex char) hex bloom from a
                block header. ``0x`` prefix optional.

        Raises:
            ValueError: If the hex is malformed or not 256 bytes.
        """
        hex_str = _strip_0x(logs_bloom_hex)
        if len(hex_str) != _BLOOM_BYTES * 2:
            raise ValueError(f"logsBloom must be {_BLOOM_BYTES} bytes, got {len(hex_str) // 2}")
        self._bitmap = bytearray(bytes.fromhex(hex_str))

    def _test_bit(self, position: int) -> bool:
        """Return whether bit ``position`` (0-2047) is set.

        Ethereum block-header ``logsBloom`` is big-endian: geth's
        ``bloom9.go`` stores bit ``p`` at byte ``255 - p//8`` (MSB-first,
        byte 0 is the most significant end). Indexing byte ``p//8``
        directly would test the wrong end of the 256-byte bloom and yield
        false negatives for every block.
        """
        byte_idx = 255 - (position >> 3)
        bit_mask = 1 << (position & 0x07)
        return bool(self._bitmap[byte_idx] & bit_mask)

    def contains_topic(self, topic: str) -> bool:
        """Whether this block may contain a log with the given topic.

        Args:
            topic: 0x-prefixed 32-byte topic hash.

        Returns:
            True if the bloom has all three bits set (may match),
            False if it cannot match (no false negatives).
        """
        return all(self._test_bit(position) for position in _bloom_bit_positions(_topic_bytes(topic)))

    def contains_address(self, address: str) -> bool:
        """Whether this block may contain a log from the given address.

        Args:
            address: 0x-prefixed 20-byte contract address.

        Returns:
            True if the bloom has all three bits set (may match),
            False if it cannot match (no false negatives).
        """
        return all(self._test_bit(position) for position in _bloom_bit_positions(_address_bytes(address)))

    @property
    def hex(self) -> str:
        """The canonical ``0x``-prefixed hex of the bloom."""
        return "0x" + self._bitmap.hex()


def _parse_bloom_hex(value: Any) -> BloomFilter | None:
    """Parse a raw block header ``logsBloom`` value safely.

    Args:
        value: Raw JSON value from an RPC response (str or None).

    Returns:
        A :class:`BloomFilter`, or None when the value is unusable.
    """
    if not isinstance(value, str):
        return None
    try:
        return BloomFilter(value)
    except (ValueError, TypeError):
        return None


# ── Bloom Cache ────────────────────────────────────────────────────────


class BloomCache:
    """In-memory LRU cache of per-(chain, block) bloom filters.

    Optionally mirrors entries to Redis (when ``REDIS_URL`` is set and
    ``redis`` is installed) so multiple indexer processes share warm
    blooms. Redis failures degrade gracefully to the memory cache.

    Keys are ``(chain, block_number)``; values are :class:`BloomFilter`.
    Entries expire after :data:`BLOOM_CACHE_TTL` seconds.
    """

    def __init__(
        self,
        max_size: int = BLOOM_CACHE_MAX,
        ttl_seconds: int = BLOOM_CACHE_TTL,
        redis_url: str = "",
    ) -> None:
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._redis_url = redis_url or REDIS_URL
        self._cache: OrderedDict[tuple[str, int], tuple[float, BloomFilter]] = OrderedDict()
        self._redis: Any = None
        self._hits = 0
        self._misses = 0
        self._redis_errors = 0

    @property
    def hits(self) -> int:
        """Number of cache hits (memory + redis)."""
        return self._hits

    @property
    def misses(self) -> int:
        """Number of cache misses."""
        return self._misses

    @property
    def size(self) -> int:
        """Number of entries currently cached in memory."""
        return len(self._cache)

    def _redis_key(self, chain: str, block: int) -> str:
        return f"lci:bloom:{chain}:{block}"

    async def _get_redis(self) -> Any:
        """Lazily create the Redis client (returns None when unavailable)."""
        if self._redis is not None:
            return self._redis
        if not self._redis_url:
            return None
        try:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(self._redis_url, socket_connect_timeout=1)
            return self._redis
        except ImportError:
            return None

    async def set(self, chain: str, block: int, bloom_hex: str) -> None:
        """Store a bloom filter for ``(chain, block)``.

        Args:
            chain: Chain name.
            block: Block number.
            bloom_hex: ``logsBloom`` hex string from the block header.
        """
        bloom = _parse_bloom_hex(bloom_hex)
        if bloom is None:
            return
        now = time.monotonic()
        self._cache[(chain, block)] = (now, bloom)
        self._cache.move_to_end((chain, block))
        while len(self._cache) > self._max_size:
            self._cache.popitem(last=False)
        if self._redis_url:
            try:
                redis_client = await self._get_redis()
                if redis_client is not None:
                    await redis_client.setex(self._redis_key(chain, block), self._ttl, bloom.hex)
            except Exception as exc:  # pragma: no cover - env dependent
                self._redis_errors += 1
                logger.debug("bloom_redis_set_failed", error=str(exc)[:60])

    async def get(self, chain: str, block: int) -> BloomFilter | None:
        """Return the cached bloom for ``(chain, block)`` or None.

        Checks memory first, then Redis. Expired entries are evicted.

        Args:
            chain: Chain name.
            block: Block number.

        Returns:
            :class:`BloomFilter` or None when absent/expired.
        """
        now = time.monotonic()
        key = (chain, block)
        entry = self._cache.get(key)
        if entry is not None:
            ts, bloom = entry
            if now - ts <= self._ttl:
                self._cache.move_to_end(key)
                self._hits += 1
                return bloom
            del self._cache[key]
        if self._redis_url:
            try:
                redis_client = await self._get_redis()
                if redis_client is not None:
                    raw = await redis_client.get(self._redis_key(chain, block))
                    if raw:
                        self._hits += 1
                        return _parse_bloom_hex(raw)
            except Exception as exc:  # pragma: no cover - env dependent
                self._redis_errors += 1
                logger.debug("bloom_redis_get_failed", error=str(exc)[:60])
        self._misses += 1
        return None

    async def has(self, chain: str, block: int) -> bool:
        """Whether a bloom is cached (memory or Redis) for the block.

        Args:
            chain: Chain name.
            block: Block number.

        Returns:
            True when a cached bloom exists.
        """
        if (chain, block) in self._cache:
            return True
        if self._redis_url:
            try:
                redis_client = await self._get_redis()
                if redis_client is not None:
                    return bool(await redis_client.exists(self._redis_key(chain, block)))
            except Exception:  # pragma: no cover - env dependent
                self._redis_errors += 1
        return False

    async def contains_address(self, chain: str, block: int, address: str) -> bool | None:
        """Test a cached bloom for an address.

        Args:
            chain: Chain name.
            block: Block number.
            address: 0x-prefixed contract address.

        Returns:
            True/False when a bloom is cached, None when not cached.
        """
        bloom = await self.get(chain, block)
        if bloom is None:
            return None
        try:
            return bloom.contains_address(address)
        except ValueError:
            return None

    async def contains_topic(self, chain: str, block: int, topic: str) -> bool | None:
        """Test a cached bloom for a topic.

        Args:
            chain: Chain name.
            block: Block number.
            topic: 0x-prefixed 32-byte topic hash.

        Returns:
            True/False when a bloom is cached, None when not cached.
        """
        bloom = await self.get(chain, block)
        if bloom is None:
            return None
        try:
            return bloom.contains_topic(topic)
        except ValueError:
            return None


# ── Bloom Fetching (block header pre-filter) ───────────────────────────


async def fetch_block_blooms(
    chain: str,
    blocks: set[int],
    *,
    client: httpx.AsyncClient,
    endpoints: list[str],
    cache: BloomCache,
    concurrency: int = BLOOM_FETCH_CONCURRENCY,
) -> dict[int, BloomFilter]:
    """Fetch and cache ``logsBloom`` for blocks missing from the cache.

    Uses ``eth_getBlockByNumber`` with ``full_tx=false`` (header only —
    small payloads). Already-cached blocks are skipped, so repeated
    address queries over the same range cost zero RPC calls.

    Args:
        chain: Chain name.
        blocks: Block numbers to warm.
        client: Shared HTTP client.
        endpoints: Candidate RPC endpoints for the chain.
        cache: Bloom cache to populate/consult.
        concurrency: Max parallel header fetches.

    Returns:
        Dict of ``{block: BloomFilter}`` for blocks with a usable bloom
        (cached or freshly fetched).
    """
    if not blocks or not endpoints:
        return {}

    from lite_indexer.rpc import get_best_endpoint, record_endpoint_failure

    # Bound concurrent bloom fetches. The semaphore caps in-flight HTTP
    # requests; the bounded pool below additionally caps live coroutines so
    # a multi-million-block range never materializes one coroutine per block.
    semaphore = asyncio.Semaphore(concurrency)

    async def _fetch_one(block: int) -> tuple[int, BloomFilter | None]:
        existing = await cache.get(chain, block)
        if existing is not None:
            return block, existing
        endpoint = get_best_endpoint(endpoints)
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [hex(block), False],
        }
        async with semaphore:
            try:
                resp = await client.post(endpoint, json=payload, timeout=RPC_TIMEOUT)
                data = resp.json()
                header = data.get("result") if isinstance(data, dict) else None
                if isinstance(header, dict) and header.get("logsBloom"):
                    bloom_hex = str(header["logsBloom"])
                    bloom = _parse_bloom_hex(bloom_hex)
                    if bloom is not None:
                        await cache.set(chain, block, bloom_hex)
                        return block, bloom
                if not (isinstance(data, dict) and "result" in data and data["result"] is None):
                    record_endpoint_failure(endpoint)
            except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError) as exc:
                logger.warning("bloom_fetch_failed", block=block, error=str(exc)[:60])
                record_endpoint_failure(endpoint)
        return block, None

    result: dict[int, BloomFilter] = {}
    in_flight: set[asyncio.Task[tuple[int, BloomFilter | None]]] = set()

    def _collect(done: set[asyncio.Task[tuple[int, BloomFilter | None]]]) -> None:
        for task in done:
            try:
                outcome = task.result()
            except Exception:  # pragma: no cover - defensive; _fetch_one logs expected errors
                continue
            if outcome[1] is not None:
                result[outcome[0]] = outcome[1]

    for block in blocks:
        in_flight.add(asyncio.create_task(_fetch_one(block)))
        if len(in_flight) >= concurrency:
            done, in_flight = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            _collect(done)
    if in_flight:
        done, _ = await asyncio.wait(in_flight)
        _collect(done)
    return result


async def candidate_blocks_for_address(
    chain: str,
    blocks: set[int],
    address: str,
    cache: BloomCache,
) -> set[int]:
    """Return blocks whose bloom may contain ``address``.

    Only blocks with a cached bloom are evaluated; blocks without a
    cached bloom are conservatively treated as candidates (we never
    skip a block on an unknown bloom — blooms have no false negatives).

    Args:
        chain: Chain name.
        blocks: Candidate block numbers.
        address: 0x-prefixed contract address.
        cache: Bloom cache.

    Returns:
        Subset of ``blocks`` that may contain the address.
    """
    candidates: set[int] = set()
    for block in blocks:
        maybe = await cache.contains_address(chain, block, address)
        if maybe is None or maybe:
            candidates.add(block)
    return candidates


async def candidate_blocks_for_topic(
    chain: str,
    blocks: set[int],
    topic: str,
    cache: BloomCache,
) -> set[int]:
    """Return blocks whose bloom may contain ``topic``.

    See :func:`candidate_blocks_for_address` for the unknown-bloom
    conservative policy.

    Args:
        chain: Chain name.
        blocks: Candidate block numbers.
        topic: 0x-prefixed 32-byte topic hash.
        cache: Bloom cache.

    Returns:
        Subset of ``blocks`` that may contain the topic.
    """
    candidates: set[int] = set()
    for block in blocks:
        maybe = await cache.contains_topic(chain, block, topic)
        if maybe is None or maybe:
            candidates.add(block)
    return candidates


__all__ = [
    "BloomFilter",
    "BloomCache",
    "candidate_blocks_for_address",
    "candidate_blocks_for_topic",
    "fetch_block_blooms",
    "_keccak256",
    "_bloom_bit_positions",
]
