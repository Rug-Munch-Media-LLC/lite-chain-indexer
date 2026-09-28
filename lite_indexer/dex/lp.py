"""
Uniswap V2/V3 LP event decoders — Mint/Burn -> add_liquidity/remove_liquidity.

The SWAP path (:mod:`lite_indexer.dex.parsers`) derives its ``type`` from
the topic; this module does the same for LP legs so ``dex_events.type`` is
never hardcoded (the previous writer emitted ``"swap"`` for every row,
which is why LP activity could never reach the scanner).

  V2 Pair.Mint(address indexed sender, uint256 amount0, uint256 amount1)
    topics[1] = sender, data = amount0 + amount1
  V2 Pair.Burn(address indexed sender, uint256 amount0, uint256 amount1,
               address indexed to)
    topics[1] = sender, topics[2] = to, data = amount0 + amount1
  V3 Pool.Mint(address indexed owner, address indexed sender, int24 tickLower,
               int24 tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
    topics[1] = owner, topics[2] = sender,
    data = tickLower + tickUpper + amount + amount0 + amount1 + fee
  V3 Pool.Burn(address indexed owner, address indexed sender, int24 tickLower,
               int24 tickUpper, uint128 amount, uint256 amount0, uint256 amount1)
    topics[1] = owner, topics[2] = sender, same data layout as Mint.

V3 amount0/amount1 are int256 in the pool source; the magnitude is stored
(identical to the existing V3 Swap handling) so a negative leg does not
decode to ~2^256 and flag every burn as a whale.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import (
    BURN_V2_TOPIC,
    BURN_V3_TOPIC,
    LP_TOPIC_TYPES,
    MINT_V2_TOPIC,
    MINT_V3_TOPIC,
)
from lite_indexer.logging import get_logger

from .common import parse_block_number, parse_log_index, unpad_address
from .log_context import LogDecodeContext
from .v3 import _decode_int256_magnitude

logger = get_logger(__name__)

_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# topic0 -> number of indexed params after the signature (V2: 1/2, V3: 2)
_LP_INDEXED_PARAMS: dict[str, int] = {
    MINT_V2_TOPIC: 1,
    BURN_V2_TOPIC: 2,
    MINT_V3_TOPIC: 2,
    BURN_V3_TOPIC: 2,
}


def lp_event_type(topic: str) -> str:
    """Return the ``dex_events.type`` for an LP topic hash.

    The single derivation rule: the event signature decides the type.
    ``Mint`` is an LP add (``add_liquidity``) and ``Burn`` is an LP remove
    (``remove_liquidity``), for both V2 pairs and V3 pools. Nothing else
    is a valid LP topic, so an unknown topic yields the empty string and
    the caller drops the log rather than guessing.
    """
    entry = LP_TOPIC_TYPES.get(topic.lower())
    return entry[0] if entry else ""


def _decode_lp_v2_amounts(raw: str) -> tuple[str, str]:
    """Decode V2 data = amount0 + amount1 (2 × uint256)."""
    if len(raw) < 128:
        return "0", "0"
    return str(int(raw[0:64], 16)), str(int(raw[64:128], 16))


def _decode_lp_v3_amounts(raw: str) -> tuple[str, str]:
    """Decode V3 data tail: tickLower, tickUpper, amount, amount0, amount1, fee.

    Returns (amount0_magnitude, amount1_magnitude). Both are int256 on
    the V3 pool, so the magnitude is taken (see module docstring).
    """
    if len(raw) < 384:
        return "0", "0"
    amount0 = str(_decode_int256_magnitude(raw[192:256]))
    amount1 = str(_decode_int256_magnitude(raw[256:320]))
    return amount0, amount1


def parse_lp_log(log: dict[str, Any], ctx: LogDecodeContext) -> dict[str, Any] | None:
    """Parse one LP Mint/Burn log into a ``dex_events``-shaped dict.

    Returns None for anything that is not a well-formed LP log in range —
    the caller must not infer "no LP activity" from that, only from an
    empty result for a complete fetch.
    """
    event_type = lp_event_type(ctx.topic)
    if not event_type:
        return None

    topics = log.get("topics", [])
    expected = _LP_INDEXED_PARAMS[ctx.topic.lower()]
    if len(topics) < 1 + expected:
        return None

    block_num = parse_block_number(log)
    if not ctx.in_range(block_num):
        return None

    _, version, dex = LP_TOPIC_TYPES[ctx.topic.lower()]
    raw = str(log.get("data", "0x")).removeprefix("0x")
    amount0, amount1 = _decode_lp_v3_amounts(raw) if version == "v3" else _decode_lp_v2_amounts(raw)

    # V2: topics[1] is the LP (sender) and topics[2] the burn destination.
    # V3: topics[1] is the owner, topics[2] the sender that deposited.
    first = unpad_address(topics[1])
    second = unpad_address(topics[2]) if expected > 1 else _ZERO_ADDRESS
    sender, recipient = (first, second) if version == "v2" else (first, first)

    pair = str(log.get("address", ""))
    token0, token1 = ctx.pair_tokens.get(pair.lower(), ("", ""))

    return {
        "type": event_type,
        "dex_type": version,
        "dex": dex,
        "chain": ctx.chain,
        "blockNumber": block_num,
        "blockTimestamp": "",
        "transactionHash": log.get("transactionHash", ""),
        "logIndex": parse_log_index(log),
        "sender": sender,
        "recipient": recipient,
        "token0": token0,
        "token1": token1,
        # Liquidity legs are inflows/outflows of the same pair, so they
        # populate amount0/amount1 — the columns a Mint/Burn has data
        # for. amountN_in/out stay "0": a mint has no "out" leg, and
        # filling them would make the swap-shaped volume math read a
        # burn as a trade in both directions.
        "amount0In": "0",
        "amount1In": "0",
        "amount0Out": "0",
        "amount1Out": "0",
        "amount0": amount0,
        "amount1": amount1,
        "pairAddress": pair,
        "price_usd": 0.0,
        "volume_usd": 0.0,
        "source": "lite_indexer",
    }
