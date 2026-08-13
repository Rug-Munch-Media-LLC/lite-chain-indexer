"""
Tron token-launch detection — SunPump launches + SunSwap pair creation.

SunPump (https://sunpump.meme) is the Tron meme-coin launchpad:
  - The new-launch event ``TokenCreate`` is emitted by the SunPump
    contract ``TTfvyrAz86hbZk5iDpKD78pqLGgi8C7AAw`` (topic0
    ``1ff0a01c…``). The new token address is encoded in the log data as
    ``"41" + data[24:64]`` — the rightmost 20 bytes of the first 32-byte
    word with the 0x41 version byte — and is base58check-encoded before
    storage. The creator and token index are event args.
  - Graduation happens when the same contract is called with
    ``launchToDEX``, seeding the token onto SunSwap.
  - Buy/sell activity uses the 4-byte selectors ``1cc2c911`` (buy),
    ``2f70d762`` (first-buy) and ``d19aa2b9`` (sell) in the tx data.

SunSwap is a Uniswap-V2-style DEX: the factory
``TKWJdrQkqHisa1X8HUdHEfREvTzw4pMAaY`` emits ``PairCreated`` with the
EVM-identical topic ``0x0d3648bd…`` ([REVIEW] — treated as a measured
constant, see :data:`PAIR_CREATED_TOPIC`).

Transport note: Tron has NO WebSocket events — everything is polled.
The strategy polls TronGrid ``/v1/contracts/{address}/events``
(fingerprint pagination) for ``TokenCreate`` / ``PairCreated`` events,
maps each event into an EVM-style receipt (``logs`` list), feeds it
through the pure detectors here, and calls :func:`write_launch` to
persist to ``wallet_memory.tron_launches`` and publish to NATS
``chain.tron.launch``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

from .parsing import _hex_to_base58check, _normalize_tron_address

logger = get_logger(__name__)

# ── Contract Addresses (mainnet) ─────────────────────────────────────

# SunPump launchpad contract (emits TokenCreate / accepts launchToDEX).
SUNPUMP_CONTRACT = "TTfvyrAz86hbZk5iDpKD78pqLGgi8C7AAw"
# SunSwap V2 factory (Uniswap-V2-style PairCreated).
SUNSWAP_FACTORY = "TKWJdrQkqHisa1X8HUdHEfREvTzw4pMAaY"

# ── Event Topics (bare hex; compared lowercased, 0x-optional) ────────

# keccak256("TokenCreate(...)") — new-launch event on SunPump.
TOKEN_CREATE_TOPIC = "1ff0a01c8968e3551472812164f233abb579247de887db8cbb18281c149bee7a"

# [REVIEW] SunSwap V2 PairCreated topic. SunSwap mirrors Uniswap V2's
# ``PairCreated(address,address,address,uint256)`` so the EVM-identical
# topic 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9
# (same value as lite_indexer.config.PAIR_CREATED_TOPIC) is treated as a
# measured constant. Confirm against the deployed factory's ABI once —
# a Tron-specific signature would hash to a different topic0.
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

# ── Buy / Sell Selectors (4-byte, bare hex, in tx data) ──────────────

SELECTOR_BUY = "1cc2c911"
SELECTOR_FIRST_BUY = "2f70d762"
SELECTOR_SELL = "d19aa2b9"


def _selector(signature: str) -> str:
    """Compute the 4-byte function selector for a signature.

    The selector is the first 4 bytes of ``keccak256(signature)``,
    returned as bare lowercase hex (8 chars). Returns "" when no keccak
    implementation is available so callers degrade to "match nothing".

    Args:
        signature: Canonical function signature, e.g. ``launchToDEX(address)``.

    Returns:
        Selector hex string, or "" when pycryptodome is unavailable.
    """
    try:
        from Crypto.Hash import keccak
    except ImportError:  # pragma: no cover - environment dependent
        return ""
    hasher = keccak.new(digest_bits=256)
    hasher.update(signature.encode("ascii"))
    return hasher.hexdigest()[:8]


# [REVIEW] Derived from keccak256("launchToDEX(address)") — the launchToDEX
# selector is NOT part of the spec's measured selectors. The signature is
# assumed to take a single address (the token being seeded onto SunSwap);
# confirm against the deployed SunPump ABI. When pycryptodome is missing
# this is "" and detect_graduation() matches nothing.
LAUNCH_TO_DEX_SELECTOR = _selector("launchToDEX(address)")

# NATS routing for the launch stream (subject is chain.tron.launch).
NATS_EVENT_TYPE = "launch"
NATS_SUBJECT = "chain.tron.launch"

# One-time warning guard for an unmapped NATS subject.
_nats_subject_warned = False


# ── Event Models ─────────────────────────────────────────────────────


class LaunchEvent(BaseModel):
    """A SunPump TokenCreate launch.

    All addresses are base58check (e.g. ``T…``) to match the rest of the
    Tron strategy's normalized output.
    """

    type: str = "sunpump_launch"
    chain: str = "tron"
    contract_address: str = SUNPUMP_CONTRACT
    token_address: str
    creator: str = ""
    token_index: int = 0
    tx_hash: str = ""
    block_number: int = 0
    log_index: int = 0
    block_timestamp: str = ""
    source: str = "lite_indexer"


class PairEvent(BaseModel):
    """A SunSwap V2 PairCreated event (the token's DEX pool)."""

    type: str = "sunswap_pair"
    chain: str = "tron"
    factory: str = SUNSWAP_FACTORY
    pair_address: str
    token0: str = ""
    token1: str = ""
    tx_hash: str = ""
    block_number: int = 0
    log_index: int = 0
    block_timestamp: str = ""
    source: str = "lite_indexer"


# ── Low-level decoders (reuse parsing.py's base58check helpers) ──────


def _normalize_topic(topic: Any) -> str:
    """Normalize a log topic to lowercase bare hex (no 0x prefix)."""
    return str(topic or "").removeprefix("0x").lower()


def _data_hex(log: dict[str, Any]) -> str:
    """Return log data as lowercase bare hex (no 0x prefix)."""
    return str(log.get("data") or "0x").removeprefix("0x").lower()


def _word(data_hex: str, index: int) -> str:
    """Return the i-th 32-byte word of a bare-hex data string."""
    start = index * 64
    return data_hex[start : start + 64]


def _topic_address(topic: Any) -> str | None:
    """Decode an indexed address topic to base58check.

    Indexed addresses are 32-byte padded words whose rightmost 42 hex
    chars are the 0x41 version byte plus the 20-byte address.

    Args:
        topic: Padded topic hex (0x optional).

    Returns:
        Base58check address, or None when the topic is not a 0x41 address.
    """
    raw = str(topic or "").removeprefix("0x")
    if len(raw) < 42:
        return None
    addr_hex = raw[-42:]
    if not addr_hex.startswith("41"):
        return None
    return _hex_to_base58check(addr_hex)


def _data_address(data_hex: str, word: int) -> str | None:
    """Decode a 0x41 address stored in the rightmost 20 bytes of a data word."""
    word_hex = _word(data_hex, word)
    if len(word_hex) < 40:
        return None
    return _hex_to_base58check("41" + word_hex[-40:])


def _hex_int(value: Any) -> int:
    """Parse a block/log index that may be int or hex string."""
    try:
        if isinstance(value, str) and value.startswith("0x"):
            return int(value, 16)
        return int(value)
    except (ValueError, TypeError):
        return 0


def _decode_creator(topics: list[Any], data: str) -> str:
    """Resolve the TokenCreate creator address.

    Prefers the indexed layout (``TokenCreate(address indexed creator,
    address token, uint256 tokenIndex, …)`` → creator in topics[1]) and
    falls back to a non-indexed layout (creator as the second data word).

    Args:
        topics: Raw topics list from the log.
        data: Bare-hex log data.

    Returns:
        Base58check creator address, or "" when undecodable.
    """
    if len(topics) > 1:
        addr = _topic_address(topics[1])
        if addr:
            return addr
    return _data_address(data, 1) or ""


def _decode_token_index(topics: list[Any], data: str) -> int:
    """Resolve the TokenCreate token index (uint256 event arg).

    The token index sits right after the token word in data. When the
    creator was indexed (topics[1]) the index is the second data word,
    otherwise it is the third.

    Args:
        topics: Raw topics list from the log.
        data: Bare-hex log data.

    Returns:
        Token index int (0 when undecodable).
    """
    if len(topics) > 1 and _topic_address(topics[1]):
        index_word = 1
    else:
        index_word = 2
    try:
        return int(_word(data, index_word) or "0", 16)
    except ValueError:
        return 0


# ── Pure Detectors ───────────────────────────────────────────────────


def detect_sunpump_launch(receipt: dict[str, Any]) -> LaunchEvent | None:
    """Match a SunPump ``TokenCreate`` log inside a transaction receipt.

    Pure function — no I/O. The receipt is an EVM-style dict with a
    ``logs`` list; each log carries ``address`` (base58 or hex),
    ``topics`` (hex list), ``data`` (hex) and optional
    ``transactionHash`` / ``blockNumber`` / ``logIndex`` /
    ``blockTimestamp``. The strategy maps TronGrid polled events into
    this shape before calling.

    Args:
        receipt: Transaction receipt dict.

    Returns:
        :class:`LaunchEvent` with the new token (base58check), creator
        and token index, or None when the receipt holds no TokenCreate
        from the SunPump contract.
    """
    for log in receipt.get("logs") or []:
        if not isinstance(log, dict):
            continue
        if _normalize_tron_address(log.get("address")) != SUNPUMP_CONTRACT:
            continue
        topics = log.get("topics") or []
        if not topics or _normalize_topic(topics[0]) != _normalize_topic(TOKEN_CREATE_TOPIC):
            continue
        data = _data_hex(log)
        if len(data) < 64:
            continue
        # Token address lives in the first data word: "41" + data[24:64].
        token = _hex_to_base58check("41" + data[24:64])
        if not token:
            continue
        return LaunchEvent(
            token_address=token,
            creator=_decode_creator(topics, data),
            token_index=_decode_token_index(topics, data),
            tx_hash=str(log.get("transactionHash") or log.get("txID") or ""),
            block_number=_hex_int(log.get("blockNumber")),
            log_index=_hex_int(log.get("logIndex") or log.get("event_index")),
            block_timestamp=str(log.get("blockTimestamp") or log.get("block_timestamp") or ""),
        )
    return None


def detect_sunswap_pair(receipt: dict[str, Any]) -> PairEvent | None:
    """Match a SunSwap V2 ``PairCreated`` log inside a transaction receipt.

    SunSwap is Uniswap-V2-style: token0/token1 are the indexed topics
    [1] and [2], and the pair address is the rightmost 20 bytes of the
    first data word. All three are returned as base58check addresses.

    Args:
        receipt: Transaction receipt dict (EVM-style, see
            :func:`detect_sunpump_launch`).

    Returns:
        :class:`PairEvent`, or None when the receipt holds no
        PairCreated from the SunSwap factory.
    """
    for log in receipt.get("logs") or []:
        if not isinstance(log, dict):
            continue
        if _normalize_tron_address(log.get("address")) != SUNSWAP_FACTORY:
            continue
        topics = log.get("topics") or []
        if len(topics) < 3 or _normalize_topic(topics[0]) != _normalize_topic(PAIR_CREATED_TOPIC):
            continue
        data = _data_hex(log)
        if len(data) < 64:
            continue
        token0 = _topic_address(topics[1]) or ""
        token1 = _topic_address(topics[2]) or ""
        pair = _hex_to_base58check("41" + data[24:64]) or ""
        return PairEvent(
            pair_address=pair,
            token0=token0,
            token1=token1,
            tx_hash=str(log.get("transactionHash") or log.get("txID") or ""),
            block_number=_hex_int(log.get("blockNumber")),
            log_index=_hex_int(log.get("logIndex") or log.get("event_index")),
            block_timestamp=str(log.get("blockTimestamp") or log.get("block_timestamp") or ""),
        )
    return None


def _tx_contracts(tx: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract the contract-call list from a Tron transaction dict.

    Handles the Tron raw transaction shape (``raw_data.contract``) and
    a flattened ``contract`` list for test fixtures.

    Args:
        tx: Tron transaction dict.

    Returns:
        List of contract-call dicts (possibly empty).
    """
    raw_data = tx.get("raw_data")
    if not isinstance(raw_data, dict):
        raw_data = {}
    contracts = raw_data.get("contract") or []
    if not isinstance(contracts, list) and isinstance(tx.get("contract"), list):
        contracts = tx.get("contract") or []
    return [c for c in contracts if isinstance(c, dict)]


def _contract_value(contract: dict[str, Any]) -> dict[str, Any]:
    """Return the ``parameter.value`` dict of a Tron contract call."""
    parameter = contract.get("parameter")
    if not isinstance(parameter, dict):
        parameter = {}
    value = parameter.get("value")
    if not isinstance(value, dict):
        value = {}
    return value


def detect_graduation(tx: dict[str, Any]) -> str | None:
    """Match a SunPump ``launchToDEX`` call and return the seeded token.

    Graduation is the moment a launched token leaves the bonding curve
    and is seeded onto SunSwap: a ``launchToDEX`` call to the SunPump
    contract whose address argument names the token.

    Args:
        tx: Tron transaction dict (``raw_data.contract[].parameter.value``
            with ``to_address`` + ``data``).

    Returns:
        Base58check token address being seeded, or None when the tx does
        not call launchToDEX on the SunPump contract.
    """
    if not LAUNCH_TO_DEX_SELECTOR:
        logger.debug("tron_launch_to_dex_selector_missing")
        return None

    for contract in _tx_contracts(tx):
        value = _contract_value(contract)
        target = _normalize_tron_address(value.get("to_address"))
        if target != SUNPUMP_CONTRACT:
            continue
        data = str(value.get("data") or "").removeprefix("0x").lower()
        if not data.startswith(LAUNCH_TO_DEX_SELECTOR):
            continue
        # Single address arg: the first 32-byte word after the selector.
        arg = data[len(LAUNCH_TO_DEX_SELECTOR) : len(LAUNCH_TO_DEX_SELECTOR) + 64]
        if len(arg) < 40:
            continue
        token = _hex_to_base58check("41" + arg[-40:])
        if token:
            return token
    return None


# ── Persistence / Streaming ──────────────────────────────────────────


async def write_launch(
    launch: LaunchEvent,
    ch_writer: Any,
    nats: Any | None = None,
) -> None:
    """Persist a SunPump launch to ClickHouse and publish to NATS.

    Dependency-injected sinks keep the detectors pure:
      - ``ch_writer`` — a
        :class:`lite_indexer.storage.clickhouse.ClickHouseWriter` (or a
        duck-typed equivalent exposing ``client()``). The raw client is
        used for the INSERT because the writer has no Tron-specific
        method; this is the documented purpose of ``ClickHouseWriter.client()``.
      - ``nats`` — a
        :class:`lite_indexer.streaming.nats_publisher.NATSPublisher`
        (optional). Publishes to ``chain.tron.launch`` (event type
        ``launch``).

    Best-effort by design: ClickHouse or NATS failures are logged and
    swallowed — NATS is a fan-out layer, ClickHouse the durable store.
    The row's ``block_timestamp`` falls back to now when the receipt
    carried no timestamp.

    [REVIEW] The NATS subject ``chain.tron.launch`` requires a
    ``"launch": "chain.{chain}.launch"`` entry in
    ``NATSPublisher.SUBJECTS`` — not added here because
    ``nats_publisher.py`` is out of scope. Until the integration point
    wires that mapping, the publish is skipped with a one-time warning.

    Args:
        launch: Detected launch event.
        ch_writer: ClickHouse writer (or raw client duck-type).
        nats: Optional NATS publisher.
    """
    client = ch_writer.client() if hasattr(ch_writer, "client") else ch_writer

    if client is not None:
        now = datetime.now(UTC)
        ts_str = launch.block_timestamp or ""
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")) if ts_str else now
        except (ValueError, TypeError):
            ts = now

        try:
            client.execute(
                "INSERT INTO wallet_memory.tron_launches "
                "(chain, block_number, block_timestamp, tx_hash, log_index, "
                "event_type, token_address, creator, token_index, "
                "contract_address, source, ingested_at) VALUES",
                [
                    (
                        launch.chain,
                        launch.block_number,
                        ts,
                        launch.tx_hash,
                        launch.log_index,
                        launch.type,
                        launch.token_address,
                        launch.creator,
                        launch.token_index,
                        launch.contract_address,
                        launch.source,
                        now,
                    )
                ],
            )
            logger.info(
                "tron_launch_written",
                token=launch.token_address,
                tx=launch.tx_hash[:16],
                block=launch.block_number,
            )
            metrics_incr("clickhouse_inserts")
        except Exception as exc:  # noqa: BLE001 — best-effort persistence
            metrics_incr("clickhouse_errors")
            logger.error(
                "tron_launch_insert_failed",
                token=launch.token_address,
                error=str(exc)[:150],
            )

    if nats is None or not hasattr(nats, "publish_event"):
        return

    from lite_indexer.streaming.nats_publisher import NATSPublisher

    global _nats_subject_warned
    if NATSPublisher.subject_for(launch.chain, NATS_EVENT_TYPE) is None:
        if not _nats_subject_warned:
            _nats_subject_warned = True
            logger.warning(
                "tron_launch_nats_subject_unmapped",
                subject=NATS_SUBJECT,
                event_type=NATS_EVENT_TYPE,
                hint="add 'launch': 'chain.{chain}.launch' to NATSPublisher.SUBJECTS",
            )
        return

    try:
        await nats.publish_event(launch.chain, NATS_EVENT_TYPE, launch.model_dump())
    except Exception as exc:  # noqa: BLE001 — NATS is best-effort
        logger.warning(
            "tron_launch_publish_failed",
            token=launch.token_address,
            error=str(exc)[:120],
        )


async def write_pair(
    pair: PairEvent,
    ch_writer: Any,
    nats: Any | None = None,
) -> None:
    """Persist a SunSwap PairCreated event to ClickHouse + NATS.

    Mirrors :func:`write_launch`; writes the ``factory``/``pair_address``
    columns. Best-effort by design — ClickHouse/NATS failures are logged
    and swallowed. [REVIEW] The SunSwap PairCreated topic is an assumed
    EVM-identical constant; verify against the deployed factory ABI.
    """
    client = ch_writer.client() if hasattr(ch_writer, "client") else ch_writer
    if client is not None:
        now = datetime.now(UTC)
        ts_str = pair.block_timestamp or ""
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")) if ts_str else now
        except (ValueError, TypeError):
            ts = now
        try:
            client.execute(
                "INSERT INTO wallet_memory.tron_launches "
                "(chain, block_number, block_timestamp, tx_hash, log_index, "
                "event_type, token_address, factory, pair_address, "
                "contract_address, source, ingested_at) VALUES",
                [
                    (
                        pair.chain,
                        pair.block_number,
                        ts,
                        pair.tx_hash,
                        pair.log_index,
                        pair.type,
                        pair.token0,
                        pair.factory,
                        pair.pair_address,
                        pair.factory,
                        pair.source,
                        now,
                    )
                ],
            )
            logger.info(
                "tron_pair_written",
                pair=pair.pair_address,
                tx=pair.tx_hash[:16],
                block=pair.block_number,
            )
            metrics_incr("clickhouse_inserts")
        except Exception as exc:  # noqa: BLE001 — best-effort
            metrics_incr("clickhouse_errors")
            logger.error("tron_pair_insert_failed", pair=pair.pair_address, error=str(exc)[:150])

    if nats is not None and hasattr(nats, "publish_event"):
        try:
            await nats.publish_event(pair.chain, NATS_EVENT_TYPE, pair.model_dump())
        except Exception as exc:  # noqa: BLE001 — NATS is best-effort
            logger.warning("tron_pair_publish_failed", pair=pair.pair_address, error=str(exc)[:120])


__all__ = [
    "SUNPUMP_CONTRACT",
    "SUNSWAP_FACTORY",
    "TOKEN_CREATE_TOPIC",
    "PAIR_CREATED_TOPIC",
    "SELECTOR_BUY",
    "SELECTOR_FIRST_BUY",
    "SELECTOR_SELL",
    "LAUNCH_TO_DEX_SELECTOR",
    "NATS_EVENT_TYPE",
    "NATS_SUBJECT",
    "LaunchEvent",
    "PairEvent",
    "detect_sunpump_launch",
    "detect_sunswap_pair",
    "detect_graduation",
    "write_launch",
    "write_pair",
]
