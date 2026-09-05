"""
Solana token-launch detection — pump.fun / Bonk.fun / AMM pool creations.

Detects meme-coin launch lifecycle events from the SAME ``getTransaction``
(jsonParsed) payloads the Solana strategy already fetches for SPL balance
changes — zero extra RPC cost. Mirrors ``lite_indexer.strategies.tron.launch``
but for the Solana instruction model (program ID + discriminator / config
account, instead of event logs).

Venues and their launch signals:

  =================  =====================================================
  venue              signal
  =================  =====================================================
  ``pumpfun``        pump.fun program + 8-byte create discriminator
                     (exact: ``sha256("global:create")[:8]``)
  ``bonkfun``        LaunchLab program + LetsBonk platform config account
                     present in the instruction (initialize_v2 = Bonk.fun)
  ``launchlab``      LaunchLab program + parsed ``initialize_v2`` name
                     (no Bonk config account)
  ``pumpswap``       PumpSwap program presence — ``create_pool`` is the
                     bonding-curve graduation signal ([REVIEW] the
                     create_pool discriminator is not in the spec, so this
                     matches every PumpSwap instruction, swaps included —
                     confidence ``low``)
  ``raydium_amm_v4`` Raydium AMM v4 + parsed ``initialize2`` name
  ``raydium_cpmm``   Raydium CPMM + parsed ``initialize``/``createPool``
  ``raydium_clmm``   Raydium CLMM + parsed ``initialize`` name
  =================  =====================================================

[REVIEW] The stock Solana RPC parses only a fixed program set (system,
vote, stake, token, AToken, memo, token-2022, name-service, secp, LUT).
pump.fun, PumpSwap, Raydium and LaunchLab instructions therefore arrive as
raw base58 ``data``, and the venues above that rely on parsed instruction
names (launchlab, raydium_*) will not match until their exact 8-byte
discriminators are added to :data:`VENUES` — the spec provided only the
pump.fun create discriminator. ``pumpfun`` (exact discriminator) and
``bonkfun`` (exact config account) work out of the box.

One event per transaction:

  * ``launch_signal``     — True when the tx created the token (launchpad
    ``create`` / ``initialize_v2``).
  * ``graduation_signal`` — True when the tx moved the token onto an AMM
    (PumpSwap ``create_pool``, Raydium pool creation). A single tx can set
    both (the pump.fun migration tx carries create + create_pool).

The new token mint is ``meta.postTokenBalances[0].mint`` (spec) and the
creator is the transaction fee payer (``accountKeys[0]`` — same convention
as the strategy's deployer capture).

Bonding-curve layout (spec): the pump.fun curve account is 49 bytes —
8-byte discriminator ``0x17b7f83760d8ac60``, five little-endian u64s
(virtualTokenReserves, virtualSolReserves, realTokenReserves,
realSolReserves, tokenTotalSupply), then the ``complete`` flag byte.
Price = virtualSolReserves / virtualTokenReserves (constant product over
VIRTUAL reserves). [REVIEW] The spec also names a 32-byte creator at
offset 49–80, which requires an 81-byte account; the real pump.fun
``BondingCurve`` struct is 49 bytes with no creator field, so
:func:`decode_bonding_curve` decodes the 49-byte layout and only extracts
the creator when the account is ≥81 bytes.

The PDA is ``find_program_address(["bonding-curve", mint], pump_fun_program)``
(see :func:`bonding_curve_pda`, requires the optional ``solders`` package).

Integration point: in ``SolanaStrategy._fetch_and_parse`` (strategy.py),
after ``parse_solana_transfer_events``, call ``detect_launch(tx_data)`` and
buffer hits (e.g. ``self._last_launch_events``, mirroring
``_last_deployer_events``); then flush with ``await write_launch(evt,
ch_writer, nats_publisher)`` from the indexer's sync cycle where both the
ClickHouse writer and the NATS publisher are available.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# ── Programs / Accounts (mainnet, exact — from task spec) ─────────────

# pump.fun launchpad program.
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
# pump.fun `create` instruction discriminator = sha256("global:create")[:8]
# (big-endian), i.e. bytes 0x18 0x1e 0xc8 0x28 0x05 0x1c 0x07 0x77.
PUMP_FUN_CREATE_DISCRIMINATOR = bytes([24, 30, 200, 40, 5, 28, 7, 119])

# PumpSwap AMM — `create_pool` is the bonding-curve graduation.
PUMP_SWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

# Raydium AMM v4 (`initialize2`), CPMM (`initialize`/`createPool`), CLMM.
RAYDIUM_AMM_V4_PROGRAM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CPMM_PROGRAM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
RAYDIUM_CLMM_PROGRAM = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"

# LaunchLab launchpad (`initialize_v2`).
LAUNCHLAB_PROGRAM = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
# LetsBonk platform config account — when present on a LaunchLab
# initialize_v2, the launch is a Bonk.fun launch.
LETSBONK_PLATFORM_CONFIG = "FfYek5vEz23cMkWsdJwG2oa6EphsvXSHrGpdALN4g6W1"

# ── Bonding curve ─────────────────────────────────────────────────────

# pump.fun bonding-curve account discriminator (8-byte, big-endian).
BONDING_CURVE_DISCRIMINATOR = bytes.fromhex("17b7f83760d8ac60")
# PDA seed prefix: find_program_address(["bonding-curve", mint], pump.fun).
BONDING_CURVE_SEED = b"bonding-curve"

# ── Persistence / Streaming ───────────────────────────────────────────

# ClickHouse table for Solana launch events. Override with
# CH_SOLANA_LAUNCHES_TABLE for operators who prefer a different database.
SOLANA_LAUNCHES_TABLE: str = os.environ.get("CH_SOLANA_LAUNCHES_TABLE", "wallet_memory.solana_launches")

# NATS routing for the launch stream (subject is chain.solana.launch).
# Mirrors lite_indexer.strategies.tron.launch: event type "launch" maps to
# the shared "launch": "chain.{chain}.launch" subject template.
NATS_EVENT_TYPE = "launch"
NATS_SUBJECT = "chain.solana.launch"

# One-time warning guard for an unmapped NATS subject.
_nats_subject_warned = False


# ── Event Models ──────────────────────────────────────────────────────


class LaunchEvent(BaseModel):
    """A Solana token-launch or graduation event.

    One event per transaction. ``launch_signal`` is True when the tx
    created the token (pump.fun create / Bonk.fun / LaunchLab
    initialize_v2); ``graduation_signal`` is True when the tx moved the
    token onto an AMM (PumpSwap create_pool, Raydium pool creation).
    A single tx (e.g. the pump.fun migration tx) can set both.

    ``mint`` is the new token (``meta.postTokenBalances[0].mint``) and
    ``creator`` the transaction fee payer (``accountKeys[0]``).
    """

    type: str = "solana_launch"
    chain: str = "solana"
    signature: str = ""
    slot: int = 0
    block_timestamp: str = ""  # ISO-8601 UTC
    mint: str = ""
    creator: str = ""
    venue: str = ""
    launch_signal: bool = False
    graduation_signal: bool = False
    confidence: str = "low"  # high (exact) | medium (name match) | low (program presence)
    instruction_name: str = ""
    source: str = "lite_indexer"
    ingested_at: str = ""


class CurveState(BaseModel):
    """Decoded pump.fun bonding-curve account state (49-byte layout).

    All reserve/supply values are raw token/SOL integer units; convert
    with the token decimals for display. ``price_sol`` is the constant-
    product price over the VIRTUAL reserves (SOL per token).
    """

    virtual_token_reserves: int = 0
    virtual_sol_reserves: int = 0
    real_token_reserves: int = 0
    real_sol_reserves: int = 0
    token_total_supply: int = 0
    complete: bool = False
    creator: str = ""  # base58 pubkey (only when the account is ≥81 bytes)
    price_sol: float = 0.0


@dataclass(frozen=True)
class VenueSpec:
    """Matching rules for one launch venue.

    Attributes:
        venue: Venue key stored in ``LaunchEvent.venue``.
        program: Program ID that owns the launch instruction.
        launch_instruction_names: Parsed instruction names treated as a
            launch signal (only fires for RPC-parsed programs).
        launch_discriminators: Exact 8-byte discriminators treated as a
            launch signal (raw instruction data prefix).
        requires_config_account: When set, program presence plus this
            account in the instruction (or anywhere in the tx account
            keys) is an exact launch match (LetsBonk / Bonk.fun).
        graduation: When True, program presence alone is a graduation
            signal (PumpSwap create_pool — see module docstring REVIEW).
    """

    venue: str
    program: str
    launch_instruction_names: frozenset[str] = frozenset()
    launch_discriminators: tuple[bytes, ...] = ()
    requires_config_account: str | None = None
    graduation: bool = False


# Venue priority order — when a tx matches several venues, the earliest
# entry wins as the primary venue (launchpad creates first, then
# bonding-curve graduation, then AMM listings).
VENUES: tuple[VenueSpec, ...] = (
    VenueSpec("pumpfun", PUMP_FUN_PROGRAM, launch_discriminators=(PUMP_FUN_CREATE_DISCRIMINATOR,)),
    VenueSpec(
        "bonkfun",
        LAUNCHLAB_PROGRAM,
        launch_instruction_names=frozenset({"initialize_v2"}),
        requires_config_account=LETSBONK_PLATFORM_CONFIG,
    ),
    VenueSpec("launchlab", LAUNCHLAB_PROGRAM, launch_instruction_names=frozenset({"initialize_v2"})),
    VenueSpec("pumpswap", PUMP_SWAP_PROGRAM, graduation=True),
    VenueSpec("raydium_amm_v4", RAYDIUM_AMM_V4_PROGRAM, launch_instruction_names=frozenset({"initialize2"})),
    VenueSpec("raydium_cpmm", RAYDIUM_CPMM_PROGRAM, launch_instruction_names=frozenset({"initialize", "createPool"})),
    VenueSpec("raydium_clmm", RAYDIUM_CLMM_PROGRAM, launch_instruction_names=frozenset({"initialize"})),
)

_VENUE_PRIORITY: dict[str, int] = {spec.venue: i for i, spec in enumerate(VENUES)}


@dataclass(frozen=True)
class _Match:
    """An instruction matched one venue."""

    venue: str
    launch_signal: bool
    graduation_signal: bool
    confidence: str
    instruction_name: str = ""


# ── Base58 (self-contained — the RPC returns raw data base58-encoded) ──

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58decode(value: str) -> bytes:
    """Decode a base58 string (Solana addresses / raw instruction data)."""
    if not value:
        return b""
    n = 0
    for ch in value:
        idx = _B58_ALPHABET.find(ch)
        if idx < 0:
            raise ValueError(f"invalid base58 character: {ch!r}")
        n = n * 58 + idx
    pad = len(value) - len(value.lstrip("1"))
    body = n.to_bytes((n.bit_length() + 7) // 8 or 1, "big")
    return b"\x00" * pad + body


def _b58encode(data: bytes) -> str:
    """Encode bytes as base58 (bonding-curve creator pubkey)."""
    if not data:
        return ""
    pad = len(data) - len(data.lstrip(b"\x00"))
    n = int.from_bytes(data, "big")
    chars: list[str] = []
    while n:
        n, rem = divmod(n, 58)
        chars.append(_B58_ALPHABET[rem])
    return "1" * pad + "".join(reversed(chars))


# ── Transaction shape helpers ─────────────────────────────────────────


def _iter_instructions(message: dict[str, Any], meta: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield outer and CPI inner instructions from a jsonParsed tx.

    Both shapes carry ``programId`` / ``accounts`` and either ``parsed``
    (known programs) or raw base58 ``data`` (unknown programs).
    """
    for ix in message.get("instructions") or []:
        if isinstance(ix, dict):
            yield ix
    for group in meta.get("innerInstructions") or []:
        if not isinstance(group, dict):
            continue
        for ix in group.get("instructions") or []:
            if isinstance(ix, dict):
                yield ix


def _parsed_instruction_name(ix: dict[str, Any]) -> str:
    """Return the parsed instruction name (``type`` or ``name``), or ''."""
    parsed = ix.get("parsed")
    if isinstance(parsed, dict):
        name = parsed.get("type") or parsed.get("name")
        if name:
            return str(name)
    return ""


def _pubkey_set(account_keys: list[Any]) -> set[str]:
    """Resolve a list of account keys (dict or plain string) to pubkeys."""
    result: set[str] = set()
    for key in account_keys:
        if isinstance(key, dict):
            pubkey = key.get("pubkey")
            if pubkey:
                result.add(str(pubkey))
        elif key:
            result.add(str(key))
    return result


def _account_pubkey(account_keys: list[Any], index: int) -> str:
    """Resolve the pubkey at ``index`` in the transaction account keys."""
    try:
        key = account_keys[index]
    except (IndexError, TypeError):
        return ""
    if isinstance(key, dict):
        pubkey = key.get("pubkey")
        return str(pubkey) if pubkey is not None else ""
    return str(key) if key else ""


def _iso_timestamp(value: Any) -> str:
    """Format a Unix timestamp as ISO-8601 UTC, or '' when invalid."""
    try:
        if isinstance(value, (int, float)) and value > 0:
            return datetime.fromtimestamp(int(value), tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError, OverflowError) as exc:
        logger.debug("solana_launch_ts_parse_failed", error=str(exc)[:60])
    return ""


def _match_instruction(ix: dict[str, Any], venue: VenueSpec, message_pubkeys: set[str]) -> _Match | None:
    """Match one instruction against one venue spec (see module docstring)."""
    if str(ix.get("programId") or "") != venue.program:
        return None
    accounts = ix.get("accounts") or []
    if venue.requires_config_account is not None:
        config = venue.requires_config_account
        if config not in _pubkey_set(accounts) and config not in message_pubkeys:
            return None
        return _Match(venue.venue, True, False, "high", _parsed_instruction_name(ix))
    name = _parsed_instruction_name(ix)
    if name and name in venue.launch_instruction_names:
        return _Match(venue.venue, not venue.graduation, venue.graduation, "medium", name)
    data = str(ix.get("data") or "")
    if data and venue.launch_discriminators:
        try:
            raw = _b58decode(data)
        except ValueError:
            raw = b""
        if len(raw) >= 8 and raw[:8] in venue.launch_discriminators:
            return _Match(venue.venue, not venue.graduation, venue.graduation, "high", name)
    if venue.graduation:
        return _Match(venue.venue, False, True, "low", name)
    return None


# ── Pure Detectors ────────────────────────────────────────────────────


def detect_launch(tx: dict[str, Any]) -> LaunchEvent | None:
    """Detect a Solana token-launch / graduation event in a transaction.

    Pure function — no I/O. ``tx`` is a ``getTransaction`` result
    (jsonParsed encoding) with the shape the strategy already parses:
    ``transaction.message.accountKeys/instructions``,
    ``meta.postTokenBalances``, ``meta.innerInstructions``, ``slot`` and
    ``blockTime``.

    Matching is exact where the spec gives exact constants (pump.fun
    create discriminator, LetsBonk config account on LaunchLab) and
    name/program-presence based elsewhere — see the module docstring for
    the per-venue confidence model.

    Args:
        tx: Parsed Solana transaction result dict.

    Returns:
        :class:`LaunchEvent` with the new mint, creator, venue and
        launch/graduation signals, or None when the tx contains no
        launch signal (or no post-token-balance mint to attribute).
    """
    transaction = tx.get("transaction")
    if not isinstance(transaction, dict):
        return None
    message = transaction.get("message")
    if not isinstance(message, dict):
        return None
    meta = tx.get("meta")
    if not isinstance(meta, dict):
        return None

    # New mint (spec): meta.postTokenBalances[0].mint.
    post_balances = meta.get("postTokenBalances")
    if not isinstance(post_balances, list) or not post_balances:
        logger.debug("solana_launch_skip_no_post_token_balances")
        return None
    first = post_balances[0]
    if not isinstance(first, dict):
        return None
    mint = str(first.get("mint") or "")
    if not mint:
        return None

    account_keys = message.get("accountKeys")
    account_keys = account_keys if isinstance(account_keys, list) else []
    message_pubkeys = _pubkey_set(account_keys)

    matches: list[_Match] = []
    for ix in _iter_instructions(message, meta):
        for venue in VENUES:
            match = _match_instruction(ix, venue, message_pubkeys)
            if match is not None:
                matches.append(match)
                break

    if not matches:
        return None

    matches.sort(key=lambda m: _VENUE_PRIORITY.get(m.venue, len(VENUES)))
    launch_matches = [m for m in matches if m.launch_signal]
    graduation_matches = [m for m in matches if m.graduation_signal]

    signatures = transaction.get("signatures")
    signature = str(signatures[0]) if isinstance(signatures, list) and signatures else ""
    try:
        slot = int(tx.get("slot") or 0)
    except (ValueError, TypeError):
        slot = 0

    common: dict[str, Any] = {
        "signature": signature,
        "slot": slot,
        "block_timestamp": _iso_timestamp(tx.get("blockTime")),
        "mint": mint,
        "creator": _account_pubkey(account_keys, 0),
        "ingested_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    if launch_matches:
        primary = launch_matches[0]
        return LaunchEvent(
            **common,
            venue=primary.venue,
            launch_signal=True,
            graduation_signal=bool(graduation_matches),
            confidence=primary.confidence,
            instruction_name=primary.instruction_name,
        )

    if graduation_matches:
        primary = graduation_matches[0]
        return LaunchEvent(
            **common,
            venue=primary.venue,
            launch_signal=False,
            graduation_signal=True,
            confidence=primary.confidence,
            instruction_name=primary.instruction_name,
        )

    return None


def decode_bonding_curve(data: bytes) -> CurveState | None:
    """Decode a pump.fun bonding-curve account (49-byte layout).

    Layout (spec): 8-byte discriminator ``0x17b7f83760d8ac60``, five
    little-endian u64s — virtualTokenReserves, virtualSolReserves,
    realTokenReserves, realSolReserves, tokenTotalSupply — then the
    ``complete`` flag byte. Price = virtualSolReserves /
    virtualTokenReserves (constant product over VIRTUAL reserves).

    [REVIEW] The spec names a 32-byte creator at offset 49–80 (an
    81-byte account). The real pump.fun ``BondingCurve`` struct is 49
    bytes with no creator field; this decoder extracts the creator only
    when the account is ≥81 bytes and leaves it '' for the standard
    49-byte account.

    Args:
        data: Raw account data bytes.

    Returns:
        :class:`CurveState`, or None when the data is not a valid
        bonding-curve account (wrong length or discriminator).
    """
    if len(data) < 49:
        return None
    if data[:8] != BONDING_CURVE_DISCRIMINATOR:
        logger.debug("solana_bonding_curve_bad_discriminator")
        return None
    virtual_token = int.from_bytes(data[8:16], "little")
    virtual_sol = int.from_bytes(data[16:24], "little")
    real_token = int.from_bytes(data[24:32], "little")
    real_sol = int.from_bytes(data[32:40], "little")
    total_supply = int.from_bytes(data[40:48], "little")
    complete = data[48] != 0
    creator = _b58encode(data[49:81]) if len(data) >= 81 else ""
    price_sol = (virtual_sol / virtual_token) if virtual_token else 0.0
    return CurveState(
        virtual_token_reserves=virtual_token,
        virtual_sol_reserves=virtual_sol,
        real_token_reserves=real_token,
        real_sol_reserves=real_sol,
        token_total_supply=total_supply,
        complete=complete,
        creator=creator,
        price_sol=price_sol,
    )


def bonding_curve_pda(mint: str) -> str | None:
    """Derive the pump.fun bonding-curve PDA address for a mint.

    PDA seeds are ``["bonding-curve", mint]`` under the pump.fun program
    (spec). Requires the optional ``solders`` package; returns None when
    it is unavailable so callers can fall back to RPC-side resolution.

    Args:
        mint: Token mint base58 address.

    Returns:
        Bonding-curve PDA base58 address, or None when solders is
        unavailable or the mint address is invalid.
    """
    try:
        from solders.pubkey import Pubkey
    except ImportError:  # pragma: no cover — environment dependent
        return None
    try:
        mint_pubkey = Pubkey.from_string(mint)
        program_id = Pubkey.from_string(PUMP_FUN_PROGRAM)
    except Exception:  # noqa: BLE001 — invalid address
        return None
    for bump in range(255, -1, -1):
        try:
            pda = Pubkey.create_program_address(
                [BONDING_CURVE_SEED, bytes(mint_pubkey), bytes([bump])],
                program_id,
            )
            return str(pda)
        except ValueError:
            continue
    return None


# ── Persistence / Streaming ───────────────────────────────────────────


_SOLANA_LAUNCHES_DDL = """
CREATE TABLE IF NOT EXISTS wallet_memory.solana_launches (
    chain LowCardinality(String) DEFAULT 'solana',
    slot UInt64,
    block_timestamp DateTime64(3),
    signature String,
    mint String CODEC(ZSTD(3)),
    creator String DEFAULT '',
    venue LowCardinality(String) DEFAULT '',
    launch_signal UInt8 DEFAULT 0,
    graduation_signal UInt8 DEFAULT 0,
    confidence LowCardinality(String) DEFAULT 'low',
    instruction_name String DEFAULT '',
    source String DEFAULT 'lite_indexer',
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (mint, slot)
TTL block_timestamp + INTERVAL 365 DAY
"""

_SOLANA_LAUNCH_INSERT_COLUMNS = (
    "chain",
    "slot",
    "block_timestamp",
    "signature",
    "mint",
    "creator",
    "venue",
    "launch_signal",
    "graduation_signal",
    "confidence",
    "instruction_name",
    "source",
    "ingested_at",
)


def ensure_solana_launches_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.solana_launches`` if it does not exist.

    Idempotent and best-effort, mirroring
    ``lite_indexer.traces.solana.ensure_solana_transfers_table``: the
    table is also provisioned by ``migrations/007_solana_launch.sql``;
    this exists so the table appears on first launch write without
    operator DDL.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False on failure.
    """
    try:
        ch_client.execute(_SOLANA_LAUNCHES_DDL)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("solana_launches_table_skip", error=str(exc)[:80])
        else:
            logger.warning("solana_launches_table_failed", error=str(exc)[:100])
        return False


def _parse_block_timestamp(value: Any, fallback: datetime) -> datetime:
    """Parse an ISO string or Unix timestamp into an aware datetime."""
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return fallback
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(int(value), tz=UTC)
        except (ValueError, TypeError, OverflowError):
            return fallback
    return fallback


def _launch_row(launch: LaunchEvent, ts: datetime, now: datetime) -> tuple[Any, ...]:
    """Flatten a LaunchEvent into the ClickHouse insert row tuple."""
    return (
        launch.chain or "solana",
        int(launch.slot or 0),
        ts,
        launch.signature or "",
        launch.mint or "",
        launch.creator or "",
        launch.venue or "",
        int(launch.launch_signal),
        int(launch.graduation_signal),
        launch.confidence or "low",
        launch.instruction_name or "",
        launch.source or "lite_indexer",
        now,
    )


async def write_launch(
    launch: LaunchEvent,
    ch_writer: Any,
    nats: Any | None = None,
) -> None:
    """Persist a Solana launch event to ClickHouse and publish to NATS.

    Dependency-injected sinks keep the detectors pure:
      - ``ch_writer`` — a
        :class:`lite_indexer.storage.clickhouse.ClickHouseWriter` (or a
        duck-typed equivalent exposing ``client()``). The raw client is
        used for the INSERT because the writer has no Solana-specific
        method; this is the documented purpose of ``ClickHouseWriter.client()``.
      - ``nats`` — a
        :class:`lite_indexer.streaming.nats_publisher.NATSPublisher`
        (optional). Publishes to ``chain.solana.launch`` (event type
        ``launch``).

    Best-effort by design: ClickHouse or NATS failures are logged and
    swallowed — NATS is a fan-out layer, ClickHouse the durable store.
    The row's ``block_timestamp`` falls back to now when the event
    carried no timestamp.

    [REVIEW] The NATS subject ``chain.solana.launch`` requires a
    ``"launch": "chain.{chain}.launch"`` entry in
    ``NATSPublisher.SUBJECTS`` — not added here because
    ``nats_publisher.py`` is out of scope (same convention as
    ``lite_indexer.strategies.tron.launch``). Until the integration
    point wires that mapping, the publish is skipped with a one-time
    warning.

    Args:
        launch: Detected launch event.
        ch_writer: ClickHouse writer (or raw client duck-type).
        nats: Optional NATS publisher.
    """
    client = ch_writer.client() if hasattr(ch_writer, "client") else ch_writer

    if client is not None:
        ensure_solana_launches_table(client)
        now = datetime.now(UTC)
        ts = _parse_block_timestamp(launch.block_timestamp, now)
        try:
            client.execute(
                f"INSERT INTO {SOLANA_LAUNCHES_TABLE} ({', '.join(_SOLANA_LAUNCH_INSERT_COLUMNS)}) VALUES",
                [_launch_row(launch, ts, now)],
            )
            logger.info(
                "solana_launch_written",
                mint=launch.mint,
                venue=launch.venue,
                signature=launch.signature[:16],
            )
            metrics_incr("clickhouse_inserts")
        except Exception as exc:  # noqa: BLE001 — best-effort persistence
            metrics_incr("clickhouse_errors")
            logger.error(
                "solana_launch_insert_failed",
                mint=launch.mint,
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
                "solana_launch_nats_subject_unmapped",
                subject=NATS_SUBJECT,
                event_type=NATS_EVENT_TYPE,
                hint="add 'launch': 'chain.{chain}.launch' to NATSPublisher.SUBJECTS",
            )
        return

    try:
        await nats.publish_event(launch.chain, NATS_EVENT_TYPE, launch.model_dump())
    except Exception as exc:  # noqa: BLE001 — NATS is best-effort
        logger.warning(
            "solana_launch_publish_failed",
            mint=launch.mint,
            error=str(exc)[:120],
        )


__all__ = [
    "PUMP_FUN_PROGRAM",
    "PUMP_FUN_CREATE_DISCRIMINATOR",
    "PUMP_SWAP_PROGRAM",
    "RAYDIUM_AMM_V4_PROGRAM",
    "RAYDIUM_CPMM_PROGRAM",
    "RAYDIUM_CLMM_PROGRAM",
    "LAUNCHLAB_PROGRAM",
    "LETSBONK_PLATFORM_CONFIG",
    "BONDING_CURVE_DISCRIMINATOR",
    "BONDING_CURVE_SEED",
    "SOLANA_LAUNCHES_TABLE",
    "NATS_EVENT_TYPE",
    "NATS_SUBJECT",
    "LaunchEvent",
    "CurveState",
    "VENUES",
    "detect_launch",
    "decode_bonding_curve",
    "bonding_curve_pda",
    "ensure_solana_launches_table",
    "write_launch",
]
