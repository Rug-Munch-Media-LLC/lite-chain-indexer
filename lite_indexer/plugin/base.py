"""
Plugin base types — the contract plugin authors extend.

A plugin is a directory containing a ``manifest.yaml`` (see
:func:`parse_manifest` for the schema) and a Python module (default
``plugin.py``) that defines a subclass of :class:`BasePlugin`. The
manifest declares which contracts and events the plugin cares about;
the loader computes the event topic hash from each event signature and
registers the handler method named in the manifest.

Decoding
--------
The loader ships a small, dependency-free ABI decoder
(:func:`decode_event_log`) that understands the common event types:
``uintN``/``intN`` (8..256), ``address``, ``bool``, ``bytes32``,
``string`` and ``bytes``. Indexed parameters are read from ``topics[1:]``
and non-indexed parameters from ``data``. The manifest may mark which
parameter positions are indexed with the optional ``indexed`` list
(default: none indexed).

Example manifest::

    name: uniswap-v3-positions
    version: 1.0.0
    author: RugMunch
    description: Tracks Uniswap V3 liquidity positions
    chain: ethereum
    contracts:
      - address: "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"
        start_block: 12369621
    events:
      - name: IncreaseLiquidity
        signature: "IncreaseLiquidity(uint256,address,uint256,uint256)"
        params: [tokenId, owner, amount0, amount1]
        indexed: [0]
        handler: on_increase_liquidity
    schema:
      table: uniswap_v3_positions
      columns:
        token_id: UInt256
        owner: String
        liquidity: UInt256
        tick_lower: Int32
        tick_upper: Int32
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── ClickHouse Type Mapping ──────────────────────────────────────────

# Map Python shorthand types to ClickHouse column types. All other
# strings (including existing ClickHouse types like ``UInt256``,
# ``Int32``, ``String``, ``DateTime``, etc.) pass through unchanged.
_PYTHON_TO_CH_TYPE: dict[str, str] = {
    "int": "Int64",
    "float": "Float64",
    "str": "String",
    "bool": "UInt8",
    "bytes": "String",
}


def map_ch_type(raw_type: str) -> str:
    """Map a Python shorthand type to a ClickHouse column type.

    Known ClickHouse types (``UInt*``, ``Int*``, ``Float*``, ``String``,
    ``DateTime``, ``Decimal``, ``Bool``, ``Date``, ``Array``, ``FixedString``,
    ``Nullable``, ``Enum``, ``UUID``, ``IPv4``, ``IPv6``) pass through as-is.
    Python shorthands ``int``, ``float``, ``str``, ``bool``, ``bytes``
    are translated to their ClickHouse equivalents.
    """
    t = raw_type.strip()
    # Already a ClickHouse type?
    if any(
        t.startswith(prefix)
        for prefix in (
            "UInt",
            "Int",
            "Float",
            "Decimal",
            "DateTime",
            "Date",
            "String",
            "FixedString",
            "Array",
            "Nullable",
            "Enum",
            "UUID",
            "IPv4",
            "IPv6",
            "Bool",
            "LowCardinality",
            "Map",
            "Tuple",
            "AggregateFunction",
            "SimpleAggregateFunction",
        )
    ):
        return t
    # Python shorthand
    return _PYTHON_TO_CH_TYPE.get(t.lower(), "String")


# ── Manifest Schema ────────────────────────────────────────────────────

_SIGNATURE_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\(([^)]*)\)$")
# u?intN (8..256), address, bool, bytesN (N optional), string.
_ABI_TYPE_RE = re.compile(r"^(u?int[0-9]*|address|bool|bytes[0-9]*|string)$")
_UINT_RE = re.compile(r"^uint(\d+)$")
_INT_RE = re.compile(r"^int(\d+)$")
_BYTES_RE = re.compile(r"^bytes(\d+)$")


class PluginValidationError(ValueError):
    """Raised when a plugin manifest or handler module is invalid."""


@dataclass(frozen=True)
class ContractSpec:
    """A contract the plugin indexes."""

    address: str
    start_block: int = 0


@dataclass(frozen=True)
class EventSpec:
    """A single event the plugin handles."""

    name: str
    signature: str
    handler: str
    topic: str = ""
    abi_types: tuple[str, ...] = ()
    params: tuple[str, ...] = ()
    indexed: frozenset[int] = field(default_factory=frozenset)


@dataclass(frozen=True)
class PluginSchema:
    """Output table schema declared by the plugin.

    Columns map parameter names to ClickHouse column types (or Python
    shorthand types). The indexer auto-creates the table on plugin load.
    """

    table: str
    columns: dict[str, str]
    order_by: tuple[str, ...] = ()


@dataclass(frozen=True)
class PluginManifest:
    """Validated plugin manifest.

    Parsed from ``manifest.yaml`` by :func:`parse_manifest`. The loader
    uses it to build :class:`BasePlugin` instances and register event
    handlers.
    """

    name: str
    version: str
    author: str
    description: str
    chain: str
    contracts: tuple[ContractSpec, ...]
    events: tuple[EventSpec, ...]
    schema: PluginSchema | None
    module: str = "plugin.py"
    enabled: bool = True

    def contract_addresses(self) -> list[str]:
        """Return the 0x contract addresses declared by the plugin."""
        return [c.address for c in self.contracts]

    def event(self, name: str) -> EventSpec | None:
        """Return the event spec with the given name, or None."""
        for spec in self.events:
            if spec.name == name:
                return spec
        return None

    def event_by_topic(self, topic: str) -> EventSpec | None:
        """Return the event spec matching a topic0 hash, or None."""
        for spec in self.events:
            if spec.topic and spec.topic.lower() == topic.lower():
                return spec
        return None


# ── Event Topic Hash ───────────────────────────────────────────────────


def event_topic(signature: str) -> str:
    """Compute the topic0 hash of an event signature.

    ``keccak256(signature)`` — the standard Ethereum event topic. Uses
    ``pycryptodome`` (an optional dependency, shared with the bloom
    filter extra). Raises :class:`PluginValidationError` when the
    keccak implementation is unavailable.
    """
    try:
        from Crypto.Hash import keccak  # nosec B413
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise PluginValidationError(
            "keccak256 is unavailable: install the 'bloom' extra (pycryptodome) to use plugins with event topics"
        ) from exc

    hasher = keccak.new(digest_bits=256)
    hasher.update(signature.encode("ascii"))
    return "0x" + hasher.hexdigest()


# ── ABI Decoding (dependency-free) ─────────────────────────────────────


def _validate_abi_type(abi_type: str) -> bool:
    """Return True when the ABI type string is supported."""
    return bool(_ABI_TYPE_RE.match(abi_type))


def _parse_signature(signature: str) -> tuple[str, list[str]]:
    """Split ``Name(type1,type2)`` into (name, abi_types)."""
    match = _SIGNATURE_RE.match(signature.strip())
    if not match:
        raise PluginValidationError(f"invalid event signature: {signature!r}")
    name, args = match.groups()
    types = [t.strip() for t in args.split(",") if t.strip()]
    for t in types:
        if not _validate_abi_type(t):
            raise PluginValidationError(f"unsupported ABI type {t!r} in signature {signature!r}")
    return name, types


def _word_int(word: str) -> int:
    """Parse a 32-byte hex word into an int."""
    raw = word.removeprefix("0x")
    if not raw:
        return 0
    try:
        return int(raw, 16)
    except ValueError:
        return 0


def _word_address(word: str) -> str:
    """Extract the address (rightmost 20 bytes) from a 32-byte hex word."""
    raw = word.removeprefix("0x")
    if len(raw) > 40:
        raw = raw[-40:]
    return "0x" + raw.lower()


def _decode_indexed_word(word: str, abi_type: str) -> Any:
    """Decode a single indexed parameter from a topic word."""
    if abi_type == "address":
        return _word_address(word)
    if abi_type == "bool":
        return _word_int(word) != 0
    if abi_type == "bytes32":
        return word
    if _BYTES_RE.match(abi_type):
        n = int(_BYTES_RE.match(abi_type).group(1))  # type: ignore[union-attr]
        return "0x" + word.removeprefix("0x")[: n * 2]
    if _UINT_RE.match(abi_type) or _INT_RE.match(abi_type) or abi_type == "string" or abi_type == "bytes":
        return _word_int(word)
    return word


def _decode_data_words(words: list[str], abi_type: str, offset: int) -> tuple[Any, int]:
    """Decode one non-indexed parameter from the data words list.

    Returns ``(value, next_offset)``. Dynamic types (``string``,
    ``bytes``) follow the standard offset→length→payload layout.
    """
    if abi_type == "address":
        return _word_address(words[offset]), offset + 1
    if abi_type == "bool":
        return _word_int(words[offset]) != 0, offset + 1
    if abi_type == "bytes32":
        return words[offset], offset + 1
    if _BYTES_RE.match(abi_type):
        n = int(_BYTES_RE.match(abi_type).group(1))  # type: ignore[union-attr]
        return "0x" + words[offset].removeprefix("0x")[: n * 2], offset + 1
    if _UINT_RE.match(abi_type):
        bits = int(_UINT_RE.match(abi_type).group(1))  # type: ignore[union-attr]
        value = _word_int(words[offset])
        return value & ((1 << bits) - 1), offset + 1
    if _INT_RE.match(abi_type):
        bits = int(_INT_RE.match(abi_type).group(1))  # type: ignore[union-attr]
        value = _word_int(words[offset])
        if value >= 1 << (bits - 1):
            value -= 1 << bits
        return value, offset + 1
    if abi_type in {"string", "bytes"}:
        # Offset word points at the length word; length word is followed
        # by ceil(len/32) words of payload.
        data_offset = _word_int(words[offset])
        if data_offset + 1 >= len(words):
            return "", offset + 1
        length = _word_int(words[data_offset])
        payload = "".join(words[data_offset + 1 : data_offset + 1 + ((length + 31) // 32)])
        raw = payload[: length * 2]
        if abi_type == "string":
            try:
                return bytes.fromhex(raw).decode("utf-8", errors="replace"), offset + 1
            except ValueError:
                return "", offset + 1
        return "0x" + raw if raw else "0x", offset + 1
    # Unknown type — pass through the raw word.
    return words[offset], offset + 1


def decode_event_log(
    log: Any,
    spec: EventSpec,
) -> dict[str, Any] | None:
    """Decode a raw ``eth_getLogs`` entry against an event spec.

    Indexed parameters are read from ``topics[1:]`` (in order), then
    non-indexed parameters from ``data``. Returned dict maps parameter
    names (``params`` list, falling back to ``p0..pN``) to decoded
    values. Returns None when the log is malformed (missing topics /
    data) or its topic0 does not match the spec.
    """
    if not isinstance(log, dict):
        return None
    topics = log.get("topics") or []
    if not topics or topics[0].lower() != spec.topic.lower():
        return None

    indexed_positions = spec.indexed
    data = log.get("data") or "0x"
    data_hex = data.removeprefix("0x")
    if len(data_hex) % 64 != 0:
        # Pad to a whole number of words; some RPCs trim trailing zeroes.
        data_hex = data_hex.ljust(((len(data_hex) + 63) // 64) * 64, "0")
    data_words = [data_hex[i : i + 64] for i in range(0, len(data_hex), 64)] if data_hex else []

    # Indexed params come from topics[1:], non-indexed from data words.
    topic_args = topics[1:]
    topic_cursor = 0
    data_cursor = 0
    decoded: dict[str, Any] = {}
    for idx, abi_type in enumerate(spec.abi_types):
        name = spec.params[idx] if idx < len(spec.params) else f"p{idx}"
        if idx in indexed_positions:
            if topic_cursor >= len(topic_args):
                return None
            decoded[name] = _decode_indexed_word(str(topic_args[topic_cursor]), abi_type)
            topic_cursor += 1
        else:
            if data_cursor >= len(data_words):
                # Missing data word — treat as 0 / empty rather than failing
                # the whole batch (best-effort plugin layer).
                decoded[name] = None
                data_cursor += 1
                continue
            value, next_cursor = _decode_data_words(data_words, abi_type, data_cursor)
            decoded[name] = value
            data_cursor = next_cursor
    return decoded


# ── Manifest Parsing ───────────────────────────────────────────────────


def parse_manifest(data: dict[str, Any]) -> PluginManifest:
    """Validate and parse a manifest dict into a :class:`PluginManifest`.

    Required fields: ``name``, ``version``, ``author``, ``description``,
    ``chain``, ``contracts``, ``events``. The ``schema`` and ``module``
    fields are optional.

    Raises:
        PluginValidationError: on missing/invalid fields.
    """
    if not isinstance(data, dict):
        raise PluginValidationError("manifest must be a mapping")

    name = str(data.get("name", "")).strip()
    if not name:
        raise PluginValidationError("manifest missing required field: name")

    contracts_raw = data.get("contracts")
    if not isinstance(contracts_raw, list) or not contracts_raw:
        raise PluginValidationError("manifest missing required field: contracts (non-empty list)")
    contracts: list[ContractSpec] = []
    for c in contracts_raw:
        if not isinstance(c, dict):
            raise PluginValidationError("manifest contract entries must be mappings")
        address = str(c.get("address", "")).strip()
        if not address or not address.startswith("0x"):
            raise PluginValidationError(f"plugin {name!r}: contract address must be 0x-prefixed")
        try:
            start_block = int(c.get("start_block", 0))
        except (TypeError, ValueError):
            start_block = 0
        contracts.append(ContractSpec(address=address.lower(), start_block=start_block))

    events_raw = data.get("events")
    if not isinstance(events_raw, list) or not events_raw:
        raise PluginValidationError("manifest missing required field: events (non-empty list)")
    events: list[EventSpec] = []
    for ev in events_raw:
        if not isinstance(ev, dict):
            raise PluginValidationError("manifest event entries must be mappings")
        ev_name = str(ev.get("name", "")).strip()
        signature = str(ev.get("signature", "")).strip()
        handler = str(ev.get("handler", "")).strip()
        if not ev_name or not signature or not handler:
            raise PluginValidationError(f"plugin {name!r}: event requires name, signature and handler")
        sig_name, abi_types = _parse_signature(signature)
        if sig_name != ev_name:
            raise PluginValidationError(
                f"plugin {name!r}: event name {ev_name!r} does not match signature {sig_name!r}"
            )
        params_raw = ev.get("params")
        params: tuple[str, ...] = ()
        if params_raw is not None:
            if not isinstance(params_raw, list) or len(params_raw) != len(abi_types):
                raise PluginValidationError(f"plugin {name!r}: event {ev_name!r} params length must match signature")
            params = tuple(str(p).strip() for p in params_raw)
        indexed_raw = ev.get("indexed")
        indexed: frozenset[int] = frozenset()
        if indexed_raw is not None:
            if not isinstance(indexed_raw, list):
                raise PluginValidationError(f"plugin {name!r}: event {ev_name!r} indexed must be a list of positions")
            positions: set[int] = set()
            for pos in indexed_raw:
                try:
                    positions.add(int(pos))
                except (TypeError, ValueError):
                    raise PluginValidationError(
                        f"plugin {name!r}: event {ev_name!r} indexed positions must be ints"
                    ) from None
            if any(p < 0 or p >= len(abi_types) for p in positions):
                raise PluginValidationError(f"plugin {name!r}: event {ev_name!r} indexed position out of range")
            indexed = frozenset(positions)
        events.append(
            EventSpec(
                name=ev_name,
                signature=signature,
                handler=handler,
                topic=event_topic(signature),
                abi_types=tuple(abi_types),
                params=params,
                indexed=indexed,
            )
        )

    schema_raw = data.get("schema")
    schema: PluginSchema | None = None
    if schema_raw is not None:
        if not isinstance(schema_raw, dict):
            raise PluginValidationError(f"plugin {name!r}: schema must be a mapping")
        table = str(schema_raw.get("table", "")).strip()
        columns_raw = schema_raw.get("columns")
        if not table or not isinstance(columns_raw, dict) or not columns_raw:
            raise PluginValidationError(f"plugin {name!r}: schema requires table and non-empty columns")
        columns = {str(k).strip(): str(v).strip() for k, v in columns_raw.items()}
        order_by_raw = schema_raw.get("order_by")
        order_by: tuple[str, ...] = ()
        if order_by_raw is not None:
            if isinstance(order_by_raw, list):
                order_by = tuple(str(o).strip() for o in order_by_raw)
            elif isinstance(order_by_raw, str):
                order_by = tuple(s.strip() for s in order_by_raw.split(","))
        schema = PluginSchema(table=table, columns=columns, order_by=order_by)

    return PluginManifest(
        name=name,
        version=str(data.get("version", "0.0.0")).strip(),
        author=str(data.get("author", "")).strip(),
        description=str(data.get("description", "")).strip(),
        chain=str(data.get("chain", "")).strip(),
        contracts=tuple(contracts),
        events=tuple(events),
        schema=schema,
        module=str(data.get("module", "plugin.py")).strip(),
        enabled=bool(data.get("enabled", True)),
    )


# ── Decoded Event ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class DecodedEvent:
    """A decoded plugin event delivered to a handler."""

    name: str
    chain: str
    block_number: int
    tx_hash: str
    log_index: int
    contract_address: str
    args: dict[str, Any]


@dataclass(frozen=True)
class PluginContext:
    """Context passed to plugin handlers with each event.

    Attributes:
        chain: The chain being indexed.
        from_block / to_block: The block range of the current sync cycle.
        data_dir: Data directory path (for plugin output).
        ch_writer: ClickHouse writer handle — plugins can call
            ``ctx.ch_writer.client()`` for direct DDL or inserts.
        logger: Structured logger scoped for plugin use.
    """

    chain: str
    from_block: int
    to_block: int
    data_dir: Any = None  # Path | None — kept as Any to avoid import cycles
    ch_writer: Any = None  # ClickHouseWriter | None — kept as Any to avoid import cycles
    logger: Any = None  # structlog logger — kept as Any to avoid import cycles


# ── Base Plugin ────────────────────────────────────────────────────────


class BasePlugin:
    """Base class plugin authors extend.

    Subclasses override the handler methods named in the manifest (for
    example ``async def on_increase_liquidity(self, event, ctx)``).
    The loader routes decoded events to the matching handler based on
    the manifest's ``handler`` field.

    Lifecycle hooks (:meth:`on_startup` / :meth:`on_shutdown`) are
    called by the loader when plugins are loaded/unloaded; both default
    to no-ops.
    """

    def __init__(self, manifest: PluginManifest) -> None:
        if not isinstance(manifest, PluginManifest):
            raise PluginValidationError("BasePlugin requires a PluginManifest instance")
        self._manifest = manifest

    # ── Metadata ────────────────────────────────────────────────────

    @property
    def manifest(self) -> PluginManifest:
        """The validated manifest for this plugin."""
        return self._manifest

    @property
    def name(self) -> str:
        """Plugin name."""
        return self._manifest.name

    @property
    def chain(self) -> str:
        """Chain the plugin indexes."""
        return self._manifest.chain

    @property
    def contracts(self) -> list[str]:
        """0x contract addresses the plugin indexes."""
        return self._manifest.contract_addresses()

    # ── Handler Resolution ──────────────────────────────────────────

    def handler_for(self, event_name: str) -> Any:
        """Return the callable handler for an event name, or None."""
        spec = self._manifest.event(event_name)
        if spec is None:
            return None
        handler = getattr(self, spec.handler, None)
        return handler if callable(handler) else None

    # ── Decoding ────────────────────────────────────────────────────

    def decode_log(self, log: Any) -> DecodedEvent | None:
        """Decode a raw eth_getLogs entry into a :class:`DecodedEvent`.

        Matches the log's topic0 against the manifest events, decodes
        the parameters, and attaches block/tx metadata. Returns None
        when the log is not for this plugin or is malformed.
        """
        if not isinstance(log, dict):
            return None
        topics = log.get("topics") or []
        if not topics:
            return None
        topic0 = str(topics[0])
        spec = self._manifest.event_by_topic(topic0)
        if spec is None:
            return None
        args = decode_event_log(log, spec)
        if args is None:
            return None

        block_hex = log.get("blockNumber", "0x0")
        try:
            block_number = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
        except (ValueError, TypeError):
            block_number = 0
        log_index_hex = log.get("logIndex", "0x0")
        try:
            log_index = int(log_index_hex, 16) if isinstance(log_index_hex, str) else int(log_index_hex)
        except (ValueError, TypeError):
            log_index = 0

        return DecodedEvent(
            name=spec.name,
            chain=str(log.get("_chain", "")),
            block_number=block_number,
            tx_hash=str(log.get("transactionHash", "")),
            log_index=log_index,
            contract_address=str(log.get("address", "")),
            args=args,
        )

    async def handle_log(self, log: dict[str, Any], ctx: PluginContext) -> bool:
        """Decode a raw log and dispatch it to the manifest handler.

        Returns True when the log was decoded and dispatched to a
        handler, False when the log is not for this plugin or the
        handler is missing.
        """
        decoded = self.decode_log(log)
        if decoded is None:
            return False
        handler = self.handler_for(decoded.name)
        if handler is None:
            logger.warning(
                "plugin_handler_missing",
                plugin=self.name,
                event_name=decoded.name,
            )
            return False
        result = handler(decoded, ctx)
        if hasattr(result, "__await__"):
            await result
        return True

    # ── Lifecycle ───────────────────────────────────────────────────

    async def on_startup(self) -> None:
        """Called once when the plugin is loaded (default no-op)."""

    async def on_shutdown(self) -> None:
        """Called when the plugin is unloaded (default no-op)."""

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.name}@{self.chain} v{self._manifest.version}>"


__all__ = [
    "BasePlugin",
    "ContractSpec",
    "DecodedEvent",
    "EventSpec",
    "PluginContext",
    "PluginManifest",
    "PluginSchema",
    "PluginValidationError",
    "decode_event_log",
    "event_topic",
    "map_ch_type",
    "parse_manifest",
]
