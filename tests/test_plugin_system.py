"""Tests for the lite-indexer plugin system.

Covers manifest parsing/validation, event topic hashing, ABI decoding,
plugin loading/discovery, handler dispatch, and the manager CLI's
enable/disable marker behavior. The example plugin under
``examples/plugins/uniswap-v3-positions`` is used as the fixture.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lite_indexer.plugin import (
    BasePlugin,
    PluginContext,
    PluginLoader,
    PluginValidationError,
    decode_event_log,
    event_topic,
    parse_manifest,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_PLUGINS = REPO_ROOT / "examples" / "plugins"
UNISWAP_DIR = EXAMPLES_PLUGINS / "uniswap-v3-positions"

OWNER = "0x1111111111111111111111111111111111111111"
POSITION_MANAGER = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88"
INCREASE_TOPIC = event_topic("IncreaseLiquidity(uint256,address,uint256,uint256)")


def _word_int(value: int) -> str:
    return "0x" + format(value, "064x")


def _word_addr(address: str) -> str:
    return "0x" + "00" * 12 + address[2:].lower()


def _increase_log() -> dict:
    """A synthetic IncreaseLiquidity log (tokenId indexed)."""
    return {
        "address": POSITION_MANAGER.lower(),
        "topics": [INCREASE_TOPIC, _word_int(0x1234)],
        "data": _word_addr(OWNER) + _word_int(1000)[2:] + _word_int(500)[2:],
        "blockNumber": "0xbc99a5",
        "transactionHash": "0xabc",
        "logIndex": "0x7",
        "_chain": "ethereum",
    }


# ── Manifest parsing ───────────────────────────────────────────────────


def test_manifest_parse_valid() -> None:
    raw = {
        "name": "uniswap-v3-positions",
        "version": "1.0.0",
        "author": "RugMunch",
        "description": "Tracks Uniswap V3 liquidity positions",
        "chain": "ethereum",
        "contracts": [{"address": POSITION_MANAGER, "start_block": 12369621}],
        "events": [
            {
                "name": "IncreaseLiquidity",
                "signature": "IncreaseLiquidity(uint256,address,uint256,uint256)",
                "params": ["tokenId", "owner", "amount0", "amount1"],
                "indexed": [0],
                "handler": "on_increase_liquidity",
            }
        ],
        "schema": {"table": "uniswap_v3_positions", "columns": {"token_id": "UInt256"}},
    }
    manifest = parse_manifest(raw)
    assert manifest.name == "uniswap-v3-positions"
    assert manifest.chain == "ethereum"
    assert manifest.contracts[0].address == POSITION_MANAGER.lower()
    assert manifest.contracts[0].start_block == 12369621
    spec = manifest.event("IncreaseLiquidity")
    assert spec is not None
    assert spec.topic == INCREASE_TOPIC
    assert spec.abi_types == ("uint256", "address", "uint256", "uint256")
    assert spec.indexed == frozenset({0})
    assert manifest.schema is not None and manifest.schema.table == "uniswap_v3_positions"


def test_manifest_parse_requires_fields() -> None:
    with pytest.raises(PluginValidationError):
        parse_manifest({})
    with pytest.raises(PluginValidationError):
        parse_manifest({"name": "x", "contracts": [], "events": []})
    with pytest.raises(PluginValidationError):
        parse_manifest(
            {
                "name": "x",
                "contracts": [{"address": "not-hex"}],
                "events": [{"name": "E", "signature": "E()", "handler": "on_e"}],
            }
        )


def test_manifest_rejects_bad_signature() -> None:
    with pytest.raises(PluginValidationError):
        parse_manifest(
            {
                "name": "x",
                "contracts": [{"address": POSITION_MANAGER}],
                "events": [{"name": "E", "signature": "not-a-signature", "handler": "on_e"}],
            }
        )
    with pytest.raises(PluginValidationError):
        parse_manifest(
            {
                "name": "x",
                "contracts": [{"address": POSITION_MANAGER}],
                "events": [{"name": "E", "signature": "E(foo)", "handler": "on_e"}],
            }
        )


def test_manifest_params_length_must_match() -> None:
    with pytest.raises(PluginValidationError):
        parse_manifest(
            {
                "name": "x",
                "contracts": [{"address": POSITION_MANAGER}],
                "events": [
                    {
                        "name": "E",
                        "signature": "E(uint256,address)",
                        "params": ["only_one"],
                        "handler": "on_e",
                    }
                ],
            }
        )


# ── Topic hashing ──────────────────────────────────────────────────────


def test_event_topic_known_value() -> None:
    # keccak256("Transfer(address,address,uint256)") — well-known topic.
    assert event_topic("Transfer(address,address,uint256)") == (
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    )
    assert event_topic("IncreaseLiquidity(uint256,address,uint256,uint256)") == INCREASE_TOPIC


# ── ABI decoding ───────────────────────────────────────────────────────


def test_decode_event_log_indexed_and_data() -> None:
    manifest = parse_manifest(
        {
            "name": "u",
            "contracts": [{"address": POSITION_MANAGER}],
            "events": [
                {
                    "name": "IncreaseLiquidity",
                    "signature": "IncreaseLiquidity(uint256,address,uint256,uint256)",
                    "params": ["tokenId", "owner", "amount0", "amount1"],
                    "indexed": [0],
                    "handler": "h",
                }
            ],
        }
    )
    spec = manifest.event("IncreaseLiquidity")
    assert spec is not None
    args = decode_event_log(_increase_log(), spec)
    assert args == {
        "tokenId": 0x1234,
        "owner": OWNER.lower(),
        "amount0": 1000,
        "amount1": 500,
    }


def test_decode_event_log_wrong_topic_returns_none() -> None:
    manifest = parse_manifest(
        {
            "name": "u",
            "contracts": [{"address": POSITION_MANAGER}],
            "events": [
                {
                    "name": "IncreaseLiquidity",
                    "signature": "IncreaseLiquidity(uint256,address,uint256,uint256)",
                    "handler": "h",
                }
            ],
        }
    )
    spec = manifest.event("IncreaseLiquidity")
    assert spec is not None
    log = _increase_log()
    log["topics"] = ["0x" + "00" * 32, log["topics"][1]]
    assert decode_event_log(log, spec) is None


# ── Loading / discovery ────────────────────────────────────────────────


def test_loader_loads_example_plugin() -> None:
    loader = PluginLoader(plugins_dir=EXAMPLES_PLUGINS)
    plugins = loader.load_all(chain="ethereum")
    names = [p.name for p in plugins]
    assert "uniswap-v3-positions" in names
    plugin = next(p for p in plugins if p.name == "uniswap-v3-positions")
    assert isinstance(plugin, BasePlugin)
    assert plugin.handler_for("IncreaseLiquidity") is not None


def test_loader_chain_filter() -> None:
    loader = PluginLoader(plugins_dir=EXAMPLES_PLUGINS)
    assert loader.load_all(chain="solana") == []


def test_loader_skips_disabled_marker(tmp_path: Path) -> None:
    # Copy the example plugin and mark it disabled.
    target = tmp_path / "uniswap-v3-positions"
    import shutil

    shutil.copytree(UNISWAP_DIR, target)
    (target / ".disabled").touch()
    loader = PluginLoader(plugins_dir=tmp_path)
    assert loader.discover() == []
    assert loader.load_all() == []


def test_loader_missing_manifest_dir_returns_empty(tmp_path: Path) -> None:
    loader = PluginLoader(plugins_dir=tmp_path / "does-not-exist")
    assert loader.discover() == []
    assert loader.load_all() == []


# ── Dispatch ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_handle_log_dispatches_to_handler(tmp_path: Path) -> None:
    loader = PluginLoader(plugins_dir=EXAMPLES_PLUGINS)
    plugins = loader.load_all(chain="ethereum")
    plugin = next(p for p in plugins if p.name == "uniswap-v3-positions")
    ctx = PluginContext(chain="ethereum", from_block=0, to_block=10**9, data_dir=tmp_path)

    assert await plugin.handle_log(_increase_log(), ctx) is True

    out = tmp_path / "plugin-data" / "uniswap-v3-positions" / "events.jsonl"
    assert out.is_file()
    import json

    row = json.loads(out.read_text(encoding="utf-8"))
    assert row["action"] == "increase"
    assert row["token_id"] == 0x1234
    assert row["owner"] == OWNER.lower()


@pytest.mark.asyncio
async def test_handle_log_unknown_event_returns_false(tmp_path: Path) -> None:
    loader = PluginLoader(plugins_dir=EXAMPLES_PLUGINS)
    plugin = loader.load_all(chain="ethereum")[0]
    ctx = PluginContext(chain="ethereum", from_block=0, to_block=1, data_dir=tmp_path)
    log = _increase_log()
    log["topics"] = ["0x" + "11" * 32]
    assert await plugin.handle_log(log, ctx) is False


# ── Type Mapping ────────────────────────────────────────────────────────


def test_map_ch_type_python_shorthands() -> None:
    from lite_indexer.plugin.base import map_ch_type

    assert map_ch_type("int") == "Int64"
    assert map_ch_type("float") == "Float64"
    assert map_ch_type("str") == "String"
    assert map_ch_type("bool") == "UInt8"
    assert map_ch_type("bytes") == "String"


def test_map_ch_type_clickhouse_types_passthrough() -> None:
    from lite_indexer.plugin.base import map_ch_type

    assert map_ch_type("UInt256") == "UInt256"
    assert map_ch_type("Int64") == "Int64"
    assert map_ch_type("Float64") == "Float64"
    assert map_ch_type("String") == "String"
    assert map_ch_type("DateTime") == "DateTime"
    assert map_ch_type("UInt8") == "UInt8"
    assert map_ch_type("Int32") == "Int32"
    assert map_ch_type("Decimal(19,4)") == "Decimal(19,4)"
    assert map_ch_type("Array(String)") == "Array(String)"
    assert map_ch_type("Nullable(UInt64)") == "Nullable(UInt64)"
    assert map_ch_type("FixedString(66)") == "FixedString(66)"
    assert map_ch_type("LowCardinality(String)") == "LowCardinality(String)"


def test_map_ch_type_unknown_defaults_to_string() -> None:
    from lite_indexer.plugin.base import map_ch_type

    assert map_ch_type("weird_custom_type") == "String"
    assert map_ch_type("") == "String"


# ── PluginContext ───────────────────────────────────────────────────────


def test_plugin_context_with_writer_and_logger() -> None:
    """PluginContext carries ch_writer and logger (docs: Gap 2 fix)."""
    ctx = PluginContext(
        chain="ethereum",
        from_block=0,
        to_block=100,
        data_dir=None,
        ch_writer="fake_writer",  # type: ignore[arg-type]
        logger="fake_logger",  # type: ignore[arg-type]
    )
    assert ctx.chain == "ethereum"
    assert ctx.ch_writer == "fake_writer"
    assert ctx.logger == "fake_logger"


def test_plugin_context_defaults_none() -> None:
    """Backward-compat: default ch_writer/logger are None."""
    ctx = PluginContext(chain="ethereum", from_block=0, to_block=0)
    assert ctx.ch_writer is None
    assert ctx.logger is None


# ── Schema order_by ─────────────────────────────────────────────────────


def test_manifest_schema_order_by() -> None:
    raw = {
        "name": "test-order-by",
        "version": "1.0.0",
        "author": "RugMunch",
        "description": "Tests order_by in schema",
        "chain": "ethereum",
        "contracts": [{"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"}],
        "events": [
            {
                "name": "Transfer",
                "signature": "Transfer(address,address,uint256)",
                "handler": "on_transfer",
            }
        ],
        "schema": {
            "table": "test_table",
            "columns": {"a": "String", "b": "UInt64"},
            "order_by": ["b", "a"],
        },
    }
    manifest = parse_manifest(raw)
    assert manifest.schema is not None
    assert manifest.schema.order_by == ("b", "a")


def test_manifest_schema_no_order_by_defaults_empty() -> None:
    raw = {
        "name": "test-no-order",
        "version": "1.0.0",
        "author": "RugMunch",
        "description": "No order_by",
        "chain": "ethereum",
        "contracts": [{"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"}],
        "events": [
            {
                "name": "Transfer",
                "signature": "Transfer(address,address,uint256)",
                "handler": "on_transfer",
            }
        ],
        "schema": {"table": "test_table", "columns": {"a": "String"}},
    }
    manifest = parse_manifest(raw)
    assert manifest.schema is not None
    assert manifest.schema.order_by == ()


# ── ensure_plugin_tables (Gap 1 fix) ────────────────────────────────────


def test_ensure_plugin_tables_generates_ddl(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Verify _ensure_plugin_tables generates correct DDL for plugin schemas."""
    from unittest.mock import MagicMock

    from lite_indexer.indexer import LiteIndexer
    from lite_indexer.plugin.base import PluginManifest, EventSpec, ContractSpec, PluginSchema

    # Fabricate a minimal plugin-manifest that carries a schema.
    manifest = PluginManifest(
        name="test_plugin",
        version="1.0.0",
        author="x",
        description="x",
        chain="ethereum",
        contracts=(ContractSpec(address="0xaaa", start_block=0),),
        events=(
            EventSpec(
                name="Transfer",
                signature="Transfer(address,address,uint256)",
                handler="on_transfer",
                topic="0xabc",
                abi_types=("address", "address", "uint256"),
            ),
        ),
        schema=PluginSchema(
            table="test_plugin_table",
            columns={"sender": "String", "receiver": "String", "value": "UInt256"},
            order_by=("value",),
        ),
    )

    FakePlugin = type("FakePlugin", (), {"name": "test_plugin", "manifest": manifest})

    # Create LiteIndexer with a known chain.
    idx = LiteIndexer(chain="ethereum")
    idx._plugins = [FakePlugin()]  # type: ignore[list-item]

    mock_client = MagicMock()
    mock_client.execute.return_value = None

    # Patch ch_writer.client() to return our mock.
    idx.ch_writer._client = mock_client  # type: ignore[attr-defined]

    idx._ensure_plugin_tables()

    # Verify CREATE TABLE was issued.
    calls = [call[0][0] for call in mock_client.execute.call_args_list]  # type: ignore[union-attr]
    assert any("CREATE TABLE IF NOT EXISTS test_plugin_table" in c for c in calls), f"Expected DDL in: {calls}"

    # Verify the DDL includes the ordering key.
    ddl = next(c for c in calls if "CREATE TABLE" in c)
    assert "ORDER BY (value)" in ddl
    assert "sender String" in ddl
    assert "value UInt256" in ddl
    assert "_block_number UInt64" in ddl


def test_ensure_plugin_tables_no_schema_is_skipped(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Plugins without a schema block don't generate DDL."""
    from unittest.mock import MagicMock

    from lite_indexer.indexer import LiteIndexer
    from lite_indexer.plugin.base import PluginManifest, EventSpec, ContractSpec

    manifest = PluginManifest(
        name="no_schema_plugin",
        version="1.0.0",
        author="x",
        description="x",
        chain="ethereum",
        contracts=(ContractSpec(address="0xaaa", start_block=0),),
        events=(
            EventSpec(
                name="Transfer",
                signature="Transfer(address,address,uint256)",
                handler="on_transfer",
                topic="0xabc",
                abi_types=("address", "address", "uint256"),
            ),
        ),
        schema=None,
    )

    FakePlugin = type("FakePlugin", (), {"name": "no_schema_plugin", "manifest": manifest})

    idx = LiteIndexer(chain="ethereum")
    idx._plugins = [FakePlugin()]  # type: ignore[list-item]

    mock_client = MagicMock()
    idx.ch_writer._client = mock_client  # type: ignore[attr-defined]

    idx._ensure_plugin_tables()

    # No DDL should have been issued.
    calls = [c[0][0] for c in mock_client.execute.call_args_list]  # type: ignore[union-attr]
    assert not any("CREATE TABLE" in c for c in calls)


def test_ensure_plugin_tables_no_clickhouse_skips_gracefully(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """When ClickHouse is unavailable, _ensure_plugin_tables skips without error."""
    from lite_indexer.indexer import LiteIndexer
    from lite_indexer.plugin.base import PluginManifest, EventSpec, ContractSpec, PluginSchema

    manifest = PluginManifest(
        name="test_plugin",
        version="1.0.0",
        author="x",
        description="x",
        chain="ethereum",
        contracts=(ContractSpec(address="0xaaa", start_block=0),),
        events=(
            EventSpec(
                name="Transfer",
                signature="Transfer(address,address,uint256)",
                handler="on_transfer",
                topic="0xabc",
                abi_types=("address", "address", "uint256"),
            ),
        ),
        schema=PluginSchema(
            table="test_plugin_table",
            columns={"sender": "String"},
        ),
    )

    FakePlugin = type("FakePlugin", (), {"name": "test_plugin", "manifest": manifest})

    idx = LiteIndexer(chain="ethereum")
    idx._plugins = [FakePlugin()]  # type: ignore[list-item]

    # Patch client() to return None (ClickHouse unavailable).
    def _fake_client():
        return None

    monkeypatch.setattr(idx.ch_writer, "client", _fake_client)

    # Should not raise — gracefully skip.
    idx._ensure_plugin_tables()

    # Verify plugin list is intact (not mutated).
    assert len(idx._plugins) == 1  # type: ignore[arg-type]


# ── New example plugins ─────────────────────────────────────────────────


def test_new_plugins_load() -> None:
    """Both new example plugins load via PluginLoader."""
    loader = PluginLoader(plugins_dir=EXAMPLES_PLUGINS)
    plugins = loader.load_all(chain="ethereum")
    names = {p.name for p in plugins}
    assert "erc20-tracker" in names
    assert "nft-transfers" in names

    erc20 = next(p for p in plugins if p.name == "erc20-tracker")
    assert erc20.handler_for("Transfer") is not None
    assert erc20.manifest.schema is not None
    assert erc20.manifest.schema.table == "erc20_transfers"

    nft = next(p for p in plugins if p.name == "nft-transfers")
    assert nft.handler_for("Transfer") is not None
    assert nft.manifest.schema is not None
    assert nft.manifest.schema.table == "nft_transfers"
    assert nft.manifest.schema.order_by == ("token_id",)
