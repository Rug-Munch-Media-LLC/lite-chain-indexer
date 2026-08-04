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
    plugin = loader.load_all(chain="ethereum")[0]
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
