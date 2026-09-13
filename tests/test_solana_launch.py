"""Unit tests for Solana launch detection (lite_indexer/strategies/solana/launch.py).

Pure-function tests with fixture transaction dicts — no live RPC. Covers
``detect_launch`` (pump.fun / Bonk.fun / LaunchLab / PumpSwap / Raydium
venues, confidence levels, launch+graduation combos) and
``decode_bonding_curve`` (49-byte layout, 81-byte creator variant, bad
discriminator / short data).
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.strategies.solana.launch import (
    BONDING_CURVE_DISCRIMINATOR,
    LAUNCHLAB_PROGRAM,
    LETSBONK_PLATFORM_CONFIG,
    PUMP_FUN_CREATE_DISCRIMINATOR,
    PUMP_FUN_PROGRAM,
    PUMP_SWAP_PROGRAM,
    RAYDIUM_AMM_V4_PROGRAM,
    _b58encode,
    decode_bonding_curve,
    detect_launch,
)

MINT = "So11111111111111111111111111111111111111112"
CREATOR = "11111111111111111111111111111111111111111"
SIGNATURE = "5" * 87
FEE_PAYER = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"


def _outer_ix(
    program_id: str,
    *,
    accounts: list[str] | None = None,
    data: str = "",
    parsed: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ix: dict[str, Any] = {"programId": program_id, "accounts": accounts or []}
    if data:
        ix["data"] = data
    if parsed is not None:
        ix["parsed"] = parsed
    return ix


def _tx(
    *,
    instructions: list[dict[str, Any]] | None = None,
    post_mint: str = MINT,
    post_token_balance: int = 1_000_000,
    inner: list[dict[str, Any]] | None = None,
    slot: int = 1_000,
    block_time: int = 1_700_000_000,
) -> dict[str, Any]:
    return {
        "slot": slot,
        "blockTime": block_time,
        "transaction": {
            "signatures": [SIGNATURE],
            "message": {
                "accountKeys": [FEE_PAYER, CREATOR],
                "instructions": instructions or [],
            },
        },
        "meta": {
            "postTokenBalances": [
                {
                    "mint": post_mint,
                    "owner": "buyer1",
                    "accountIndex": 2,
                    "uiTokenAmount": {"amount": str(post_token_balance)},
                }
            ],
            "innerInstructions": inner or [],
        },
    }


def _pumpfun_create_ix() -> dict[str, Any]:
    """pump.fun ``create`` — raw base58 data whose first 8 bytes are the
    create discriminator (the RPC does not parse unknown programs)."""
    data = _b58encode(PUMP_FUN_CREATE_DISCRIMINATOR + b"\x00" * 16)
    return _outer_ix(PUMP_FUN_PROGRAM, accounts=[MINT], data=data)


class TestDetectLaunchPumpfun:
    def test_pumpfun_create_detected_high_confidence(self):
        evt = detect_launch(_tx(instructions=[_pumpfun_create_ix()]))
        assert evt is not None
        assert evt.venue == "pumpfun"
        assert evt.launch_signal is True
        assert evt.graduation_signal is False
        assert evt.confidence == "high"
        assert evt.mint == MINT
        assert evt.creator == FEE_PAYER  # fee payer = accountKeys[0]
        assert evt.signature == SIGNATURE
        assert evt.block_timestamp == "2023-11-14T22:13:20Z"

    def test_pumpfun_create_not_matched_with_wrong_discriminator(self):
        bad_data = _b58encode(b"\x00" * 8 + b"\xff" * 8)
        evt = detect_launch(_tx(instructions=[_outer_ix(PUMP_FUN_PROGRAM, data=bad_data)]))
        assert evt is None

    def test_pumpfun_swap_ix_alone_is_no_launch(self):
        # A swap instruction on pump.fun with a non-create discriminator.
        swap_data = _b58encode(b"\x02" * 8 + b"\x00" * 8)
        evt = detect_launch(_tx(instructions=[_outer_ix(PUMP_FUN_PROGRAM, data=swap_data)]))
        assert evt is None


class TestDetectLaunchPumpswap:
    def test_pumpswap_presence_is_low_confidence_graduation(self):
        # PumpSwap has no verified create_pool discriminator: program
        # presence alone is a ``low``-confidence graduation signal.
        evt = detect_launch(_tx(instructions=[_outer_ix(PUMP_SWAP_PROGRAM)]))
        assert evt is not None
        assert evt.venue == "pumpswap"
        assert evt.launch_signal is False
        assert evt.graduation_signal is True
        assert evt.confidence == "low"

    def test_create_plus_pumpswap_sets_both_signals(self):
        # The pump.fun migration tx carries create + create_pool.
        evt = detect_launch(_tx(instructions=[_pumpfun_create_ix(), _outer_ix(PUMP_SWAP_PROGRAM)]))
        assert evt is not None
        assert evt.venue == "pumpfun"  # launchpad create wins venue priority
        assert evt.launch_signal is True
        assert evt.graduation_signal is True


class TestDetectLaunchLaunchLab:
    def _launchlab_ix(self, *, with_bonk: bool = False) -> dict[str, Any]:
        accounts = [MINT]
        if with_bonk:
            accounts.append(LETSBONK_PLATFORM_CONFIG)
        return _outer_ix(
            LAUNCHLAB_PROGRAM,
            accounts=accounts,
            parsed={"type": "initialize_v2", "info": {"name": "Token"}},
        )

    def test_bonkfun_config_account_is_high_confidence(self):
        evt = detect_launch(_tx(instructions=[self._launchlab_ix(with_bonk=True)]))
        assert evt is not None
        assert evt.venue == "bonkfun"
        assert evt.launch_signal is True
        assert evt.confidence == "high"

    def test_launchlab_without_bonk_config_is_medium(self):
        evt = detect_launch(_tx(instructions=[self._launchlab_ix(with_bonk=False)]))
        assert evt is not None
        assert evt.venue == "launchlab"
        assert evt.launch_signal is True
        assert evt.confidence == "medium"
        assert evt.instruction_name == "initialize_v2"

    def test_bonk_config_in_message_keys_counts(self):
        # requires_config_account matches account keys anywhere in the tx.
        ix = self._launchlab_ix(with_bonk=False)
        tx = _tx(instructions=[ix])
        tx["transaction"]["message"]["accountKeys"].append(LETSBONK_PLATFORM_CONFIG)
        evt = detect_launch(tx)
        assert evt is not None
        assert evt.venue == "bonkfun"


class TestDetectLaunchRaydium:
    def test_amm_v4_initialize2_name_match(self):
        ix = _outer_ix(
            RAYDIUM_AMM_V4_PROGRAM,
            parsed={"type": "initialize2", "info": {}},
        )
        evt = detect_launch(_tx(instructions=[ix]))
        assert evt is not None
        assert evt.venue == "raydium_amm_v4"
        assert evt.confidence == "medium"
        assert evt.launch_signal is True

    def test_raydium_unknown_name_not_matched(self):
        ix = _outer_ix(RAYDIUM_AMM_V4_PROGRAM, parsed={"type": "swap", "info": {}})
        assert detect_launch(_tx(instructions=[ix])) is None


class TestDetectLaunchEdgeCases:
    def test_no_post_token_balances_returns_none(self):
        tx = _tx(instructions=[_pumpfun_create_ix()])
        tx["meta"]["postTokenBalances"] = []
        assert detect_launch(tx) is None

    def test_missing_transaction_returns_none(self):
        assert detect_launch({"slot": 1}) is None

    def test_missing_meta_returns_none(self):
        tx = _tx(instructions=[_pumpfun_create_ix()])
        del tx["meta"]
        assert detect_launch(tx) is None

    def test_non_dict_instruction_skipped(self):
        tx = _tx(instructions=[_pumpfun_create_ix()])
        tx["transaction"]["message"]["instructions"].append("junk")  # type: ignore[arg-type]
        assert detect_launch(tx) is not None

    def test_empty_tx_no_match(self):
        assert detect_launch(_tx()) is None

    def test_explicit_slot_overrides_missing_payload_slot(self):
        # getBlock per-transaction dicts carry no top-level ``slot``; the
        # caller's block slot must win instead of the sentinel 0.
        tx = _tx(instructions=[_pumpfun_create_ix()])
        tx.pop("slot")
        evt = detect_launch(tx, 441_421_301)
        assert evt is not None
        assert evt.slot == 441_421_301


class TestDecodeBondingCurve:
    def _curve_bytes(self, *, with_creator: bool = False, complete: bool = False) -> bytes:
        data = bytearray(BONDING_CURVE_DISCRIMINATOR)
        # virtualToken, virtualSol, realToken, realSol, totalSupply
        for value in (1_000_000_000_000, 30_000_000_000, 500_000_000_000, 15_000_000_000, 1_000_000_000_000):
            data += int(value).to_bytes(8, "little")
        data += bytes([1 if complete else 0])
        if with_creator:
            data += bytes(range(32))  # 32-byte creator pubkey
        return bytes(data)

    def test_decodes_49_byte_layout(self):
        state = decode_bonding_curve(self._curve_bytes())
        assert state is not None
        assert state.virtual_token_reserves == 1_000_000_000_000
        assert state.virtual_sol_reserves == 30_000_000_000
        assert state.real_token_reserves == 500_000_000_000
        assert state.real_sol_reserves == 15_000_000_000
        assert state.token_total_supply == 1_000_000_000_000
        assert state.complete is False
        assert state.creator == ""  # standard 49-byte account has no creator
        # constant-product price over VIRTUAL reserves.
        assert state.price_sol == pytest.approx(30_000_000_000 / 1_000_000_000_000)

    def test_complete_flag(self):
        assert decode_bonding_curve(self._curve_bytes(complete=True)).complete is True  # type: ignore[union-attr]

    def test_81_byte_creator_extracted(self):
        state = decode_bonding_curve(self._curve_bytes(with_creator=True))
        assert state is not None
        assert state.creator != ""
        # bytes(range(32)) base58-encodes deterministically.
        assert state.creator == _b58encode(bytes(range(32)))

    def test_wrong_discriminator_returns_none(self):
        data = b"\x00" * 8 + b"\x01" * 41
        assert decode_bonding_curve(data) is None

    def test_too_short_returns_none(self):
        assert decode_bonding_curve(b"\x01" * 48) is None

    def test_zero_virtual_token_reserves_price_zero(self):
        data = bytearray(BONDING_CURVE_DISCRIMINATOR)
        data += (0).to_bytes(8, "little")  # virtualToken = 0
        data += (30_000_000_000).to_bytes(8, "little")
        data += (0).to_bytes(8, "little")
        data += (0).to_bytes(8, "little")
        data += (0).to_bytes(8, "little")
        data += b"\x00"
        state = decode_bonding_curve(bytes(data))
        assert state is not None
        assert state.price_sol == 0.0
