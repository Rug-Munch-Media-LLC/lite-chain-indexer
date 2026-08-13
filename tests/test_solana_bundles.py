"""Unit tests for Jito bundle detection (lite_indexer/strategies/solana/bundles.py).

Pure-function tests with fixture transaction dicts — no live RPC. Covers
``find_tip_transfer`` (outer + CPI, thresholds, wrong destinations) and
``detect_bundle`` (create → fresh buys → last-tx tip), plus the negative
classification paths.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.strategies.solana.bundles import (
    MIN_TIP_LAMPORTS,
    TIP_ACCOUNTS,
    TOKEN_2022_PROGRAM,
    TOKEN_PROGRAM,
    detect_bundle,
    find_tip_transfer,
)

MINT = "So11111111111111111111111111111111111111112"
TIP_ACCOUNT = next(iter(TIP_ACCOUNTS))
SYSTEM_PROGRAM = "11111111111111111111111111111111"

CREATE_SIG = "C" * 87
BUY_SIG_1 = "B1" * 44  # 88 chars, fine
BUY_SIG_2 = "B2" * 44
TIP_SIG = "T" * 87


def _system_transfer_ix(
    destination: str,
    lamports: int,
    *,
    seed: str | None = None,
) -> dict[str, Any]:
    ptype = "transferWithSeed" if seed else "transfer"
    info: dict[str, Any] = {"destination": destination, "lamports": lamports}
    if seed:
        info["source"] = "seedSource"
        info["seed"] = seed
        info["sourceOwner"] = "seedOwner"
    return {
        "programId": SYSTEM_PROGRAM,
        "parsed": {"type": ptype, "info": info},
    }


def _create_tx(sig: str = CREATE_SIG, *, token2022: bool = False) -> dict[str, Any]:
    program = TOKEN_2022_PROGRAM if token2022 else TOKEN_PROGRAM
    return {
        "slot": 5_000,
        "transaction": {
            "signatures": [sig],
            "message": {
                "accountKeys": ["11111111111111111111111111111111"],
                "instructions": [
                    {
                        "programId": program,
                        "parsed": {"type": "createMint", "info": {"mint": MINT, "decimals": 9}},
                    }
                ],
            },
        },
        "meta": {},
    }


def _buy_tx(
    sig: str,
    owner: str,
    *,
    post_amount: int = 1_000,
    pre_amount: int = 0,
    account_index: int = 3,
) -> dict[str, Any]:
    meta: dict[str, Any] = {}
    if pre_amount > 0:
        meta["preTokenBalances"] = [
            {
                "mint": MINT,
                "owner": owner,
                "accountIndex": account_index,
                "uiTokenAmount": {"amount": str(pre_amount)},
            }
        ]
    else:
        meta["preTokenBalances"] = []
    meta["postTokenBalances"] = [
        {
            "mint": MINT,
            "owner": owner,
            "accountIndex": account_index,
            "uiTokenAmount": {"amount": str(post_amount)},
        }
    ]
    return {
        "slot": 5_000,
        "transaction": {
            "signatures": [sig],
            "message": {
                "accountKeys": ["11111111111111111111111111111111"],
                "instructions": [{"programId": SYSTEM_PROGRAM, "parsed": {"type": "transfer", "info": {}}}],
            },
        },
        "meta": meta,
    }


def _tip_tx(sig: str = TIP_SIG, *, lamports: int | None = None, inner: bool = True) -> dict[str, Any]:
    lamports = lamports if lamports is not None else MIN_TIP_LAMPORTS
    ix = _system_transfer_ix(TIP_ACCOUNT, lamports)
    tx = {
        "slot": 5_000,
        "transaction": {
            "signatures": [sig],
            "message": {
                "accountKeys": ["11111111111111111111111111111111"],
                "instructions": [] if inner else [ix],
            },
        },
        "meta": {},
    }
    if inner:
        tx["meta"]["innerInstructions"] = [{"index": 0, "instructions": [ix]}]
    return tx


class TestFindTipTransfer:
    def test_inner_cpi_tip_detected(self):
        assert find_tip_transfer(_tip_tx(inner=True)) == TIP_ACCOUNT

    def test_outer_instruction_tip_detected(self):
        assert find_tip_transfer(_tip_tx(inner=False)) == TIP_ACCOUNT

    def test_below_threshold_not_tip(self):
        assert find_tip_transfer(_tip_tx(lamports=MIN_TIP_LAMPORTS - 1)) is None

    def test_non_tip_destination_ignored(self):
        tx = _tip_tx()
        tx["meta"]["innerInstructions"][0]["instructions"][0]["parsed"]["info"]["destination"] = (
            "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"
        )
        assert find_tip_transfer(tx) is None

    def test_transfer_with_seed_matched(self):
        tx = _tip_tx()
        tx["meta"]["innerInstructions"][0]["instructions"] = [
            _system_transfer_ix(TIP_ACCOUNT, MIN_TIP_LAMPORTS, seed="seed")
        ]
        assert find_tip_transfer(tx) == TIP_ACCOUNT

    def test_non_system_program_ignored(self):
        tx = _tip_tx()
        tx["meta"]["innerInstructions"][0]["instructions"][0]["programId"] = TOKEN_PROGRAM
        assert find_tip_transfer(tx) is None

    def test_no_instructions_returns_none(self):
        assert find_tip_transfer({"transaction": {"message": {}}, "meta": {}}) is None


class TestDetectBundle:
    def _bundle_run(self) -> list[dict[str, Any]]:
        return [
            _create_tx(),
            _buy_tx(BUY_SIG_1, "walletA"),
            _buy_tx(BUY_SIG_2, "walletB"),
            _tip_tx(),
        ]

    def test_detects_bundled_launch(self):
        evt = detect_bundle(self._bundle_run())
        assert evt is not None
        assert evt.slot == 5_000
        assert evt.create_tx_signature == CREATE_SIG
        assert evt.tip_tx_signature == TIP_SIG
        assert evt.tip_account == TIP_ACCOUNT
        assert evt.buyer_count == 2
        assert set(evt.buyer_wallets) == {"walletA", "walletB"}
        assert evt.mint == MINT

    def test_token2022_create_detected(self):
        run = self._bundle_run()
        run[0] = _create_tx(token2022=True)
        evt = detect_bundle(run)
        assert evt is not None
        assert evt.mint == MINT

    def test_single_tx_returns_none(self):
        assert detect_bundle([_create_tx()]) is None

    def test_empty_slot_returns_none(self):
        assert detect_bundle([]) is None

    def test_no_create_tx_returns_none(self):
        run = [_buy_tx(BUY_SIG_1, "walletA"), _tip_tx()]
        assert detect_bundle(run) is None

    def test_tip_not_in_last_tx_returns_none(self):
        run = [_create_tx(), _tip_tx(), _buy_tx(BUY_SIG_1, "walletA")]
        assert detect_bundle(run) is None

    def test_no_fresh_buyers_returns_none(self):
        run = [_create_tx(), _buy_tx(BUY_SIG_1, "walletA", pre_amount=500), _tip_tx()]
        assert detect_bundle(run) is None

    def test_buyer_with_zero_post_amount_not_counted(self):
        run = [_create_tx(), _buy_tx(BUY_SIG_1, "walletA", post_amount=0), _tip_tx()]
        assert detect_bundle(run) is None

    def test_duplicate_wallets_deduped(self):
        run = [
            _create_tx(),
            _buy_tx(BUY_SIG_1, "walletA"),
            _buy_tx(BUY_SIG_2, "walletA"),
            _tip_tx(),
        ]
        evt = detect_bundle(run)
        assert evt is not None
        assert evt.buyer_count == 1
        assert evt.buyer_wallets == ["walletA"]

    def test_create_in_inner_instruction_detected(self):
        create = _create_tx()
        create["transaction"]["message"]["instructions"] = []
        create["meta"]["innerInstructions"] = [
            {
                "index": 0,
                "instructions": [
                    {
                        "programId": TOKEN_PROGRAM,
                        "parsed": {"type": "createMint", "info": {"mint": MINT}},
                    }
                ],
            }
        ]
        run = [create, _buy_tx(BUY_SIG_1, "walletA"), _tip_tx()]
        evt = detect_bundle(run)
        assert evt is not None
        assert evt.buyer_count == 1
