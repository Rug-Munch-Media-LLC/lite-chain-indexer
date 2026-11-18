"""The public-dataset registry must describe reality, not aspiration.

These tests exist because the registry's whole value is that an operator can add
a chain by editing data instead of code — which only helps if the data is
honest about which datasets are actually readable with the existing mapper.
"""

from __future__ import annotations

from lite_indexer.query.bigquery_datasets import (
    PUBLIC_DATASETS,
    by_chain,
    evm_datasets,
    unmapped,
)


def test_every_chain_name_is_lowercase_and_unique() -> None:
    seen: set[str] = set()
    for ds in PUBLIC_DATASETS:
        for chain in ds.chains:
            assert chain == chain.lower(), f"{chain} is not lowercase"
            assert chain not in seen, f"{chain} declared by two datasets"
            seen.add(chain)


def test_every_kind_is_known() -> None:
    # A typo'd kind would silently make a dataset "non-EVM" and drop it from the
    # ready set without anyone noticing.
    for ds in PUBLIC_DATASETS:
        assert ds.kind in {"evm", "utxo", "account"}, f"{ds.dataset}: {ds.kind}"


def test_ethereum_and_solana_are_the_wired_ones() -> None:
    m = by_chain()
    assert "ethereum" in m
    assert "solana" in m
    # Both were wired before this registry existed; losing them would be a
    # regression in the only two chains bigquery already served.
    assert m["ethereum"].dataset == "crypto_ethereum"
    assert m["solana"].dataset == "crypto_solana_mainnet_us"


def test_polygon_is_evm_ready() -> None:
    # Verified by `bq ls bigquery-public-data:crypto_polygon`: blocks, contracts,
    # logs, token_transfers, tokens, traces, transactions. Same shape as
    # crypto_ethereum, so the existing mapper reads it with no new code.
    ready = {ds.dataset for ds in evm_datasets()}
    assert "crypto_polygon" in ready


def test_bitcoin_is_declared_utxo_not_evm() -> None:
    # crypto_bitcoin exposes inputs/outputs VIEWS — a UTXO shape, not (from,to).
    # If it were ever relabelled "evm" the mapper would produce garbage rows
    # rather than fail, which is worse.
    m = by_chain()
    assert m["bitcoin"].kind == "utxo"
    assert "bitcoin" not in {c for ds in evm_datasets() for c in ds.chains}


def test_unmapped_covers_everything_not_evm() -> None:
    ready = set(evm_datasets())
    for ds in unmapped():
        assert ds not in ready
    # The two sets must partition the registry, or a dataset is counted twice.
    assert len(evm_datasets()) + len(unmapped()) == len(PUBLIC_DATASETS)


def test_fully_qualified_name_shape() -> None:
    for ds in PUBLIC_DATASETS:
        assert ds.fq == f"bigquery-public-data.{ds.dataset}"
