"""Stablecoin address map used to compute USD values for DEX swaps.

The warehouse has no ``token_prices`` table (checked 2026-08-03), so the
whales endpoint derives USD directly from the stablecoin side of a swap:
for a pair like USDC/WETH the ``amount0``/``amount1`` held by the USDC
address IS the USD value of the trade. This is exact to the stablecoin's
peg and needs no external price feed.

Addresses are lowercase hex (the form stored in ``dex_events.token0`` /
``token1``). The map is public data — not a secret — and can be extended
by adding entries here or patching the table in a fork. Only EVM chains
are listed: ``dex_events`` holds EVM swaps; Solana balance changes live
in ``solana_spl_balance_changes`` (no USD pairing in the warehouse).
"""

from __future__ import annotations

# Chain → set of stablecoin token addresses. The first tuple element is
# the address; (symbol is implied by the address map below for display).
STABLECOIN_ADDRESSES: dict[str, frozenset[str]] = {
    "ethereum": frozenset(
        {
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
            "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
            "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
            "0x4c9edd5852cd905f086c759e8383e09bff1e68b3",  # USDe
            "0x6c3ea9036406852006290770bedfcaba0e23a0e8",  # PYUSD
        }
    ),
    "base": frozenset(
        {
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",  # USDC
            "0x50c5725949a6f0c72e6c4a641f24049a917db0cb",  # USDT/DAI (bridged)
        }
    ),
    "bsc": frozenset(
        {
            "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC
            "0x55d398326f99059ff775485246999027b3197955",  # USDT
            "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD
            "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",  # DAI
        }
    ),
    "polygon": frozenset(
        {
            "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",  # USDC.e
            "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",  # USDC native
            "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",  # USDT
            "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063",  # DAI
        }
    ),
    "arbitrum": frozenset(
        {
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",  # USDC
            "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",  # USDC.e
            "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",  # USDT
            "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",  # DAI
        }
    ),
    "optimism": frozenset(
        {
            "0x0b2c639c533813f4aa9d7837caf62653d097ff85",  # USDC
            "0x7f5c764cbc14f9669b88837ca1490cca17c31607",  # USDC.e
            "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58",  # USDT
            "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",  # DAI
        }
    ),
    "avalanche": frozenset(
        {
            "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",  # USDC
            "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664",  # USDC.e
            "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7",  # USDT
            "0xd586e7f844cea2f87f50152665bcbc2c279d8d70",  # DAI
        }
    ),
    "fantom": frozenset(
        {
            "0x04068da6c83afcfa0e13ba15a6696662335d5b75",  # USDC
            "0x049d68029688eabf473097a2fc38ef61633a3c7a",  # USDT
            "0x8d11ec38a3eb5e956b052f67da8bdc9bef8abf3e",  # DAI
        }
    ),
    "linea": frozenset(
        {
            "0x176211869ca2b568f2a7d4ee941e073a542ee1fd",  # USDC
            "0xa219439258ca9da29e9cc4ce5596924745e12b93",  # USDT
            "0x4af15ec2a0bd43db75dd04e62faa3b8ef36b00d5",  # DAI
        }
    ),
    "scroll": frozenset(
        {
            "0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4",  # USDC
            "0xf55bec9cafdbe8730f096aa55dad6d22d44099df",  # USDT
            "0xca77eb3fefe3725dc33bccb8edfed6abf3d0a9f0",  # DAI
        }
    ),
    "zksync": frozenset(
        {
            "0x1d17cbcf0d6d143135ae902365d2e5e2a3ae9848",  # USDC
            "0x493257fd37edb34451f62edf8d2a0c418852ba4c",  # USDT
            "0x4b9eb6c0b6ea15176bbf62841c6b2a8a9cb43c42",  # DAI
        }
    ),
    "gnosis": frozenset(
        {
            "0xddafbb505ad214d7b80b1f830fccc89b60fb7a83",  # USDC
            "0x4ecaba5870353805a9f068101a40e0f32ed605c6",  # USDT
            "0xe91d153e0b41518a2ce8dd3d7944fa863463a97d",  # DAI
        }
    ),
    "celo": frozenset(
        {
            "0x765de816845861e75a25fca122bb6898b8b1282a",  # cUSD
            "0xceba9300f2b948710d2653dd7f07f8688c839bc9",  # USDC
            "0x48065fbbe25f71c9282ddf5e1cd6d6a887483d5e",  # USDT
        }
    ),
}

# Known symbols for display (best-effort; not all addresses listed).
STABLECOIN_SYMBOLS: dict[str, str] = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
    "0x4c9edd5852cd905f086c759e8383e09bff1e68b3": "USDe",
    "0x6c3ea9036406852006290770bedfcaba0e23a0e8": "PYUSD",
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": "USDC",
    "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": "USDT",
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": "USDC",
    "0x55d398326f99059ff775485246999027b3197955": "USDT",
    "0xe9e7cea3dedca5984780bafc599bd69add087d56": "BUSD",
    "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3": "DAI",
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": "USDC.e",
    "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359": "USDC",
    "0xc2132d05d31c914a87c6611c10748aeb04b58e8f": "USDT",
    "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063": "DAI",
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": "USDC",
    "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": "USDC.e",
    "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": "USDT",
    "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": "DAI",
    "0x0b2c639c533813f4aa9d7837caf62653d097ff85": "USDC",
    "0x7f5c764cbc14f9669b88837ca1490cca17c31607": "USDC.e",
    "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58": "USDT",
    "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": "USDC",
    "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664": "USDC.e",
    "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7": "USDT",
    "0xd586e7f844cea2f87f50152665bcbc2c279d8d70": "DAI",
    "0x04068da6c83afcfa0e13ba15a6696662335d5b75": "USDC",
    "0x049d68029688eabf473097a2fc38ef61633a3c7a": "USDT",
    "0x8d11ec38a3eb5e956b052f67da8bdc9bef8abf3e": "DAI",
    "0x176211869ca2b568f2a7d4ee941e073a542ee1fd": "USDC",
    "0xa219439258ca9da29e9cc4ce5596924745e12b93": "USDT",
    "0x4af15ec2a0bd43db75dd04e62faa3b8ef36b00d5": "DAI",
    "0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4": "USDC",
    "0xf55bec9cafdbe8730f096aa55dad6d22d44099df": "USDT",
    "0xca77eb3fefe3725dc33bccb8edfed6abf3d0a9f0": "DAI",
    "0x1d17cbcf0d6d143135ae902365d2e5e2a3ae9848": "USDC",
    "0x493257fd37edb34451f62edf8d2a0c418852ba4c": "USDT",
    "0x4b9eb6c0b6ea15176bbf62841c6b2a8a9cb43c42": "DAI",
    "0xddafbb505ad214d7b80b1f830fccc89b60fb7a83": "USDC",
    "0x4ecaba5870353805a9f068101a40e0f32ed605c6": "USDT",
    "0xe91d153e0b41518a2ce8dd3d7944fa863463a97d": "DAI",
    "0x765de816845861e75a25fca122bb6898b8b1282a": "cUSD",
    "0xceba9300f2b948710d2653dd7f07f8688c839bc9": "USDC",
    "0x48065fbbe25f71c9282ddf5e1cd6d6a887483d5e": "USDT",
}


def stablecoin_symbol(address: str) -> str:
    """Return the display symbol for a stablecoin address (or '')."""
    return STABLECOIN_SYMBOLS.get(address.lower(), "")


__all__ = [
    "STABLECOIN_ADDRESSES",
    "STABLECOIN_SYMBOLS",
    "stablecoin_symbol",
]
