"""Registry of the BigQuery public crypto datasets we can read.

WHY THIS MODULE EXISTS
    `bigquery_tables.py` hard-codes two datasets (eth, solana). `bq ls` against
    `bigquery-public-data` shows **twenty** crypto datasets, every one free and
    full-history. Adding a chain was therefore a code edit in a 200-line module
    instead of a data entry — and twenty available chains stayed at two.

    This is a registry, not a client. `bigquery_tables.py` and
    `bigquery_source.py` keep the SQL building and the quota rotation; this file
    only says which datasets exist, which chains they serve, and what shape they
    are in.

VERIFIED 2026-11-18 via `bq ls bigquery-public-data:<dataset>`:
    A "table set" dataset exposes blocks/transactions/logs/token_transfers/
    traces/contracts/tokens, day-partitioned on block_timestamp — the same shape
    `crypto_ethereum` already parses. A "view set" dataset (crypto_bitcoin)
    exposes blocks/transactions/inputs/outputs as VIEWS, which is a different
    row shape and needs its own mapper before it can be used.

DO NOT ADD A DATASET WITHOUT CHECKING ITS SHAPE
    `crypto_bitcoin` looks like the obvious BTC win and is the one that does not
    fit the EVM table map — it is UTXO (inputs/outputs), not (from,to). Listing it
    here with kind="utxo" records that it exists and is not yet mappable, rather
    than silently pretending it works.
"""

from __future__ import annotations

from dataclasses import dataclass

#: BigQuery's shared public project.
PUBLIC_PROJECT = "bigquery-public-data"


@dataclass(frozen=True)
class PublicDataset:
    """One free public crypto dataset.

    Attributes:
        dataset: BigQuery dataset id (without the project).
        chains: Local chain names this dataset can answer for.
        kind: ``evm`` (blocks/transactions/logs/token_transfers/...) or
            ``utxo`` (blocks/transactions/inputs/outputs) or ``account``
            (non-EVM account model, e.g. solana/aptos/sui/near).
        note: Why it is included, or what blocks its use.
    """

    dataset: str
    chains: tuple[str, ...]
    kind: str
    note: str = ""

    @property
    def fq(self) -> str:
        """Fully-qualified ``project.dataset`` for a ``bq`` query."""
        return f"{PUBLIC_PROJECT}.{self.dataset}"


#: Datasets verified present. ``kind`` decides whether the existing EVM mapper
#: can read it; anything not ``evm`` needs a mapper before it is usable.
PUBLIC_DATASETS: tuple[PublicDataset, ...] = (
    # ── already wired ────────────────────────────────────────────────
    PublicDataset("crypto_ethereum", ("ethereum",), "evm", "wired in bigquery_tables"),
    PublicDataset(
        "crypto_solana_mainnet_us",
        ("solana",),
        "account",
        "wired; day-partitioned on block_timestamp, Instructions clustered on program_id",
    ),
    # ── free full-history wins, same EVM shape as ethereum ───────────
    PublicDataset(
        "crypto_polygon",
        ("polygon",),
        "evm",
        "verified: blocks/contracts/logs/token_transfers/tokens/traces/transactions, "
        "day-partitioned on block_timestamp (traces on block_timestamp)",
    ),
    PublicDataset("crypto_ethereum_classic", ("ethereum-classic",), "evm", "EVM shape"),
    # ── UTXO: exists, needs its own mapper (inputs/outputs, not from/to) ──
    PublicDataset(
        "crypto_bitcoin",
        ("bitcoin",),
        "utxo",
        "blocks/transactions/inputs/outputs are VIEWS; UTXO shape, not (from,to). "
        "AWS aws-public-blockchain/v1.0/btc is the better BTC source (parquet, not views).",
    ),
    PublicDataset("crypto_bitcoin_cash", ("bitcoin-cash",), "utxo", "UTXO shape"),
    PublicDataset("crypto_litecoin", ("litecoin",), "utxo", "UTXO shape"),
    PublicDataset("crypto_dogecoin", ("dogecoin",), "utxo", "UTXO shape"),
    PublicDataset("crypto_dash", ("dash",), "utxo", "UTXO shape"),
    # ── other account-model chains: need a mapper ────────────────────
    PublicDataset("crypto_aptos_mainnet_us", ("aptos",), "account", "Move chain"),
    PublicDataset("crypto_sui_mainnet_us", ("sui",), "account", "Move chain"),
    PublicDataset("crypto_near_mainnet_us", ("near",), "account", "NEAR"),
    PublicDataset("crypto_polkadot", ("polkadot",), "account", "Substrate"),
    PublicDataset("crypto_kusama", ("kusama",), "account", "Substrate"),
    PublicDataset("crypto_iotex", ("iotex",), "account", "IoTeX"),
    PublicDataset("crypto_multiversx_mainnet_eu", ("multiversx",), "account", "MultiversX"),
)


def by_chain() -> dict[str, PublicDataset]:
    """chain -> dataset. First declaration wins when a chain appears twice."""
    out: dict[str, PublicDataset] = {}
    for ds in PUBLIC_DATASETS:
        for chain in ds.chains:
            out.setdefault(chain, ds)
    return out


def evm_datasets() -> tuple[PublicDataset, ...]:
    """Only the datasets the existing EVM mapper can already read."""
    return tuple(ds for ds in PUBLIC_DATASETS if ds.kind == "evm")


def unmapped() -> tuple[PublicDataset, ...]:
    """Datasets present in BigQuery that need a row mapper before use.

    Reported by the router so an unsupported chain answers "no tier covers this"
    instead of silently returning empty rows, which reads as "no activity".
    """
    return tuple(ds for ds in PUBLIC_DATASETS if ds.kind != "evm")


__all__ = [
    "PUBLIC_DATASETS",
    "PUBLIC_PROJECT",
    "PublicDataset",
    "by_chain",
    "evm_datasets",
    "unmapped",
]
