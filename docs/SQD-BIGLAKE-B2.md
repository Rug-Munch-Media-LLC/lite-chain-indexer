# SQD / BigLake / B2 — which is which

These are **three different things** and the names are deliberately distinct.
Mixing them is what makes backfill work hard to reason about.

## The roles

| name | what it is | role | code |
|---|---|---|---|
| **SQD** (Squid) | a portal with complete EVM history from 2021+, ~13 chains | **SOURCE** (one of several) | `lite_indexer/query/sqd_*.py` |
| **BigLake** | BigQuery's `aws-public-blockchain` dataset (eth/btc only) | **SOURCE** (a different one) | `lite_indexer/query/biglake_*.py` |
| **B2** | Backblaze object storage | **DESTINATION** (the only retention) | `lite_indexer/archive/`, `b2_*` scripts |

There is no "sqd bglake" and no "biglake sqd". They are never interchangeable:
SQD covers the EVM chains, BigLake covers eth/btc, and neither is the archive.

## Module naming rule

One module, one job. The prefix says the **source**, the rest says the **job**:

```
sqd_source.py        SQD client (HTTP, retries, pagination)
sqd_day.py           UTC day -> block range for SQD
sqd_rows.py          SQD records -> archive row shapes
sqd_evm.py           EVM dataset readers
sqd_solana.py        Solana dataset readers
sqd_tron.py          Tron dataset readers
sqd_tables.py        table discovery
sqd_common.py        shared constants
sqd_rotation.py      provider rotation (which SQD endpoint to use)
sqd_cli.py           CLI

biglake_source.py    BigLake reader
biglake_read.py      build the lake SQL + detect truncation
biglake_rotation.py  rotate BigQuery projects
```

The backfill pipeline that sits on top of those sources:

```
sqd_day_state.py     ONE day's fetch progress (cursor)
sqd_sitting.py       ONE sitting (fetch a window, stop at the ceiling)
sqd_parts.py         the parquet/ndjson parts on disk
sqd_window.py        facade: open_day + re-exports
```

## Why the split matters

A single 302-line `sqd_window.py` held the day state, the sitting loop and the
part assembly -- three jobs in one module. It failed the size ratchet and, more
importantly, meant "where do I change the cursor?" had three possible answers.

## Adding a new source

A new source (e.g. Envio) gets its own prefix and its own modules:

```
envio_source.py
envio_day.py
envio_rows.py
```

and the generic writer stays generic: `archive/unit_writer.py` owns the
verify-and-upload, so a source adapter only has to produce rows.