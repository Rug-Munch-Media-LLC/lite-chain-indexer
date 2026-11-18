# BTC TRACING — the UTXO adapter and what it costs

**Status:** implemented (`lite_indexer/query/btc_source.py`) · **date:** 2026-11-18

## Why this exists

Before this adapter BTC was **a label lookup, not tracing**: we held 39.3M BTC
labels (`wallet_memory.wallet_labels`, `chain_id='btc'`) and 90,597 labelled BTC
wallets with real USD flows (`default.realcats_btc`), and **zero** BTC
transaction tables. `biglake_source` said so in one line: `btc/ exists in the
lake but is not mapped yet`.

The lake supplies the transactions. This document records what the mapping
preserves, and — more importantly — what it cannot.

## Source

Public open data, anonymous, no credentials:

```
s3://aws-public-blockchain/v1.0/btc/transactions/date=YYYY-MM-DD/*.parquet   ~0.5 GB/day, ~1M rows
s3://aws-public-blockchain/v1.0/btc/blocks/date=YYYY-MM-DD/*.parquet         ~144 rows/day
```

Read in place by DuckDB `httpfs`. Nothing is downloaded to local disk: the
backfill streams one batch at a time into the existing verified writer, whose
`TemporaryDirectory` holds only the unit's own parquet output.

## Tables

| logical | lake dir | archive table | rows/day |
|---|---|---|---|
| `btc_transactions` | `transactions/` | `btc/<table>/<date>/part-000.parquet` | ~1M |
| `btc_blocks` | `blocks/` | `btc/<table>/<date>/part-000.parquet` | ~144 |

Day granularity, no `hour`: 0.5 GB/day fits one unit and the lake's own
partition *is* the day.

## The UTXO mapping — and where it is lossy

Bitcoin is UTXO. A transaction has `inputs` and `outputs` and **no**
`from_address` / `to_address`. The row keeps the UTXO shape rather than
fabricating a pair the source does not contain.

### Lossy, in descending order of consequence

1. **No attributable sender → recipient amount.** One transaction can spend 30
   inputs into 200 outputs, and Bitcoin records **no per-edge link at all**. Any
   "A paid B X BTC" claim derived from a single transaction is a fabrication.
   What exists is: this transaction consumed these outpoints and produced those
   outputs. A caller needing edges must join an input's `spent_transaction_hash`
   + `spent_output_index` to the parent transaction's output at that index — the
   join is exact per input, but the *amount attribution* to a recipient stays
   undefined for fan-out transactions.
2. **A join is available, an attribution is not.** `input_spent_tx[i]` plus
   `input_spent_index[i]` address the parent output exactly, so an edge
   (parent output → this input) is reconstructible for every input. What is
   *still* unavailable is a per-edge *amount* on the receiving side: the input's
   value came from that outpoint, but which of this transaction's outputs it
   funded is undefined.
3. **Addresses are not normalised.** BTC has no checksum-stable canonical form
   across legacy / P2SH / bech32, and a `bc1q…` and its `1…` predecessor are
   different strings for economically related keys. Address clustering is a
   downstream heuristic (common-input-ownership), not a property of this row.
4. **NULL address is preserved, not blanked.** A coinbase has no inputs
   (`input_*` all NULL, `input_count = 0`, `is_coinbase = true`); an OP_RETURN
   or non-standard output has no address (a NULL element inside
   `output_addresses`). Coercing either to `""` would invent an address and
   corrupt every unknown-address aggregate.
5. **Values are integer sats, a unit change and not a rounding.** The lake
   stores `DOUBLE` BTC; `round(value * 1e8)` is exact for every legal amount
   (total supply 2.1e15 sats ≪ 2**53). Keeping the float would import artefacts
   such as `0.30595074000000005` into the archive. `fee_sats` is NULL on
   coinbase rows — a block reward is not a fee, and reporting 0 would be wrong.
6. **No script/script_asm/txinwitness.** Deliberately dropped: they are the bulk
   of the 0.5 GB/day and no tracing query needs them. `output_types` (e.g.
   `witness_v0_keyhash`) is kept because it determines whether an address exists.

### Preserved exactly

`tx_hash`, `tx_index` (position in block), `block_number`, `block_hash`,
`block_timestamp`, `is_coinbase`, `input_count`, `output_count`, per-input
address/value/parent-hash/parent-index, per-output address/value/type, `fee_sats`.

## Reader contract (same shape as `biglake_source`)

```python
from lite_indexer.query.btc_source import BtcSource

src = BtcSource()
src.available()                                  # enabled + duckdb importable
rows, truncated = src.query(
    "btc_transactions",
    dates=["2026-11-15"],                        # exact days; default = BTC_SCOPE month
    from_block=976000, to_block=976100,
    limit=100,
)
```

* An **absent partition returns `([], False)`** — a fact, not a failure.
* A malformed date **raises** `ValueError` before any SQL runs; a silently
  skipped partition would look like a quiet day.
* `limit` above `LAKE_MAX_ROWS` raises, never truncates silently; truncation is
  proven by a probe row (`LIMIT limit + 1`).

## Backfill

```bash
python -m lite_indexer.query.btc_backfill --dates 2026-11-15 --dry-run
python -m lite_indexer.query.btc_backfill --dates 2026-11-15
python -m lite_indexer.query.btc_backfill --dates 2026-11-15 --table btc_blocks
```

`expected_rows` is counted from the same S3 glob before publishing (DuckDB
answers `count(*)` from parquet metadata), so `UnitSpec` carries a fact rather
than a guess. `write_unit` then refuses to report success unless the object
exists at the size written and the parquet row count matches exactly. A
zero-row partition raises `WriteError("empty_unit")` instead of publishing a day
that would look complete and empty.

## Measured

```
2026-11-15 transactions: 963,618 rows
2011-01-01 transactions:     659 rows   (181 coinbase)
2011-01-01 blocks:           181 rows
```

End-to-end write verified on 2011-01-01 (659 rows → 100,077 B parquet;
181 blocks → 25,779 B), including `count(*)`, sats reconstruction and coinbase
NULL handling read back out of the published parquet.
