# Data sources — Google is one fallback, never the only path

Doctrine: bulk history and catch-up must always be obtainable without
Google. BigQuery is a *rotating fallback* (4 service accounts, monthly
free tiers) and is never assumed to be available.

## Bulk history / catch-up fills

| Path | Chains | Entry point | Metering |
|------|--------|-------------|----------|
| AWS sonarx lake (B2/S3 parquet) | EVM (base, arbitrum, …) | `python -m lite_indexer lake backfill --chain base --from-date … --to-date …` | own bucket, no query meter |
| SQD Portal bulk fill | EVM | `python -m lite_indexer lake sqd-fill --chain … --from-block … --to-block …` | free portal quota |
| GCP BigQuery (4-account rotation) | solana (token transfers / instructions) + eth | `python -m lite_indexer fill-solana …` | 1 TiB/month scanned per account |
| ClickHouse (already-indexed) | all | hot/cold query legs | none |

## On-demand query legs (`QueryRouter`)

`lite_indexer/query/` — ClickHouse (hot + cold), SQD portals (EVM, Solana,
Tron readers), Envio, Indexed/Goldsky, AWS Parquet (lake), GCP BigQuery.
Selection is health/latency aware; BigQuery reads require day partitions
(unbounded scans are refused).

## BigQuery account rotation

- Registry: `BIGQUERY_ACCOUNTS` (JSON: project + key path) — see
  `/srv/secrets/lite-chain-indexer.env`; legacy `BIGQUERY_PROJECT_ID` /
  `GOOGLE_APPLICATION_CREDENTIALS` (+`ALT_`) still work.
- `lite_indexer/query/bigquery_accounts.py`: exhausted accounts (monthly
  free tier consumed) are marked until the 1st of next month in the state
  file (`BIGQUERY_STATE_FILE`, default
  `/var/data/chain-indexer/state/bigquery_accounts.json`) and skipped;
  healthy accounts rotate least-recently-used.
- All four accounts burnt → the fill CLI fails loudly (exit 3) instead of
  silently writing nothing; use the AWS/SQD/ClickHouse paths until reset.
- Cost guards: `--estimate` (free dryRun quote), `--max-scan-gib`,
  server-side day/slot bounds; OFFSET paging is disabled (re-scans).
