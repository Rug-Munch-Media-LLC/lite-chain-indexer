# Lite Chain Indexer — FAQ

**What is it?** A lightweight multi-chain indexer (EVM + Solana): block/event ingestion to Parquet + queryable stores, no heavyweight framework.

**Which chains?** EVM family via RPC + Solana. Chain registry is config-driven — add a chain by adding an entry.

**Storage?** Daily Parquet dumps (S3/B2 compatible) + optional serving layer; see scripts/ for the daily job.

**RPC requirements?** Any standard RPC endpoint; free tiers work with hedging + auto-tune enabled. Free-first ordering invariant is tested.

**How is this different from chain-indexer?** chain-indexer (archived) was the Subsquid-based build; lite-chain-indexer is the dependency-light successor.

**Backfills?** Range backfill tooling included; see README usage section.
