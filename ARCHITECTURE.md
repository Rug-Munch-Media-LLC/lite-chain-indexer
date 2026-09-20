# lite-chain-indexer — Architecture



---

## House Standard

Module <=200 lines hard (new), one primary function per module, zero monkeypatch in production, shrink-only ratchets — all CI-enforced. See **[docs/STANDARDS.md](docs/STANDARDS.md)**.

## Data-Flow Boundary (added 2026-08-24)

The indexer is a STANDALONE data producer with one-way flow:

    indexer → ClickHouse (wallet_memory) ← backend reads via _indexer_port

Invariants:
- The indexer NEVER calls backend APIs or DataBus.
- The backend NEVER writes indexer tables.
- The bot reaches indexer data only through the backend gateway or its
  vendored patterns client — never direct ClickHouse.
- RPC pools here are self-contained (own registries, own vault-free
  env keys); DataBus key management does not apply to this daemon.

Full cross-system map: rmi-backend `app/databus/ARCHITECTURE.md`.
