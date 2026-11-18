"""SQLite schema and hard limits for the durable RPC cache.

Kept separate from the store so the DDL is readable on its own and every
sizing constant lives in one place. Imported by the cache store modules;
nothing here touches a connection.
"""

from __future__ import annotations

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rpc_cache (
    key          TEXT PRIMARY KEY,
    chain        TEXT NOT NULL,
    method       TEXT NOT NULL,
    from_block   INTEGER,
    to_block     INTEGER,
    block_number INTEGER,
    block_hash   TEXT,
    payload      TEXT NOT NULL,
    size_bytes   INTEGER NOT NULL,
    hits         INTEGER NOT NULL DEFAULT 0,
    created_at   REAL NOT NULL,
    accessed_at  REAL NOT NULL,
    expires_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rpc_cache_expires ON rpc_cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_rpc_cache_range ON rpc_cache(chain, from_block, to_block);
CREATE INDEX IF NOT EXISTS idx_rpc_cache_block ON rpc_cache(chain, block_number);
CREATE INDEX IF NOT EXISTS idx_rpc_cache_accessed ON rpc_cache(accessed_at);
CREATE TABLE IF NOT EXISTS gating_state (
    kind       TEXT NOT NULL,
    key        TEXT NOT NULL,
    payload    TEXT NOT NULL,
    updated_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    PRIMARY KEY (kind, key)
);
CREATE INDEX IF NOT EXISTS idx_gating_state_expires ON gating_state(expires_at);
"""

# Cap for the gating-state table (capability verdicts, health, bans). Small
# and bounded; prune oldest rows when exceeded. 0 disables the cap.
GATING_STATE_MAX_ENTRIES = 5000

# Enforce the size cap every N writes (SUM over 200k rows is not free).
SIZE_CHECK_EVERY = 32

__all__ = ["GATING_STATE_MAX_ENTRIES", "SIZE_CHECK_EVERY"]
