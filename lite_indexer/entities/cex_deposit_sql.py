"""ClickHouse queries behind CEX-deposit clustering.

Every query is a plain string with ``%(name)s`` placeholders bound by
``clickhouse-driver`` (the style used in ``query/wallet_graph_sql.py``); no value
is ever interpolated into SQL. Two deliberate constraints:

* **No ``WITH`` clauses** — the read-only MCP user runs with ``readonly=2``,
  which rejects CTEs outright.
* **The label filter runs in a ``HAVING``, not after the fact** — the read-only
  user caps a result at 500k rows and ``evm_transfers`` holds ~1.4M distinct
  wallets on Ethereum alone, so returning every wallet and filtering in Python
  fails outright. ``HAVING`` keeps the aggregation server-side and the result is
  only the clustered minority.
* **The label set is a subquery, never a bound list** — Ethereum alone carries
  ~25k exchange-labelled addresses, and inlining them as an ``IN`` literal
  exceeds ``max_query_size`` (measured: 256 KB).

The ``HAVING`` is what makes the heuristic mean what it claims: the wallet's
**first** inbound — not merely *an* inbound — must come from an exchange.
"""

from __future__ import annotations

#: Native-value first inbound per wallet, kept only when that first funder is a
#: labelled exchange address. ``trace_address=[]`` keeps top-level calls only (a
#: nested internal transfer is not a deposit); ``error=''`` drops reverted calls.
NATIVE_FIRST_INBOUND = """
SELECT
    to_address AS wallet_address,
    argMin(lower(from_address), block_timestamp) AS source_address,
    toUnixTimestamp(min(block_timestamp)) AS first_seen_epoch
FROM wallet_memory.internal_transfers
WHERE chain = %(chain)s
  AND value_wei > 0
  AND trace_address = []
  AND error = ''
GROUP BY wallet_address
HAVING source_address IN (
    SELECT lower(address) FROM wallet_memory.wallet_labels
    WHERE chain_id = %(chain)s AND label_category IN %(categories)s
)
"""

#: Earliest ERC20 inbound per wallet — the weaker tier. ``evm_transfers`` has no
#: native rows, so this sees token movement only and is scored as such.
TOKEN_FIRST_INBOUND = """
SELECT
    to_address AS wallet_address,
    argMin(lower(from_address), block_timestamp) AS source_address,
    toUnixTimestamp(min(block_timestamp)) AS first_seen_epoch
FROM wallet_memory.evm_transfers
WHERE chain = %(chain)s
GROUP BY wallet_address
HAVING source_address IN (
    SELECT lower(address) FROM wallet_memory.wallet_labels
    WHERE chain_id = %(chain)s AND label_category IN %(categories)s
)
"""

#: Exchange labels, one row per address. The label set carries several rows per
#: address from different sources, so the strongest one wins deterministically
#: (deposit wallet > hot wallet > tier A > anything else).
EXCHANGE_LABELS = """
SELECT
    source_address,
    argMin(label_name, priority) AS label_name,
    argMin(label_category, priority) AS label_category,
    any(tier) AS tier,
    any(label_subtype) AS label_subtype
FROM (
    SELECT
        lower(address) AS source_address,
        label_name,
        label_category,
        tier,
        label_subtype,
        if(
            label_subtype = 'cex_deposit_wallet', 0,
            if(label_subtype = 'cex_hot_wallet', 1, if(tier = 'A', 2, 3))
        ) AS priority
    FROM wallet_memory.wallet_labels
    WHERE chain_id = %(chain)s
      AND label_category IN %(categories)s
)
GROUP BY source_address
"""

#: Earliest native movement we hold for a chain — the left edge of "first
#: inbound". Both funding tables are TTL-bounded, which is why the edge penalty
#: needs a measured boundary rather than a fixed guess.
WINDOW_START = """
SELECT toUnixTimestamp(min(block_timestamp)) AS window_start_epoch
FROM wallet_memory.internal_transfers
WHERE chain = %(chain)s
  AND value_wei > 0
  AND trace_address = []
  AND error = ''
"""

INSERT_COLUMNS = (
    "entity_id",
    "wallet_address",
    "chain_id",
    "heuristic_type",
    "confidence_score",
    "entity_label",
    "entity_category",
    "first_linked_at",
    "last_updated_at",
)

#: ``wallet_entities`` is ``ReplacingMergeTree(last_updated_at)`` ordered by
#: ``(wallet_address, chain_id)``, so re-inserting the same row with the same
#: ``last_updated_at`` collapses on merge. ``first_linked_at`` and
#: ``last_updated_at`` both come from the evidence, never ``now()``, so a re-run
#: is byte-identical and idempotent.
INSERT_ENTITIES = "INSERT INTO wallet_memory.wallet_entities (" + ", ".join(INSERT_COLUMNS) + ") VALUES"

#: Existence probe — ``system.tables`` is readable by the read-only user and
#: costs nothing, so a missing target table fails cleanly before any insert.
TABLE_EXISTS = """
SELECT count() FROM system.tables
WHERE database = 'wallet_memory' AND name = %(name)s
"""
