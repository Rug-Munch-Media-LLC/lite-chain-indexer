"""
On-demand historical RPC query system (the "no-full-indexing" layer).

Lets the indexer answer queries about ANY block/tx/address historically
without indexing the whole chain:

* :class:`QueryRouter` — intelligently routes each query to the best data
  source: ClickHouse (indexed, hot) → B2 parquet (cold) → live RPC
  (on-demand). It never says "data not available": RPC is always the last
  resort.
* :class:`RPCFetcher` — makes targeted RPC calls (one tx receipt, one
  address-window getLogs, one block, one deployer lookup) instead of
  full-chain scans.
* :class:`QueryCache` — bounded TTL cache for fetched RPC results so
  repeated queries stay off the (free-tier, rate-limited) RPC pool.
"""

from __future__ import annotations

from lite_indexer.query.cache import QueryCache
from lite_indexer.query.router import QueryRouter
from lite_indexer.query.rpc_fetcher import RPCFetcher

__all__ = ["QueryCache", "QueryRouter", "RPCFetcher"]
