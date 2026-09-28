"""
Configuration constants for LiteChainIndexer.

Split into sub-modules: chains, storage, indexing, rpc. This package
re-exports every public constant so ``from lite_indexer.config import X``
keeps working unchanged.
"""

from lite_indexer.config.chains import *  # noqa: F401,F403
from lite_indexer.config.indexing import *  # noqa: F401,F403
from lite_indexer.config.lp_topics import (  # noqa: F401
    BURN_V2_TOPIC,
    BURN_V3_TOPIC,
    LP_ADD_TYPE,
    LP_REMOVE_TYPE,
    LP_TOPIC_TYPES,
    MINT_V2_TOPIC,
    MINT_V3_TOPIC,
)
from lite_indexer.config.metrics import *  # noqa: F401,F403
from lite_indexer.config.rpc import *  # noqa: F401,F403
from lite_indexer.config.rpc_cost import (  # noqa: F401
    COST_TIERS,
    RPC_COST_TIER_UNKNOWN,
    RPC_COST_TIERS,
    RPC_SPILLOVER_ALLOWED,
    parse_cost_tiers,
)
from lite_indexer.config.storage import *  # noqa: F401,F403
from lite_indexer.config.webhooks import (  # noqa: F401
    ALLOW_UNAUTHENTICATED_WRITE,
    WEBHOOK_API_KEY,
    WEBHOOK_BACKOFF_SECONDS,
    WEBHOOK_DB_PATH,
    WEBHOOK_ENABLED,
    WEBHOOK_MAX_PER_IP,
    WEBHOOK_MAX_RETRIES,
    WEBHOOK_TIMEOUT_SECONDS,
    WEBHOOK_TOTAL_TIMEOUT_SECONDS,
)

# mypy cannot see a re-export unless the re-exporting module lists it in
# __all__ — that attr-defined class is most of the type-check debt. Only
# the names imported BY NAME below are listed; the star-imported modules
# keep their pre-existing (unlisted) behaviour so this cannot drift.
__all__ = [
    "ALLOW_UNAUTHENTICATED_WRITE",
    "BURN_V2_TOPIC",
    "BURN_V3_TOPIC",
    "COST_TIERS",
    "LP_ADD_TYPE",
    "LP_REMOVE_TYPE",
    "LP_TOPIC_TYPES",
    "MINT_V2_TOPIC",
    "MINT_V3_TOPIC",
    "RPC_COST_TIERS",
    "RPC_COST_TIER_UNKNOWN",
    "RPC_SPILLOVER_ALLOWED",
    "WEBHOOK_API_KEY",
    "WEBHOOK_BACKOFF_SECONDS",
    "WEBHOOK_DB_PATH",
    "WEBHOOK_ENABLED",
    "WEBHOOK_MAX_PER_IP",
    "WEBHOOK_MAX_RETRIES",
    "WEBHOOK_TIMEOUT_SECONDS",
    "WEBHOOK_TOTAL_TIMEOUT_SECONDS",
    "parse_cost_tiers",
]
