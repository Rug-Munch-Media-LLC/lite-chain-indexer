"""
Configuration constants for LiteChainIndexer.

Split into sub-modules: chains, storage, indexing, rpc. This package
re-exports every public constant so ``from lite_indexer.config import X``
keeps working unchanged.
"""

from lite_indexer.config.chains import *  # noqa: F401,F403
from lite_indexer.config.indexing import *  # noqa: F401,F403
from lite_indexer.config.lp_topics import *  # noqa: F401,F403
from lite_indexer.config.metrics import *  # noqa: F401,F403
from lite_indexer.config.rpc import *  # noqa: F401,F403
from lite_indexer.config.storage import *  # noqa: F401,F403
from lite_indexer.config.webhooks import (  # noqa: F401
    WEBHOOK_BACKOFF_SECONDS,
    WEBHOOK_DB_PATH,
    WEBHOOK_ENABLED,
    WEBHOOK_MAX_PER_IP,
    WEBHOOK_MAX_RETRIES,
    WEBHOOK_TIMEOUT_SECONDS,
    WEBHOOK_TOTAL_TIMEOUT_SECONDS,
)
