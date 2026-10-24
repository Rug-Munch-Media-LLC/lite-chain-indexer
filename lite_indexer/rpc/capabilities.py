"""
Endpoint capability probing — archive support, getLogs range, block receipts.

Discovers per-endpoint service capabilities beyond raw liveness so the
pool can make admission / chunk-sizing decisions that are currently
blind guesses:

  - ``archive`` — whether the node can serve state at genesis-era
    blocks (``eth_getBalance`` at block 1 / 256). Backfill and
    historical scans can only be routed to archive-capable endpoints.
  - ``getlogs_max_range`` — a conservative lower bound on the largest
    ``eth_getLogs`` block range the endpoint accepts. Probed with a small,
    fixed call budget (ascending ladder, stop at first failure); a
    client-side timeout is classified as a range error because a slow
    endpoint is effectively range-limited at the requested size. The exact
    maximum is intentionally not binary-searched — routing only needs a
    safe chunk size, and an exact search dominated startup latency.
  - ``block_receipts`` — whether ``eth_getBlockReceipts`` is supported
    (one-call receipt fetches; unsupported endpoints must fall back to
    the per-tx loop in ``lite_indexer.strategies.evm.receipts``).
  - ``cu_tariff`` — seeded per-method compute-unit weights from
    provider docs (KeyedProvider-style CU table) for cost-aware endpoint
    ranking.

Each probe is best-effort and never raises: it records what it can and
leaves unknown fields at their defaults. Results are persisted per
endpoint in a module-level :class:`CapabilityRecord` registry keyed by
endpoint URL (the same registry the pool admission / routing layer can
consult via :func:`get_capability`).
"""

from __future__ import annotations

import httpx  # noqa: F401 — tests patch capabilities.httpx.AsyncClient

from lite_indexer.rpc.archive_routing import (
    ARCHIVE_METHOD_PREFIXES,
    ARCHIVE_METHODS,
    ArchiveRoutingError,
    NoArchiveEndpointError,
    archive_capable_endpoints,
    archive_guard_reason,
    is_archive_capable,
    method_requires_archive,
)
from lite_indexer.rpc.capability_classify import (
    CAPABILITY_BODY_MARKERS,
    CAPABILITY_ERROR_CODES,
    CAPABILITY_ERROR_MARKERS,
    CAPABILITY_HTTP_STATUSES,
)
from lite_indexer.rpc.capability_classify import (
    GetLogsCapability as GetLogsCapability,
)
from lite_indexer.rpc.capability_classify import (
    _GetLogsProbeStatus as _GetLogsProbeStatus,
)
from lite_indexer.rpc.capability_classify import (
    classify_logs_capability as classify_logs_capability,
)
from lite_indexer.rpc.capability_classify import (
    is_logs_capability_failure as is_logs_capability_failure,
)
from lite_indexer.rpc.capability_constants import (
    CAPABILITY_PERSIST_TTL,
    CAPABILITY_PROBE_TIMEOUT,
    GETLOGS_PROBE_CANDIDATES,
    GETLOGS_PROBE_MAX_CALLS,
    GETLOGS_PROBE_TIMEOUT,
    GETLOGS_RANGE_LADDER,
    SEEDED_CU_TARIFF,
    TRANSFER_TOPIC,
)
from lite_indexer.rpc.capability_probes import (
    probe_archive,
    probe_block_receipts,
    probe_cu_tariff,
    probe_endpoint_capabilities,
)
from lite_indexer.rpc.capability_record import CapabilityRecord
from lite_indexer.rpc.capability_registry import _CAPABILITY_KIND as _CAPABILITY_KIND
from lite_indexer.rpc.capability_registry import _CAPABILITY_RECORDS as _CAPABILITY_RECORDS
from lite_indexer.rpc.capability_registry import (
    capability_records,
    clear_capabilities,
    get_capability,
    load_capabilities,
    peek_capability,
    set_capability,
)
from lite_indexer.rpc.getlogs_probe import _probe_getlogs_once as _probe_getlogs_once
from lite_indexer.rpc.getlogs_probe import probe_logs_capability
from lite_indexer.rpc.getlogs_range_probe import probe_getlogs_range
from lite_indexer.rpc.logs_capability import (
    is_logs_capability_excluded,
    is_logs_capability_reprobe_due,
    logs_capable_endpoints,
    set_logs_capability,
)

__all__ = [
    "ARCHIVE_METHODS",
    "ARCHIVE_METHOD_PREFIXES",
    "CAPABILITY_BODY_MARKERS",
    "CAPABILITY_ERROR_CODES",
    "CAPABILITY_ERROR_MARKERS",
    "CAPABILITY_HTTP_STATUSES",
    "CAPABILITY_PERSIST_TTL",
    "CAPABILITY_PROBE_TIMEOUT",
    "ArchiveRoutingError",
    "CapabilityRecord",
    "GETLOGS_RANGE_LADDER",
    "GETLOGS_PROBE_CANDIDATES",
    "GETLOGS_PROBE_MAX_CALLS",
    "GETLOGS_PROBE_TIMEOUT",
    "GetLogsCapability",
    "NoArchiveEndpointError",
    "SEEDED_CU_TARIFF",
    "TRANSFER_TOPIC",
    "archive_capable_endpoints",
    "archive_guard_reason",
    "capability_records",
    "classify_logs_capability",
    "clear_capabilities",
    "get_capability",
    "is_archive_capable",
    "is_logs_capability_excluded",
    "is_logs_capability_failure",
    "is_logs_capability_reprobe_due",
    "load_capabilities",
    "logs_capable_endpoints",
    "method_requires_archive",
    "peek_capability",
    "probe_archive",
    "probe_block_receipts",
    "probe_cu_tariff",
    "probe_endpoint_capabilities",
    "probe_getlogs_range",
    "probe_logs_capability",
    "set_capability",
    "set_logs_capability",
]
