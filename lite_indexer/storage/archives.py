"""
Backfill-only archive fetchers — the third rung of the read ladder.

Escalation position
-------------------
The on-demand read path escalates through three rungs:

1. **Lake** — ClickHouse hot rows + B2 parquet cold tier
   (``lite_indexer.storage.cold_store``).
2. **Live RPC** — targeted calls for exactly one block / tx / address
   (``lite_indexer.query.rpc_fetcher``), routed through the pool's
   health scoring / circuit breakers / throttling.
3. **Archive** — this module: free bulk archives that cover ranges the
   live RPC layer is too expensive or too rate-limited to serve (deep
   history, multi-day backfill windows).

**BACKFILL-ONLY.** Every fetcher in this module is for historical
backfill jobs only — none of them may be used for tip-following. Old
Faithful CAR archives and the BigQuery public Solana dataset ingest with
a lag (BigQuery is multi-day), and TronScan API is a best-effort public
tier. Treating any of them as a live source would poison the indexer's
tip pipeline. The indexer loop does NOT wire these in; a backfill job
calls them directly with an explicit historical window.

Design constraints
------------------
* No hard external deps at import time: ``google-cloud-bigquery`` is
  imported lazily inside ``fetch_solana_bigquery``; Old Faithful is
  driven through the ``faithful-cli`` subprocess; TronScan uses httpx
  (a project dependency) with a guarded import.
* Every fetcher degrades gracefully: missing binary / missing library /
  timeout / non-zero exit returns ``None`` or ``[]`` with a log + metric
  — a backfill job skips the lane instead of crashing.
* ``source``/``provenance`` labels on rows so backfilled data can never
  be confused with lake or live-RPC data downstream.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:  # pragma: no cover — optional dependency
    httpx = None  # type: ignore[assignment]
    HTTPX_AVAILABLE = False

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# BigQuery public Solana dataset. NOTE: the public export ingests with a
# multi-day lag (usually 2–5 days behind the chain tip) — the reason this
# lane is BACKFILL-ONLY and must never be used for tip-following.
BIGQUERY_SOLANA_DATASET = "bigquery-public-data.crypto_solana_mainnet_us"

# TronScan public API (https://docs.tronscan.org). Free tier works without
# a key at low volume; TRON-PRO-API-KEY header raises the rate limit.
TRONSCAN_API_BASE = "https://apilist.tronscanapi.com/api"

# Old Faithful CLI binary (rpcpool/yellowstone-faithful, RFC-stage CAR
# archive tool). This lane is a MONITORED FALLBACK, never a sole
# dependency — if the CAR archive or CLI is unavailable the backfill
# falls through to the lake / live-RPC lanes.
FAITHFUL_CLI_BIN = "faithful-cli"

# RPC method → faithful-cli subcommand. Old Faithful's CLI surface is
# RFC-stage and may change; keep the mapping isolated here so a flag
# drift is a one-line fix.
_FAITHFUL_COMMANDS: dict[str, str] = {
    "getBlock": "get_block",
    "getTransaction": "get_transaction",
    "getSignaturesForAddress": "get_signatures_for_address",
}


def _faithful_bin_ok(faithful_cli_path: str) -> bool:
    """True when the faithful-cli binary is resolvable.

    Accepts an absolute path or a name resolvable via PATH.
    """
    if not faithful_cli_path:
        return False
    path = Path(faithful_cli_path)
    if path.is_file() and os.access(path, os.X_OK):
        return True
    return shutil.which(faithful_cli_path) is not None


async def fetch_solana_old_faithful(
    method: str,
    params: list[Any] | None = None,
    *,
    faithful_cli_path: str = FAITHFUL_CLI_BIN,
    timeout_seconds: float = 180.0,
    car_roots: list[str] | None = None,
) -> dict[str, Any] | None:
    """Fetch Solana data from an Old Faithful CAR archive via ``faithful-cli``.

    BACKFILL-ONLY. Old Faithful is an RFC-stage CAR-format archive of
    finalized Solana blocks; it serves confirmed historical data and must
    never be treated as a tip source (the archive itself lags the live
    cluster by design). Backfill jobs call this for slots well past the
    finality horizon.

    Supported methods (mapped to ``faithful-cli`` subcommands):
    ``getBlock``, ``getTransaction``, ``getSignaturesForAddress``.

    Args:
        method: RPC-style method name (see ``_FAITHFUL_COMMANDS``).
        params: Positional params rendered as strings (slot / tx hash /
            address, etc.).
        faithful_cli_path: Path (or PATH name) of the ``faithful-cli``
            binary. Defaults to ``faithful-cli`` on PATH.
        timeout_seconds: Hard cap on the subprocess lifetime.
        car_roots: Optional CAR roots passed as ``--car <path>`` when the
            CLI requires an explicit archive location.

    Returns:
        Parsed JSON result dict, or None when the binary is missing, the
        method is unsupported, the process times out / exits non-zero, or
        stdout is not JSON. Never raises.

    Integration note: this lane is the monitored fallback inside the
    archive rung — wire it into a backfill job guarded by a health flag
    (e.g. ``archive_solana_old_faithful_enabled``) and alert when it
    fails repeatedly; it must never become the sole dependency for any
    data class.
    """
    subcommand = _FAITHFUL_COMMANDS.get(method)
    if subcommand is None:
        metrics_incr("archives_faithful_unsupported")
        logger.warning("archives_faithful_unsupported", method=method)
        return None
    if not _faithful_bin_ok(faithful_cli_path):
        metrics_incr("archives_faithful_missing")
        logger.warning("archives_faithful_missing", bin=faithful_cli_path)
        return None

    cmd = [faithful_cli_path, subcommand]
    cmd += [str(p) for p in (params or [])]
    for root in car_roots or []:
        cmd += ["--car", str(root)]
    cmd += ["--output", "json"]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
    except (TimeoutError, OSError, subprocess.SubprocessError) as exc:
        metrics_incr("archives_faithful_errors")
        logger.warning("archives_faithful_failed", method=method, error=str(exc)[:150])
        return None
    if proc.returncode != 0:
        metrics_incr("archives_faithful_errors")
        logger.warning(
            "archives_faithful_exit",
            method=method,
            returncode=proc.returncode,
            error=(stderr or b"").decode(errors="replace")[-200:],
        )
        return None
    try:
        payload = json.loads(stdout.decode(errors="replace"))
    except (ValueError, UnicodeDecodeError) as exc:
        metrics_incr("archives_faithful_errors")
        logger.warning("archives_faithful_bad_json", method=method, error=str(exc)[:100])
        return None
    if not isinstance(payload, dict):
        metrics_incr("archives_faithful_errors")
        logger.warning("archives_faithful_shape", method=method)
        return None
    metrics_incr("archives_faithful_ok")
    return payload


async def fetch_solana_bigquery(
    query: str,
    *,
    project: str | None = None,
    credentials_path: str | None = None,
    timeout_seconds: float = 300.0,
    max_results: int = 1000,
) -> list[dict[str, Any]]:
    """Query the BigQuery public Solana dataset (backfill only).

    BACKFILL-ONLY. The public export
    (``bigquery-public-data.crypto_solana_mainnet_us``) ingests with a
    multi-day lag (usually 2–5 days behind tip), so results are safe only
    for historical windows — never for tip-following. Recommended tables:
    ``transactions``, ``blocks``, ``token_transfers``, ``accounts``,
    ``instructions``, ``transfers``.

    Args:
        query: Read-only SQL. Only ``SELECT`` statements are accepted
            (case-insensitive) as a guard against accidental writes.
        project: GCP project for billing/credentials resolution (None =
            ADC / default project).
        credentials_path: Optional path to a service-account JSON file.
            When omitted, Application Default Credentials are used.
        timeout_seconds: Hard cap on the BigQuery job result wait.
        max_results: Max rows returned (cheap safety cap on memory).

    Returns:
        Rows as dicts (BigQuery column → value), [] when the client
        library is missing, the query is not a SELECT, the job fails, or
        the wait times out. Never raises.

    Integration note: this lane is the workhorse for deep Solana
    backfill — it is free, has no per-request key, and covers the whole
    mainnet history, but the ingestion lag means a backfill job must
    bound its window to ``now - ~5 days`` and rely on the lake / live-RPC
    lanes for anything fresher. Requires
    ``pip install google-cloud-bigquery`` — imported lazily so importing
    this module never depends on it.
    """
    if not query or not query.lstrip().upper().startswith("SELECT"):
        metrics_incr("archives_bigquery_rejected")
        logger.warning("archives_bigquery_rejected")
        return []

    try:
        from google.cloud import bigquery  # type: ignore[attr-defined]  # lazy import — optional dep
    except ImportError:  # pragma: no cover — optional dependency
        metrics_incr("archives_bigquery_missing_lib")
        logger.warning("archives_bigquery_missing_lib", hint="pip install google-cloud-bigquery")
        return []

    try:
        if credentials_path:
            from google.oauth2 import service_account  # lazy import

            credentials = service_account.Credentials.from_service_account_file(  # type: ignore[no-untyped-call]
                credentials_path,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            client = bigquery.Client(project=project, credentials=credentials)
        else:
            client = bigquery.Client(project=project)

        def _run() -> list[dict[str, Any]]:
            job = client.query(query)
            rows = list(job.result(timeout=timeout_seconds))[: max(1, int(max_results))]
            return [{key: row[key] for key in row} for row in rows]

        rows = await asyncio.to_thread(_run)
    except Exception as exc:  # noqa: BLE001 — backfill lanes are best-effort
        metrics_incr("archives_bigquery_errors")
        logger.warning("archives_bigquery_failed", error=str(exc)[:200])
        return []

    metrics_incr("archives_bigquery_ok", len(rows))
    return rows


async def fetch_tron_tronscan(
    tx_hash: str,
    *,
    api_key: str | None = None,
    base_url: str = TRONSCAN_API_BASE,
    timeout_seconds: float = 30.0,
) -> dict[str, Any] | None:
    """Fetch decoded TRON transaction detail from the TronScan public API.

    BACKFILL-ONLY. TronScan is a best-effort public explorer API — it is
    used as a fallback for decoded transaction detail when the lake /
    live-RPC lanes cannot produce a full decode (e.g. contract calls with
    no transfer event). It must never be used for tip-following; the free
    tier is rate-limited and the indexer's live path has its own sources.

    Calls ``GET {base_url}/transaction-info?hash={tx_hash}`` with an
    optional ``TRON-PRO-API-KEY`` header.

    Args:
        tx_hash: TRON transaction hash (hex, with or without 0x).
        api_key: TronScan PRO API key. When None, the ``TRON_PRO_API_KEY``
            environment variable is consulted; neither is required at low
            volume.
        base_url: TronScan API base URL (defaults to
            ``https://apilist.tronscanapi.com/api``).
        timeout_seconds: HTTP timeout.

    Returns:
        The decoded transaction-info JSON as a dict, or None when httpx
        is unavailable, the request fails / times out, the API returns a
        non-dict body, or the API reports an error. Never raises.

    Integration note: TronScan returns decoded detail (owner, contract
    calls, token transfers) that raw TronGrid nodes do not. Backfill jobs
    should use this lane to enrich ``tron`` rows during historical
    windows, tag rows with ``source="tron_archive"``, and rate-limit the
    lane (the free tier is a few requests per second).
    """
    key = api_key or os.getenv("TRON_PRO_API_KEY")
    if not HTTPX_AVAILABLE:
        metrics_incr("archives_tronscan_missing_lib")
        logger.warning("archives_tronscan_missing_lib", hint="pip install httpx")
        return None

    url = f"{base_url.rstrip('/')}/transaction-info?hash={str(tx_hash).lstrip('0x')}"
    headers = {"TRON-PRO-API-KEY": key} if key else {}
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001 — backfill lanes are best-effort
        metrics_incr("archives_tronscan_errors")
        logger.warning("archives_tronscan_failed", tx_hash=str(tx_hash)[:20], error=str(exc)[:150])
        return None
    if not isinstance(payload, dict):
        metrics_incr("archives_tronscan_errors")
        logger.warning("archives_tronscan_shape", tx_hash=str(tx_hash)[:20])
        return None
    if payload.get("error") or payload.get("result") is False:
        metrics_incr("archives_tronscan_errors")
        logger.warning(
            "archives_tronscan_api_error",
            tx_hash=str(tx_hash)[:20],
            error=str(payload.get("error") or payload.get("message", ""))[:150],
        )
        return None
    metrics_incr("archives_tronscan_ok")
    return payload


__all__ = [
    "BIGQUERY_SOLANA_DATASET",
    "FAITHFUL_CLI_BIN",
    "TRONSCAN_API_BASE",
    "fetch_solana_bigquery",
    "fetch_solana_old_faithful",
    "fetch_tron_tronscan",
]
