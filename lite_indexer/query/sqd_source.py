"""SQDSource — on-demand history via the SQD Portal API (portal.sqd.dev).

Sixth data source in the query family. Covers what the local index + B2 +
BigLake + Envio HyperRPC cannot: Solana (per-owner token balance changes),
Tron, Bitcoin, EVM, and Hyperliquid fills — arbitrary historical ranges
with server-side filtering and JSONL streaming.

This module is the FACADE + core portal client (transport, throttling,
``head``/``stream``). The per-chain readers live in sibling modules
(:mod:`sqd_solana`, :mod:`sqd_tron`, :mod:`sqd_evm`) and are re-exposed as
``SQDSource`` methods so the public surface is unchanged.

Auth: optional API key (``SQD_API_KEY``) sent as ``Authorization: Bearer``.
The public portal currently serves these datasets **without enforcing
keys** (verified 2026-09-16: valid/invalid/absent keys all return 200);
the key is still sent when present, for the dedicated pool once enforced.

Transport: ``httpx`` — plain urllib is Cloudflare-blocked (error 1010).

Rate limits: the public portal is ~20 req / 10 s. The client serializes
calls (``SQD_MIN_INTERVAL``, default 0.55s) and backs off on 429/529.

Stream semantics: a response is a *batch* that may end before
``toBlock`` (worker boundaries / dataset head). :meth:`SQDSource.stream`
paginates (``fromBlock = last + 1``) until the bound is reached or HTTP
204 signals the dataset height.

Env:
    SQD_API_KEY         optional bearer key
    SQD_MIN_INTERVAL    seconds between calls (default 0.55)

CLI:
    python -m lite_indexer.query.sqd_source head solana-mainnet
    python -m lite_indexer.query.sqd_source token-changes <owner> --from-slot N --to-slot M
    python -m lite_indexer.query.sqd_source stream hyperliquid-fills --from-block N --to-block M \
        --fields '{"block":{"number":true}}'
"""

from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query import sqd_evm, sqd_solana, sqd_tron, sqd_tron_addr
from lite_indexer.query.sqd_common import ERC20_TRANSFER_TOPIC, ts

__all__ = ["ERC20_TRANSFER_TOPIC", "SQDPortalError", "SQDSource"]

logger = get_logger(__name__)


class SQDPortalError(RuntimeError):
    """Raised when the Portal returns a non-retryable error."""


class SQDSource:
    """Read-only on-demand access to the SQD Portal.

    Args:
        api_key: Bearer key; defaults to ``SQD_API_KEY``.
        min_interval: Minimum seconds between HTTP calls (0 in tests).
        retry_backoffs: Sleeps before retrying 429/529 responses.
        transport: Optional transport override
            ``(method, url, payload, timeout) -> (status, text)`` for tests.
    """

    PORTAL = "https://portal.sqd.dev"

    #: Local chain name → Portal dataset.
    DATASETS: dict[str, str] = {
        "solana": "solana-mainnet",
        "tron": "tron-mainnet",
        "ethereum": "ethereum-mainnet",
        "bitcoin": "bitcoin-mainnet",
        "hyperliquid": "hyperliquid-fills",
    }

    #: Dataset → request ``type`` enum value.
    DATASET_TYPES: dict[str, str] = {
        "solana-mainnet": "solana",
        "tron-mainnet": "tron",
        "ethereum-mainnet": "evm",
        "arbitrum-one": "evm",
        "arbitrum-nova": "evm",
        "base-mainnet": "evm",
        "polygon-mainnet": "evm",
        "avalanche-mainnet": "evm",
        "optimism-mainnet": "evm",
        "gnosis-mainnet": "evm",
        "celo-mainnet": "evm",
        "zksync-mainnet": "evm",
        "scroll-mainnet": "evm",
        "linea-mainnet": "evm",
        "mantle-mainnet": "evm",
        "bitcoin-mainnet": "bitcoin",
        "hyperliquid-fills": "hyperliquidFills",
    }

    #: EVM chains → SQD dataset slug (portal serves FULL-history traces).
    EVM_DATASETS: dict[str, str] = {
        "ethereum": "ethereum-mainnet",
        "arbitrum": "arbitrum-one",
        "arbitrum-nova": "arbitrum-nova",
        "base": "base-mainnet",
        "polygon": "polygon-mainnet",
        "avalanche": "avalanche-mainnet",
        "optimism": "optimism-mainnet",
        "gnosis": "gnosis-mainnet",
        "celo": "celo-mainnet",
        "zksync": "zksync-mainnet",
        "scroll": "scroll-mainnet",
        "linea": "linea-mainnet",
        "mantle": "mantle-mainnet",
    }

    #: Epoch → datetime helper (kept for backwards compatibility).
    _ts = staticmethod(ts)

    #: Tron address helpers (re-exported for callers/tests).
    _tron_hex40 = staticmethod(sqd_tron_addr.tron_hex40)
    _tron_topic = staticmethod(sqd_tron_addr.tron_topic)
    _tron_base58 = staticmethod(sqd_tron_addr.tron_base58)

    def __init__(
        self,
        *,
        api_key: str | None = None,
        min_interval: float | None = None,
        retry_backoffs: tuple[float, ...] = (2.0, 5.0, 10.0),
        transport: Any | None = None,
    ) -> None:
        self._key = (api_key if api_key is not None else os.getenv("SQD_API_KEY", "")).strip()
        if min_interval is None:
            try:
                min_interval = float(os.getenv("SQD_MIN_INTERVAL", "0.55"))
            except ValueError:
                min_interval = 0.55
        self._min_interval = max(0.0, float(min_interval))
        self._backoffs = tuple(retry_backoffs)
        self._transport = transport
        self._last_call = 0.0

    # ── Availability / transport ─────────────────────────────────────

    def available(self) -> bool:
        if self._transport is not None:
            return True
        try:
            import httpx  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import
        except ImportError:
            return False
        return True

    def _throttle(self) -> None:
        if self._min_interval <= 0:
            return
        wait = self._min_interval - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _default_transport(
        self, method: str, url: str, payload: dict[str, Any] | None, timeout: float
    ) -> tuple[int, str]:
        import httpx  # pylint: disable=import-outside-toplevel

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
            ),
            # httpx's gzip decoder can trip over the portal's compressed
            # JSONL streams ("decompressobj multiple times") — ask for
            # identity encoding and skip decompression entirely.
            "Accept-Encoding": "identity",
        }
        if self._key:
            headers["Authorization"] = f"Bearer {self._key}"
        with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
            if method == "GET":
                resp = client.get(url)
            else:
                resp = client.post(url, json=payload)
        return resp.status_code, resp.text

    def _request(
        self, method: str, url: str, payload: dict[str, Any] | None = None, *, timeout: float = 60.0
    ) -> tuple[int, str]:
        attempts = len(self._backoffs) + 1
        for attempt in range(attempts):
            self._throttle()
            try:
                status, text = (self._transport or self._default_transport)(method, url, payload, timeout)
            except Exception as exc:  # noqa: BLE001 — transport errors are retried below
                if attempt == attempts - 1:
                    raise SQDPortalError(f"transport failed: {exc}") from exc
                time.sleep(self._backoffs[attempt])
                continue
            if status in (429, 529):
                if attempt == attempts - 1:
                    raise SQDPortalError(f"portal overloaded (HTTP {status}) after {attempts} attempts")
                time.sleep(self._backoffs[attempt])
                continue
            return status, text
        raise SQDPortalError("unreachable")  # pragma: no cover

    # ── Endpoints ────────────────────────────────────────────────────

    def head(self, dataset: str) -> int | None:
        """Latest block number for a dataset (by name or local chain name)."""
        name = self.DATASETS.get(dataset, dataset)
        status, text = self._request("GET", f"{self.PORTAL}/datasets/{name}/head", timeout=30.0)
        if status != 200:
            return None
        try:
            return int(json.loads(text).get("number"))
        except (ValueError, TypeError):
            return None

    def stream(
        self,
        dataset: str,
        from_block: int,
        to_block: int | None = None,
        *,
        fields: dict[str, Any],
        selectors: dict[str, Any] | None = None,
        max_batches: int = 32,
        timeout: float = 90.0,
    ) -> list[dict[str, Any]]:
        """Paginated JSONL stream. Returns the concatenated block objects."""
        name = self.DATASETS.get(dataset, dataset)
        type_value = self.DATASET_TYPES.get(name)
        if type_value is None:
            raise ValueError(f"unknown SQD dataset: {dataset}")
        # TODO(lake-router): full rotation wiring pending; advisory gate only.
        from lite_indexer.lakes.router import LakeProviderRouter

        router = LakeProviderRouter()
        prov = router.select(dataset, "sqd")
        if prov is None:
            raise SQDPortalError(f"lake router: no provider available for {dataset} (quota/health)")
        blocks: list[dict[str, Any]] = []
        cursor = int(from_block)
        for _ in range(max(1, int(max_batches))):
            body: dict[str, Any] = {"type": type_value, "fromBlock": cursor, "fields": fields}
            if to_block is not None:
                body["toBlock"] = int(to_block)
            if selectors:
                body.update(selectors)
            status, text = self._request("POST", f"{self.PORTAL}/datasets/{name}/stream", body, timeout=timeout)
            if status == 204:  # above dataset height — done
                break
            if status != 200:
                router.report_failure(prov.name, error=f"HTTP {status}")
                raise SQDPortalError(f"portal stream error HTTP {status}: {text[:160]}")
            batch = [json.loads(line) for line in text.splitlines() if line.strip()]
            if not batch:
                break
            blocks.extend(batch)
            last = max(int(b.get("header", {}).get("number", cursor)) for b in batch)
            if to_block is not None and last >= int(to_block):
                break
            cursor = last + 1
        router.report_success(prov.name)
        return blocks

    # ── Per-chain readers ────────────────────────────────────────────
    # Implemented in sibling modules and bound here as methods so the
    # public surface (names, keywords, return shapes) is unchanged.

    solana_owner_activity = sqd_solana.solana_owner_activity
    solana_token_balances_for_accounts = sqd_solana.solana_token_balances_for_accounts
    tron_trc20_transfers = sqd_tron.tron_trc20_transfers
    tron_internal_transfers = sqd_tron.tron_internal_transfers
    evm_traces = sqd_evm.evm_traces

    # ── Hyperliquid: per-user fills ──────────────────────────────────

    def hyperliquid_owner_fills(
        self,
        user: str,
        from_block: int,
        to_block: int,
        *,
        limit: int = 200,
        max_batches: int = 48,
    ) -> list[dict[str, Any]]:
        """Fill records for a Hyperliquid user (``user`` selector filter)."""
        if not self.available():
            return []
        fields = {
            "block": {"number": True, "timestamp": True},
            "fill": {"user": True, "coin": True, "px": True, "sz": True, "side": True, "dir": True, "fee": True},
        }
        blocks = self.stream(
            "hyperliquid-fills",
            from_block,
            to_block,
            fields=fields,
            selectors={"fills": [{"user": [user]}]},
            max_batches=max_batches,
        )
        rows: list[dict[str, Any]] = []
        for block in blocks:
            header = block.get("header", {})
            bn = int(header.get("number", 0))
            block_ts = ts(header.get("timestamp"))
            for rec in block.get("fills", []) or []:
                if user and str(rec.get("user") or "").lower() != user.lower():
                    continue
                rows.append(
                    {
                        "chain": "hyperliquid",
                        "block_number": bn,
                        "block_timestamp": block_ts,
                        "user": rec.get("user") or "",
                        "coin": rec.get("coin") or "",
                        "px": rec.get("px") or "",
                        "sz": rec.get("sz") or "",
                        "side": rec.get("side") or "",
                        "direction": rec.get("dir") or "",
                        "fee": rec.get("fee") or "",
                        "ingested_at": datetime.now(UTC),
                    }
                )
        rows.sort(key=lambda r: r["block_number"], reverse=True)
        return rows[: max(1, int(limit))]

if __name__ == "__main__":
    from lite_indexer.query.sqd_cli import cli

    raise SystemExit(cli())
