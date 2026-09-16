"""SQDSource — on-demand history via the SQD Portal API (portal.sqd.dev).

Sixth data source in the query family. Covers what the local index + B2 +
BigLake + Envio HyperRPC cannot: Solana (per-owner token balance changes),
Tron, Bitcoin, EVM, and Hyperliquid fills — arbitrary historical ranges
with server-side filtering and JSONL streaming.

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

import argparse
import json
import os
import time
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger

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
        "bitcoin-mainnet": "bitcoin",
        "hyperliquid-fills": "hyperliquidFills",
    }

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
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0 Safari/537.36"
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
                status, text = (self._transport or self._default_transport)(
                    method, url, payload, timeout
                )
            except Exception as exc:  # noqa: BLE001 — transport errors are retried below
                if attempt == attempts - 1:
                    raise SQDPortalError(f"transport failed: {exc}") from exc
                time.sleep(self._backoffs[attempt])
                continue
            if status in (429, 529):
                if attempt == attempts - 1:
                    raise SQDPortalError(
                        f"portal overloaded (HTTP {status}) after {attempts} attempts"
                    )
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
        blocks: list[dict[str, Any]] = []
        cursor = int(from_block)
        for _ in range(max(1, int(max_batches))):
            body: dict[str, Any] = {"type": type_value, "fromBlock": cursor, "fields": fields}
            if to_block is not None:
                body["toBlock"] = int(to_block)
            if selectors:
                body.update(selectors)
            status, text = self._request(
                "POST", f"{self.PORTAL}/datasets/{name}/stream", body, timeout=timeout
            )
            if status == 204:  # above dataset height — done
                break
            if status != 200:
                raise SQDPortalError(f"portal stream error HTTP {status}: {text[:160]}")
            batch = [json.loads(line) for line in text.splitlines() if line.strip()]
            if not batch:
                break
            blocks.extend(batch)
            last = max(int(b.get("header", {}).get("number", cursor)) for b in batch)
            if to_block is not None and last >= int(to_block):
                break
            cursor = last + 1
        return blocks

    # ── Solana: per-owner token balance changes ──────────────────────

    @staticmethod
    def _ts(value: Any) -> datetime | None:
        try:
            return datetime.fromtimestamp(int(value), tz=UTC)
        except (TypeError, ValueError):
            return None

    def solana_owner_activity(
        self,
        owner: str,
        from_slot: int,
        to_slot: int,
        *,
        limit: int = 500,
        max_batches: int = 48,
    ) -> list[dict[str, Any]]:
        """Instructions mentioning ``owner`` — wallet-scoped Solana history.

        Portal's ``mentionsAccount`` selector is the supported owner filter.
        ``preOwner``/``postOwner`` are **not** filterable (verified
        2026-09-16: unknown filter keys are dropped and the response
        degrades to worker-boundary blocks with no records).
        """
        fields = {
            "block": {"number": True, "timestamp": True},
            "instruction": {"programId": True, "accounts": True, "data": True, "transactionIndex": True},
        }
        blocks = self.stream(
            "solana-mainnet",
            from_slot,
            to_slot,
            fields=fields,
            selectors={"instructions": [{"mentionsAccount": [owner]}]},
            max_batches=max_batches,
        )
        rows: list[dict[str, Any]] = []
        for block in blocks:
            header = block.get("header", {})
            slot = int(header.get("number", 0))
            ts = self._ts(header.get("timestamp"))
            for rec in block.get("instructions", []) or []:
                accounts = rec.get("accounts") or []
                if owner not in accounts:
                    continue
                rows.append(
                    {
                        "chain": "solana",
                        "slot": slot,
                        "block_timestamp": ts,
                        "tx_hash": "",
                        "transaction_index": rec.get("transactionIndex"),
                        "program_id": rec.get("programId") or "",
                        "accounts": accounts,
                        "account_count": len(accounts),
                        "data": rec.get("data") or "",
                        "owner": owner,
                        "ingested_at": datetime.now(UTC),
                    }
                )
        rows.sort(key=lambda r: (r["slot"], r.get("transaction_index") or 0), reverse=True)
        return rows[: max(1, int(limit))]

    def solana_token_balances_for_accounts(
        self,
        accounts: list[str],
        from_slot: int,
        to_slot: int,
        *,
        limit: int = 500,
        max_batches: int = 48,
    ) -> list[dict[str, Any]]:
        """Token balance diffs for token accounts (``account`` filter is supported).

        Rows use the ``solana_transfers`` shape; ``tx_hash`` is empty
        (not part of the portal record).
        """
        if not accounts:
            return []
        fields = {
            "block": {"number": True, "timestamp": True},
            "tokenBalance": {
                "account": True,
                "preMint": True,
                "postMint": True,
                "preAmount": True,
                "postAmount": True,
                "preOwner": True,
                "postOwner": True,
                "transactionIndex": True,
            },
        }
        blocks = self.stream(
            "solana-mainnet",
            from_slot,
            to_slot,
            fields=fields,
            selectors={"tokenBalances": [{"account": list(accounts)}]},
            max_batches=max_batches,
        )
        seen: set[tuple[Any, ...]] = set()
        rows: list[dict[str, Any]] = []
        for block in blocks:
            header = block.get("header", {})
            slot = int(header.get("number", 0))
            ts = self._ts(header.get("timestamp"))
            for rec in block.get("tokenBalances", []) or []:
                pre = int(rec.get("preAmount") or 0)
                post = int(rec.get("postAmount") or 0)
                key = (slot, rec.get("transactionIndex"), rec.get("account"), pre, post)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "chain": "solana",
                        "slot": slot,
                        "block_timestamp": ts,
                        "tx_hash": "",
                        "token_account": rec.get("account") or "",
                        "mint": rec.get("postMint") or rec.get("preMint") or "",
                        "owner": rec.get("postOwner") or rec.get("preOwner") or "",
                        "pre_amount": str(pre),
                        "post_amount": str(post),
                        "delta": str(post - pre),
                        "decimals": 0,
                        "token_symbol": "",
                        "token_name": "",
                        "ingested_at": datetime.now(UTC),
                    }
                )
        rows.sort(key=lambda r: (r["slot"], str(r.get("token_account"))), reverse=True)
        return rows[: max(1, int(limit))]


# ── CLI ──────────────────────────────────────────────────────────────


def _cmd_head(args: argparse.Namespace) -> int:
    src = SQDSource()
    number = src.head(args.dataset)
    print(json.dumps({"dataset": args.dataset, "head": number}))  # noqa: T201
    return 0 if number is not None else 1


def _cmd_stream(args: argparse.Namespace) -> int:
    src = SQDSource()
    fields = json.loads(args.fields)
    selectors = json.loads(args.selectors) if args.selectors else None
    blocks = src.stream(
        args.dataset,
        int(args.from_block, 0),
        int(args.to_block, 0) if args.to_block else None,
        fields=fields,
        selectors=selectors,
        max_batches=args.max_batches,
    )
    print(json.dumps(blocks[: args.limit], default=str, indent=2))  # noqa: T201
    return 0


def _cmd_owner_activity(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.solana_owner_activity(args.owner, int(args.from_slot, 0), int(args.to_slot, 0), limit=args.limit)
    print(json.dumps(rows[: args.limit], default=str, indent=2))  # noqa: T201
    return 0


def _cmd_token_balances(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.solana_token_balances_for_accounts(
        list(args.accounts), int(args.from_slot, 0), int(args.to_slot, 0), limit=args.limit
    )
    print(json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


def _cli() -> int:
    parser = argparse.ArgumentParser(prog="sqd_source", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_h = sub.add_parser("head", help="latest block of a dataset")
    p_h.add_argument("dataset")
    p_h.set_defaults(fn=_cmd_head)

    p_s = sub.add_parser("stream", help="raw paginated stream")
    p_s.add_argument("dataset")
    p_s.add_argument("--from-block", required=True)
    p_s.add_argument("--to-block", default=None)
    p_s.add_argument("--fields", required=True, help='JSON, e.g. \'{"block":{"number":true}}\'')
    p_s.add_argument(
        "--selectors",
        default=None,
        help='JSON, e.g. \'{"tokenBalances":[{"postOwner":["addr"]}]}\'',
    )
    p_s.add_argument("--max-batches", type=int, default=4)
    p_s.add_argument("--limit", type=int, default=20)
    p_s.set_defaults(fn=_cmd_stream)

    p_o = sub.add_parser("owner-activity", help="Solana instructions mentioning an owner")
    p_o.add_argument("owner")
    p_o.add_argument("--from-slot", required=True)
    p_o.add_argument("--to-slot", required=True)
    p_o.add_argument("--limit", type=int, default=200)
    p_o.set_defaults(fn=_cmd_owner_activity)

    p_b = sub.add_parser("token-balances", help="Solana token balance diffs for token accounts")
    p_b.add_argument("accounts", nargs="+")
    p_b.add_argument("--from-slot", required=True)
    p_b.add_argument("--to-slot", required=True)
    p_b.add_argument("--limit", type=int, default=200)
    p_b.set_defaults(fn=_cmd_token_balances)

    args = parser.parse_args()
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(_cli())
