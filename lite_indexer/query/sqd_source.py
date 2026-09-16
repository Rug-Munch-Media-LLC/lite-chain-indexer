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

#: ERC-20 ``Transfer`` event topic (Tron logs carry it without the 0x prefix).
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


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
        """Epoch → datetime; auto-detects milliseconds (Tron uses ms)."""
        try:
            seconds = int(value)
        except (TypeError, ValueError):
            return None
        if seconds > 10**12:
            seconds //= 1000
        try:
            return datetime.fromtimestamp(seconds, tz=UTC)
        except (OverflowError, OSError, ValueError):
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


    # ── Tron: TRC-20 transfers + internal transactions ───────────────

    @staticmethod
    def _tron_hex40(address: str) -> str:
        """Normalize a Tron address (``0x..`` / ``41..`` / plain hex) to 40 hex chars."""
        a = address.strip().lower()
        if a.startswith("0x"):
            a = a[2:]
        if a.startswith("41") and len(a) == 42:
            a = a[2:]
        if len(a) != 40 or any(c not in "0123456789abcdef" for c in a):
            raise ValueError(f"invalid tron address: {address!r}")
        return a

    @classmethod
    def _tron_topic(cls, address: str) -> str:
        return "0" * 24 + cls._tron_hex40(address)

    @staticmethod
    def _tron_base58(address: str) -> str:
        """Tron hex (40 chars, optional 0x/41) → base58check T-address."""
        import hashlib

        hex40 = address.strip().lower()
        if hex40.startswith("0x"):
            hex40 = hex40[2:]
        if hex40.startswith("41") and len(hex40) == 42:
            hex40 = hex40[2:]
        payload = bytes.fromhex("41" + hex40)
        checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
        num = int.from_bytes(payload + checksum, "big")
        alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        out = ""
        while num:
            num, rem = divmod(num, 58)
            out = alphabet[rem] + out
        return out

    def tron_trc20_transfers(
        self,
        address: str,
        from_block: int,
        to_block: int,
        *,
        limit: int = 200,
        max_batches: int = 48,
    ) -> list[dict[str, Any]]:
        """TRC-20 transfers where ``address`` is sender or recipient.

        Logs are filtered by the (padded) wallet in ``topic1``/``topic2``
        with the ERC-20 ``Transfer`` topic — verified supported 2026-09-16.
        Rows use the ``evm_transfers`` shape (Tron hex addresses, base58
        token address to match the warehouse).
        """
        if not self.available():
            return []
        topic = self._tron_topic(address)
        topic0 = ERC20_TRANSFER_TOPIC[2:]  # Tron topics carry no 0x prefix
        fields = {
            "block": {"number": True, "timestamp": True},
            "log": {"address": True, "topics": True, "data": True, "transactionIndex": True},
        }
        seen: set[tuple[Any, ...]] = set()
        rows: list[dict[str, Any]] = []
        for side_key in ("topic1", "topic2"):
            blocks = self.stream(
                "tron-mainnet",
                from_block,
                to_block,
                fields=fields,
                selectors={"logs": [{"topic0": [topic0], side_key: [topic]}]},
                max_batches=max_batches,
            )
            for block in blocks:
                header = block.get("header", {})
                bn = int(header.get("number", 0))
                ts = self._ts(header.get("timestamp"))
                for rec in block.get("logs", []) or []:
                    topics = rec.get("topics") or []
                    if len(topics) < 3:
                        continue
                    frm = ("0x" + str(topics[1])[-40:]).lower()
                    to = ("0x" + str(topics[2])[-40:]).lower()
                    data = rec.get("data") or ""
                    try:
                        value = str(int(data, 16))
                    except (TypeError, ValueError):
                        value = "0"
                    key = (bn, rec.get("transactionIndex"), frm, to, data)
                    if key in seen:
                        continue
                    seen.add(key)
                    try:
                        token = self._tron_base58(str(rec.get("address") or ""))
                    except ValueError:
                        token = str(rec.get("address") or "")
                    rows.append(
                        {
                            "chain": "tron",
                            "block_number": bn,
                            "tx_hash": "",
                            "from_address": frm,
                            "to_address": to,
                            "token_address": token,
                            "value": value,
                            "block_timestamp": ts,
                            "log_index": int(rec.get("transactionIndex") or 0),
                            "ingested_at": datetime.now(UTC),
                        }
                    )
        rows.sort(key=lambda r: (r["block_number"], r["log_index"]), reverse=True)
        return rows[: max(1, int(limit))]

    def tron_internal_transfers(
        self,
        address: str,
        from_block: int,
        to_block: int,
        *,
        limit: int = 200,
        max_batches: int = 48,
    ) -> list[dict[str, Any]]:
        """Native (TRX) internal transactions touching ``address``.

        Uses the ``transferTo`` / ``caller`` filters (41-prefixed hex) and
        maps to the ``internal_transfers`` row shape.
        """
        if not self.available():
            return []
        addr41 = "41" + self._tron_hex40(address)
        fields = {
            "block": {"number": True, "timestamp": True},
            "internalTransaction": {
                "callerAddress": True,
                "transferToAddress": True,
                "callValueInfo": True,
                "internalTransactionIndex": True,
                "transactionIndex": True,
                "rejected": True,
            },
        }
        seen: set[tuple[Any, ...]] = set()
        rows: list[dict[str, Any]] = []
        for key_name in ("transferTo", "caller"):
            blocks = self.stream(
                "tron-mainnet",
                from_block,
                to_block,
                fields=fields,
                selectors={"internalTransactions": [{key_name: [addr41]}]},
                max_batches=max_batches,
            )
            for block in blocks:
                header = block.get("header", {})
                bn = int(header.get("number", 0))
                ts = self._ts(header.get("timestamp"))
                for rec in block.get("internalTransactions", []) or []:
                    caller = str(rec.get("callerAddress") or "")
                    to = str(rec.get("transferToAddress") or "")
                    value = 0
                    for ci in rec.get("callValueInfo") or []:
                        try:
                            value += int(ci.get("callValue") or 0)
                        except (TypeError, ValueError):
                            continue
                    trace = f"{rec.get('transactionIndex', 0)}.{rec.get('internalTransactionIndex', 0)}"
                    rk = (bn, trace, caller, to)
                    if rk in seen:
                        continue
                    seen.add(rk)
                    rows.append(
                        {
                            "chain": "tron",
                            "block_number": bn,
                            "block_timestamp": ts,
                            "tx_hash": "",
                            "trace_address": trace,
                            "from_address": ("0x" + caller[2:]).lower() if caller.startswith("41") else caller.lower(),
                            "to_address": ("0x" + to[2:]).lower() if to.startswith("41") else to.lower(),
                            "value_wei": str(value),
                            "call_type": "call",
                            "gas_used": 0,
                            "error": "rejected" if rec.get("rejected") else "",
                            "input_method_id": "",
                            "ingested_at": datetime.now(UTC),
                        }
                    )
        rows.sort(key=lambda r: (r["block_number"], r["trace_address"]), reverse=True)
        return rows[: max(1, int(limit))]

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
            ts = self._ts(header.get("timestamp"))
            for rec in block.get("fills", []) or []:
                if user and str(rec.get("user") or "").lower() != user.lower():
                    continue
                rows.append(
                    {
                        "chain": "hyperliquid",
                        "block_number": bn,
                        "block_timestamp": ts,
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


def _cmd_tron_transfers(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.tron_trc20_transfers(args.address, int(args.from_block, 0), int(args.to_block, 0), limit=args.limit)
    print(json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


def _cmd_tron_internal(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.tron_internal_transfers(args.address, int(args.from_block, 0), int(args.to_block, 0), limit=args.limit)
    print(json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


def _cmd_hl_fills(args: argparse.Namespace) -> int:
    src = SQDSource()
    rows = src.hyperliquid_owner_fills(args.user, int(args.from_block, 0), int(args.to_block, 0), limit=args.limit)
    print(json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


def _silence_console_logs() -> None:
    """CLI stdout must stay machine-parseable (JSON only); logs go to stderr."""
    try:
        from loguru import logger as _loguru

        _loguru.remove()
    except ImportError:
        pass


def _cli() -> int:
    _silence_console_logs()
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

    p_tt = sub.add_parser("tron-transfers", help="Tron TRC-20 wallet transfers")
    p_tt.add_argument("address")
    p_tt.add_argument("--from-block", required=True)
    p_tt.add_argument("--to-block", required=True)
    p_tt.add_argument("--limit", type=int, default=100)
    p_tt.set_defaults(fn=_cmd_tron_transfers)

    p_ti = sub.add_parser("tron-internal", help="Tron native internal transactions for a wallet")
    p_ti.add_argument("address")
    p_ti.add_argument("--from-block", required=True)
    p_ti.add_argument("--to-block", required=True)
    p_ti.add_argument("--limit", type=int, default=100)
    p_ti.set_defaults(fn=_cmd_tron_internal)

    p_hf = sub.add_parser("hl-fills", help="Hyperliquid fills for a user")
    p_hf.add_argument("user")
    p_hf.add_argument("--from-block", required=True)
    p_hf.add_argument("--to-block", required=True)
    p_hf.add_argument("--limit", type=int, default=100)
    p_hf.set_defaults(fn=_cmd_hl_fills)

    args = parser.parse_args()
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(_cli())
