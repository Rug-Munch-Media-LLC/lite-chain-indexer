"""EnvioSource — on-demand history via Envio HyperRPC (and HyperSync API).

Fifth data source in the query family. Uses Envio's HyperRPC endpoints
(JSON-RPC over the HyperSync data layer) for chains/ranges the local
index + B2 + BigLake tiers cannot serve:

* ``eth_getLogs``-based wallet transfer history (ERC-20 Transfer topic),
  any HyperRPC-supported chain.
* ``trace_block`` execution traces on the ``-traces`` subdomains
  (ethereum, base verified) → ``internal_transfers`` row shape.

Auth: token embedded in the URL path (``https://{chainId}.rpc.hypersync.xyz/{token}``).
The free tier is fair-use rate limited (permits bursts; ~100 rpm sustained
is the house guidance) — the client enforces a polite minimum interval
(``ENVIO_RPC_MIN_INTERVAL``, default 0.5s) and never batches wide ranges
without a response cap: HyperRPC rejects ``eth_getLogs`` responses over
50,000 logs, and :meth:`EnvioSource.get_logs` splits the block range
adaptively when that cap is hit.

Env:
    ENVIO_RPC_TOKEN        HyperRPC path token (required for any call)
    ENVIO_RPC_MIN_INTERVAL seconds between calls (default 0.5)

CLI:
    python -m lite_indexer.query.envio_source transfers ethereum 0xADDR --from-block 0x18C0000 --to-block 0x18C1000
    python -m lite_indexer.query.envio_source trace-block base 0x3000000
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
MAX_LOGS_PER_CALL = 50_000  # observed HyperRPC response cap
_MAX_SPLIT_DEPTH = 12


class EnvioLimitError(RuntimeError):
    """Raised when HyperRPC rejects a request because the response is too large."""


class EnvioSource:
    """Read-only on-demand access to Envio HyperRPC.

    Args:
        rpc_token: HyperRPC token; defaults to ``ENVIO_RPC_TOKEN``.
        min_interval: Minimum seconds between HTTP calls (0 in tests).
        post: Optional transport override ``(url, payload, timeout) -> dict``
            (used by tests; defaults to urllib).
    """

    #: Local chain name → Envio HyperRPC subdomain (chain id).
    CHAIN_IDS: dict[str, str] = {
        "ethereum": "1",
        "base": "8453",
        "bsc": "56",
        "polygon": "137",
        "arbitrum": "42161",
        "optimism": "10",
        "avalanche": "43114",
        "fantom": "250",
        "gnosis": "100",
        "zksync": "324",
        "mantle": "5000",
        "linea": "59144",
        "scroll": "534352",
        "celo": "42220",
        "fraxtal": "252",
        "blast": "81457",
        "tron": "728126428",
        "hyperliquid": "999",
    }

    #: Chains with a verified ``-traces`` HyperRPC subdomain.
    TRACES_CHAINS: frozenset[str] = frozenset({"ethereum", "base"})

    def __init__(
        self,
        *,
        rpc_token: str | None = None,
        min_interval: float | None = None,
        post: Any | None = None,
    ) -> None:
        self._token = (
            rpc_token if rpc_token is not None else os.getenv("ENVIO_RPC_TOKEN", "")
        ).strip()
        if min_interval is None:
            try:
                min_interval = float(os.getenv("ENVIO_RPC_MIN_INTERVAL", "0.5"))
            except ValueError:
                min_interval = 0.5
        self._min_interval = max(0.0, float(min_interval))
        self._post_impl = post
        self._last_call = 0.0

    # ── Availability / HTTP plumbing ─────────────────────────────────

    def available(self) -> bool:
        return bool(self._token)

    def _endpoint(self, chain: str, *, traces: bool = False) -> str:
        cid = self.CHAIN_IDS.get(chain)
        if not cid:
            raise ValueError(f"chain not supported by Envio HyperRPC: {chain}")
        suffix = "-traces" if traces else ""
        return f"https://{cid}{suffix}.rpc.hypersync.xyz/{self._token}"

    def _throttle(self) -> None:
        if self._min_interval <= 0:
            return
        wait = self._min_interval - (time.monotonic() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.monotonic()

    def _post(self, url: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
        if self._post_impl is not None:
            return self._post_impl(url, payload, timeout)  # type: ignore[no-any-return]
        body = json.dumps(payload).encode()
        req = Request(url, data=body, headers={"Content-Type": "application/json"})
        try:
            with urlopen(req, timeout=timeout) as resp:  # noqa: S310 - fixed https endpoints
                return json.loads(resp.read().decode())  # type: ignore[no-any-return]
        except HTTPError as exc:
            if exc.code == 429:
                raise EnvioLimitError("rate limited (429)") from exc
            raise

    def call(
        self,
        chain: str,
        method: str,
        params: list[Any],
        *,
        traces: bool = False,
        timeout: float = 30.0,
    ) -> Any:
        """One JSON-RPC call. Returns the result, or None on transport failure."""
        if not self.available():
            return None
        url = self._endpoint(chain, traces=traces)
        self._throttle()
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
        try:
            data = self._post(url, payload, timeout)
        except EnvioLimitError:
            raise
        except (URLError, OSError, ValueError) as exc:
            logger.warning("envio_call_failed", chain=chain, method=method, error=str(exc)[:120])
            return None
        error = data.get("error")
        if error:
            message = str(error.get("message", error))
            if "Limit exceeded" in message:
                raise EnvioLimitError(message[:120])
            logger.warning("envio_rpc_error", chain=chain, method=method, error=message[:120])
            return None
        return data.get("result")

    # ── Logs with adaptive range splitting ───────────────────────────

    def get_logs(
        self,
        chain: str,
        from_block: int,
        to_block: int,
        *,
        address: str | list[str] | None = None,
        topics: list[Any] | None = None,
        traces: bool = False,
        _depth: int = 0,
    ) -> list[dict[str, Any]]:
        """``eth_getLogs`` with automatic range splitting under the 50k cap."""
        if from_block > to_block:
            return []
        params: dict[str, Any] = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
        if address:
            params["address"] = address
        if topics:
            params["topics"] = topics
        try:
            result = self.call(chain, "eth_getLogs", [params], traces=traces)
        except EnvioLimitError:
            if to_block - from_block < 2 or _depth >= _MAX_SPLIT_DEPTH:
                logger.warning(
                    "envio_getlogs_unsplittable",
                    chain=chain,
                    from_block=from_block,
                    to_block=to_block,
                )
                return []
            mid = (from_block + to_block) // 2
            return self.get_logs(
                chain,
                from_block,
                mid,
                address=address,
                topics=topics,
                traces=traces,
                _depth=_depth + 1,
            ) + self.get_logs(
                chain,
                mid + 1,
                to_block,
                address=address,
                topics=topics,
                traces=traces,
                _depth=_depth + 1,
            )
        return result or []

    # ── Wallet transfer history ──────────────────────────────────────

    @staticmethod
    def _padded_topic(address: str) -> str:
        addr = address.strip().lower()
        return "0x" + "0" * 24 + addr[2:] if addr.startswith("0x") else "0x" + "0" * 24 + addr

    def address_transfers(
        self,
        chain: str,
        address: str,
        from_block: int,
        to_block: int,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """ERC-20 transfers where ``address`` is sender or recipient.

        Two topic-filtered queries (from-position, to-position) merged and
        normalized to the ``evm_transfers`` row shape. ``block_timestamp``
        is None (logs do not carry timestamps; callers may enrich).
        """
        if not self.available() or chain not in self.CHAIN_IDS:
            return []
        padded = self._padded_topic(address)
        logs = self.get_logs(chain, from_block, to_block, topics=[TRANSFER_TOPIC, padded])
        logs += self.get_logs(chain, from_block, to_block, topics=[TRANSFER_TOPIC, None, padded])
        rows = [self._map_transfer_log(chain, log) for log in logs]
        rows.sort(key=lambda r: (r["block_number"], r["log_index"]), reverse=True)
        return rows[: max(1, int(limit))]

    def _map_transfer_log(self, chain: str, log: dict[str, Any]) -> dict[str, Any]:
        topics = log.get("topics") or []
        from_addr = "0x" + str(topics[1])[-40:] if len(topics) > 1 else ""
        to_addr = "0x" + str(topics[2])[-40:] if len(topics) > 2 else ""
        data = log.get("data") or "0x0"
        try:
            value = str(int(data, 16))
        except (TypeError, ValueError):
            value = "0"
        return {
            "chain": chain,
            "block_number": int(log.get("blockNumber") or "0x0", 16),
            "tx_hash": log.get("transactionHash") or "",
            "from_address": from_addr.lower(),
            "to_address": to_addr.lower(),
            "token_address": str(log.get("address") or "").lower(),
            "value": value,
            "block_timestamp": None,
            "log_index": int(log.get("logIndex") or "0x0", 16),
            "ingested_at": datetime.now(UTC),
        }

    # ── Traces ───────────────────────────────────────────────────────

    def block_traces(self, chain: str, block: int) -> list[dict[str, Any]]:
        """Raw ``trace_block`` output (ethereum/base only)."""
        if chain not in self.TRACES_CHAINS:
            raise ValueError(f"trace_block is not available for chain: {chain}")
        result = self.call(chain, "trace_block", [hex(block)], traces=True, timeout=60.0)
        return result or []

    def block_traces_rows(self, chain: str, block: int) -> list[dict[str, Any]]:
        """``trace_block`` normalized to the ``internal_transfers`` row shape."""
        rows: list[dict[str, Any]] = []
        for trace in self.block_traces(chain, block):
            action = trace.get("action") or {}
            result = trace.get("result") or {}
            trace_address = trace.get("traceAddress") or []
            try:
                gas_used = int(str(result.get("gasUsed", "0x0")), 16)
            except (TypeError, ValueError):
                gas_used = 0
            rows.append(
                {
                    "chain": chain,
                    "block_number": int(trace.get("blockNumber") or "0x0", 16),
                    "block_timestamp": None,
                    "tx_hash": trace.get("transactionHash") or "",
                    "trace_address": ".".join(str(x) for x in trace_address),
                    "from_address": str(action.get("from") or "").lower(),
                    "to_address": str(action.get("to") or "").lower(),
                    "value_wei": str(action.get("value") or "0"),
                    "call_type": str(action.get("callType") or trace.get("type") or ""),
                    "gas_used": gas_used,
                    "error": "" if not trace.get("error") else str(trace["error"])[:200],
                    "input_method_id": str(action.get("input") or "")[:10],
                    "ingested_at": datetime.now(UTC),
                }
            )
        return rows


# ── CLI ──────────────────────────────────────────────────────────────


def _cmd_transfers(args: argparse.Namespace) -> int:
    src = EnvioSource()
    if not src.available():
        print("envio unavailable (ENVIO_RPC_TOKEN missing)", file=sys.stderr)  # noqa: T201
        return 1
    rows = src.address_transfers(
        args.chain,
        args.address,
        int(args.from_block, 0),
        int(args.to_block, 0),
        limit=args.limit,
    )
    print(json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


def _cmd_trace_block(args: argparse.Namespace) -> int:
    src = EnvioSource()
    rows = src.block_traces_rows(args.chain, int(args.block, 0))
    print(json.dumps(rows, default=str, indent=2))  # noqa: T201
    return 0


def _cli() -> int:
    parser = argparse.ArgumentParser(prog="envio_source", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_tr = sub.add_parser("transfers", help="wallet ERC-20 transfers via HyperRPC")
    p_tr.add_argument("chain")
    p_tr.add_argument("address")
    p_tr.add_argument("--from-block", required=True, help="int or 0x-hex")
    p_tr.add_argument("--to-block", required=True, help="int or 0x-hex")
    p_tr.add_argument("--limit", type=int, default=100)
    p_tr.set_defaults(fn=_cmd_transfers)

    p_tb = sub.add_parser(
        "trace-block", help="trace_block (ethereum/base) → internal_transfers rows"
    )
    p_tb.add_argument("chain")
    p_tb.add_argument("block", help="int or 0x-hex")
    p_tb.set_defaults(fn=_cmd_trace_block)

    args = parser.parse_args()
    return int(args.fn(args))


if __name__ == "__main__":
    raise SystemExit(_cli())
