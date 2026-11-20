"""date → block resolver: the missing piece between a date-driven backfill and SQD.

WHY THIS EXISTS
    ``lake-gaps`` / ``lake-backfill`` take DATES. ``sqd-fill`` takes BLOCKS. There
    was no function anywhere in this tree converting one to the other (searched for
    ``date_to_block``, ``blocks_for_date``, ``block_at``, ``block_timestamp``), so a
    chain that is not base/arbitrum could only be backfilled by a human typing block
    numbers. That is the single piece of code that stood between us and a 21-chain,
    date-driven, free backfill.

HOW IT RESOLVES
    Binary search over ``eth_getBlockByNumber`` on a chain's public RPC, comparing
    ``block.timestamp`` against the target instant. Block N's timestamp is monotone
    increasing, so ~log2(head) probes converge. Two answers are useful and both are
    returned:

      * ``first`` — the first block whose timestamp is >= the day's 00:00Z
      * ``last``  — the last block whose timestamp is < the next day's 00:00Z

    A caller filling ``[first, last]`` covers the whole UTC day exactly once.

CACHING
    Blocks never move, so a resolved boundary is stored in
    ``lite_indexer/chains/_block_cache.json`` keyed ``<chain>:<date>``. A second run
    for the same day costs nothing. The cache is append-only and safe to delete.

RATE LIMITS
    This is the ONLY route in the system that can burn a metered quota, so it is
    deliberately frugal: one probe per bisection step, a hard probe ceiling, and a
    polite inter-probe sleep. It never batches and never retries a failing endpoint
    more than once per probe — a 429 raises instead of hammering, which is the fleet
    rule for every metered provider.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CACHE_PATH = Path(__file__).with_name("_block_cache.json")

#: Hard ceiling on probes per resolution. log2(200_000_000) ~ 28, so 40 leaves
#: headroom for a chain with sparse early blocks and still cannot loop forever.
MAX_PROBES = 40

#: Seconds between probes. Public RPCs are free but rate-limited; a 0.15 s gap
#: keeps a 36-day × 14-chain sweep comfortably inside a keyless budget.
PROBE_INTERVAL_S = float(os.getenv("BLOCK_RESOLVER_INTERVAL_S", "0.15"))

#: Per-probe timeout. A hung public endpoint must not stall a whole backfill.
PROBE_TIMEOUT_S = float(os.getenv("BLOCK_RESOLVER_TIMEOUT_S", "20"))


class BlockResolveError(RuntimeError):
    """The resolver could not find a boundary within its probe budget."""


def _rpc_urls(chain: str) -> list[str]:
    """Public RPC endpoints for *chain*, best first.

    Reuses ``chains.endpoints`` so there is ONE endpoint list in the system rather
    than a second copy that can drift.
    """
    from lite_indexer.chains import endpoints as ep

    # The live map is GET_LOGS_ENDPOINTS (merge_endpoints applied an overlay at
    # import). CANDIDATE_ENDPOINTS is the pre-overlay pool, used only if the live
    # map somehow lacks the chain.
    for attr in ("GET_LOGS_ENDPOINTS", "CANDIDATE_ENDPOINTS"):
        table = getattr(ep, attr, None)
        if isinstance(table, dict):
            got = table.get(chain)
            if got:
                return list(got) if isinstance(got, (list, tuple)) else [got]
    fn = getattr(ep, "last_resort_endpoints", None)
    if callable(fn):
        got = fn(chain)
        if got:
            return list(got) if isinstance(got, (list, tuple)) else [got]
    raise BlockResolveError(f"no public RPC endpoint known for chain {chain!r}")


class DateBlockResolver:
    """Resolve UTC date boundaries to block numbers for one chain."""

    def __init__(self, chain: str, *, urls: list[str] | None = None) -> None:
        self.chain = chain
        self._urls = urls
        self._head: tuple[int, int] | None = None  # (block_number, timestamp)
        self._cache: dict[str, dict[str, int]] = {}
        if CACHE_PATH.is_file():
            try:
                self._cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                self._cache = {}

    # -- transport -------------------------------------------------------
    def _urls_or_raise(self) -> list[str]:
        if self._urls is None:
            self._urls = _rpc_urls(self.chain)
        return self._urls

    def _rpc(self, method: str, params: list[Any]) -> Any:
        """One JSON-RPC call, trying endpoints in order. Raises on all-fail."""
        import httpx

        last: Exception | None = None
        for url in self._urls_or_raise():
            try:
                r = httpx.post(
                    url,
                    json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                    timeout=PROBE_TIMEOUT_S,
                )
                if r.status_code == 429:
                    # a 429 is a signal to slow down, not to retry harder
                    raise BlockResolveError(f"{url} rate limited (429) on {method}")
                r.raise_for_status()
                body = r.json()
                if body.get("error"):
                    raise BlockResolveError(f"{url}: {body['error']}")
                return body.get("result")
            except BlockResolveError:
                raise
            except Exception as exc:  # noqa: BLE001 — try the next endpoint
                last = exc
                logger.debug("rpc_endpoint_failed url=%s err=%s", url, str(exc)[:80])
        raise BlockResolveError(f"all endpoints failed for {self.chain}: {last}")

    def _block_at(self, number: int) -> tuple[int, int]:
        """(number, timestamp) for *number*."""
        res = self._rpc("eth_getBlockByNumber", [hex(number), False])
        if not res:
            raise BlockResolveError(f"block {number} not found on {self.chain}")
        return number, int(res["timestamp"], 16)

    def head(self) -> tuple[int, int]:
        """(head_block, head_timestamp), fetched once."""
        if self._head is None:
            res = self._rpc("eth_getBlockByNumber", ["latest", False])
            if not res:
                raise BlockResolveError(f"no head block for {self.chain}")
            self._head = (int(res["number"], 16), int(res["timestamp"], 16))
        return self._head

    # -- resolution ------------------------------------------------------
    def _first_at_or_after(self, target_ts: int) -> int:
        """Lowest block whose timestamp >= *target_ts*."""
        lo, hi = 0, self.head()[0]
        probes = 0
        while lo < hi:
            probes += 1
            if probes > MAX_PROBES:
                raise BlockResolveError(f"{self.chain}: exceeded {MAX_PROBES} probes seeking ts>={target_ts}")
            mid = (lo + hi) // 2
            _, ts = self._block_at(mid)
            time.sleep(PROBE_INTERVAL_S)
            if ts < target_ts:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def resolve_day(self, date: str) -> dict[str, int]:
        """Block range covering the UTC day *date* (``YYYY-MM-DD``).

        Returns ``{"first": n, "last": m}`` — inclusive, so a caller filling
        ``[first, last]`` covers the day exactly once.
        """
        key = f"{self.chain}:{date}"
        cached = self._cache.get(key)
        if cached and "first" in cached and "last" in cached:
            return {"first": int(cached["first"]), "last": int(cached["last"])}

        day = dt.date.fromisoformat(date)
        start_ts = int(dt.datetime(day.year, day.month, day.day, tzinfo=dt.UTC).timestamp())
        end_ts = start_ts + 86_400

        head_n, head_ts = self.head()
        if start_ts > head_ts:
            raise BlockResolveError(f"{self.chain}: {date} is in the future (head ts {head_ts} < {start_ts})")

        first = self._first_at_or_after(start_ts)
        # last block strictly before the next day's 00:00Z
        last = self._first_at_or_after(end_ts) - 1
        if last < first:
            # a day can legitimately contain no blocks on a fast or idle chain
            last = first
        out = {"first": int(first), "last": int(max(last, 0))}
        self._cache[key] = out
        self._flush()
        return out

    def resolve_range(self, from_date: str, to_date: str) -> list[dict[str, Any]]:
        """Resolve every UTC day in ``[from_date, to_date]`` inclusive."""
        d0 = dt.date.fromisoformat(from_date)
        d1 = dt.date.fromisoformat(to_date)
        out: list[dict[str, Any]] = []
        d = d0
        while d <= d1:
            iso = d.isoformat()
            try:
                rng = self.resolve_day(iso)
                out.append({"date": iso, **rng})
            except BlockResolveError as exc:
                out.append({"date": iso, "error": str(exc)})
            d += dt.timedelta(days=1)
        return out

    def _flush(self) -> None:
        try:
            CACHE_PATH.write_text(json.dumps(self._cache, indent=0, sort_keys=True), encoding="utf-8")
        except OSError as exc:  # a cache write must never fail the resolve
            logger.debug("block_cache_write_failed err=%s", exc)


def resolve_day(chain: str, date: str) -> dict[str, int]:
    """Convenience: resolve one day for one chain."""
    return DateBlockResolver(chain).resolve_day(date)


__all__ = [
    "BlockResolveError",
    "CACHE_PATH",
    "DateBlockResolver",
    "MAX_PROBES",
    "resolve_day",
]
