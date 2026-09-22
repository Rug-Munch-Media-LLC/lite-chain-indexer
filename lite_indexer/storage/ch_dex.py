"""ClickHouse DEX swap writes — whale/USD enrichment at ingestion time.

Split out of :mod:`lite_indexer.storage.clickhouse` (2026-09-22) so the
writer stays under the per-file size ratchet. Config flags that tests
patch on the facade module are read through it at call time.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import lite_indexer.storage.clickhouse as _facade
from lite_indexer.config import CH_DEX_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.pricing import compute_swap_usd, detect_whale, get_price
from lite_indexer.storage.ch_errors import (
    ClickHouseUnavailable,
    is_unavailable_error,
    require_client,
)
from lite_indexer.storage.ch_rows import _parse_ts, _serialize_raw_payload

logger = get_logger(__name__)


def _insert_dex(client: Any, chain: str, rows: list[tuple[Any, ...]]) -> int:
    """Execute the dex_events INSERT; fail CLOSED on an outage."""
    try:
        client.execute(
            f"INSERT INTO {CH_DEX_TABLE} "  # nosemgrep: formatted-sql-query — table is a module constant; values parameterized
            "(type, chain, block_number, block_timestamp, tx_hash, log_index, "
            "contract_address, dex, version, token0, token1, sender, recipient, "
            "amount0_in, amount1_in, amount0_out, amount1_out, "
            "amount0, amount1, whale, price_usd, volume_usd, ingested_at, source, "
            "raw_payload) VALUES",
            rows,
        )
        logger.info("dex_clickhouse_inserted", chain=chain, rows=len(rows))
        metrics_incr("clickhouse_inserts")
        return len(rows)
    except Exception as exc:
        metrics_incr("clickhouse_errors")
        logger.error(
            "dex_clickhouse_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150]
        )
        if is_unavailable_error(exc):
            raise ClickHouseUnavailable(
                f"ClickHouse unreachable during dex insert (chain={chain})"
            ) from exc
        return 0


def _dex_row(
    chain: str,
    event: dict[str, Any],
    numeric_amounts: bool,
    now: datetime,
) -> tuple[Any, ...]:
    """Build one dex_events INSERT row (schema-adaptive amount columns)."""
    version = event.get("dex_type", "v2")
    amt0 = event.get("amount0In", "0") or "0"
    amt1 = event.get("amount1In", "0") or "0"

    def _amount(value: Any) -> Any:
        if numeric_amounts:
            if value in (None, ""):
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
        return "" if value is None else str(value)

    return (
        "swap",
        chain,
        event["blockNumber"],
        _parse_ts(event.get("blockTimestamp", ""), now),
        event["transactionHash"],
        event["logIndex"],
        event.get("pairAddress", ""),
        event.get("dex", ""),
        version,
        event.get("token0", ""),
        event.get("token1", ""),
        event.get("sender", ""),
        event.get("recipient", ""),
        _amount(event.get("amount0In", "0")),
        _amount(event.get("amount1In", "0")),
        _amount(event.get("amount0Out", "0")),
        _amount(event.get("amount1Out", "0")),
        _amount(amt0),
        _amount(amt1),
        detect_whale(event),
        get_price(chain, str(event.get("token0", ""))) or 0.0,
        compute_swap_usd(event),
        now,
        event.get("source", "lite_indexer"),
        _serialize_raw_payload(event),
    )


class DexMixin:
    """``dex_events`` writes with whale detection + USD normalization."""

    def _dex_amounts_numeric(self, client: Any) -> bool:
        """Return True when dex_events amount columns are Float (not String).

        Schema-adaptive probe for the 2026-08-19 dex_events incident: the
        canonical schema (bootstrap.py) stores raw uint256 swap amounts as
        String, but an ad-hoc ALTER (no repo source) converted the six
        amount columns to Nullable(Float64) on 08-18 ~21:39. Every string
        INSERT since failed with Code 53 ("argument is not a float") and
        dex_events silently stalled until this probe let the writer match
        the live schema. Migration 013 restores the canonical String
        columns; this probe makes the writer correct under either schema.

        Result is cached per process; the schema never changes mid-run
        (migration 013 is applied while the indexer is stopped).
        """
        if self._dex_numeric_amounts is not None:
            return self._dex_numeric_amounts
        try:
            types = self._column_types(client, CH_DEX_TABLE)
            amount_type = types.get("amount0_in", "")
            self._dex_numeric_amounts = "Float" in amount_type or "Decimal" in amount_type
            logger.info(
                "dex_amount_schema_probed",
                column_type=amount_type or "unknown",
                numeric=self._dex_numeric_amounts,
            )
        except Exception:  # noqa: BLE001 — probe failure falls back to canonical
            self._dex_numeric_amounts = False
        return self._dex_numeric_amounts

    def write_dex_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Insert DEX swap events into ClickHouse. Returns rows inserted.

        Whale detection + USD normalization happen here, at ingestion time:

          - ``whale`` is computed from the cached token_prices feed (USD
            notional >= ``WHALE_THRESHOLD_USD``) with the per-chain raw
            amount thresholds as fallback (see
            :func:`lite_indexer.pricing.detect_whale`).
          - ``price_usd`` is the current USD price of token0 (best-effort,
            0 when unpriced).
          - ``volume_usd`` is the estimated USD notional of the swap
            (max leg, best-effort, 0 when unpriced).
        """
        if not events:
            return 0

        client = require_client(self._connect(), chain)

        self._ensure_source_column(client, CH_DEX_TABLE)
        self._ensure_dex_usd_columns(client)
        self._ensure_raw_payload_column(client, CH_DEX_TABLE)
        if _facade.CH_TTL_ENABLED:
            self.ensure_ttl_policies(client)

        # Match the live amount-column types (String canonical vs the
        # 08-18 ad-hoc Float64 ALTER). Sending strings into Float64
        # columns is what stalled dex_events on 08-19 (Code 53).
        numeric_amounts = self._dex_amounts_numeric(client)
        now = datetime.now(UTC)
        rows = [_dex_row(chain, event, numeric_amounts, now) for event in events]
        return _insert_dex(client, chain, rows)
