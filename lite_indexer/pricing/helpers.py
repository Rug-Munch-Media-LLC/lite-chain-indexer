"""Retroactive backfill jobs: whale flags and zero price_usd/volume_usd rows."""

from __future__ import annotations

from typing import Any

from lite_indexer.config import (
    CH_DEX_TABLE,
    CH_TOKEN_PRICES_TABLE,
    WHALE_AMOUNT_THRESHOLDS,
    WHALE_THRESHOLD_USD,
    WHALE_TOP_QUANTILE,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.pricing.cache import _sane_amount, get_decimals

logger = get_logger(__name__)

_MUTATION_BATCH = 20_000


def backfill_whales(
    ch_client: Any,
    *,
    days: int = 90,
    chain: str | None = None,
    limit: int = 500_000,
    dry_run: bool = False,
    usd_threshold: float | None = None,
    amount_thresholds: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Re-scan recent dex_events and mark whale rows retroactively.

    Recomputes the whale flag for rows in the window using:
      1. USD notional from the token_prices table (Fix 3),
      2. The per-chain raw-amount heuristic (Fix 1),
      3. Top ``WHALE_TOP_QUANTILE`` (99.9%) per-pair raw amount over the
         window — the "top 0.1% for that pair" definition.

    Only rows whose flag changes are mutated (``ALTER TABLE ... UPDATE``),
    in batches, so re-runs are idempotent and cheap.

    Args:
        ch_client: ClickHouse client.
        days: How far back to scan (block_timestamp window).
        chain: Restrict to one chain.
        limit: Max rows scanned (bounds memory).
        dry_run: Preview counts without issuing mutations.
        usd_threshold: Override for ``WHALE_THRESHOLD_USD`` (testing).
        amount_thresholds: Override for ``WHALE_AMOUNT_THRESHOLDS``.

    Returns:
        Stats dict: scanned, whale_up (0->1), whale_down (1->0),
        unchanged, dry_run, errors.
    """
    threshold = WHALE_THRESHOLD_USD if usd_threshold is None else usd_threshold
    thresholds = WHALE_AMOUNT_THRESHOLDS if amount_thresholds is None else amount_thresholds
    stats: dict[str, Any] = {
        "scanned": 0,
        "whale_up": 0,
        "whale_down": 0,
        "unchanged": 0,
        "dry_run": dry_run,
        "errors": [],
    }

    chain_clause = " AND chain = %(chain)s" if chain else ""
    where = f"block_timestamp >= now() - INTERVAL %(days)s DAY{chain_clause}"
    params: dict[str, Any] = {"days": days, "chain": chain or "", "limit": limit}

    try:
        # Per-pair top-quantile raw amounts (the "top 0.1%" definition).
        pair_q: dict[str, float] = {}
        try:
            quantile_query = (
                f"SELECT contract_address, quantile({WHALE_TOP_QUANTILE})(greatest(amount0, amount1)) "
                f"FROM {CH_DEX_TABLE} WHERE {where} GROUP BY contract_address"
            )
            for row in ch_client.execute(quantile_query, params) or []:
                if len(row) >= 2 and row[1]:
                    pair_q[str(row[0])] = float(row[1])
        except Exception as exc:  # noqa: BLE001 — quantile is best-effort
            logger.debug("whale_quantile_failed", error=str(exc)[:80])

        rows = (
            ch_client.execute(
                "SELECT chain, tx_hash, log_index, token0, token1, amount0, amount1, "
                "contract_address, version, whale "
                f"FROM {CH_DEX_TABLE} WHERE {where} "
                "ORDER BY block_timestamp DESC LIMIT %(limit)s",
                params,
            )
            or []
        )
        stats["scanned"] = len(rows)

        # Load prices for the tokens in the scanned rows.
        tokens = {(str(r[0]), str(r[3])) for r in rows if len(r) > 3}
        tokens |= {(str(r[0]), str(r[4])) for r in rows if len(r) > 4}
        prices: dict[tuple[str, str], float] = {}
        if tokens:
            try:
                price_rows = ch_client.execute(
                    f"SELECT chain, token_address, price_usd FROM {CH_TOKEN_PRICES_TABLE} "
                    "WHERE (chain, token_address) IN %(tokens)s",
                    {"tokens": list(tokens)},
                )
                for pr in price_rows or []:
                    if len(pr) >= 3 and pr[2]:
                        prices[(str(pr[0]), str(pr[1]))] = float(pr[2])
            except Exception as exc:  # noqa: BLE001 — pricing is best-effort
                stats["errors"].append(f"price_lookup: {str(exc)[:60]}")

        set_1: list[tuple[Any, ...]] = []
        set_0: list[tuple[Any, ...]] = []

        for r in rows:
            rchain, tx_hash, log_index = str(r[0]), str(r[1]), int(r[2])
            token0, token1 = str(r[3]), str(r[4])
            amount0 = _sane_amount(r[5])
            amount1 = _sane_amount(r[6])
            pair = str(r[7])
            current_whale = int(r[9]) if len(r) > 9 else 0

            new_whale = 0
            # 1. USD notional.
            for token, amount in ((token0, amount0), (token1, amount1)):
                price = prices.get((rchain, token))
                if price and price > 0 and amount > 0:
                    usd = (amount / (10 ** get_decimals(rchain, token))) * price
                    if usd >= threshold:
                        new_whale = 1
                        break
            # 2. Per-chain raw heuristic.
            if not new_whale:
                thr = thresholds.get(rchain, 0)
                if thr and (amount0 >= thr or amount1 >= thr):
                    new_whale = 1
            # 3. Top 0.1% for the pair.
            if not new_whale:
                q = pair_q.get(pair, 0.0)
                if q > 0 and max(amount0, amount1) >= q:
                    new_whale = 1

            key = (rchain, tx_hash, log_index)
            if new_whale != current_whale:
                (set_1 if new_whale else set_0).append(key)

        stats["whale_up"] = len(set_1)
        stats["whale_down"] = len(set_0)
        stats["unchanged"] = stats["scanned"] - stats["whale_up"] - stats["whale_down"]

        if dry_run:
            return stats

        for keys, whale in ((set_1, 1), (set_0, 0)):
            for i in range(0, len(keys), _MUTATION_BATCH):
                batch = keys[i : i + _MUTATION_BATCH]
                ch_client.execute(
                    f"ALTER TABLE {CH_DEX_TABLE} UPDATE whale = %(whale)s "
                    "WHERE (chain, tx_hash, log_index) IN %(keys)s",
                    {"whale": whale, "keys": batch},
                )
        metrics_incr("whale_backfills")
        return stats
    except Exception as exc:  # noqa: BLE001 — CLI-facing, surface the error
        stats["errors"].append(str(exc)[:120])
        logger.error("whale_backfill_failed", error=str(exc)[:120])
        return stats


def backfill_dex_prices(
    ch_client: Any,
    *,
    days: int = 90,
    chain: str | None = None,
    limit: int = 500_000,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Backfill ``price_usd`` / ``volume_usd`` on dex_events rows that are 0.

    Ingestion writes ``price_usd = 0`` for swaps whose token price was not
    in the in-memory cache at write time (feed not yet warmed, provider
    down, token unknown). Once the ``token_prices`` table has a value,
    this job retroactively fills those columns using the exact same
    semantics as :meth:`ClickHouseWriter.write_dex_events`:

      - ``price_usd`` — cached USD price of ``token0`` (0 when unpriced),
      - ``volume_usd`` — max-leg USD notional (0 when unpriced).

    Only rows whose values actually change are mutated (``ALTER TABLE ...
    UPDATE``), in batches, so re-runs are idempotent and cheap. Rows with
    no resolvable price are left at 0 and picked up by a later run.

    Args:
        ch_client: ClickHouse client.
        days: How far back to scan (block_timestamp window).
        chain: Restrict to one chain.
        limit: Max rows scanned (bounds memory).
        dry_run: Preview counts without issuing mutations.

    Returns:
        Stats dict: scanned, updated, unchanged, unpriced, dry_run,
        errors.
    """
    stats: dict[str, Any] = {
        "scanned": 0,
        "updated": 0,
        "unchanged": 0,
        "unpriced": 0,
        "dry_run": dry_run,
        "errors": [],
    }

    chain_clause = " AND chain = %(chain)s" if chain else ""
    where = f"price_usd = 0 AND block_timestamp >= now() - INTERVAL %(days)s DAY{chain_clause}"
    params: dict[str, Any] = {"days": days, "chain": chain or "", "limit": limit}

    try:
        rows = (
            ch_client.execute(
                "SELECT chain, tx_hash, log_index, token0, token1, amount0, amount1, "
                "price_usd, volume_usd "
                f"FROM {CH_DEX_TABLE} WHERE {where} "
                "ORDER BY block_timestamp DESC LIMIT %(limit)s",
                params,
            )
            or []
        )
        stats["scanned"] = len(rows)

        # Load prices for the tokens in the scanned rows.
        tokens = {(str(r[0]), str(r[3])) for r in rows if len(r) > 3}
        tokens |= {(str(r[0]), str(r[4])) for r in rows if len(r) > 4}
        prices: dict[tuple[str, str], float] = {}
        if tokens:
            try:
                price_rows = ch_client.execute(
                    f"SELECT chain, token_address, price_usd FROM {CH_TOKEN_PRICES_TABLE} "
                    "WHERE (chain, token_address) IN %(tokens)s",
                    {"tokens": list(tokens)},
                )
                for pr in price_rows or []:
                    if len(pr) >= 3 and pr[2]:
                        prices[(str(pr[0]), str(pr[1]))] = float(pr[2])
            except Exception as exc:  # noqa: BLE001 — pricing is best-effort
                stats["errors"].append(f"price_lookup: {str(exc)[:60]}")

        # Group mutation keys by the exact (price_usd, volume_usd) target so
        # each batch is a single ALTER with the same literal values (mirrors
        # backfill_whales; no per-row FROM subquery needed).
        # (price_usd, volume_usd) -> [(chain, tx_hash, log_index), ...]
        groups: dict[tuple[float, float], list[tuple[Any, ...]]] = {}

        for r in rows:
            rchain, tx_hash, log_index = str(r[0]), str(r[1]), int(r[2])
            token0, token1 = str(r[3]), str(r[4])
            amount0 = _sane_amount(r[5])
            amount1 = _sane_amount(r[6])
            current_price = float(r[7]) if len(r) > 7 and r[7] else 0.0
            current_volume = float(r[8]) if len(r) > 8 and r[8] else 0.0

            # Mirror the write path exactly (compute_swap_usd): price_usd is
            # the token0 price; volume_usd is the max-leg USD notional with
            # each leg priced at its OWN token price.
            price = prices.get((rchain, token0)) or 0.0
            volume = 0.0
            for token, amount in ((token0, amount0), (token1, amount1)):
                leg_price = prices.get((rchain, token)) or 0.0
                if leg_price > 0 and amount > 0:
                    usd = (amount / (10 ** get_decimals(rchain, token))) * leg_price
                    if usd > volume:
                        volume = usd
            volume = round(volume, 2)

            if price <= 0 and volume <= 0:
                stats["unpriced"] += 1
                continue

            if abs(price - current_price) < 1e-12 and abs(volume - current_volume) < 1e-12:
                stats["unchanged"] += 1
                continue

            key = (rchain, tx_hash, log_index)
            groups.setdefault((price, volume), []).append(key)

        stats["updated"] = sum(len(keys) for keys in groups.values())

        if dry_run:
            return stats

        for (price, volume), keys in groups.items():
            for i in range(0, len(keys), _MUTATION_BATCH):
                batch = keys[i : i + _MUTATION_BATCH]
                ch_client.execute(
                    f"ALTER TABLE {CH_DEX_TABLE} UPDATE "
                    "price_usd = %(price_usd)s, volume_usd = %(volume_usd)s "
                    "WHERE (chain, tx_hash, log_index) IN %(keys)s",
                    {"price_usd": price, "volume_usd": volume, "keys": batch},
                )
        metrics_incr("price_backfills")
        return stats
    except Exception as exc:  # noqa: BLE001 — CLI-facing, surface the error
        stats["errors"].append(str(exc)[:120])
        logger.error("dex_price_backfill_failed", error=str(exc)[:120])
        return stats
