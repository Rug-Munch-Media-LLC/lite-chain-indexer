"""Generate the wallet intelligence example report against a live ClickHouse.

[DEPRECATED] The wallet-intel package (``lite_indexer.intel``) was removed
from the indexer in abaf431 — this script imports a module that no longer
exists and will fail with ImportError. Kept as a historical reference only.
See AUDIT-2026-08-14.md before running.

Reads ClickHouse connection from env (CH_HOST/CH_PORT/CH_USER/CH_PASSWORD/
CH_DATABASE, defaulting to localhost + wallet_memory) and writes:

    examples/intel_report_example.json

Run::

    CH_USER=rmi CH_PASSWORD=... python examples/generate_intel_example.py

The script sets INTEL_EXTERNAL_SOURCES_ENABLED=0 so the example is
deterministic (local warehouse only). External adapters are exercised
separately by the unit tests.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

os.environ.setdefault("INTEL_EXTERNAL_SOURCES_ENABLED", "0")

from clickhouse_driver import Client  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lite_indexer.intel import DataSources, IntelReport  # noqa: E402


def ch_query(sql: str, params: dict | None = None) -> list[dict]:
    """ChQuery adapter over clickhouse_driver (single connection)."""
    client = Client(
        host=os.getenv("CH_HOST", "127.0.0.1"),
        port=int(os.getenv("CH_PORT", "9000")),
        user=os.getenv("CH_USER", "default"),
        password=os.getenv("CH_PASSWORD", ""),
        database=os.getenv("CH_DATABASE", "wallet_memory"),
        connect_timeout=5,
    )
    rows, column_types = client.execute(sql, params or {}, with_column_types=True, settings={"max_execution_time": 30})
    columns = [name for name, _ in column_types]
    return [dict(zip(columns, row, strict=True)) for row in rows]


async def main() -> None:
    report = IntelReport(DataSources(query=ch_query))

    # Known scam wallet (Multichain Hack Alert — exploiter tag).
    scam_addr = "0x00077065f5760168aa838add40d4883b96a272a3"
    # Known CEX wallet (Binance 14).
    cex_addr = "0x28c6c06298d514db089934071355e5743bf21d60"
    # Fresh cross-chain deployer (base, 2026-08-03).
    deployer_addr = "0x27bf4e6eaabc305991f4386502ba62680a1509cf"

    scam_doc = await report.full_report(scam_addr, chain="ethereum")
    cex_doc = await report.full_report(cex_addr, chain="ethereum")
    deployer_doc = await report.full_report(deployer_addr, chain=None)
    alerts = await report.clusterer.detect_anomalies(chain=None)

    out = {
        "generated_at": scam_doc["generated_at"],
        "example_wallets": {
            "known_scam_wallet": scam_doc,
            "known_cex_wallet": cex_doc,
            "fresh_deployer_wallet": deployer_doc,
        },
        "recent_anomalies": alerts,
    }

    dest = Path(__file__).resolve().parent / "intel_report_example.json"
    dest.write_text(json.dumps(out, indent=2, default=str))
    print(f"wrote {dest} ({dest.stat().st_size} bytes)")  # noqa: T201 — example output


if __name__ == "__main__":
    asyncio.run(main())
