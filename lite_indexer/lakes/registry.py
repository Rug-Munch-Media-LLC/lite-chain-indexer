"""Lake provider registry — data-driven defaults + DuckDB resource bounds.

Providers (tune via ``LAKE_<NAME>_RPS`` / ``LAKE_<NAME>_DAILY_REQUESTS``):

====================  ==============  ============================  =========================
Provider              Kind            Serves (chain, table)         Quota
====================  ==============  ============================  =========================
aws-eth               s3/anonymous    (eth, token_transfers/traces) rps 10 (anonymous S3)
aws-sonarx-base       s3/anonymous    (base, transfers/traces)      rps 10 (anonymous S3)
aws-sonarx-arbitrum   s3/anonymous    (arbitrum, transfers/traces)  rps 10 (anonymous S3)
sqd                   http/api        (*, sqd) — portal datasets    rps 2, daily_requests 5000
bigquery              gcp/bigquery    (eth, transactions/…)         rps 1, daily_requests 1000, daily_bytes 1 TiB
====================  ==============  ============================  =========================

Preference order: anonymous/free first, then token/quota'd; deterministic
tie-break by name. Future providers (HyperSync free token, Old
Faithful/solana) register here with their real quotas.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class LakeProvider:
    """One registered lake provider."""

    name: str
    kind: str
    serves: frozenset[tuple[str, str]]
    rps: float = 10.0
    daily_requests: int | None = None
    daily_bytes: int | None = None
    cooldown_s: float = 30.0


def _env_num(name: str, default: float) -> float:
    try:
        return float(os.environ[name])
    except (KeyError, ValueError):
        logger.debug("lake_env_invalid", name=name)
        return default


def _default_registry() -> dict[str, LakeProvider]:
    """Data-driven default registry with ``LAKE_<NAME>_*`` env overrides.

    Each spec is ``(kind, serves, rps, daily_requests, daily_bytes)``.
    """
    spec = {
        "aws-eth": (
            "s3/anonymous",
            {("eth", "token_transfers"), ("eth", "traces")},
            10.0,
            None,
            None,
        ),
        "aws-sonarx-base": (
            "s3/anonymous",
            {("base", "transfers"), ("base", "traces")},
            10.0,
            None,
            None,
        ),
        "aws-sonarx-arbitrum": (
            "s3/anonymous",
            {("arbitrum", "transfers"), ("arbitrum", "traces")},
            10.0,
            None,
            None,
        ),
        "sqd": ("http/api", {("*", "sqd")}, 2.0, 5000, None),
        "bigquery": (
            "gcp/bigquery",
            {
                ("eth", "transactions"),
                ("eth", "token_transfers"),
                ("eth", "blocks"),
                ("eth", "contracts"),
                ("eth", "logs"),
                ("solana", "token_transfers"),
                ("solana", "instructions"),
            },
            1.0,
            1000,
            1099511627776,
        ),
    }
    reg: dict[str, LakeProvider] = {}
    for name, (kind, serves, rps, daily, daily_bytes) in spec.items():
        env = name.upper().replace("-", "_")
        reg[name] = LakeProvider(
            name=name,
            kind=kind,
            serves=frozenset(serves),
            rps=_env_num(f"LAKE_{env}_RPS", rps),
            daily_requests=int(_env_num(f"LAKE_{env}_DAILY_REQUESTS", daily or 0)) or None,
            daily_bytes=int(_env_num(f"LAKE_{env}_DAILY_BYTES", daily_bytes or 0)) or None,
        )
    return reg


def duckdb_settings() -> list[str]:
    """Canonical resource-bounded DuckDB settings every lake read must apply."""
    memory = os.environ.get("LAKE_DUCKDB_MEMORY_LIMIT", "2GB")
    threads = os.environ.get("LAKE_DUCKDB_THREADS", "4")
    return [f"SET memory_limit = '{memory}'", f"SET threads = {threads}"]
