"""Tests for LakeProviderRouter — selection, cooldowns, quotas, usage ledger."""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from lite_indexer.lakes.registry import LakeProvider, duckdb_settings
from lite_indexer.lakes.router import LakeProviderRouter


def _router(tmp_path: Any, providers: list[LakeProvider]) -> LakeProviderRouter:
    return LakeProviderRouter(registry={p.name: p for p in providers}, usage_path=tmp_path / "lake_usage.json")


def _anon(name: str = "aws-a", serves: Any = None, rps: float = 100.0) -> LakeProvider:
    return LakeProvider(name=name, kind="s3/anonymous", serves=serves or frozenset({("*", "*")}), rps=rps)


def _token(name: str = "sqd", serves: Any = None, rps: float = 100.0, daily: int | None = None) -> LakeProvider:
    return LakeProvider(
        name=name, kind="http/api", serves=serves or frozenset({("*", "*")}), rps=rps, daily_requests=daily
    )


def test_anonymous_preferred_over_token(tmp_path: Any) -> None:
    rt = _router(tmp_path, [_token(), _anon()])
    prov = rt.select("eth", "traces")
    assert prov is not None and prov.name == "aws-a"


def test_deterministic_tiebreak_by_name(tmp_path: Any) -> None:
    rt = _router(tmp_path, [_anon("zeta"), _anon("alpha")])
    prov = rt.select("x", "y")
    assert prov is not None and prov.name == "alpha"


def test_serves_filtering(tmp_path: Any) -> None:
    rt = _router(tmp_path, [_anon("a", serves=frozenset({("base", "transfers")}))])
    assert rt.select("base", "transfers") is not None
    assert rt.select("base", "traces") is None


def test_cooldown_skip_and_recovery(tmp_path: Any) -> None:
    fast = LakeProvider(name="fast", kind="s3/anonymous", serves=frozenset({("*", "*")}), rps=100.0, cooldown_s=0.05)
    slow = LakeProvider(name="slow", kind="s3/anonymous", serves=frozenset({("*", "*")}), rps=100.0, cooldown_s=0.05)
    rt = _router(tmp_path, [fast, slow])
    rt.report_failure("fast", error="boom")
    assert rt.select("x", "y").name == "slow"  # cooling provider skipped
    import time

    time.sleep(0.06)
    assert rt.select("x", "y").name == "fast"  # recovered


def test_exponential_cooldown_capped(tmp_path: Any) -> None:
    p = LakeProvider(name="p", kind="s3/anonymous", serves=frozenset({("*", "*")}), cooldown_s=10.0)
    rt = _router(tmp_path, [p])
    for _ in range(10):
        rt.report_failure("p", error="boom")
    import time

    until = rt._state["p"].cooldown_until
    assert until - time.monotonic() <= 3600.0 + 0.1  # capped


def test_rps_quota_refusal(tmp_path: Any) -> None:
    rt = _router(tmp_path, [_anon(rps=2.0)])
    assert rt.select("x", "y") is not None
    assert rt.select("x", "y") is not None
    assert rt.select("x", "y") is None  # third call in the same second refused


def test_daily_quota_refusal(tmp_path: Any) -> None:
    rt = _router(tmp_path, [_token(daily=2)])
    assert rt.select("x", "y") is not None
    rt.report_success("sqd")
    assert rt.select("x", "y") is not None
    rt.report_success("sqd")
    assert rt.select("x", "y") is None  # daily_requests exhausted


def test_day_rollover_resets_counters(tmp_path: Any) -> None:
    rt = _router(tmp_path, [_token(daily=1)])
    rt.report_success("sqd")
    rt._state["sqd"].usage["day"] = "2000-01-01"  # simulate stale day
    assert rt.select("x", "y") is not None  # rollover reset requests


def test_usage_persistence_roundtrip(tmp_path: Any) -> None:
    rt = _router(tmp_path, [_anon()])
    rt.report_success("aws-a", bytes_read=1234)
    rt.report_failure("aws-a", error="boom")
    raw = json.loads((tmp_path / "lake_usage.json").read_text())
    assert raw["aws-a"]["bytes"] == 1234
    assert raw["aws-a"]["failures"] == 1
    rt2 = LakeProviderRouter(registry={"aws-a": _anon()}, usage_path=tmp_path / "lake_usage.json")
    assert rt2._state["aws-a"].usage["bytes"] == 1234
    assert rt2._state["aws-a"].usage["failures"] == 1


def test_exclude_skips_provider(tmp_path: Any) -> None:
    rt = _router(tmp_path, [_anon("a"), _anon("b")])
    prov = rt.select("x", "y", exclude={"a"})
    assert prov is not None and prov.name == "b"


def test_duckdb_settings_defaults_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    saved = {k: os.environ.get(k) for k in ("LAKE_DUCKDB_MEMORY_LIMIT", "LAKE_DUCKDB_THREADS")}
    try:
        os.environ.pop("LAKE_DUCKDB_MEMORY_LIMIT", None)
        os.environ.pop("LAKE_DUCKDB_THREADS", None)
        assert duckdb_settings() == ["SET memory_limit = '2GB'", "SET threads = 4"]
        os.environ["LAKE_DUCKDB_MEMORY_LIMIT"] = "512MB"
        os.environ["LAKE_DUCKDB_THREADS"] = "2"
        assert duckdb_settings() == ["SET memory_limit = '512MB'", "SET threads = 2"]
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_default_registry_env_overrides(tmp_path: Any) -> None:
    saved = {k: os.environ.get(k) for k in ("LAKE_SQD_RPS", "LAKE_SQD_DAILY_REQUESTS")}
    try:
        os.environ["LAKE_SQD_RPS"] = "5"
        os.environ["LAKE_SQD_DAILY_REQUESTS"] = "100"
        rt = LakeProviderRouter(usage_path=tmp_path / "u.json")
        sqd = rt._registry["sqd"]
        assert sqd.rps == 5.0 and sqd.daily_requests == 100
        assert rt._registry["aws-eth"].kind == "s3/anonymous"
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
