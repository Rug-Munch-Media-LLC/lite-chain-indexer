"""Residential-proxy routing for RPC calls.

Covers ``lite_indexer/rpc/proxy.py`` and the client integration
(``_get_call_client`` proxy kwarg + ``_post_json`` fallback):

* every accepted proxy string shape parses; malformed entries are skipped
* default-off == direct (OSS behaviour unchanged)
* ``sticky`` pins a proxy per endpoint and fails over on failure
* ``rotate`` cycles the pool
* a proxy transport failure triggers ONE direct retry
  (``rpc_proxy_fallback_direct``) and unpins the failed proxy
* secrets never appear in labels/snapshots

Hermetic — dummy hosts only, no network.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from lite_indexer.rpc import client as rc
from lite_indexer.rpc import proxy as rpc_proxy
from lite_indexer.rpc.proxy import ProxyRouter, ProxySpec, parse_proxy_entry, parse_proxy_list


class _Resp:
    status_code = 200
    headers: dict[str, str] = {}
    text = json.dumps({"jsonrpc": "2.0", "id": 1, "result": "0x1"})
    content = text.encode()

    def json(self) -> Any:
        return json.loads(self.text)


class _DirectClient:
    is_closed = False

    def __init__(self) -> None:
        self.calls = 0

    async def post(self, url: str, **kw: Any) -> _Resp:
        self.calls += 1
        return _Resp()


class _ProxyClient:
    is_closed = False

    def __init__(self) -> None:
        self.calls = 0

    async def post(self, url: str, **kw: Any) -> _Resp:
        self.calls += 1
        raise httpx.ProxyError("proxy unreachable")


class TestParseShapes:
    @pytest.mark.parametrize(
        ("entry", "url", "label"),
        [
            ("proxy.example:8080", "http://proxy.example:8080", "proxy.example:8080"),
            (
                "proxy.example:8080:user:secret",
                "http://user:secret@proxy.example:8080",
                "proxy.example:8080",
            ),
            (
                "http://user:pass@proxy.example:8080",
                "http://user:pass@proxy.example:8080",
                "proxy.example:8080",
            ),
            ("socks5://proxy.example:1080", "socks5://proxy.example:1080", "proxy.example:1080"),
        ],
    )
    def test_accepted_shapes(self, entry: str, url: str, label: str) -> None:
        spec = parse_proxy_entry(entry)
        assert spec is not None
        assert spec.url == url
        assert spec.label == label

    @pytest.mark.parametrize("entry", ["", "nonsense", "host:", "ftp://proxy.example:8080", "proxy.example:notaport"])
    def test_malformed_entries_skipped(self, entry: str) -> None:
        assert parse_proxy_entry(entry) is None

    def test_list_dedupes_and_splits(self) -> None:
        specs = parse_proxy_list("proxy.example:8080 proxy.example:8080,proxy2.example:8081")
        assert [s.label for s in specs] == ["proxy.example:8080", "proxy2.example:8081"]

    def test_label_never_leaks_credentials(self) -> None:
        spec = parse_proxy_entry("proxy.example:8080:user:topsecret")
        assert spec is not None
        assert "topsecret" not in spec.label
        assert "user" not in spec.label


class TestRouter:
    def test_default_off_is_direct(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("WEBSHARE_PROXY_LIST", raising=False)
        monkeypatch.delenv("RPC_PROXY_ENABLED", raising=False)
        router = ProxyRouter.from_env()
        assert router.enabled is False
        assert router.proxy_for("https://rpc.example") is None

    def test_configured_but_disabled_is_direct(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("WEBSHARE_PROXY_LIST", "proxy.example:8080")
        monkeypatch.setenv("RPC_PROXY_ENABLED", "0")
        router = ProxyRouter.from_env()
        assert router.enabled is False
        assert router.proxy_for("https://rpc.example") is None

    def test_env_enables_pool(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("WEBSHARE_PROXY_LIST", "proxy.example:8080")
        monkeypatch.setenv("RPC_PROXY_ENABLED", "1")
        router = ProxyRouter.from_env()
        assert router.enabled is True
        assert router.mode == "sticky"

    def test_sticky_pins_then_fails_over(self) -> None:
        a, b = (
            ProxySpec("http://a.example:8080", "a.example:8080"),
            ProxySpec("http://b.example:8080", "b.example:8080"),
        )
        router = ProxyRouter([a, b], enabled=True, mode="sticky")
        first = router.proxy_for("https://rpc.example")
        assert first is not None
        assert router.proxy_for("https://rpc.example") is first  # pinned
        router.mark_failed("https://rpc.example", first)
        second = router.proxy_for("https://rpc.example")
        assert second is not None and second is not first  # failed over
        assert router.snapshot()["failed_proxies"] == 1

    def test_rotate_cycles(self) -> None:
        a, b = (
            ProxySpec("http://a.example:8080", "a.example:8080"),
            ProxySpec("http://b.example:8080", "b.example:8080"),
        )
        router = ProxyRouter([a, b], enabled=True, mode="rotate")
        picks = {router.proxy_for("https://rpc.example") for _ in range(4)}
        assert picks == {a, b}

    def test_snapshot_is_secret_free(self) -> None:
        spec = ProxySpec("http://user:pass@proxy.example:8080", "proxy.example:8080")
        router = ProxyRouter([spec], enabled=True)
        router.proxy_for("https://rpc.example")
        snap = router.snapshot()
        assert "pass" not in json.dumps(snap)
        assert snap["proxy_count"] == 1


class TestClientIntegration:
    @pytest.fixture(autouse=True)
    def _reset_clients(self, monkeypatch: pytest.MonkeyPatch) -> Any:
        monkeypatch.setattr(rpc_proxy, "PROXY_ROUTER", ProxyRouter([], enabled=False))
        rc._call_clients.clear()
        yield
        rc._call_clients.clear()

    @pytest.mark.asyncio
    async def test_get_call_client_applies_proxy_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        created: list[dict[str, Any]] = []

        def factory(**kwargs: Any) -> _DirectClient:
            created.append(kwargs)
            return _DirectClient()

        monkeypatch.setattr(rc.httpx, "AsyncClient", factory)
        spec = ProxySpec("http://proxy.example:8080", "proxy.example:8080")
        await rc._get_call_client(5.0, proxy=spec)
        assert created[0]["proxy"] == "http://proxy.example:8080"
        await rc._get_call_client(5.0)
        assert "proxy" not in created[1]

    @pytest.mark.asyncio
    async def test_post_json_default_off_is_direct(self, monkeypatch: pytest.MonkeyPatch) -> None:
        direct = _DirectClient()
        seen: list[Any] = []

        async def fake(timeout: float, proxy: Any = None) -> _DirectClient:
            seen.append(proxy)
            return direct

        monkeypatch.setattr(rc, "_get_call_client", fake)
        resp = await rc._post_json("https://rpc.example", {"a": 1}, 5.0)
        assert resp.status_code == 200
        assert direct.calls == 1
        assert seen == [None]

    @pytest.mark.asyncio
    async def test_proxy_failure_falls_back_direct_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        spec = ProxySpec("http://proxy.example:8080", "proxy.example:8080")
        router = ProxyRouter([spec], enabled=True, mode="sticky")
        monkeypatch.setattr(rpc_proxy, "PROXY_ROUTER", router)
        proxy_client = _ProxyClient()
        direct_client = _DirectClient()
        seen: list[Any] = []

        async def fake(timeout: float, proxy: Any = None) -> Any:
            seen.append(proxy)
            return proxy_client if proxy is not None else direct_client

        monkeypatch.setattr(rc, "_get_call_client", fake)
        resp = await rc._post_json("https://rpc.example", {"a": 1}, 5.0)
        assert resp.status_code == 200
        assert proxy_client.calls == 1
        assert direct_client.calls == 1
        assert seen[0] is spec
        assert seen[1] is None  # the direct retry
        assert router.snapshot()["failed_proxies"] == 1
