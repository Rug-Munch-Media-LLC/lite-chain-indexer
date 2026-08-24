"""Chainlist discovery tests (was 20% covered).

Offline: HTTP stubbed at httpx boundary. Contracts:

* chain_id_for: known EVM chains map; unknown/non-EVM → None
* _is_free_url: key placeholders/markers rejected, plain URLs accepted
* fetch_chain_registry: happy path caches (second call = cache hit),
  non-list payload → [], transport failure → []
* extract_rpc_candidates: chainId filter, non-dict rows skipped,
  keyed/marker URLs rejected, dedup, https-only
"""

from __future__ import annotations

import pytest

from lite_indexer.rpc import discovery as disc


@pytest.fixture(autouse=True)
def _clean_cache():
    disc._registry_cache.clear()
    yield
    disc._registry_cache.clear()


# ── chain_id_for ──────────────────────────────────────────────────────


def test_chain_id_known_chains():
    assert disc.chain_id_for("ethereum") == 1
    assert disc.chain_id_for("base") == 8453
    assert disc.chain_id_for("bsc") == 56


def test_chain_id_unknown_is_none():
    assert disc.chain_id_for("solana") is None  # non-EVM
    assert disc.chain_id_for("not-a-chain") is None


# ── _is_free_url ──────────────────────────────────────────────────────


def test_free_url_acceptance():
    assert disc._is_free_url("https://rpc.example.com") is True
    assert disc._is_free_url("wss://ws.example.com") is True


def test_keyed_urls_rejected():
    assert disc._is_free_url("https://x.com/v2/${API_KEY}") is False
    assert disc._is_free_url("https://x.com/?api-key=abc") is False
    assert disc._is_free_url("https://x.com/?apikey=abc") is False
    assert disc._is_free_url("https://x.com/v2/api_key_123") is False
    assert disc._is_free_url("https://x.com/?token=abc") is False


# ── fetch_chain_registry ──────────────────────────────────────────────


class _Resp:
    def __init__(self, payload, status=200):
        self._p = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            import httpx

            raise httpx.HTTPStatusError("err", request=None, response=None)

    def json(self):
        return self._p


class _Http:
    def __init__(self, payload, status=200, exc=None):
        self._payload = payload
        self._status = status
        self._exc = exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, *excinfo):
        return None

    async def get(self, url):
        if self._exc:
            raise self._exc
        return _Resp(self._payload, self._status)


@pytest.mark.asyncio
async def test_fetch_happy_path_and_cache(monkeypatch):
    registry = [{"chainId": 1, "rpc": ["https://a"]}]
    monkeypatch.setattr(disc.httpx, "AsyncClient", lambda **k: _Http(registry))
    first = await disc.fetch_chain_registry()
    assert first == registry
    # Second call served from the 1h cache — swap client for a poison and
    # verify the cached value still comes back.
    monkeypatch.setattr(disc.httpx, "AsyncClient", lambda **k: _Http([{"poison": True}]))
    assert await disc.fetch_chain_registry() == registry


@pytest.mark.asyncio
async def test_fetch_non_list_payload_empty(monkeypatch):
    monkeypatch.setattr(disc.httpx, "AsyncClient", lambda **k: _Http({"oops": True}))
    assert await disc.fetch_chain_registry() == []


@pytest.mark.asyncio
async def test_fetch_transport_failure_empty(monkeypatch):
    import httpx

    monkeypatch.setattr(disc.httpx, "AsyncClient", lambda **k: _Http(None, exc=httpx.ConnectError("down")))
    assert await disc.fetch_chain_registry() == []


# ── extract_rpc_candidates ────────────────────────────────────────────


def test_extract_filters_by_chain_id():
    registry = [
        {"chainId": 1, "rpc": ["https://eth-only.example"]},
        {"chainId": 8453, "rpc": ["https://base-only.example"]},
    ]
    out = disc.extract_rpc_candidates("ethereum", registry)
    assert out == ["https://eth-only.example"]


def test_extract_skips_junk_rows_and_urls():
    registry = [
        "not-a-dict",
        {"chainId": 1, "rpc": "not-a-list"},
        {"chainId": 1, "rpc": [None, "", 42, "https://good.example"]},
    ]
    assert disc.extract_rpc_candidates("ethereum", registry) == ["https://good.example"]


def test_extract_rejects_keyed_and_dedups():
    registry = [
        {
            "chainId": 1,
            "rpc": [
                "https://keyed.example/${KEY}",
                "https://dup.example",
                "https://dup.example",
            ],
        }
    ]
    assert disc.extract_rpc_candidates("ethereum", registry) == ["https://dup.example"]


def test_extract_unknown_chain_empty():
    assert disc.extract_rpc_candidates("solana", [{"chainId": 1, "rpc": ["https://x"]}]) == []
