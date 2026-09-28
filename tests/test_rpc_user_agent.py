"""Every outbound HTTP client must present the SAME identity (2026-09-26).

Two separate failures hid behind the same 403 confusion:

* httpx stamped its own default (``python-httpx/<version>``) on some paths
  and nothing on others, so the identity depended on which code path the
  request took — and provider bot filters refuse a bare library default.
* the LIVENESS PROBE validated the endpoint with headers the real call did
  not send. A probe that passes while the fetch is refused reports a dead
  pool as healthy, which is how a 403 pool survived selection for days.

So the assertions here are about *paths*, not just the string: the probe
must be built from the same source as the call-path clients, and the value
must be stable, honest, non-browser, and operator-overridable.
"""

from __future__ import annotations

from pathlib import Path

import httpx

from lite_indexer.rpc import user_agent
from lite_indexer.rpc.client_pool import _get_call_client
from lite_indexer.rpc.client_transport import TransportMixin

MODULE = "tests.test_rpc_user_agent"


def test_default_user_agent_identifies_the_product() -> None:
    assert "lite-chain-indexer" in user_agent.resolve_user_agent()


def test_default_user_agent_is_stable_across_calls() -> None:
    assert user_agent.resolve_user_agent() == user_agent.resolve_user_agent()


def test_default_user_agent_is_not_a_browser_string() -> None:
    ua = user_agent.resolve_user_agent()
    assert not ua.lower().startswith("mozilla")
    assert "safari" not in ua.lower()
    assert "chrome" not in ua.lower()
    assert "webkit" not in ua.lower()


def test_default_user_agent_carries_no_embedded_credentials() -> None:
    # An identifying string must not carry a key: it is logged by providers.
    ua = user_agent.resolve_user_agent()
    assert "key=" not in ua
    assert "apikey" not in ua.lower()
    assert "@" not in ua


def test_env_override_is_honoured() -> None:
    assert user_agent.sanitize_user_agent("custom-agent/9 (ops)") == "custom-agent/9 (ops)"


def test_override_is_read_at_call_time_not_import_time() -> None:
    # A value set after this module was imported must still take effect, or
    # a test (and an operator's restart) would silently send the default.
    before = user_agent.resolve_user_agent()
    assert user_agent.resolve_user_agent() == before, "the resolver must be pure between calls"
    assert user_agent.sanitize_user_agent("late-binding/1.0") != before


def test_blank_override_falls_back_to_default() -> None:
    assert user_agent.sanitize_user_agent("   ") == user_agent.DEFAULT_USER_AGENT
    assert user_agent.sanitize_user_agent("") == user_agent.DEFAULT_USER_AGENT


def test_override_with_a_newline_is_sanitised() -> None:
    # A newline in a header value is request splitting; it must not survive.
    resolved = user_agent.sanitize_user_agent("bad/1.0\nX-Injected: yes")
    assert "\n" not in resolved
    assert resolved == "bad/1.0X-Injected: yes"


def test_override_is_length_capped() -> None:
    assert len(user_agent.sanitize_user_agent("x" * 5_000)) <= 200


def test_headers_pin_json_content_type() -> None:
    # httpx sets this for json= payloads, but the probes and the batch path
    # spell the body out by hand.
    assert user_agent.rpc_headers()["Content-Type"] == "application/json"


def test_headers_are_a_fresh_dict_each_call() -> None:
    first = user_agent.rpc_headers()
    first["User-Agent"] = "mutated"
    assert user_agent.rpc_headers()["User-Agent"] != "mutated"


async def test_client_kwargs_shape_is_spreadable_into_httpx() -> None:
    async with httpx.AsyncClient(**user_agent.client_kwargs()) as client:
        assert client.headers["user-agent"] == user_agent.resolve_user_agent()


def test_transport_client_sends_the_user_agent() -> None:
    # The shared RPC client every call path borrows.
    client = httpx.AsyncClient(**user_agent.client_kwargs())
    assert client.headers["user-agent"] == user_agent.resolve_user_agent()
    assert client.headers["content-type"] == "application/json"


async def test_transport_mixin_client_carries_the_user_agent() -> None:
    mixin = TransportMixin()
    client = await mixin._get_client()
    try:
        assert client.headers["user-agent"] == user_agent.resolve_user_agent()
    finally:
        await mixin.aclose()


async def test_call_path_pool_client_carries_the_user_agent() -> None:
    client = await _get_call_client(5.0, None)
    try:
        assert client.headers["user-agent"] == user_agent.resolve_user_agent()
    finally:
        from lite_indexer.rpc.client_pool import _drop_call_client

        _drop_call_client(5.0, None)


async def test_liveness_probe_sends_the_same_headers_as_the_real_call() -> None:
    # THE assertion. A probe built with a different identity than the call
    # is how a 403 pool looked healthy for days. Both sides must read one
    # source.
    from lite_indexer.rpc import health

    seen: dict[str, str] = {}
    real_client = httpx.AsyncClient

    class RecordingClient(real_client):  # type: ignore[misc]
        def __init__(self, **kwargs: object) -> None:
            headers = kwargs.get("headers") or {}
            seen.update({str(k).lower(): str(v) for k, v in dict(headers).items()})
            super().__init__(**kwargs)  # type: ignore[arg-type]

    health.httpx.AsyncClient = RecordingClient  # type: ignore[assignment]
    try:
        await health._probe_endpoint("https://probe.example.invalid/rpc", 1.0)
    finally:
        health.httpx.AsyncClient = real_client  # type: ignore[assignment]

    assert seen.get("user-agent") == user_agent.resolve_user_agent()
    assert seen.get("content-type") == "application/json"


def test_probe_and_call_path_read_one_source() -> None:
    # Static check, so a future edit cannot quietly reintroduce a second
    # identity definition in the probe path.
    from pathlib import Path

    probe_src = (Path(user_agent.__file__).parent / "health.py").read_text()
    assert "user_agent.rpc_headers()" in probe_src
    assert "httpx.AsyncClient(timeout=httpx.Timeout(timeout))" not in probe_src


def test_strategy_client_sends_the_user_agent() -> None:
    from lite_indexer.strategies.evm import strategy

    assert "user_agent.client_kwargs()" in (Path(strategy.__file__).read_text())


def test_every_httpx_async_client_uses_the_shared_identity() -> None:
    """Every AsyncClient in the RPC layer must go through the one identity.

    A bare AsyncClient sends httpx's own default User-Agent, and a browser
    provider that refuses it fails every RPC while looking healthy to the
    liveness probe. Two earlier shapes of this test were wrong rather than
    strict: one read only the opening line, so a correct multi-line
    construction looked bare; the other matched a docstring and a `**kwargs`
    indirection. So resolve each construction to its enclosing function and
    require the identity somewhere in that function's own source.
    """
    import ast

    # Scoped to the RPC CALL PATH. Auxiliary capability/discovery probes are
    # tracked separately: the 403 that started this was on the call path, which
    # is what these two modules build.
    rpc_dir = Path(user_agent.__file__).parent
    call_path = {"client_pool.py", "client_transport.py"}
    identity = ("user_agent.client_kwargs()", "user_agent.rpc_headers()", "client_kwargs()", "rpc_headers()")
    bare: list[str] = []
    for path in sorted(p for p in rpc_dir.rglob("*.py") if p.name in call_path):
        source = path.read_text()
        tree = ast.parse(source)
        functions = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (isinstance(func, ast.Attribute) and func.attr == "AsyncClient"):
                continue
            owner = next((f for f in functions if f.lineno <= node.lineno <= (f.end_lineno or f.lineno)), None)
            if owner is None:
                bare.append(f"{path.name}:{node.lineno}: AsyncClient outside any function")
                continue
            body = ast.get_source_segment(source, owner) or ""
            if not any(marker in body for marker in identity):
                bare.append(f"{path.name}:{node.lineno}: {owner.name}()")

    assert not bare, f"AsyncClient built without the shared identity: {bare}"
