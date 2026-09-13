"""Regression tests: provider secrets must never reach the logs.

2026-09-13 incident: endpoint URLs with embedded API keys appeared in
journald via httpx and our own structured events. The `redact_secrets`
structlog processor masks query-param secrets, userinfo, and long key-like
path segments.
"""

from __future__ import annotations

from lite_indexer.logging import redact_secrets


def test_query_param_secrets_redacted():
    d = redact_secrets(
        None,
        "info",
        {"endpoint": "https://lb.drpc.org/ogrpc?network=bsc&dkey=SUPERSECRETKEYVALUE123456"},
    )
    assert "SUPERSECRETKEYVALUE123456" not in d["endpoint"]
    assert "dkey=***" in d["endpoint"]


def test_path_segment_secret_redacted():
    d = redact_secrets(
        None,
        "info",
        {"endpoint": "https://eth-mainnet.g.alchemy.com/v2/alch_JS8Hfk_boM0WTO83dLTsV"},
    )
    assert "alch_JS8Hfk_boM0WTO83dLTsV" not in d["endpoint"]
    assert d["endpoint"].endswith("/***")


def test_userinfo_redacted():
    d = redact_secrets(
        None,
        "warning",
        {"url": "https://zen-panini:tronsecretpassword@tron-mainnet.core.chainstack.com/jsonrpc"},
    )
    assert "tronsecretpassword" not in d["url"]
    assert "://***@" in d["url"]


def test_infura_and_blockpi_style_redacted():
    assert (
        "aed9e73cca9c462999d6649460e30047"
        not in redact_secrets(
            None, "info", {"endpoint": "https://mainnet.infura.io/v3/aed9e73cca9c462999d6649460e30047"}
        )["endpoint"]
    )
    assert (
        "1b0e56635142347bae8b77c148a652705055329c"
        not in redact_secrets(
            None,
            "info",
            {"endpoint": "https://ethereum.blockpi.network/v1/rpc/1b0e56635142347bae8b77c148a652705055329c"},
        )["endpoint"]
    )


def test_public_keyless_urls_untouched():
    url = "https://ethereum.publicnode.com"
    assert redact_secrets(None, "info", {"endpoint": url})["endpoint"] == url


def test_list_valued_endpoints_redacted():
    """endpoint_health_* events log lists of URLs — must be redacted too."""
    d = redact_secrets(
        None,
        "warning",
        {
            "down_endpoints": ["https://lb.drpc.org/ogrpc?network=celo&dkey=ArsRd2axgUl6gmbTmqO"],
            "endpoints": ["https://api.uniblock.dev/uni/v1/json-rpc?chainId=1&apiKey=RPqKwkwyxDgPm"],
        },
    )
    assert "dkey=ArsRd2axgUl6gmbTmqO" not in d["down_endpoints"][0]
    assert "dkey=***" in d["down_endpoints"][0]
    assert "RPqKwkwyxDgPm" not in d["endpoints"][0]
    assert "apiKey=***" in d["endpoints"][0]


def test_filesystem_path_not_mangled():
    """Only URL path segments are masked; ordinary paths stay readable."""
    p = "/srv/secrets/lite-indexer-endpoints.json"
    assert redact_secrets(None, "debug", {"path": p})["path"] == p
