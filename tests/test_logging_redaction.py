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


# The key below is SHAPED like a 32-hex provider path key so the redaction
# pattern is genuinely exercised, but it is all zeros and therefore verifies
# against no service. The previous value was a real Infura path key that
# verified against the live Infura API — a live third-party credential
# committed to a public repo purely as test bait. trufflehog-verified flagged
# it once that gate was made to actually run (it had been structurally dead:
# `git rev-parse @{u}` fails on a detached worktree, so the scan never
# executed). A placeholder that verifies against a real service is
# indistinguishable, to a scanner and to an attacker, from a live key.
# If a value here ever was a real credential, rotate it — removing it from
# this file does NOT remove it from git history.
_TEST_ONLY_PLACEHOLDER_KEY = "0" * 32


def test_infura_and_blockpi_style_redacted():
    assert (
        "00000000000000000000000000000000"
        not in redact_secrets(
            None, "info", {"endpoint": "https://mainnet.infura.io/v3/00000000000000000000000000000000"}
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
    p = "/srv/[INTERNAL_DATA_DIR]/lite-indexer-endpoints.json"
    assert redact_secrets(None, "debug", {"path": p})["path"] == p


def test_short_16plus_char_path_key_redacted():
    """Regression: an 18-char key must be masked (20-char threshold missed it)."""
    for key in ("LhEQUVP97Rqwo8xnHm6", "r492rBxdmKhhDCvY94UIY"):
        d = redact_secrets(None, "info", {"endpoint": f"https://polygon-mainnet.g.alchemy.com/v2/{key}"})
        assert key not in d["endpoint"]
        assert d["endpoint"].endswith("/***")
