"""Tests for generic per-account key detection in ``rpc/ledger.py``.

``key_id_for_endpoint`` derives a stable, NON-SECRET (hashed) account id from
an endpoint URL so per-account quota/degrade accounting can separate two keys
(and two accounts) on the same provider host.

Key sources, in precedence order:
  1. an explicit key query param (``dkey``/``apikey``/``api-key``/``api_key``/
     ``key``/``token``);
  2. URL userinfo (``user[:pass]@host``);
  3. the longest token-shaped path segment anywhere, or a ``name=<token>``
     path value.

Tokens in these tests are synthetic (never real secrets).
"""

from __future__ import annotations

import hashlib

import pytest

from lite_indexer.rpc.ledger import key_id_for_endpoint

TOKEN_A = "a" * 24
TOKEN_B = "b" * 32
TOKEN_C = "c" * 40


def _eid(value: str) -> str:
    """Expected id: sha256(value)[:16] (matches the implementation contract)."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


class TestUserinfo:
    def test_username_only(self):
        assert key_id_for_endpoint("https://alice@rpc.example.com/jsonrpc") == _eid("alice")

    def test_username_and_password(self):
        assert key_id_for_endpoint("https://alice:s3cret@rpc.example.com/jsonrpc") == _eid("alice:s3cret")

    def test_distinct_users_get_distinct_ids(self):
        a = key_id_for_endpoint("https://alice@rpc.example.com/jsonrpc")
        b = key_id_for_endpoint("https://bob@rpc.example.com/jsonrpc")
        assert a and b and a != b

    def test_percent_encoded_userinfo(self):
        assert key_id_for_endpoint("https://a%40b:p%3Aw@rpc.example.com/jsonrpc") == _eid("a@b:p:w")


class TestMidPathTokens:
    def test_token_anywhere_in_path(self):
        url = f"https://host.example.xyz/{TOKEN_A}/robinhood/rpc"
        assert key_id_for_endpoint(url) == _eid(TOKEN_A)

    def test_longest_candidate_wins(self):
        url = f"https://spectrum.example.xyz/{TOKEN_A}/{TOKEN_C}/robinhood/mn/4663/shared/archive/rpc/"
        assert key_id_for_endpoint(url) == _eid(TOKEN_C)

    def test_non_trailing_token_is_detected(self):
        # Regression: the old code only looked at the LAST path segment.
        url = f"https://h.example.com/robinhood/{TOKEN_B}/shared/rpc"
        assert key_id_for_endpoint(url) == _eid(TOKEN_B)


class TestNameValueSegments:
    def test_api_equals_token(self):
        assert key_id_for_endpoint(f"https://gateway.example.com/api={TOKEN_A}") == _eid(TOKEN_A)

    def test_key_equals_token(self):
        assert key_id_for_endpoint(f"https://h.example.com/key={TOKEN_B}") == _eid(TOKEN_B)


class TestPrecedence:
    def test_query_param_beats_path(self):
        url = f"https://h.example.com/{TOKEN_A}/x?dkey={TOKEN_B}"
        assert key_id_for_endpoint(url) == _eid(TOKEN_B)

    def test_query_param_beats_userinfo(self):
        url = f"https://alice:pw@h.example.com/x?apikey={TOKEN_B}"
        assert key_id_for_endpoint(url) == _eid(TOKEN_B)

    def test_userinfo_beats_path(self):
        url = f"https://alice@h.example.com/{TOKEN_A}/x"
        assert key_id_for_endpoint(url) == _eid("alice")


class TestUnchangedKeyedEndpoints:
    """Existing keyed endpoints must keep their prior id (no behaviour change)."""

    QUERY_CASES = [
        ("onfinality", f"https://eth.api.onfinality.io/rpc?apikey={TOKEN_B}", TOKEN_B),
        ("drpc", f"https://lb.drpc.org/ogrpc?network=ethereum&dkey={TOKEN_B}", TOKEN_B),
        ("uniblock", f"https://api.uniblock.dev/uni/v1/json-rpc?chainId=1&apiKey={TOKEN_B}", TOKEN_B),
        ("helius", f"https://mainnet.helius-rpc.com/?api-key={TOKEN_B}", TOKEN_B),
    ]
    PATH_CASES = [
        ("alchemy", f"https://eth-mainnet.g.alchemy.com/v2/{TOKEN_A}", TOKEN_A),
        ("infura", f"https://mainnet.infura.io/v3/{TOKEN_A}", TOKEN_A),
        ("blockpi", f"https://eth.blockpi.network/v1/rpc/{TOKEN_A}", TOKEN_A),
        ("blockvision", f"https://eth-mainnet.blockvision.org/v1/{TOKEN_A}", TOKEN_A),
    ]

    @pytest.mark.parametrize("label,url,token", QUERY_CASES + PATH_CASES)
    def test_id_matches_expected_hash(self, label: str, url: str, token: str):
        assert key_id_for_endpoint(url) == _eid(token), label

    def test_two_keys_same_host_are_distinct(self):
        a = key_id_for_endpoint(f"https://eth-mainnet.g.alchemy.com/v2/{TOKEN_A}")
        b = key_id_for_endpoint(f"https://eth-mainnet.g.alchemy.com/v2/{TOKEN_B}")
        assert a and b and a != b


class TestKeylessEndpoints:
    def test_plain_hosts_are_empty(self):
        for url in (
            "https://rpc.example.com",
            "https://ethereum.publicnode.com",
            "https://eth.blockscout.com/api/eth-rpc",
            "https://gateway.tenderly.co/public/mainnet",
        ):
            assert key_id_for_endpoint(url) == "", url

    def test_empty_url_is_empty(self):
        assert key_id_for_endpoint("") == ""

    def test_short_path_segments_are_not_keys(self):
        # "/public", "/rpc", "v1" are too short to be account keys.
        assert key_id_for_endpoint("https://h.example.com/rpc/public") == ""
