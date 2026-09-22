"""Coverage tests for ``lite_indexer.util`` bounded containers + HTTP limits.

Hermetic — pure in-memory containers and an httpx.Limits builder, no
network. Complements ``tests/test_bounded_collections.py`` (which pins the
eviction/LRU basics) by pinning the mapping-protocol surface, the
``maxsize`` guard, and the env-tunable connection-pool ceilings.
"""

from __future__ import annotations

import pytest

from lite_indexer.config import (
    HTTP_KEEPALIVE_EXPIRY,
    HTTP_MAX_CONNECTIONS,
    HTTP_MAX_KEEPALIVE_CONNECTIONS,
)
from lite_indexer.util.bounded import BoundedDict, BoundedSet
from lite_indexer.util.http import http_limits


# ── BoundedDict ─────────────────────────────────────────────────────────


def test_bounded_dict_rejects_non_positive_maxsize() -> None:
    with pytest.raises(ValueError, match="maxsize must be positive"):
        BoundedDict(0)
    with pytest.raises(ValueError, match="maxsize must be positive"):
        BoundedDict(-1)


def test_bounded_dict_maxsize_property() -> None:
    assert BoundedDict(3).maxsize == 3


def test_bounded_dict_seeds_from_initial_mapping() -> None:
    d: BoundedDict[str, int] = BoundedDict(5, {"a": 1, "b": 2})
    assert d["a"] == 1
    assert d["b"] == 2
    assert len(d) == 2


def test_bounded_dict_initial_mapping_respects_cap() -> None:
    d: BoundedDict[str, int] = BoundedDict(2, {"a": 1, "b": 2, "c": 3})
    assert len(d) == 2
    assert "a" not in d  # oldest evicted


def test_bounded_dict_delete_removes_key() -> None:
    d: BoundedDict[str, int] = BoundedDict(3)
    d["a"] = 1
    del d["a"]
    assert "a" not in d
    assert len(d) == 0


def test_bounded_dict_delete_missing_raises_keyerror() -> None:
    with pytest.raises(KeyError):
        del BoundedDict(3)["nope"]


def test_bounded_dict_iteration_is_insertion_order() -> None:
    d: BoundedDict[str, int] = BoundedDict(5)
    for key in ("a", "b", "c"):
        d[key] = 1
    assert list(d) == ["a", "b", "c"]


def test_bounded_dict_overwrite_does_not_grow() -> None:
    d: BoundedDict[str, int] = BoundedDict(2)
    d["a"] = 1
    d["a"] = 2
    assert len(d) == 1
    assert d["a"] == 2


def test_bounded_dict_is_a_mutable_mapping() -> None:
    d: BoundedDict[str, int] = BoundedDict(3)
    d["a"] = 1
    assert dict(d) == {"a": 1}
    assert d.get("missing") is None


# ── BoundedSet ──────────────────────────────────────────────────────────


def test_bounded_set_rejects_non_positive_maxsize() -> None:
    with pytest.raises(ValueError, match="maxsize must be positive"):
        BoundedSet(0)


def test_bounded_set_maxsize_property() -> None:
    assert BoundedSet(4).maxsize == 4


def test_bounded_set_add_and_contains() -> None:
    s: BoundedSet[str] = BoundedSet(3)
    s.add("x")
    assert "x" in s
    assert "y" not in s
    assert len(s) == 1


def test_bounded_set_duplicate_add_does_not_grow() -> None:
    s: BoundedSet[str] = BoundedSet(3)
    s.add("x")
    s.add("x")
    assert len(s) == 1


def test_bounded_set_discard_missing_is_noop() -> None:
    s: BoundedSet[str] = BoundedSet(3)
    s.discard("absent")  # must not raise
    assert len(s) == 0


def test_bounded_set_discard_removes_present() -> None:
    s: BoundedSet[str] = BoundedSet(3)
    s.add("x")
    s.discard("x")
    assert "x" not in s


def test_bounded_set_iteration_is_insertion_order() -> None:
    s: BoundedSet[str] = BoundedSet(5)
    for item in ("a", "b", "c"):
        s.add(item)
    assert list(s) == ["a", "b", "c"]


# ── http_limits ─────────────────────────────────────────────────────────


def test_http_limits_uses_config_defaults() -> None:
    limits = http_limits()
    assert limits.max_connections == HTTP_MAX_CONNECTIONS
    assert limits.max_keepalive_connections == HTTP_MAX_KEEPALIVE_CONNECTIONS
    assert limits.keepalive_expiry == HTTP_KEEPALIVE_EXPIRY


def test_http_limits_honors_explicit_overrides() -> None:
    limits = http_limits(max_connections=7, max_keepalive_connections=3)
    assert limits.max_connections == 7
    assert limits.max_keepalive_connections == 3


def test_http_limits_override_is_independent_per_field() -> None:
    limits = http_limits(max_connections=9)
    assert limits.max_connections == 9
    assert limits.max_keepalive_connections == HTTP_MAX_KEEPALIVE_CONNECTIONS
