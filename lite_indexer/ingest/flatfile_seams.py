"""Injection seams for flatfile HTTP fetching.

Keeps network I/O testable without monkeypatching: tests (and ops) set a
fetcher here; the flatfile source calls it when present.
"""
from __future__ import annotations

from collections.abc import Callable

_URL_FETCHER: Callable[[str], bytes] | None = None


def set_url_fetcher(fn: Callable[[str], bytes] | None) -> None:
    """Inject the HTTP fetcher for external flatfiles (None = ``urlopen``)."""
    global _URL_FETCHER
    _URL_FETCHER = fn


def injected_fetcher() -> Callable[[str], bytes] | None:
    """The injected fetcher, or None when the real ``urlopen`` should run."""
    return _URL_FETCHER
