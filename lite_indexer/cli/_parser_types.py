"""Shared argparse type for the CLI parser-family modules.

``argparse._SubParsersAction`` is generic over the parser class it
manufactures, so a bare annotation is a ``type-arg`` error under
``strict = true``. The parser family under ``cli/`` is split one file per
command family and repeats the same annotation a dozen times, so it is
written once here instead of at every call site.

Two branches, because the two consumers disagree:

* **mypy** (``TYPE_CHECKING``) sees the subscripted generic — this is the
  annotation that clears ``type-arg``.
* **runtime** sees the plain class: ``_SubParsersAction`` is NOT
  subscriptable on CPython 3.12 (``TypeError: type '_SubParsersAction' is
  not subscriptable``), and the use sites do a real ``import``, so a name
  has to exist.

Same shape as the inline annotation already used by
:mod:`lite_indexer.cli._orphan_sweep`.
"""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    #: The subparsers action every ``add_*_parsers(sub)`` hook receives.
    SubParsers = argparse._SubParsersAction[argparse.ArgumentParser]
else:
    SubParsers = argparse._SubParsersAction
