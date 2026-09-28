"""Provider-agnostic external flat-file bootstrap source.

Hydrate the indexer from **any** pre-indexed parquet dataset — no RPC, no
provider identity — by pointing ``lite-indexer bootstrap --source <loc>`` at:

* a **local directory** (``/path/to/dataset``);
* an **http(s) base URL** (``https://host/dataset``); or
* a **pyarrow.fs URL** (``s3://bucket/prefix``, ``gs://bucket/prefix``) when
  ``pyarrow.fs`` is available.

Expected layout
---------------

Managed datasets ship a manifest at ``<base>/manifest.json``::

    {
      "chain": "ethereum",
      "date": "2026-01-01",
      "start_block": 0,
      "end_block": 25000000,
      "tables": {
        "evm_transfers": {
          "rows": 1000,
          "size_bytes": 4096,
          "parquet_files": ["part-0.parquet", "part-1.parquet"],
          "chunks": [
            {"file": "part-0.parquet", "rows": 500, "start_block": 0, "end_block": 12000000},
            {"file": "part-1.parquet", "rows": 500, "start_block": 12000001, "end_block": 25000000}
          ]
        }
      }
    }

``chunks`` is optional: it carries per-file block ranges so the high-water
guard can skip already-indexed ranges. Without it, a dataset that overlaps
the chain high-water is skipped entirely unless ``--force`` is given.

Manifest-less datasets fall back to hive-style glob mode::

    <base>/chain=<chain>/*.parquet

with best-effort block ranges inferred from a ``block_number`` (EVM) or
``slot`` (Solana) column when present. Glob mode assumes the canonical
``evm_transfers`` table and does not verify row counts.

Import path
-----------

This source does **not** implement its own importer. It stages the selected
parquet chunks locally, builds a :class:`~lite_indexer.snapshot_manifest.SnapshotManifest`,
and reuses the existing bootstrap machinery —
:func:`~lite_indexer.bootstrap.build_import_sql`,
:meth:`~lite_indexer.bootstrap.SnapshotBootstrap._run_imports`,
``_verify_row_counts`` and ``_set_high_water`` — so a flat-file import is
byte-for-byte the same ClickHouse ``INSERT ... SELECT * FROM file(...)``
path used by the B2 snapshot bootstrap.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
import shutil
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import urlopen

from lite_indexer.bootstrap import SnapshotBootstrap, build_import_sql
from lite_indexer.config import BOOTSTRAP_CH_FILE_ROOT, FLATFILE_STAGE_DIR
from lite_indexer.ingest.flatfile_seams import injected_fetcher
from lite_indexer.logging import get_logger
from lite_indexer.snapshot_manifest import MANIFEST_NAME, SnapshotManifest, SnapshotTable
from lite_indexer.strategies import SUPPORTED_CHAINS

logger = get_logger(__name__)

# Column names we use to infer a chunk's block/slot range (best effort).
_BLOCK_COLUMNS = ("block_number", "slot")
# Glob-mode table: flat parquet files carry no table identity.
_GLOB_TABLE = "evm_transfers"
# Hive-style partition directory prefix: ``chain=<chain>/``.
_CHAIN_DIR_PREFIX = "chain="
# Date fallback for a manifest-less dataset (filesystem-safe, no semantics).
_GLOB_DATE = "flatfile"

class FlatfileSourceError(RuntimeError):
    """Raised for an invalid or unusable flat-file source."""

@dataclass(frozen=True)
class FlatfileChunk:
    """One parquet chunk within a dataset table.

    Attributes:
        name: File name relative to the dataset's chain directory (the
            name used under the local staging dir).
        locator: Source-relative path used to read the chunk (relative to
            the dataset base; includes the ``chain=<chain>/`` prefix in
            glob mode).
        rows: Declared or inferred row count (0 = unknown).
        start_block: First block/slot in the chunk (0 = unknown).
        end_block: Last block/slot in the chunk (0 = unknown).
    """

    name: str
    locator: str = ""
    rows: int = 0
    start_block: int = 0
    end_block: int = 0

@dataclass(frozen=True)
class FlatfileTable:
    """A table within a dataset (declared stats + chunk list)."""

    name: str
    declared_rows: int = 0
    chunks: tuple[FlatfileChunk, ...] = field(default_factory=tuple)

    def rows(self) -> int:
        """Declared rows when present, else the sum of chunk rows."""
        if self.declared_rows > 0:
            return self.declared_rows
        return sum(c.rows for c in self.chunks)

@dataclass(frozen=True)
class FlatfileDataset:
    """A single-chain external flat-file dataset."""

    chain: str
    date: str
    start_block: int
    end_block: int
    tables: dict[str, FlatfileTable]
    origin: str = ""
    from_manifest: bool = True

    def total_rows(self) -> int:
        """Sum of declared/inferred rows across all tables."""
        return sum(t.rows() for t in self.tables.values())

    def total_chunks(self) -> int:
        """Total parquet chunks across all tables."""
        return sum(len(t.chunks) for t in self.tables.values())

    def to_manifest(self) -> SnapshotManifest:
        """Convert to the canonical bootstrap manifest (reused by the importer)."""
        tables = {
            name: SnapshotTable(
                name=name,
                rows=table.rows(),
                size_bytes=0,
                parquet_files=tuple(c.name for c in table.chunks),
            )
            for name, table in self.tables.items()
        }
        return SnapshotManifest(
            chain=self.chain,
            date=self.date,
            start_block=self.start_block,
            end_block=self.end_block,
            tables=tables,
        )

    def above(self, block: int) -> FlatfileDataset | None:
        """Return a dataset restricted to chunks located above ``block``.

        Chunks whose range is unknown are excluded (a partial import cannot
        be proven safe for them). Returns None when no chunk qualifies.
        """
        kept: dict[str, FlatfileTable] = {}
        for name, table in self.tables.items():
            chunks = tuple(c for c in table.chunks if c.end_block > block)
            if chunks:
                # Declared table rows cover the full dataset; drop them once
                # filtered so row-count verification is not misled.
                kept[name] = FlatfileTable(name=name, declared_rows=0, chunks=chunks)
        if not kept:
            return None
        starts = [c.start_block for t in kept.values() for c in t.chunks if c.start_block > 0]
        ends = [c.end_block for t in kept.values() for c in t.chunks if c.end_block > 0]
        return FlatfileDataset(
            chain=self.chain,
            date=self.date,
            start_block=min(starts) if starts else 0,
            end_block=max(ends) if ends else self.end_block,
            tables=kept,
            origin=self.origin,
            from_manifest=self.from_manifest,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serializable summary for ``list()`` output."""
        return {
            "chain": self.chain,
            "date": self.date,
            "block_range": [self.start_block, self.end_block],
            "chunk_count": self.total_chunks(),
            "rows": self.total_rows(),
            "tables": list(self.tables),
            "origin": self.origin,
            "manifest": self.from_manifest,
        }

@dataclass
class FlatfileImportResult:
    """Outcome of one ``import_chain`` run."""

    chain: str
    source: str = ""
    status: str = "pending"  # ok / dry_run / skipped / clickhouse_unavailable / error
    date: str = ""
    start_block: int = 0
    end_block: int = 0
    imported_start_block: int = 0
    imported_end_block: int = 0
    blocks: int = 0
    rows_imported: int = 0
    rows_declared: int = 0
    tables: dict[str, int] = field(default_factory=dict)
    plan: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serializable summary (CLI contract + diagnostics)."""
        return {
            "chain": self.chain,
            "source": self.source,
            "status": self.status,
            "date": self.date,
            "block_range": [self.start_block, self.end_block],
            "imported_block_range": [self.imported_start_block, self.imported_end_block],
            "blocks": self.blocks,
            "rows_imported": self.rows_imported,
            "rows_declared": self.rows_declared,
            "tables": self.tables,
            "plan": self.plan,
            "duration_seconds": round(self.duration_seconds, 2),
            "errors": self.errors,
        }

class ExternalFlatfileSource:
    """Hydrate from a pre-indexed parquet dataset (local dir / http / pyarrow.fs).

    Args:
        source: Dataset location — a local directory, an ``http(s)://``
            base URL, or a pyarrow.fs URL (``s3://``, ``gs://``, ...). An
            empty value resolves from ``LITE_INDEXER_FLATFILE_SOURCE``.
        target_db: Target ClickHouse database.
        force: Import even when the dataset overlaps the chain high-water.
        stage_dir: Local staging dir for materialized chunks.
        ch_file_root: Path root ClickHouse sees for the staged files
            (empty = use the staging dir).
        state_dir: State directory override for the high-water mark.
    """

    def __init__(
        self,
        source: str = "",
        target_db: str = "wallet_memory",
        force: bool = False,
        stage_dir: Path = FLATFILE_STAGE_DIR,
        ch_file_root: str = BOOTSTRAP_CH_FILE_ROOT,
        state_dir: Path | None = None,
    ) -> None:
        resolved = source or os.environ.get("LITE_INDEXER_FLATFILE_SOURCE", "")
        if not resolved:
            raise FlatfileSourceError("no flat-file source: pass --source or set LITE_INDEXER_FLATFILE_SOURCE")
        self.source = resolved.rstrip("/")
        self.target_db = target_db
        self.force = force
        self.stage_dir = stage_dir
        self.ch_file_root = ch_file_root
        self._state_dir = state_dir
        self._importer = SnapshotBootstrap(state_dir=state_dir)

    # ── source classification ─────────────────────────────────────

    @property
    def scheme(self) -> str:
        """Source scheme: ``local`` / ``http`` / ``fs``."""
        parsed = urlparse(self.source)
        if parsed.scheme in ("http", "https"):
            return "http"
        if parsed.scheme:
            return "fs"
        return "local"

    def _local_base(self) -> Path:
        """The source as a local path (local scheme only)."""
        return Path(self.source)

    def _fs_parts(self) -> tuple[Any, str]:
        """Resolve a pyarrow.fs URL to ``(filesystem, base_path)``."""
        pafs: Any = self._import("pyarrow.fs", "pyarrow.fs")
        try:
            fs, base = pafs.FileSystem.from_uri(self.source)
        except Exception as exc:  # noqa: BLE001 — normalize third-party errors
            raise FlatfileSourceError(f"cannot open filesystem source {self.source!r}: {exc}") from exc
        return fs, str(base)

    @staticmethod
    def _import(module: str, label: str) -> Any:
        """Import an optional module lazily, returning ``Any`` (stable typing)."""
        try:
            return importlib.import_module(module)
        except ImportError as exc:  # pragma: no cover — optional dependency
            raise FlatfileSourceError(f"{label} is required for this flat-file source") from exc

    # ── raw reads ─────────────────────────────────────────────────

    def _read_bytes(self, locator: str) -> bytes:
        """Read a source-relative path into bytes."""
        scheme = self.scheme
        if scheme == "http":
            return self._fetch_url(f"{self.source}/{locator}")
        if scheme == "fs":
            fs, base = self._fs_parts()
            with fs.open_input_file(f"{base.rstrip('/')}/{locator}") as handle:
                data: bytes = handle.read()
            return data
        try:
            return (self._local_base() / locator).read_bytes()
        except OSError as exc:
            raise FlatfileSourceError(f"cannot read {locator!r} under {self.source!r}: {exc}") from exc

    @staticmethod
    def _fetch_url(url: str) -> bytes:
        """Fetch a URL for an external flatfile source."""
        injected = injected_fetcher()
        if injected is not None:
            return injected(url)
        try:
            with urlopen(url, timeout=60) as response:  # nosec B310
                data: bytes = response.read()
            return data
        except (HTTPError, URLError, OSError) as exc:
            raise FlatfileSourceError(f"cannot fetch {url!r}: {exc}") from exc

    def _read_json(self, locator: str) -> dict[str, Any] | None:
        """Read + parse a JSON object, or None when it is absent."""
        scheme = self.scheme
        if scheme == "http":
            try:
                raw = self._fetch_url(f"{self.source}/{locator}")
            except FlatfileSourceError as exc:
                if "404" in str(exc) or "Not Found" in str(exc):
                    return None
                raise
        else:
            try:
                raw = self._read_bytes(locator)
            except FlatfileSourceError:
                return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise FlatfileSourceError(f"{locator!r} is not valid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise FlatfileSourceError(f"{locator!r} must contain a JSON object")
        return data

    # ── manifest parsing ──────────────────────────────────────────

    def _manifest_datasets(self) -> dict[str, FlatfileDataset]:
        """Parse ``<base>/manifest.json`` into datasets keyed by chain."""
        data = self._read_json(MANIFEST_NAME)
        if data is None:
            return {}

        if "datasets" in data and isinstance(data["datasets"], list):
            entries = data["datasets"]
        elif "chains" in data and isinstance(data["chains"], dict):
            entries = []
            for chain, spec in data["chains"].items():
                if isinstance(spec, dict):
                    entries.append({**spec, "chain": spec.get("chain", chain)})
        else:
            entries = [data]

        datasets: dict[str, FlatfileDataset] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                raise FlatfileSourceError(f"{MANIFEST_NAME}: dataset entry must be an object")
            dataset = self._dataset_from_entry(entry)
            datasets[dataset.chain] = dataset
        return datasets

    def _dataset_from_entry(self, entry: dict[str, Any]) -> FlatfileDataset:
        """Build a dataset from a canonical manifest entry."""
        chain = entry.get("chain")
        if not isinstance(chain, str) or not chain:
            raise FlatfileSourceError(f"{MANIFEST_NAME}: dataset is missing a 'chain'")
        try:
            start_block = int(entry.get("start_block", 0))
            end_block = int(entry.get("end_block", 0))
            date = str(entry.get("date") or _GLOB_DATE)
        except (TypeError, ValueError) as exc:
            raise FlatfileSourceError(f"{MANIFEST_NAME}: invalid block range for {chain!r}: {exc}") from exc

        tables_raw = entry.get("tables")
        if not isinstance(tables_raw, dict):
            raise FlatfileSourceError(f"{MANIFEST_NAME}: dataset {chain!r} has no 'tables' object")

        tables: dict[str, FlatfileTable] = {}
        for name, spec in tables_raw.items():
            if not isinstance(spec, dict):
                raise FlatfileSourceError(f"{MANIFEST_NAME}: table {name!r} must be an object")
            tables[name] = self._table_from_spec(str(name), spec)
        if not tables:
            raise FlatfileSourceError(f"{MANIFEST_NAME}: dataset {chain!r} lists no tables")
        return FlatfileDataset(
            chain=chain,
            date=date,
            start_block=start_block,
            end_block=end_block,
            tables=tables,
            origin=self.source,
            from_manifest=True,
        )

    @staticmethod
    def _table_from_spec(name: str, spec: dict[str, Any]) -> FlatfileTable:
        """Build a table from a manifest table spec (files + optional chunks)."""
        try:
            declared = int(spec.get("rows", 0))
        except (TypeError, ValueError):
            declared = 0
        files_raw = spec.get("parquet_files", [])
        files = [str(f) for f in files_raw] if isinstance(files_raw, list) else []

        chunks: list[FlatfileChunk] = []
        chunks_raw = spec.get("chunks")
        if isinstance(chunks_raw, list):
            for item in chunks_raw:
                if not isinstance(item, dict):
                    continue
                file_name = str(item.get("file") or item.get("parquet_file") or "")
                if not file_name:
                    continue
                chunks.append(
                    FlatfileChunk(
                        name=Path(file_name).name,
                        locator=file_name,
                        rows=int(item.get("rows", 0) or 0),
                        start_block=int(item.get("start_block", 0) or 0),
                        end_block=int(item.get("end_block", 0) or 0),
                    )
                )
        if not chunks:
            chunks = [FlatfileChunk(name=Path(f).name, locator=f) for f in files]
        return FlatfileTable(name=name, declared_rows=declared, chunks=tuple(chunks))

    # ── glob mode ─────────────────────────────────────────────────

    def _glob_files(self, chain: str) -> list[str]:
        """List ``chain=<chain>/*.parquet`` source-relative locators."""
        prefix = f"{_CHAIN_DIR_PREFIX}{chain}"
        if self.scheme == "local":
            directory = self._local_base() / prefix
            if not directory.is_dir():
                return []
            return sorted(f"{prefix}/{p.name}" for p in directory.glob("*.parquet"))
        if self.scheme == "fs":
            pafs: Any = self._import("pyarrow.fs", "pyarrow.fs")
            fs, base = self._fs_parts()
            selector = pafs.FileSelector(f"{base.rstrip('/')}/{prefix}", recursive=False)
            infos = fs.get_file_info(selector)
            return sorted(
                f"{prefix}/{info.path.rsplit('/', 1)[-1]}" for info in infos if info.path.endswith(".parquet")
            )
        return []

    def _glob_chains(self) -> list[str]:
        """Enumerate chains available in hive-style glob mode."""
        if self.scheme == "local":
            base = self._local_base()
            if not base.is_dir():
                return []
            return sorted(
                p.name[len(_CHAIN_DIR_PREFIX) :]
                for p in base.iterdir()
                if p.is_dir() and p.name.startswith(_CHAIN_DIR_PREFIX)
            )
        if self.scheme == "fs":
            pafs: Any = self._import("pyarrow.fs", "pyarrow.fs")
            fs, fs_base = self._fs_parts()
            selector = pafs.FileSelector(fs_base, recursive=False)
            chains: list[str] = []
            for info in fs.get_file_info(selector):
                name = info.path.rstrip("/").rsplit("/", 1)[-1]
                if name.startswith(_CHAIN_DIR_PREFIX):
                    chains.append(name[len(_CHAIN_DIR_PREFIX) :])
            return sorted(chains)
        return []

    def _glob_dataset(self, chain: str) -> FlatfileDataset | None:
        """Build a manifest-less dataset from ``chain=<chain>/*.parquet``."""
        locators = self._glob_files(chain)
        if not locators:
            return None
        chunks: list[FlatfileChunk] = []
        for locator in locators:
            name = locator.rsplit("/", 1)[-1]
            start, end = self._infer_block_range(locator)
            chunks.append(
                FlatfileChunk(
                    name=name,
                    locator=locator,
                    rows=self._parquet_rows(locator),
                    start_block=start,
                    end_block=end,
                )
            )
        starts = [c.start_block for c in chunks if c.start_block > 0]
        ends = [c.end_block for c in chunks if c.end_block > 0]
        tables = {_GLOB_TABLE: FlatfileTable(name=_GLOB_TABLE, chunks=tuple(chunks))}
        return FlatfileDataset(
            chain=chain,
            date=_GLOB_DATE,
            start_block=min(starts) if starts else 0,
            end_block=max(ends) if ends else 0,
            tables=tables,
            origin=self.source,
            from_manifest=False,
        )

    def _parquet(self) -> Any:
        """Import pyarrow.parquet lazily as ``Any``."""
        return self._import("pyarrow.parquet", "pyarrow")

    def _parquet_rows(self, locator: str) -> int:
        """Best-effort row count for a parquet chunk (0 when unavailable)."""
        try:
            pq: Any = self._parquet()
            if self.scheme == "local":
                metadata = pq.ParquetFile(str(self._local_base() / locator)).metadata
            else:
                metadata = pq.ParquetFile(self._read_bytes(locator)).metadata
            return int(metadata.num_rows)
        except Exception as exc:  # noqa: BLE001 — best effort
            logger.debug("flatfile_rows_infer_failed", locator=locator, error=str(exc)[:120])
            return 0

    def _infer_block_range(self, locator: str) -> tuple[int, int]:
        """Infer ``(start, end)`` from a ``block_number``/``slot`` column."""
        try:
            pq: Any = self._parquet()
            if self.scheme == "local":
                source: Any = str(self._local_base() / locator)
            else:
                source = self._read_bytes(locator)
            names = set(pq.read_schema(source).names)
            column = next((c for c in _BLOCK_COLUMNS if c in names), None)
            if column is None:
                return (0, 0)
            values = [
                int(v) for v in pq.read_table(source, columns=[column]).column(column).to_pylist() if v is not None
            ]
            return (min(values), max(values)) if values else (0, 0)
        except Exception as exc:  # noqa: BLE001 — best effort
            logger.debug("flatfile_range_infer_failed", locator=locator, error=str(exc)[:120])
            return (0, 0)

    # ── resolution ────────────────────────────────────────────────

    def resolve(self, chain: str) -> FlatfileDataset:
        """Resolve a chain to a dataset (manifest first, then glob)."""
        datasets = self._manifest_datasets()
        if chain in datasets:
            return datasets[chain]
        globbed = self._glob_dataset(chain)
        if globbed is not None:
            return globbed
        raise FlatfileSourceError(
            f"no {MANIFEST_NAME} entry and no {_CHAIN_DIR_PREFIX}{chain}/*.parquet files under {self.source!r}"
        )

    async def list(self) -> list[dict[str, Any]]:
        """List the datasets exposed by the source.

        Returns:
            One summary per chain (manifest entries, or glob directories).

        Raises:
            FlatfileSourceError: For an http(s) source without a manifest
                (http dirs cannot be enumerated).
        """
        datasets = self._manifest_datasets()
        if datasets:
            return [d.to_dict() for d in datasets.values()]
        if self.scheme == "http":
            raise FlatfileSourceError(
                f"http source {self.source!r} has no {MANIFEST_NAME}; a base URL cannot be listed"
            )
        summaries: list[dict[str, Any]] = []
        for chain in self._glob_chains():
            dataset = self._glob_dataset(chain)
            if dataset is not None:
                summaries.append(dataset.to_dict())
        return summaries

    # ── staging ───────────────────────────────────────────────────

    def _stage_root(self) -> Path:
        """Per-source staging directory (deterministic, collision-free)."""
        digest = hashlib.sha1(self.source.encode(), usedforsecurity=False).hexdigest()[:12]  # non-crypto path key
        safe = re.sub(r"[^A-Za-z0-9_.-]", "_", self.source)[-60:]
        return self.stage_dir / f"{safe}-{digest}"

    def _materialize(self, locator: str, dest: Path) -> None:
        """Place a source chunk at ``dest`` (symlink local, download remote)."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            dest.unlink()
        if self.scheme == "local":
            source_path = (self._local_base() / locator).resolve()
            try:
                dest.symlink_to(source_path)
                return
            except OSError:
                shutil.copyfile(source_path, dest)
                return
        dest.write_bytes(self._read_bytes(locator))

    def _stage(self, dataset: FlatfileDataset) -> Path:
        """Materialize every selected chunk under ``{stage}/{date}/{chain}/``."""
        root = self._stage_root()
        chain_dir = root / dataset.date / dataset.chain
        for table in dataset.tables.values():
            for chunk in table.chunks:
                self._materialize(chunk.locator or chunk.name, chain_dir / chunk.name)
        return root

    # ── import ────────────────────────────────────────────────────

    def _high_water(self, chain: str) -> int:
        """Current chain high-water mark (0 when never indexed)."""
        import lite_indexer.state as state_mod
        from lite_indexer.state import StateHighWater

        if self._state_dir is not None:
            state_mod.STATE_DIR = self._state_dir
        return int(StateHighWater(chain).load() or 0)

    def _select(self, dataset: FlatfileDataset, high_water: int) -> FlatfileDataset | None:
        """Apply the high-water guard (None = nothing left to import)."""
        if self.force or high_water <= 0 or dataset.end_block <= 0:
            return dataset
        if high_water <= dataset.start_block:
            return dataset
        if high_water >= dataset.end_block:
            return None
        return dataset.above(high_water)

    @staticmethod
    def _table_rows(dataset: FlatfileDataset) -> dict[str, int]:
        """Per-table declared/inferred row counts."""
        return {name: table.rows() for name, table in dataset.tables.items()}

    async def import_chain(self, chain: str, dry_run: bool = False) -> dict[str, Any]:
        """Import one chain from the external dataset.

        Args:
            chain: Chain name to import.
            dry_run: Build the import plan without staging or writing.

        Returns:
            ``FlatfileImportResult.to_dict()``.
        """
        result = FlatfileImportResult(chain=chain, source=self.source)
        started = time.monotonic()
        try:
            dataset = self.resolve(chain)
            result.date = dataset.date
            result.start_block = dataset.start_block
            result.end_block = dataset.end_block
            result.blocks = max(0, dataset.end_block - dataset.start_block + 1)
            result.rows_declared = dataset.total_rows()

            high_water = self._high_water(chain)
            selected = self._select(dataset, high_water)
            if selected is None:
                result.status = "skipped"
                result.errors.append(
                    f"chain {chain!r} already indexed to block {high_water} "
                    f"(dataset ends at {dataset.end_block}); pass --force to re-import"
                )
                return result.to_dict()

            result.imported_start_block = selected.start_block
            result.imported_end_block = selected.end_block
            result.rows_declared = selected.total_rows()
            result.tables = self._table_rows(selected)
            manifest = selected.to_manifest()
            stage_root = self._stage_root()
            statements = build_import_sql(manifest, stage_root, self.target_db, self.ch_file_root)

            if dry_run:
                result.status = "dry_run"
                result.plan = statements
                result.errors.append(
                    f"would import {len(statements)} statement(s) for {selected.total_chunks()} chunk(s) "
                    f"into {self.target_db}"
                )
                return result.to_dict()

            self._stage(selected)
            client = self._connect()
            if client is None:
                result.status = "clickhouse_unavailable"
                result.errors.append("clickhouse unavailable (CH_HOST/CH_PORT/CH_DATABASE?)")
                return result.to_dict()

            self._importer._ensure_tables(client, manifest, self.target_db)
            per_table = SnapshotBootstrap._run_imports(client, statements)
            result.rows_imported = sum(per_table.values()) if per_table else 0
            if per_table:
                result.tables = per_table

            if self._verify_enabled(selected, high_water):
                mismatches = self._verify(client, manifest)
                if mismatches:
                    for table, detail in mismatches.items():
                        result.errors.append(
                            f"row count mismatch on {table}: expected {detail['expected']}, got {detail['actual']}"
                        )
                    result.status = "error"
                    return result.to_dict()

            result.status = "ok"
            self._importer._set_high_water(chain, selected.end_block, result.rows_imported)
        except FlatfileSourceError as exc:
            result.status = "error"
            result.errors.append(str(exc)[:300])
            logger.error("flatfile_import_failed", chain=chain, error=str(exc)[:300])
        except Exception as exc:  # noqa: BLE001 — CLI surfaces errors as JSON
            result.status = "error"
            result.errors.append(str(exc)[:300])
            logger.error("flatfile_import_failed", chain=chain, error=str(exc)[:300])
        finally:
            result.duration_seconds = time.monotonic() - started
        return result.to_dict()

    async def import_all(
        self,
        chains: Sequence[str] | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Import every chain the source exposes (or a subset).

        Args:
            chains: Explicit chain list; defaults to the source's chains
                (manifest entries, then glob dirs, then every supported
                chain).
            dry_run: Preview each chain without writing.

        Returns:
            ``{status, source, dry_run, chains: {...}}``.
        """
        chain_list = chains or sorted(self._manifest_datasets()) or self._glob_chains() or sorted(SUPPORTED_CHAINS)
        results: dict[str, Any] = {}
        failed = 0
        for chain in chain_list:
            res = await self.import_chain(chain, dry_run=dry_run)
            results[chain] = res
            if res["status"] not in ("ok", "dry_run", "skipped"):
                failed += 1
        return {
            "status": "ok" if failed == 0 else "partial_failure",
            "source": self.source,
            "dry_run": dry_run,
            "chains": results,
        }

    # ── clickhouse reuse hooks ────────────────────────────────────

    def _connect(self) -> Any | None:
        """Connect to ClickHouse via the shared bootstrap importer."""
        return self._importer._connect()

    def _verify_enabled(self, dataset: FlatfileDataset, high_water: int) -> bool:
        """Row-count verification is meaningful only for a full manifest import."""
        return dataset.from_manifest and dataset.total_rows() > 0 and high_water <= dataset.start_block

    def _verify(self, client: Any, manifest: SnapshotManifest) -> dict[str, dict[str, int]]:
        """Reuse the bootstrap row-count verification; return only mismatches."""
        verify = self._importer._verify_row_counts(client, manifest, self.target_db)
        return {table: stats for table, stats in verify.items() if stats["actual"] != stats["expected"]}

__all__ = [
    "ExternalFlatfileSource",
    "FlatfileChunk",
    "FlatfileDataset",
    "FlatfileImportResult",
    "FlatfileSourceError",
    "FlatfileTable",
]
