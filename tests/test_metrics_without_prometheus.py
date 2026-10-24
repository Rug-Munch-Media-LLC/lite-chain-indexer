"""The documented graceful-degradation path must be the path that works.

``metrics.py`` promises the package imports without ``prometheus_client`` and
falls back to plain counters. ``_prime_labeled_metrics()`` calls ``labels()``
on every labeled counter at import time, so the fallback object has to support
it — before the fix, ``import lite_indexer.metrics`` raised AttributeError in
exactly the no-prometheus case (and nothing else exercised that path).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

BLOCKER = '''
import sys

class _BlockPrometheus:
    """Refuse prometheus_client, as on a box where it is not installed."""

    def find_spec(self, fullname, path=None, target=None):
        if fullname == "prometheus_client" or fullname.startswith("prometheus_client."):
            raise ImportError("prometheus_client blocked for this test")
        return None

# Fresh ``python -c`` process: nothing has imported prometheus_client yet, so
# blocking the import is enough (no module eviction needed).
sys.meta_path.insert(0, _BlockPrometheus())
'''

PROBE = """
import lite_indexer.metrics as metrics

assert metrics.PROMETHEUS_AVAILABLE is False, "blocker failed: prometheus_client was importable"
metrics.incr("rpc_errors", 2.0)
metrics.incr("rpc_errors")
metrics.incr_labeled("lite_indexer_rpc_failures", 3.0, chain="base")
metrics.set_gauge("rpc_consensus_effective_rate", 1.5)
metrics.set_labeled_gauge("lite_indexer_state_last_block", 42, chain="base")
metrics.reset()
metrics.set_gauge("rpc_consensus_effective_rate", 7.0)
print(metrics.snapshot()["rpc_consensus_effective_rate"])
print(metrics.get("rpc_errors"))
"""


def _run(source: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", source],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_package_imports_without_prometheus_client() -> None:
    """Importing the package must not require prometheus_client."""
    result = _run(BLOCKER + "\nimport lite_indexer  # noqa: F401\nprint('imported')\n")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "imported"


def test_metrics_module_primes_and_reports_without_prometheus_client() -> None:
    """Prime-time labels() + a real metric read must work on the fallback path."""
    result = _run(BLOCKER + PROBE)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["7.0", "0.0"]
