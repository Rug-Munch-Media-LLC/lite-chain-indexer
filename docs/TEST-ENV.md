# Test environment — one interpreter, no stale venvs

**Canonical test command (Talos):**

```bash
cd /srv/work/repos/lite-chain-indexer
python3 -m pytest tests/ -q
```

`python3` on Talos is `/usr/bin/python3` (3.12.3) — the same interpreter the
package runs on, with the full dependency set (`prometheus_client`, `pydantic`,
`httpx`, `clickhouse-driver`, pytest 9.1.1) already present. CI uses
`pip install -e ".[dev]"` on a matrix of 3.11/3.12, so nothing in the code may
depend on 3.12-only syntax; the *local* interpreter is 3.12 because that is
what production runs.

## Local venvs are NOT the test environment

| Path | Interpreter | Status |
| --- | --- | --- |
| `/usr/bin/python3` | 3.12.3 | **canonical** — run tests here |
| `.venv/` | 3.11.15 | stale (2026-09-10), **no `pip`**, gitignored — do not install into it, do not test in it |
| `.venv-ci/` | 3.12.3 | referenced by `scripts/systemd/*.service` for `sqd-fill`; keep it working, but it is not a test interpreter |

`.venv/` and `.venv-ci/` are gitignored (`.gitignore`: `.venv/`, `.venv-*/`),
so they are machine-local artefacts, not part of the repo. A package installed
into `.venv/` is invisible to `python3 -m pytest`, which is how "the dependency
is installed but the test still fails" (and the reverse: the fallback path that
never ran) happened on 2026-10-24.

**Rules**

1. Run `python3 -m pytest`, never a bare `pytest` from an activated stale venv
   (`which pytest` inside `.venv` picks the wrong interpreter).
2. Do not `pip install` into `.venv/` — it has no `pip` of its own, so an
   install silently targets whatever `pip` is on `PATH`.
3. When a dependency must exist, install it for the canonical interpreter
   (`python3 -m pip install <pkg>`) or through `.[dev]`, as CI does.
4. A graceful-degradation path is a *test case*, not a comment: the
   no-`prometheus_client` path is proven in
   `tests/test_metrics_without_prometheus.py`.
