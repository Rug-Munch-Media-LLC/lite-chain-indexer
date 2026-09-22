"""Two-account alternation tests (env pairs + quota swap)."""
import os
from typing import Any
from unittest.mock import patch

import pytest

from lite_indexer.query import bigquery_cli as cli
from lite_indexer.query.bigquery_common import BigQueryQuotaError

@pytest.fixture(autouse=True)
def _state_tmp(tmp_path: Any) -> Any:
    """Keep the account-registry state file inside the test sandbox."""
    import os
    from unittest.mock import patch

    with patch.dict(os.environ, {"BIGQUERY_STATE_FILE": str(tmp_path / "bqstate.json")}):
        yield

PRIMARY = {"BIGQUERY_PROJECT_ID": "proj-a", "GOOGLE_APPLICATION_CREDENTIALS": "/k/a.json"}
ALT = {"BIGQUERY_ALT_PROJECT_ID": "proj-b", "BIGQUERY_ALT_CREDENTIALS": "/k/b.json"}


def test_bq_accounts_primary_only() -> None:
    with patch.dict(os.environ, {**PRIMARY, "BIGQUERY_STATE_FILE": os.environ["BIGQUERY_STATE_FILE"]}, clear=True):
        assert cli.bq_accounts() == [("proj-a", "/k/a.json")]


def test_bq_accounts_with_alt() -> None:
    with patch.dict(os.environ, {**PRIMARY, **ALT, "BIGQUERY_STATE_FILE": os.environ["BIGQUERY_STATE_FILE"]}, clear=True):
        assert cli.bq_accounts() == [("proj-a", "/k/a.json"), ("proj-b", "/k/b.json")]


def test_bq_env_forces_credential_override() -> None:
    with patch.dict(os.environ, {}, clear=True):
        env = cli.bq_env("/k/a.json")
        assert env["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"] == "/k/a.json"
        plain = cli.bq_env(None)
        assert "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE" not in plain


class _Proc:
    def __init__(self, rc: int, out: str = "", err: str = "") -> None:
        self.returncode, self.stdout, self.stderr = rc, out, err


def test_quota_on_primary_swaps_to_alt() -> None:
    calls: list[dict[str, Any]] = []

    def fake_run(cmd: list[str], **kw: Any) -> _Proc:
        calls.append({"cmd": cmd, "env": kw.get("env")})
        if "proj-a" in cmd:
            return _Proc(1, err="Quota exceeded: free query bytes scanned")
        return _Proc(0, out='[{"ok": 1}]')

    state_file = os.environ["BIGQUERY_STATE_FILE"]
    with patch.dict(os.environ, {**PRIMARY, **ALT, "BIGQUERY_STATE_FILE": state_file}, clear=True), \
            patch("lite_indexer.query.bigquery_cli.subprocess.run", fake_run):
        rows = cli.run_bq("/usr/bin/bq", "SELECT 1", [])
    assert rows == [{"ok": 1}]
    assert len(calls) == 2
    assert calls[1]["env"]["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"] == "/k/b.json"


def test_quota_on_last_account_raises() -> None:
    state_file = os.environ["BIGQUERY_STATE_FILE"]
    with patch.dict(os.environ, {**PRIMARY, "BIGQUERY_STATE_FILE": state_file}, clear=True), \
            patch("lite_indexer.query.bigquery_cli.subprocess.run", lambda cmd, **kw: _Proc(1, err="Quota exceeded: free query bytes scanned")):
        with pytest.raises(BigQueryQuotaError):
            cli.run_bq("/usr/bin/bq", "SELECT 1", [])


# ── Registry rotation ────────────────────────────────────────────────


def _registry(monkeypatch_env: dict[str, str]) -> None:
    import os
    from unittest.mock import patch

    with patch.dict(os.environ, monkeypatch_env, clear=True):
        yield


def test_registry_parses_json_accounts() -> None:
    import json as _json
    import os
    from unittest.mock import patch

    from lite_indexer.query.bigquery_accounts import load_accounts

    payload = _json.dumps([{"project": "p1", "creds": "/k1"}, {"project": "p2", "creds": "/k2"}])
    with patch.dict(os.environ, {"BIGQUERY_ACCOUNTS": payload}, clear=True):
        accounts = load_accounts()
    assert [(a.project, a.creds) for a in accounts] == [("p1", "/k1"), ("p2", "/k2")]


def test_exhausted_accounts_sink_and_recover_after_reset() -> None:
    import json as _json
    import os
    from unittest.mock import patch

    from lite_indexer.query import bigquery_accounts as reg

    payload = _json.dumps([{"project": "p1", "creds": "/k1"}, {"project": "p2", "creds": "/k2"}])
    state_file = os.environ["BIGQUERY_STATE_FILE"]
    with patch.dict(os.environ, {"BIGQUERY_ACCOUNTS": payload, "BIGQUERY_STATE_FILE": state_file}, clear=True):
        now = 1_800_000_000.0
        reg.mark_exhausted("p1", now=now)
        order = [a.project for a in reg.ordered_accounts(now=now)]
        assert order == ["p2", "p1"]  # burnt account tried last
        # after the monthly reset the account is healthy again
        later = now + 40 * 24 * 3600
        order2 = [a.project for a in reg.ordered_accounts(now=later)]
        assert set(order2) == {"p1", "p2"}


def test_lru_rotation_prefers_least_recently_used() -> None:
    import json as _json
    import os
    from unittest.mock import patch

    from lite_indexer.query import bigquery_accounts as reg

    payload = _json.dumps([{"project": "p1"}, {"project": "p2"}, {"project": "p3"}])
    state_file = os.environ["BIGQUERY_STATE_FILE"]
    with patch.dict(os.environ, {"BIGQUERY_ACCOUNTS": payload, "BIGQUERY_STATE_FILE": state_file}, clear=True):
        now = 1_800_000_000.0
        reg.mark_used("p2", now=now)
        reg.mark_used("p1", now=now + 10)
        order = [a.project for a in reg.ordered_accounts(now=now + 20)]
        assert order[0] == "p3"  # never used -> first
        assert order.index("p2") < order.index("p1")
