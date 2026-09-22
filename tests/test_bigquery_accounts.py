"""Two-account alternation tests (env pairs + quota swap)."""
import os
import subprocess
from typing import Any
from unittest.mock import patch

import pytest

from lite_indexer.query import bigquery_cli as cli
from lite_indexer.query.bigquery_common import BigQueryQuotaError

PRIMARY = {"BIGQUERY_PROJECT_ID": "proj-a", "GOOGLE_APPLICATION_CREDENTIALS": "/k/a.json"}
ALT = {"BIGQUERY_ALT_PROJECT_ID": "proj-b", "BIGQUERY_ALT_CREDENTIALS": "/k/b.json"}


def test_bq_accounts_primary_only() -> None:
    with patch.dict(os.environ, PRIMARY, clear=True):
        assert cli.bq_accounts() == [("proj-a", "/k/a.json")]


def test_bq_accounts_with_alt() -> None:
    with patch.dict(os.environ, {**PRIMARY, **ALT}, clear=True):
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

    with patch.dict(os.environ, {**PRIMARY, **ALT}, clear=True), \
            patch.object(subprocess, "run", fake_run):
        rows = cli.run_bq("/usr/bin/bq", "SELECT 1", [])
    assert rows == [{"ok": 1}]
    assert len(calls) == 2
    assert calls[1]["env"]["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"] == "/k/b.json"


def test_quota_on_last_account_raises() -> None:
    with patch.dict(os.environ, PRIMARY, clear=True), \
            patch.object(subprocess, "run", lambda cmd, **kw: _Proc(1, err="Quota exceeded: free query bytes scanned")):
        with pytest.raises(BigQueryQuotaError):
            cli.run_bq("/usr/bin/bq", "SELECT 1", [])
