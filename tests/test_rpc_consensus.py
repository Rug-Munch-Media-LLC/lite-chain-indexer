"""Cross-provider consensus verification tests.

Contracts verified offline (stub transport, no network):

* a quorum of distinct providers returning the same content hash yields the
  majority result + agreement count
* when successful answers disagree and no quorum forms, the read FAILS
  (``mismatch``), the outlier is penalized, and no single answer is picked
* the sample rate being 0 disables verification entirely
* ``min_agreement`` is honoured (providers < required → insufficient)
* two endpoints behind ONE provider account do NOT count as two providers
* the normal ``get_logs`` read path is untouched when sampling misses, and
  fails the read when consensus reports a mismatch
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.rpc import consensus
from lite_indexer.rpc.client import RpcClient
from lite_indexer.rpc.consensus import (
    ConsensusResult,
    canonical_hash,
    should_sample,
    verify_across_providers,
)

# Distinct registrable domains → distinct provider accounts.
ALPHA = "https://alpha.example"
BETA = "https://beta.example"
GAMMA = "https://gamma.example"
# Two subdomains of ONE registrable domain → the SAME provider account.
SAME_1 = "https://rpc.same-rpc.test"
SAME_2 = "https://other.same-rpc.test"

LOGS_A: list[dict[str, Any]] = [{"blockNumber": "0x1", "logIndex": "0x0"}]
LOGS_B: list[dict[str, Any]] = [{"blockNumber": "0x1", "logIndex": "0x1"}]
TOPIC = "0x" + "ab" * 32


class StubClient:
    """Transport stub returning a scripted ``result`` per endpoint."""

    def __init__(self, results: dict[str, Any], errors: set[str] | None = None) -> None:
        self.results = results
        self.errors = errors or set()
        self.calls: list[str] = []

    async def call(
        self,
        endpoint: str,
        method: str,
        params: list[Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any] | None:
        self.calls.append(endpoint)
        if endpoint in self.errors:
            return {"jsonrpc": "2.0", "id": 1, "error": {"code": -1, "message": "boom"}}
        return {"jsonrpc": "2.0", "id": 1, "result": self.results.get(endpoint)}


@pytest.fixture(autouse=True)
def _default_consensus(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin consensus config to deterministic defaults for every test."""
    monkeypatch.setattr(consensus, "RPC_CONSENSUS_ENABLED", True)
    monkeypatch.setattr(consensus, "RPC_CONSENSUS_MIN_AGREEMENT", 2)
    monkeypatch.setattr(consensus, "RPC_CONSENSUS_SAMPLE_RATE", 1.0)
    monkeypatch.setattr(consensus, "RPC_CONSENSUS_MAX_ENDPOINTS", 3)


def test_canonical_hash_is_order_insensitive() -> None:
    assert canonical_hash([{"a": 1}, {"b": 2}]) == canonical_hash([{"b": 2}, {"a": 1}])
    assert canonical_hash({"b": 2, "a": 1}) == canonical_hash({"a": 1, "b": 2})
    assert canonical_hash(LOGS_A) != canonical_hash(LOGS_B)


async def test_agreement_returns_majority() -> None:
    client = StubClient({ALPHA: LOGS_A, BETA: LOGS_A, GAMMA: LOGS_B})
    out = await verify_across_providers(client, "ethereum", "eth_getLogs", [{}], endpoints=[ALPHA, BETA, GAMMA])
    assert out.verified is True
    assert out.mismatch is False
    assert out.agreement == 2
    assert out.total == 3
    assert out.result == LOGS_A


async def test_disagreement_fails_and_penalizes_outlier(monkeypatch: pytest.MonkeyPatch) -> None:
    penalized: list[str] = []
    monkeypatch.setattr(consensus, "record_endpoint_failure", penalized.append)

    client = StubClient({ALPHA: LOGS_A, BETA: LOGS_A, GAMMA: LOGS_B})
    out = await verify_across_providers(
        client, "ethereum", "eth_getLogs", [{}], min_agreement=3, endpoints=[ALPHA, BETA, GAMMA]
    )

    assert out.mismatch is True
    assert out.verified is False
    assert out.result is None
    assert out.agreement == 2
    # Only the outlier is penalized — the agreeing quorum keeps its score.
    assert penalized == [GAMMA]


async def test_successful_answers_below_min_agreement_is_not_a_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two providers split 1-1 form no quorum → the read fails (mismatch)."""
    penalized: list[str] = []
    monkeypatch.setattr(consensus, "record_endpoint_failure", penalized.append)

    client = StubClient({ALPHA: LOGS_A, BETA: LOGS_B})
    out = await verify_across_providers(client, "ethereum", "eth_getLogs", [{}], endpoints=[ALPHA, BETA])

    # 1-1 reaches min_agreement=2 providers but no quorum → mismatch.
    assert out.mismatch is True
    assert out.verified is False
    assert len(penalized) == 1


async def test_errored_provider_leaves_insufficient_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    penalized: list[str] = []
    monkeypatch.setattr(consensus, "record_endpoint_failure", penalized.append)

    client = StubClient({ALPHA: LOGS_A}, errors={BETA})
    out = await verify_across_providers(client, "ethereum", "eth_getLogs", [{}], endpoints=[ALPHA, BETA])

    # Only one usable answer for a min_agreement of 2 → fail-open, not fail.
    assert out.verified is False
    assert out.mismatch is False
    assert out.reason == "insufficient_evidence"
    assert penalized == []


async def test_errored_provider_does_not_block_quorum(monkeypatch: pytest.MonkeyPatch) -> None:
    penalized: list[str] = []
    monkeypatch.setattr(consensus, "record_endpoint_failure", penalized.append)

    client = StubClient({ALPHA: LOGS_A, BETA: LOGS_A}, errors={GAMMA})
    out = await verify_across_providers(client, "ethereum", "eth_getLogs", [{}], endpoints=[ALPHA, BETA, GAMMA])

    assert out.verified is True
    assert out.mismatch is False
    assert out.agreement == 2
    assert penalized == []


async def test_min_agreement_above_provider_count() -> None:
    client = StubClient({ALPHA: LOGS_A, BETA: LOGS_A})
    out = await verify_across_providers(
        client, "ethereum", "eth_getLogs", [{}], min_agreement=3, endpoints=[ALPHA, BETA]
    )

    assert out.verified is False
    assert out.mismatch is False
    assert out.reason == "insufficient_providers"
    assert client.calls == []


def test_sample_rate_zero_disables(monkeypatch: pytest.MonkeyPatch) -> None:
    assert should_sample() is True  # fixture pins rate to 1.0

    monkeypatch.setattr(consensus, "RPC_CONSENSUS_SAMPLE_RATE", 0.0)
    assert should_sample() is False


def test_disabled_flag_short_circuits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(consensus, "RPC_CONSENSUS_ENABLED", False)
    assert should_sample() is False


def test_same_account_counts_as_one_provider() -> None:
    selected = consensus._select_distinct_endpoints(
        [SAME_1, SAME_2], method="eth_getLogs", chain="ethereum", max_endpoints=3
    )
    assert len(selected) == 1


async def test_same_account_pool_is_insufficient() -> None:
    client = StubClient({SAME_1: LOGS_A, SAME_2: LOGS_A})
    out = await verify_across_providers(client, "ethereum", "eth_getLogs", [{}], endpoints=[SAME_1, SAME_2])

    assert out.verified is False
    assert out.mismatch is False
    assert out.reason == "insufficient_providers"
    assert client.calls == []


# ── get_logs adoption ──────────────────────────────────────────────────


async def _no_split(_once: Any, from_block: int, to_block: int, fetch_many: Any = None) -> list[dict[str, Any]]:
    return [{"blockNumber": "0x1"}]


async def test_getlogs_skips_consensus_when_sampling_misses(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"verify": 0}

    async def spy_verify(*args: Any, **kwargs: Any) -> ConsensusResult:
        calls["verify"] += 1
        return ConsensusResult(verified=False)

    monkeypatch.setattr(consensus, "should_sample", lambda: False)
    monkeypatch.setattr(consensus, "verify_across_providers", spy_verify)
    monkeypatch.setattr("lite_indexer.rpc.range_split.fetch_logs_split", _no_split)

    client = RpcClient()
    logs = await client.get_logs(ALPHA, 1, 100, TOPIC, chain="ethereum")

    assert logs == [{"blockNumber": "0x1"}]
    assert calls["verify"] == 0


async def test_getlogs_mismatch_fails_read(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_verify(*args: Any, **kwargs: Any) -> ConsensusResult:
        return ConsensusResult(mismatch=True, reason="mismatch")

    monkeypatch.setattr(consensus, "should_sample", lambda: True)
    monkeypatch.setattr(consensus, "verify_across_providers", fake_verify)
    monkeypatch.setattr("lite_indexer.rpc.range_split.fetch_logs_split", _no_split)

    client = RpcClient()
    logs = await client.get_logs(ALPHA, 1, 100, TOPIC, chain="ethereum")

    assert logs is None


async def test_getlogs_returns_verified_majority(monkeypatch: pytest.MonkeyPatch) -> None:
    majority = [{"blockNumber": "0x2"}]

    async def fake_verify(*args: Any, **kwargs: Any) -> ConsensusResult:
        return ConsensusResult(result=majority, verified=True, agreement=2, total=3)

    monkeypatch.setattr(consensus, "should_sample", lambda: True)
    monkeypatch.setattr(consensus, "verify_across_providers", fake_verify)
    monkeypatch.setattr("lite_indexer.rpc.range_split.fetch_logs_split", _no_split)

    client = RpcClient()
    logs = await client.get_logs(ALPHA, 1, 100, TOPIC, chain="ethereum")

    assert logs == majority


async def test_getlogs_without_chain_is_unverified(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"verify": 0}

    async def spy_verify(*args: Any, **kwargs: Any) -> ConsensusResult:
        calls["verify"] += 1
        return ConsensusResult(verified=False)

    monkeypatch.setattr(consensus, "should_sample", lambda: True)
    monkeypatch.setattr(consensus, "verify_across_providers", spy_verify)
    monkeypatch.setattr("lite_indexer.rpc.range_split.fetch_logs_split", _no_split)

    client = RpcClient()
    logs = await client.get_logs(ALPHA, 1, 100, TOPIC)

    assert logs == [{"blockNumber": "0x1"}]
    assert calls["verify"] == 0


# ── Intrinsic integrity validators (RPC_CONSENSUS_VALIDATORS) ──────────────


def test_intrinsic_range_and_monotonic_violations() -> None:
    params = [{"fromBlock": "0x64", "toBlock": "0xc8"}]  # [100, 200]
    logs = [
        {"blockNumber": "0xc8", "logIndex": "0x1"},  # 200 ok
        {"blockNumber": "0x12c", "logIndex": "0x0"},  # 300 out of range
        {"blockNumber": "0x64", "logIndex": "0x0"},  # 100 out of range + reversal
    ]
    codes = consensus._intrinsic_violations("eth_getLogs", params, logs)  # noqa: SLF001
    assert "range" in codes
    assert "monotonic" in codes


def test_intrinsic_shape_and_empty_violations() -> None:
    assert consensus._intrinsic_violations("eth_getLogs", [{}], None) == ["shape"]  # noqa: SLF001
    assert consensus._intrinsic_violations("eth_getLogs", [{}], []) == []  # noqa: SLF001
    assert consensus._intrinsic_violations("eth_getLogs", [{}], [], expect_non_empty=True) == ["empty"]  # noqa: SLF001
    # Non-getLogs methods have no intrinsic structure → no change.
    assert consensus._intrinsic_violations("eth_call", None, "0x1") == []  # noqa: SLF001


async def test_intrinsic_validators_off_is_observe_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(consensus, "RPC_CONSENSUS_VALIDATORS", False)
    params = [{"fromBlock": "0x64", "toBlock": "0xc8"}]
    out_of_range = [{"blockNumber": "0x12c", "logIndex": "0x0"}]
    good = [{"blockNumber": "0x96", "logIndex": "0x0"}]
    client = StubClient({ALPHA: out_of_range, BETA: out_of_range, GAMMA: good})

    out = await verify_across_providers(
        client, "ethereum", "eth_getLogs", params, endpoints=[ALPHA, BETA, GAMMA]
    )
    # Observe-only: the invalid pair still counts, so quorum is reached.
    assert out.verified is True
    assert out.agreement == 2


async def test_intrinsic_validators_on_rejects_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(consensus, "RPC_CONSENSUS_VALIDATORS", True)
    params = [{"fromBlock": "0x64", "toBlock": "0xc8"}]
    out_of_range = [{"blockNumber": "0x12c", "logIndex": "0x0"}]
    good = [{"blockNumber": "0x96", "logIndex": "0x0"}]
    client = StubClient({ALPHA: out_of_range, BETA: out_of_range, GAMMA: good})

    out = await verify_across_providers(
        client, "ethereum", "eth_getLogs", params, endpoints=[ALPHA, BETA, GAMMA]
    )
    # Validators on: the two invalid answers are rejected, leaving one
    # provider — not enough evidence, so the read is not failed either.
    assert out.verified is False
    assert out.reason == "insufficient_evidence"
