"""
Tests for NATS JetStream real-time streaming (lite_indexer.streaming).

Covers:
  1. Subject map resolution (per event type, per chain, unknown types)
  2. publish_event success / failure / disabled
  3. JetStream provisioning + graceful degradation to core NATS
  4. Circuit breaker (open on consecutive failures, drop while open,
     half-open probe, recovery on success)
  5. stream_block_batch fan-out with block injection
  6. Indexer wiring (_publish_nats_events routing + summary)
"""

from __future__ import annotations

# Test methods intentionally omit return annotations (repo-wide test
# convention); scope mypy's no-untyped-def to this file only.
# mypy: disable-error-code="no-untyped-def"
import json
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import AsyncMock, patch

from lite_indexer.streaming import NATSPublisher

# ── Fakes ─────────────────────────────────────────────────────────────


class FakeJetStream:
    """Stand-in for nats.js.JetStreamContext."""

    def __init__(
        self,
        *,
        add_stream_error: Exception | None = None,
        update_stream_error: Exception | None = None,
        stream_exists: bool = True,
    ) -> None:
        self.published: list[tuple[str, bytes]] = []
        self.add_stream_calls: list[dict[str, Any]] = []
        self.update_stream_calls: list[dict[str, Any]] = []
        self._add_stream_error = add_stream_error
        self._update_stream_error = update_stream_error
        self._stream_exists = stream_exists

    async def add_stream(self, **kwargs: Any) -> dict[str, Any]:
        self.add_stream_calls.append(kwargs)
        if self._add_stream_error is not None:
            raise self._add_stream_error
        return {"name": kwargs.get("name")}

    async def stream_info(self, name: str) -> dict[str, Any] | None:
        if not self._stream_exists:
            raise RuntimeError("stream not found")
        return {"name": name, "config": {}}

    async def update_stream(self, **kwargs: Any) -> dict[str, Any]:
        self.update_stream_calls.append(kwargs)
        if self._update_stream_error is not None:
            raise self._update_stream_error
        return {"name": kwargs.get("name")}

    async def publish(self, subject: str, payload: bytes) -> None:
        self.published.append((subject, payload))


class FakeNC:
    """Stand-in for nats.aio.client.Client."""

    is_connected = True

    def __init__(self, *, js: FakeJetStream | None = None, raise_publish: bool = False) -> None:
        self._js = js if js is not None else FakeJetStream()
        self.published: list[tuple[str, bytes]] = []
        self.attempts = 0
        self.raise_publish = raise_publish
        self.closed = False

    def jetstream(self) -> FakeJetStream:
        return self._js

    async def publish(self, subject: str, payload: bytes) -> None:
        self.attempts += 1
        if self.raise_publish:
            raise ConnectionError("nats server unavailable")
        self.published.append((subject, payload))

    async def close(self) -> None:
        self.closed = True


def _decode(payload: bytes) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(payload.decode("utf-8")))


# ── Subject map ───────────────────────────────────────────────────────


class TestSubjectMap:
    def test_evm_transfer_subject(self):
        assert NATSPublisher.subject_for("ethereum", "evm_transfer") == "chain.ethereum.transfer"

    def test_dex_swap_subject_chain_substituted(self):
        assert NATSPublisher.subject_for("base", "dex_swap") == "chain.base.dex.swap"

    def test_deployer_event_subject(self):
        assert NATSPublisher.subject_for("bsc", "deployer_event") == "chain.bsc.deploy"

    def test_whale_alert_subject_namespace(self):
        assert NATSPublisher.subject_for("solana", "whale_alert") == "alerts.whale.solana"

    def test_unknown_event_type_returns_none(self):
        assert NATSPublisher.subject_for("ethereum", "nope") is None

    def test_known_event_types(self):
        assert set(NATSPublisher.known_event_types()) == {
            "evm_transfer",
            "dex_swap",
            "deployer_event",
            "launch",
            "jito_bundle",
            "whale_alert",
        }


# ── publish_event ─────────────────────────────────────────────────────


class TestPublishEvent:
    async def test_publish_success_core(self):
        fake = FakeNC(js=FakeJetStream(), raise_publish=False)
        publisher = NATSPublisher(enabled=True, jetstream_enabled=False)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            ok = await publisher.publish_event("ethereum", "evm_transfer", {"from": "0xa", "value": "1"})

        assert ok is True
        assert len(fake.published) == 1
        subject, payload = fake.published[0]
        assert subject == "chain.ethereum.transfer"
        envelope = _decode(payload)
        assert envelope["chain"] == "ethereum"
        assert envelope["type"] == "evm_transfer"
        assert envelope["data"] == {"from": "0xa", "value": "1"}
        assert "published_at" in envelope
        assert publisher.circuit_state == "closed"

    async def test_publish_success_jetstream(self):
        js = FakeJetStream()
        fake = FakeNC(js=js)
        publisher = NATSPublisher(enabled=True, jetstream_enabled=True)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            ok = await publisher.publish_event("base", "dex_swap", {"pair": "0xp"})

        assert ok is True
        # Stream auto-provisioned on first connect.
        assert js.add_stream_calls, "stream provisioning should run once"
        assert js.add_stream_calls[0]["name"] == "CHAIN_EVENTS"
        assert set(js.add_stream_calls[0]["subjects"]) == {"chain.>", "alerts.>"}
        # Published through JetStream, not core.
        assert len(js.published) == 1
        assert js.published[0][0] == "chain.base.dex.swap"
        assert fake.published == []

    async def test_jetstream_degrades_to_core_when_unavailable(self):
        # Server with JetStream disabled: add_stream AND stream_info both
        # fail → reconcile cannot run → degrade to core NATS publish.
        js = FakeJetStream(add_stream_error=RuntimeError("jetstream disabled"), stream_exists=False)
        fake = FakeNC(js=js)
        publisher = NATSPublisher(enabled=True, jetstream_enabled=True)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            ok = await publisher.publish_event("bsc", "deployer_event", {"tx": "0x1"})

        assert ok is True
        # add_stream failed → fell back to core publish.
        assert js.published == []
        assert len(fake.published) == 1
        assert fake.published[0][0] == "chain.bsc.deploy"

    async def test_stream_config_mismatch_reconciles_via_update(self):
        # NATS err 10058: stream exists with a different config. The
        # publisher must reconcile with update_stream (non-destructive)
        # instead of degrading to core NATS.
        js = FakeJetStream(
            add_stream_error=RuntimeError(
                "code=400 err_code=10058 'stream name already in use with a different configuration'"
            )
        )
        fake = FakeNC(js=js)
        publisher = NATSPublisher(enabled=True, jetstream_enabled=True)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            ok = await publisher.publish_event("base", "dex_swap", {"pair": "0xp"})

        assert ok is True
        # add_stream failed with 10058 → update_stream reconciles.
        assert js.update_stream_calls, "update_stream should reconcile the mismatch"
        update_cfg = js.update_stream_calls[0]
        assert update_cfg["name"] == "CHAIN_EVENTS"
        assert set(update_cfg["subjects"]) == {"chain.>", "alerts.>"}
        # The desired config mirrors the deployed server (file storage).
        assert update_cfg["storage"] == "file"
        # Publish still goes through JetStream after reconciliation.
        assert len(js.published) == 1
        assert js.published[0][0] == "chain.base.dex.swap"
        assert fake.published == []

    async def test_stream_reconcile_failure_degrades_to_core(self):
        # Stream exists, add_stream mismatches, but update_stream also
        # fails → degrade to core NATS (never block publishing).
        js = FakeJetStream(
            add_stream_error=RuntimeError(
                "code=400 err_code=10058 'stream name already in use with a different configuration'"
            ),
            update_stream_error=RuntimeError("permission denied"),
        )
        fake = FakeNC(js=js)
        publisher = NATSPublisher(enabled=True, jetstream_enabled=True)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            ok = await publisher.publish_event("bsc", "deployer_event", {"tx": "0x1"})

        assert ok is True
        assert js.update_stream_calls, "update_stream should be attempted"
        assert js.published == []
        assert len(fake.published) == 1
        assert fake.published[0][0] == "chain.bsc.deploy"

    async def test_unknown_event_type_returns_false_without_connect(self):
        publisher = NATSPublisher(enabled=True, jetstream_enabled=False)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock()) as mock_connect:
            ok = await publisher.publish_event("ethereum", "bogus", {})
        assert ok is False
        mock_connect.assert_not_awaited()

    async def test_publish_failure_returns_false(self):
        fake = FakeNC(raise_publish=True)
        publisher = NATSPublisher(enabled=True, jetstream_enabled=False)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            ok = await publisher.publish_event("ethereum", "evm_transfer", {})
        assert ok is False
        assert publisher.consecutive_failures == 1

    async def test_connect_failure_returns_false(self):
        publisher = NATSPublisher(enabled=True, jetstream_enabled=False)
        with patch(
            "lite_indexer.streaming.nats_publisher.nats.connect",
            new=AsyncMock(side_effect=ConnectionError("no server")),
        ):
            ok = await publisher.publish_event("ethereum", "evm_transfer", {})
        assert ok is False
        assert publisher.consecutive_failures == 1


class TestDisabled:
    async def test_disabled_returns_false_without_connect(self):
        publisher = NATSPublisher(enabled=False)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock()) as mock_connect:
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False
            assert await publisher.stream_block_batch("ethereum", 1, [("evm_transfer", {})]) == 0
            assert await publisher.connect() is False
        mock_connect.assert_not_awaited()

    async def test_disabled_publisher_never_opens_connection(self):
        publisher = NATSPublisher(enabled=False)
        assert await publisher.connect() is False
        assert publisher.connected is False


# ── Circuit breaker ───────────────────────────────────────────────────


def _clock_harness(start: float = 1000.0) -> tuple[list[float], Callable[[], float]]:
    """Return ([now], clock) so tests can advance a fake monotonic clock."""
    now = [start]

    def clock() -> float:
        return now[0]

    return now, clock


class TestCircuitBreaker:
    async def test_opens_after_threshold_failures(self):
        now, clock = _clock_harness()
        fake = FakeNC(raise_publish=True)
        publisher = NATSPublisher(
            enabled=True,
            failure_threshold=2,
            cooldown_seconds=30,
            jetstream_enabled=False,
            clock=clock,
        )
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False
            assert publisher.circuit_state == "closed"
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False
            assert publisher.circuit_state == "open"
            assert publisher.consecutive_failures == 2

    async def test_drops_while_open_without_network_attempt(self):
        now, clock = _clock_harness()
        fake = FakeNC(raise_publish=True)
        publisher = NATSPublisher(
            enabled=True,
            failure_threshold=1,
            cooldown_seconds=30,
            jetstream_enabled=False,
            clock=clock,
        )
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False  # trip
            assert publisher.circuit_state == "open"
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False  # dropped
            assert fake.attempts == 1, "open circuit must not hit the network"

    async def test_half_open_probe_failure_reopens(self):
        now, clock = _clock_harness()
        fake = FakeNC(raise_publish=True)
        publisher = NATSPublisher(
            enabled=True,
            failure_threshold=1,
            cooldown_seconds=30,
            jetstream_enabled=False,
            clock=clock,
        )
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False  # trip
            now[0] += 31  # cooldown expired → half-open
            assert publisher.circuit_state == "half_open"
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False  # probe fails
            assert publisher.circuit_state == "open"
            assert fake.attempts == 2

    async def test_half_open_probe_success_closes(self):
        now, clock = _clock_harness()
        fake = FakeNC(raise_publish=True)
        publisher = NATSPublisher(
            enabled=True,
            failure_threshold=1,
            cooldown_seconds=30,
            jetstream_enabled=False,
            clock=clock,
        )
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False  # trip
            now[0] += 31  # cooldown expired → half-open
            fake.raise_publish = False
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is True  # probe succeeds
            assert publisher.circuit_state == "closed"
            assert publisher.consecutive_failures == 0

    async def test_success_resets_failure_count(self):
        now, clock = _clock_harness()
        fake = FakeNC(raise_publish=True)
        publisher = NATSPublisher(
            enabled=True,
            failure_threshold=3,
            cooldown_seconds=30,
            jetstream_enabled=False,
            clock=clock,
        )
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False
            fake.raise_publish = False
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is True
            fake.raise_publish = True
            assert await publisher.publish_event("ethereum", "evm_transfer", {}) is False
            assert publisher.consecutive_failures == 1, "failure count must reset after success"


# ── stream_block_batch ────────────────────────────────────────────────


class TestStreamBlockBatch:
    async def test_fans_out_and_injects_block(self):
        fake = FakeNC()
        publisher = NATSPublisher(enabled=True, jetstream_enabled=False)
        events = [
            ("evm_transfer", {"from": "0xa", "value": "1"}),
            ("dex_swap", {"pair": "0xp"}),
            ("whale_alert", {"pair": "0xp"}),
        ]
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            published = await publisher.stream_block_batch("ethereum", 12_345, events)

        assert published == 3
        assert len(fake.published) == 3
        subjects = {s for s, _ in fake.published}
        assert subjects == {"chain.ethereum.transfer", "chain.ethereum.dex.swap", "alerts.whale.ethereum"}
        for _, payload in fake.published:
            assert _decode(payload)["data"]["block"] == 12_345

    async def test_skips_unknown_types_in_batch(self):
        fake = FakeNC()
        publisher = NATSPublisher(enabled=True, jetstream_enabled=False)
        events: list[tuple[str, dict[str, Any]]] = [("evm_transfer", {"from": "0xa"}), ("bogus", {"x": 1})]
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            published = await publisher.stream_block_batch("ethereum", 1, events)
        assert published == 1
        assert len(fake.published) == 1

    async def test_empty_batch_returns_zero(self):
        publisher = NATSPublisher(enabled=True, jetstream_enabled=False)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock()) as mock_connect:
            assert await publisher.stream_block_batch("ethereum", 1, []) == 0
        mock_connect.assert_not_awaited()

    async def test_close_is_idempotent_and_never_raises(self):
        fake = FakeNC()
        publisher = NATSPublisher(enabled=True, jetstream_enabled=False)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            await publisher.publish_event("ethereum", "evm_transfer", {})
        await publisher.close()
        assert fake.closed is True
        await publisher.close()  # second close must be a no-op


# ── Indexer wiring ────────────────────────────────────────────────────


class TestIndexerNatsWiring:
    def test_nats_publisher_property_none_when_disabled(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=False)
        assert indexer.nats_publisher is None

    def test_nats_publisher_property_created_when_enabled(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=True)
        publisher = indexer.nats_publisher
        assert publisher is not None
        assert publisher.enabled is True
        # Property is cached.
        assert indexer.nats_publisher is publisher

    async def test_publish_nats_events_routes_and_counts(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=True)
        # Use a core-only publisher so the fake records on the client.
        indexer._nats_publisher = NATSPublisher(enabled=True, jetstream_enabled=False)
        fake = FakeNC()
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock(return_value=fake)):
            published = await indexer._publish_nats_events(
                "ethereum",
                12345,
                transfers=[{"tx_hash": "0x1", "from": "0xa", "to": "0xb"}],
                dex_events=[
                    {"tx_hash": "0x2", "pair": "0xp", "whale": 1},
                    {"tx_hash": "0x3", "pair": "0xq", "whale": 0},
                ],
                deployer_events=[{"tx_hash": "0x4", "factory": "0xf"}],
                trace_events=[{"tx_hash": "0x5", "type": "internal_transfer"}],
            )

        # 1 transfer + 2 dex + 1 whale + 1 deployer + 1 trace = 6
        assert published == 6
        subjects = {s for s, _ in fake.published}
        assert "chain.ethereum.transfer" in subjects
        assert "chain.ethereum.dex.swap" in subjects
        assert "alerts.whale.ethereum" in subjects
        assert "chain.ethereum.deploy" in subjects
        # whale flag present on the whale alert payload
        whale_payloads = [p for s, p in fake.published if s == "alerts.whale.ethereum"]
        assert len(whale_payloads) == 1
        assert _decode(whale_payloads[0])["data"]["whale"] == 1

    async def test_publish_nats_events_disabled_returns_zero(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=False)
        with patch("lite_indexer.streaming.nats_publisher.nats.connect", new=AsyncMock()) as mock_connect:
            published = await indexer._publish_nats_events("ethereum", 1, [{"tx_hash": "0x1"}], [], [], [])
        assert published == 0
        mock_connect.assert_not_awaited()

    async def test_publish_nats_events_swallows_publisher_errors(self, tmp_path):
        from lite_indexer.indexer import LiteIndexer

        indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path, nats_enabled=True)
        # A publisher whose connect raises must not propagate — best-effort.
        with patch(
            "lite_indexer.streaming.nats_publisher.nats.connect",
            new=AsyncMock(side_effect=ConnectionError("boom")),
        ):
            published = await indexer._publish_nats_events("ethereum", 1, [{"tx_hash": "0x1"}], [], [], [])
        assert published == 0
