"""Consecutive-failure circuit breaker for the NATS publisher (split)."""
from __future__ import annotations

import time
from collections.abc import Callable


class CircuitBreaker:
    """Consecutive-failure circuit breaker with a time-based cooldown.

    States:

      - ``closed``: publishing normally. Each failure increments the
        counter; reaching ``failure_threshold`` opens the circuit.
      - ``open``: publishes are dropped without a network attempt until
        ``cooldown_seconds`` elapses.
      - ``half_open``: cooldown expired; the next publish is a probe.
        Success closes the circuit, failure reopens it.
    """

    def __init__(
        self,
        failure_threshold: int,
        cooldown_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.failure_threshold = max(1, failure_threshold)
        self.cooldown_seconds = max(0.0, cooldown_seconds)
        self._clock = clock
        self._consecutive_failures = 0
        self._open_until = 0.0

    @property
    def consecutive_failures(self) -> int:
        """Number of consecutive failures recorded (0 when closed)."""
        return self._consecutive_failures

    @property
    def state(self) -> str:
        """One of ``closed``, ``open`` or ``half_open``."""
        if self._open_until > 0.0:
            if self._clock() < self._open_until:
                return "open"
            # Cooldown expired — the next probe will run on the next
            # publish; report half-open so callers can observe it.
            return "half_open"
        return "closed"

    @property
    def is_open(self) -> bool:
        """True when publishes should be dropped without attempting.

        Also transitions an expired open circuit to half-open so the next
        publish acts as a probe.
        """
        if self._open_until == 0.0:
            return False
        if self._clock() < self._open_until:
            return True
        # Cooldown expired → half-open. Reset so the next publish probes.
        self._open_until = 0.0
        self._consecutive_failures = 0
        return False

    def record_success(self) -> None:
        """Close the circuit after a successful publish."""
        self._consecutive_failures = 0
        self._open_until = 0.0

    def record_failure(self) -> bool:
        """Record a failure; returns True if this failure opened the circuit."""
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.failure_threshold:
            self._open_until = self._clock() + self.cooldown_seconds
            return True
        return False

