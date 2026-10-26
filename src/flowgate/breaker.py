"""Per-upstream circuit breaker. State is local to each replica, which is the usual choice:
breakers react to what this replica sees, and need no coordination."""

from __future__ import annotations

import time


class CircuitBreaker:
    CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"

    def __init__(self, failure_threshold: int = 5, cooldown_s: float = 10.0, clock=time.monotonic):
        self.failure_threshold = failure_threshold
        self.cooldown_s = cooldown_s
        self.clock = clock
        self.failures = 0
        self.opened_at = 0.0
        self._state = self.CLOSED
        self._probe_in_flight = False

    @property
    def state(self) -> str:
        if self._state == self.OPEN and self.clock() - self.opened_at >= self.cooldown_s:
            self._state = self.HALF_OPEN
            self._probe_in_flight = False
        return self._state

    def allow(self) -> bool:
        s = self.state
        if s == self.CLOSED:
            return True
        if s == self.HALF_OPEN and not self._probe_in_flight:
            self._probe_in_flight = True
            return True
        return False

    def record_success(self) -> None:
        self.failures = 0
        self._state = self.CLOSED
        self._probe_in_flight = False

    def record_failure(self) -> None:
        self.failures += 1
        if self._state == self.HALF_OPEN or self.failures >= self.failure_threshold:
            self._state = self.OPEN
            self.opened_at = self.clock()
            self._probe_in_flight = False
