"""Limitador de tasa adaptativo según headers de Alegra."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field


@dataclass
class RateLimiter:
    max_requests_per_minute: int = 150
    min_interval_seconds: float = field(init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _last_request_at: float = 0.0
    _remaining: int | None = None
    _reset_seconds: int | None = None
    _consecutive_successes: int = 0

    def __post_init__(self) -> None:
        self.min_interval_seconds = 60.0 / max(self.max_requests_per_minute, 1)

    def update_from_headers(self, remaining: str | None, reset: str | None) -> None:
        if remaining is not None:
            try:
                self._remaining = int(remaining)
            except ValueError:
                pass
        if reset is not None:
            try:
                self._reset_seconds = int(reset)
            except ValueError:
                pass
        if self._remaining is not None and self._reset_seconds and self._remaining > 0:
            header_interval = self._reset_seconds / self._remaining
            self.min_interval_seconds = min(
                60.0,
                max(60.0 / max(self.max_requests_per_minute, 1), header_interval * 0.9),
            )

    def penalize(self) -> None:
        """Reduce la presión inmediatamente después de un 429."""
        self.min_interval_seconds = min(max(self.min_interval_seconds * 2, 0.5), 60.0)
        self._consecutive_successes = 0

    def reward(self) -> None:
        """Recupera gradualmente el límite tras respuestas estables."""
        self._consecutive_successes += 1
        if self._consecutive_successes >= 10:
            base = 60.0 / max(self.max_requests_per_minute, 1)
            self.min_interval_seconds = max(base, self.min_interval_seconds * 0.9)
            self._consecutive_successes = 0

    async def acquire(self) -> None:
        async with self._lock:
            if self._remaining is not None and self._remaining <= 0 and self._reset_seconds:
                await asyncio.sleep(max(self._reset_seconds, 1))
                self._remaining = None
                self._reset_seconds = None

            elapsed = time.monotonic() - self._last_request_at
            wait_time = self.min_interval_seconds - elapsed
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self._last_request_at = time.monotonic()
