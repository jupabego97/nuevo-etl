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
