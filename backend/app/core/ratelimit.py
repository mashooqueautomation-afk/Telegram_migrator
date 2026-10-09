"""Small in-process sliding-window limiter for OUR auth endpoints (not Telegram's limits)."""
from __future__ import annotations

import time
from collections import defaultdict, deque


class SlidingWindowLimiter:
    def __init__(self, limit: int, window_seconds: int = 60) -> None:
        self.limit = limit
        self.window = window_seconds
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def check(self, key: str, now: float | None = None) -> int:
        """Record a hit. Returns 0 if allowed, otherwise seconds until the next slot."""
        now = time.monotonic() if now is None else now
        hits = self._hits[key]
        while hits and now - hits[0] >= self.window:
            hits.popleft()
        if len(hits) >= self.limit:
            return max(1, int(self.window - (now - hits[0])) + 1)
        hits.append(now)
        return 0
