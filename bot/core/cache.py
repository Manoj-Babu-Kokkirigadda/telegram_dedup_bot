"""Per-chat in-memory TTL cache for known hashes."""
from __future__ import annotations

import threading
from typing import Iterable

from cachetools import TTLCache

HashSet = set[tuple[str, str | None]]
SecondarySet = set[tuple[str, str]]


class HashCache:
    """Thread-safe TTLCache wrapper keyed by chat_id."""

    def __init__(self, maxsize: int = 2000, ttl: int = 300) -> None:
        self._cache: TTLCache[int, HashSet] = TTLCache(maxsize=maxsize, ttl=ttl)
        self._secondary: TTLCache[int, SecondarySet] = TTLCache(maxsize=maxsize, ttl=ttl)
        self._lock = threading.RLock()

    def get_hashes(self, chat_id: int) -> HashSet | None:
        with self._lock:
            return self._cache.get(chat_id)

    def set_hashes(self, chat_id: int, data: Iterable[tuple[str, str | None]]) -> None:
        with self._lock:
            self._cache[chat_id] = set(data)

    def add(self, chat_id: int, file_hash: str, phash: str | None) -> None:
        with self._lock:
            existing = self._cache.get(chat_id)
            if existing is not None:
                existing.add((file_hash, phash))

    def get_secondary_hashes(self, chat_id: int) -> SecondarySet | None:
        with self._lock:
            return self._secondary.get(chat_id)

    def set_secondary_hashes(
        self, chat_id: int, data: Iterable[tuple[str, str]]
    ) -> None:
        with self._lock:
            self._secondary[chat_id] = set(data)

    def add_secondary(self, chat_id: int, sec_hash: str, file_id: str) -> None:
        with self._lock:
            existing = self._secondary.get(chat_id)
            if existing is not None:
                existing.add((sec_hash, file_id))

    def invalidate(self, chat_id: int) -> None:
        with self._lock:
            self._cache.pop(chat_id, None)
            self._secondary.pop(chat_id, None)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._secondary.clear()
