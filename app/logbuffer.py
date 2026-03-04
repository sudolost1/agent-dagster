"""Application log buffer. Redis-backed logging.Handler with local deque fallback. Identical to agent-base."""

from __future__ import annotations

import json
import logging
import socket
from collections import deque
from datetime import datetime, timezone

import redis as redis_lib

MAX_ENTRIES = 2000

_redis: redis_lib.Redis | None = None
_key: str = ""
_pending: deque[dict[str, str]] = deque(maxlen=MAX_ENTRIES)


def init_redis(client: redis_lib.Redis) -> None:
    """Switch to Redis-backed storage and flush any locally buffered entries."""
    global _redis, _key
    _redis = client
    _key = f"logs:app:{socket.gethostname()}"
    while _pending:
        entry = _pending.popleft()
        _push(entry)


def _push(entry: dict[str, str]) -> None:
    _redis.lpush(_key, json.dumps(entry))
    _redis.ltrim(_key, 0, MAX_ENTRIES - 1)


class BufferHandler(logging.Handler):
    """Logging handler that persists records to Redis, keyed by container ID."""

    def emit(self, record: logging.LogRecord) -> None:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if _redis is None:
            _pending.append(entry)
            return
        try:
            _push(entry)
        except redis_lib.RedisError:
            _pending.append(entry)


def get_logs(limit: int = 100) -> list[dict[str, str]]:
    """Return log entries from Redis, most recent first, capped at *limit*."""
    if _redis is None:
        return list(_pending)[-limit:][::-1]
    try:
        raw = _redis.lrange(_key, 0, limit - 1)
        return [json.loads(entry) for entry in raw]
    except redis_lib.RedisError:
        return list(_pending)[-limit:][::-1]
