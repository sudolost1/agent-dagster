from __future__ import annotations

import json
import socket
from collections import deque
from datetime import datetime, timezone

import redis as redis_lib

MAX_ENTRIES = 1000

_redis: redis_lib.Redis | None = None
_key: str = ""
_pending: deque[dict] = deque(maxlen=MAX_ENTRIES)


def init_redis(client: redis_lib.Redis) -> None:
    """Switch to Redis-backed storage and flush any locally buffered entries."""
    global _redis, _key
    _redis = client
    _key = f"logs:task:{socket.gethostname()}"
    while _pending:
        entry = _pending.popleft()
        _push(entry)


def _push(entry: dict) -> None:
    _redis.lpush(_key, json.dumps(entry))
    _redis.ltrim(_key, 0, MAX_ENTRIES - 1)


def record_task(
    *,
    task: str,
    result: str,
    processing_time_s: float,
    received_at: datetime,
) -> None:
    """Append a completed task record to Redis (or local buffer pre-init)."""
    entry = {
        "task": task[:300],
        "result": result[:600],
        "processing_time_s": round(processing_time_s, 3),
        "received_at": received_at.astimezone(timezone.utc).isoformat(),
    }
    if _redis is None:
        _pending.append(entry)
        return
    try:
        _push(entry)
    except redis_lib.RedisError:
        _pending.append(entry)


def get_task_log(limit: int = 100) -> list[dict]:
    """Return task log entries from Redis, most recent first, capped at *limit*."""
    if _redis is None:
        return list(_pending)[-limit:][::-1]
    try:
        raw = _redis.lrange(_key, 0, limit - 1)
        return [json.loads(entry) for entry in raw]
    except redis_lib.RedisError:
        return list(_pending)[-limit:][::-1]
