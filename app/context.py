from __future__ import annotations

import logging
from typing import Any

import redis as redis_lib

from app.config import get_editable

log = logging.getLogger(__name__)

_client: redis_lib.Redis | None = None


def _get_client() -> redis_lib.Redis:
    global _client
    cfg = get_editable()
    host = cfg.get("redis_host", "cache")
    port = int(cfg.get("redis_port", 6379))
    if _client is None:
        _client = redis_lib.Redis(host=host, port=port, decode_responses=True)
    return _client


def _stream_key(name: str) -> str:
    """Map a context name to its Redis stream key.

    The special name '*' represents the shared global stream.
    """
    return "context:global" if name == "*" else f"context:{name}"


def load_history() -> None:
    """Read and log full history from all configured context streams.

    Called once at startup so the agent is aware of any prior context
    stored in Redis before it began receiving tasks.
    """
    cfg = get_editable()
    contexts: list[str] = cfg.get("contexts", [])
    if not contexts:
        return
    r = _get_client()
    for ctx in contexts:
        key = _stream_key(ctx)
        try:
            entries: list[tuple[str, dict[str, Any]]] = r.xrange(key)
            log.info(
                "Context stream '%s' (key=%s): %d historical entries loaded",
                ctx,
                key,
                len(entries),
            )
            for msg_id, fields in entries:
                content = fields.get("content", "")
                if content:
                    log.debug("  [%s] %s: %s", ctx, msg_id, content[:120])
        except redis_lib.RedisError:
            log.warning("Could not read history for context stream '%s'", ctx, exc_info=True)


def get_history() -> list[dict[str, str]]:
    """Return all entries from configured context streams as a list of dicts.

    Each entry contains ``stream``, ``id``, and all fields stored in the Redis
    stream message (typically ``content``).
    """
    cfg = get_editable()
    contexts: list[str] = cfg.get("contexts", [])
    if not contexts:
        return []

    r = _get_client()
    result: list[dict[str, str]] = []
    for ctx in contexts:
        key = _stream_key(ctx)
        try:
            entries: list[tuple[str, dict[str, Any]]] = r.xrange(key)
            for msg_id, fields in entries:
                result.append({"stream": ctx, "id": msg_id, **fields})
        except redis_lib.RedisError:
            log.warning("Could not read context stream '%s'", ctx, exc_info=True)
    return result


def build_prefix() -> str:
    """Read all configured context streams and return a formatted prefix string.

    Returns an empty string when no context entries exist, so the user
    message is passed through unchanged.
    """
    cfg = get_editable()
    contexts: list[str] = cfg.get("contexts", [])
    if not contexts:
        return ""

    r = _get_client()
    lines: list[str] = []
    for ctx in contexts:
        key = _stream_key(ctx)
        try:
            entries: list[tuple[str, dict[str, Any]]] = r.xrange(key)
            for _msg_id, fields in entries:
                content = fields.get("content", "")
                if content:
                    lines.append(f"[{ctx}] {content}")
        except redis_lib.RedisError:
            log.warning("Could not read context stream '%s'", ctx, exc_info=True)

    if not lines:
        return ""

    return "--- Context ---\n" + "\n".join(lines) + "\n--- End Context ---\n\n"


def build_prefix_with_ids() -> tuple[str, dict[str, str]]:
    """Like build_prefix but also returns a map of stream -> last message ID.

    Used by the extended build loop to establish the baseline for incremental
    context refresh via ``get_new_entries``.
    """
    cfg = get_editable()
    contexts: list[str] = cfg.get("contexts", [])
    if not contexts:
        return "", {}

    r = _get_client()
    lines: list[str] = []
    last_ids: dict[str, str] = {}
    for ctx in contexts:
        key = _stream_key(ctx)
        try:
            entries: list[tuple[str, dict[str, Any]]] = r.xrange(key)
            for msg_id, fields in entries:
                last_ids[ctx] = msg_id
                content = fields.get("content", "")
                if content:
                    lines.append(f"[{ctx}] {content}")
        except redis_lib.RedisError:
            log.warning("Could not read context stream '%s'", ctx, exc_info=True)

    prefix = ""
    if lines:
        prefix = "--- Context ---\n" + "\n".join(lines) + "\n--- End Context ---\n\n"
    return prefix, last_ids


def get_new_entries(last_ids: dict[str, str]) -> tuple[list[dict[str, str]], dict[str, str]]:
    """Read new entries from subscribed context streams since ``last_ids``.

    Returns a list of new entries and an updated last_ids map.
    """
    cfg = get_editable()
    contexts: list[str] = cfg.get("contexts", [])
    if not contexts:
        return [], last_ids

    r = _get_client()
    streams: dict[str, str] = {}
    for ctx in contexts:
        key = _stream_key(ctx)
        streams[key] = last_ids.get(ctx, "0-0")

    new_entries: list[dict[str, str]] = []
    updated_ids = dict(last_ids)

    try:
        results = r.xread(streams, count=100, block=0)
    except redis_lib.RedisError:
        log.warning("Failed to XREAD context streams", exc_info=True)
        return [], last_ids

    if not results:
        return [], last_ids

    key_to_ctx = {_stream_key(ctx): ctx for ctx in contexts}
    for stream_key, entries in results:
        ctx = key_to_ctx.get(stream_key, stream_key)
        for msg_id, fields in entries:
            updated_ids[ctx] = msg_id
            content = fields.get("content", "")
            if content:
                new_entries.append({"stream": ctx, "id": msg_id, "content": content})

    return new_entries, updated_ids
