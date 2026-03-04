"""Service registry. TTL-expiring Redis hash with background heartbeat. Identical to agent-base."""

from __future__ import annotations

import logging
import os
import socket
import threading
import time

import redis as redis_lib

from app.config import get_config, get_editable

log = logging.getLogger(__name__)

TTL_SECONDS = 30
HEARTBEAT_INTERVAL = 15


def _build_entry(service_type: str) -> tuple[str, dict[str, str]]:
    """Build Redis key and hash data for this service instance."""
    cfg = get_editable()
    static = get_config().get("static", {})
    hostname = socket.gethostname()
    name = cfg.get("agent-name", static.get("app_name", "unknown"))
    port = 8000

    key = f"registry:{service_type}:{name}:{hostname}"
    data = {
        "name": name,
        "type": service_type,
        "hostname": hostname,
        "url": f"http://{hostname}:{port}",
        "port": str(port),
        "version": static.get("version", "0.0.0"),
    }
    return key, data


def _heartbeat_loop(client: redis_lib.Redis, service_type: str) -> None:
    """Daemon loop: re-publish registry entry every HEARTBEAT_INTERVAL seconds."""
    while True:
        try:
            key, data = _build_entry(service_type)
            client.hset(key, mapping=data)
            client.expire(key, TTL_SECONDS)
            log.debug("Registry heartbeat: %s", key)
        except redis_lib.RedisError:
            log.warning("Registry heartbeat failed", exc_info=True)
        time.sleep(HEARTBEAT_INTERVAL)


def start_heartbeat(service_type: str = "agent") -> None:
    """Register this service and start the background heartbeat thread."""
    cfg = get_editable()
    host = os.environ.get("REDIS_HOST") or cfg.get("redis_host", "cache")
    port = int(os.environ.get("REDIS_PORT") or cfg.get("redis_port", 6379))
    client = redis_lib.Redis(host=host, port=port, decode_responses=True)

    key, data = _build_entry(service_type)
    client.hset(key, mapping=data)
    client.expire(key, TTL_SECONDS)
    log.info("Registered as %s", key)

    t = threading.Thread(target=_heartbeat_loop, args=(client, service_type), daemon=True)
    t.start()
