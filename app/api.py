from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import get_editable, update_editable
from app.context import get_history
from app.logbuffer import get_logs
from app.tasklog import get_task_log

router = APIRouter()


class ConfigValue(BaseModel):
    value: Any


@router.get("/log")
def read_log(limit: int = 100) -> list[dict[str, str]]:
    """Return application log entries, most recent first."""
    return get_logs(limit=limit)


@router.get("/context")
def read_context() -> list[dict[str, str]]:
    """Return all entries from the subscribed Redis context streams."""
    return get_history()


@router.get("/config")
def list_editable() -> dict[str, Any]:
    """Return all editable config values."""
    return get_editable()


@router.get("/queue/count")
def queue_count() -> dict[str, Any]:
    """Return the number of messages waiting in the agent's RabbitMQ task queue."""
    cfg = get_editable()
    host = cfg.get("rabbitmq_host", "rabbitmq")
    mgmt_port = int(cfg.get("rabbitmq_mgmt_port", 15672))
    user = cfg.get("rabbitmq_user", "guest")
    password = cfg.get("rabbitmq_password", "guest")
    agent_name = cfg.get("agent-name", "agent-base")
    queue = f"q.tasks.{agent_name}"

    url = f"http://{host}:{mgmt_port}/api/queues/%2F/{urllib.request.quote(queue, safe='')}"
    credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {credentials}"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        return {
            "queue": queue,
            "pending": data.get("messages", 0),
            "consumers": data.get("consumers", 0),
        }
    except urllib.error.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"RabbitMQ management API error: {exc.code}")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Could not reach RabbitMQ management API: {exc}")


@router.get("/queue/log")
def queue_log(limit: int = 100) -> list[dict[str, Any]]:
    """Return a log of completed tasks with result, processing time, and receipt timestamp."""
    return get_task_log(limit=limit)


@router.get("/{key}")
def get_config_key(key: str) -> dict[str, Any]:
    """Return a single editable config value."""
    editable = get_editable()
    if key not in editable:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found in editable config")
    return {key: editable[key]}


@router.put("/{key}")
def set_config_key(key: str, body: ConfigValue) -> dict[str, Any]:
    """Update a single editable config value."""
    try:
        updated = update_editable(key, body.value)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {key: updated[key]}
