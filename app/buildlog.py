from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import redis as redis_lib

_redis: redis_lib.Redis | None = None

MAX_STEPS = 5000


def init_redis(client: redis_lib.Redis) -> None:
    global _redis
    _redis = client


def _meta_key(project: str, job: str, build_id: str) -> str:
    return f"builds:meta:{project}:{job}:{build_id}"


def _steps_key(project: str, job: str, build_id: str) -> str:
    return f"builds:steps:{project}:{job}:{build_id}"


def _index_key() -> str:
    return "builds:index"


def _composite(project: str, job: str, build_id: str) -> str:
    return f"{project}:{job}:{build_id}"


def start_build(
    build_id: str,
    project_name: str,
    job_name: str,
    container_id: str,
) -> None:
    if _redis is None:
        return
    now = datetime.now(tz=timezone.utc).isoformat()
    key = _meta_key(project_name, job_name, build_id)
    _redis.hset(key, mapping={
        "build_id": build_id,
        "project": project_name,
        "job": job_name,
        "container_id": container_id,
        "status": "running",
        "started_at": now,
        "finished_at": "",
    })
    _redis.zadd(_index_key(), {_composite(project_name, job_name, build_id): time.time()})


def log_step(
    build_id: str,
    project_name: str,
    job_name: str,
    step_type: str,
    content: str,
) -> None:
    if _redis is None:
        return
    entry = json.dumps({
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "step_type": step_type,
        "content": content[:4000],
    })
    key = _steps_key(project_name, job_name, build_id)
    _redis.rpush(key, entry)
    _redis.ltrim(key, -MAX_STEPS, -1)


def finish_build(
    build_id: str,
    project_name: str,
    job_name: str,
    status: str,
) -> None:
    if _redis is None:
        return
    key = _meta_key(project_name, job_name, build_id)
    _redis.hset(key, mapping={
        "status": status,
        "finished_at": datetime.now(tz=timezone.utc).isoformat(),
    })


def get_build_log(project_name: str, job_name: str, build_id: str) -> dict:
    if _redis is None:
        return {"meta": {}, "steps": []}
    meta_key = _meta_key(project_name, job_name, build_id)
    steps_key = _steps_key(project_name, job_name, build_id)
    meta = _redis.hgetall(meta_key)
    raw_steps = _redis.lrange(steps_key, 0, -1)
    steps = [json.loads(s) for s in raw_steps]
    return {"meta": meta, "steps": steps}


def list_builds(limit: int = 50) -> list[dict]:
    if _redis is None:
        return []
    entries = _redis.zrevrange(_index_key(), 0, limit - 1)
    results = []
    for composite in entries:
        parts = composite.split(":", 2)
        if len(parts) != 3:
            continue
        project, job, build_id = parts
        meta = _redis.hgetall(_meta_key(project, job, build_id))
        if meta:
            results.append(meta)
    return results


def list_builds_for_job(project_name: str, job_name: str, limit: int = 50) -> list[dict]:
    if _redis is None:
        return []
    pattern = _meta_key(project_name, job_name, "*")
    results = []
    cursor = 0
    while True:
        cursor, keys = _redis.scan(cursor, match=pattern, count=100)
        for key in keys:
            meta = _redis.hgetall(key)
            if meta:
                results.append(meta)
        if cursor == 0:
            break
    results.sort(key=lambda m: m.get("started_at", ""), reverse=True)
    return results[:limit]
