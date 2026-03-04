from __future__ import annotations

import concurrent.futures
import json
import os
import logging
import socket
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pika
import redis as redis_lib
from fastapi import FastAPI

from app.agent import Agent
from app.api import router
from app import buildlog
from app.config import get_editable, load_config
from app.context import load_history
from app.logbuffer import BufferHandler, init_redis as init_log_redis
from app.registry import start_heartbeat
from app.tasklog import init_redis as init_task_redis, record_task

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger().addHandler(BufferHandler())
# Suppress uvicorn access logs (GET /config, GET /queue/log, etc.)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

agent = Agent()


def _queue_name() -> str:
    name = get_editable().get("agent-name", "agent-dagster")
    return f"q.tasks.{name}"


def _connect(host: str, user: str = "guest", password: str = "guest") -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(
        host=host,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=600,
    )
    for attempt in range(1, 11):
        try:
            conn = pika.BlockingConnection(params)
            log.info("Connected to RabbitMQ at %s", host)
            return conn
        except (pika.exceptions.AMQPConnectionError, OSError):
            wait = min(attempt * 2, 30)
            log.warning("RabbitMQ not ready (attempt %d/10), retrying in %ds…", attempt, wait)
            time.sleep(wait)
    raise RuntimeError(f"Could not connect to RabbitMQ at {host} after 10 attempts")


_task_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
_agent_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)


def _build_response(payload: dict, response_obj: dict, agent_name: str) -> str:
    return json.dumps({
        "task": payload,
        "response": response_obj,
        "source": agent_name,
        "container_id": socket.gethostname(),
    })


def _process_task(
    ch: pika.adapters.blocking_connection.BlockingChannel,
    method: pika.spec.Basic.Deliver,
    connection: pika.BlockingConnection,
    payload: dict,
    build_id: str,
) -> None:
    """Run in worker thread so the connection thread stays responsive for heartbeats."""
    cfg = get_editable()
    timeout = cfg.get("task_timeout_seconds", 600)
    agent_name = cfg.get("agent-name", "agent-dagster")
    received_at = datetime.now(tz=timezone.utc)
    source = payload["source"]
    task_text = payload["task"]
    project_name = payload["project-name"]
    job_name = payload["job-name"]

    def nack(requeue: bool = True):
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)

    user_message = json.dumps(payload, indent=2)
    start = time.monotonic()
    future = _agent_executor.submit(
        agent.run,
        user_message,
        build_id=build_id,
        project_name=project_name,
        job_name=job_name,
    )
    try:
        result = future.result(timeout=timeout)
        processing_time = time.monotonic() - start
        log.info("Build %s: agent returned: %s", build_id, str(result)[:500])

        # Agent returns a dict (structured output)
        if isinstance(result, dict):
            response_obj = result
        else:
            try:
                response_obj = json.loads(str(result))
            except (json.JSONDecodeError, TypeError):
                response_obj = {"status": "complete", "summary": str(result)}

        response_obj.setdefault("build_id", build_id)
        # Ensure response string for queue consumers (e.g. PM)
        if "response" not in response_obj and "pending_response" in response_obj:
            response_obj["response"] = response_obj["pending_response"]
        elif "response" not in response_obj:
            response_obj["response"] = response_obj.get("summary", "")
        status = response_obj.get("status", "complete")
        buildlog.finish_build(build_id, project_name, job_name, status)

        record_task(
            task=task_text,
            result=json.dumps(response_obj) if isinstance(response_obj, dict) else str(result),
            processing_time_s=processing_time,
            received_at=received_at,
        )

        response_queue = f"q.tasks.{source}"
        response_body = _build_response(payload, response_obj, agent_name)

        def do_publish_and_ack():
            ch.queue_declare(
                queue=response_queue,
                durable=True,
                arguments={"x-queue-type": "quorum"},
            )
            ch.basic_publish(
                exchange="",
                routing_key=response_queue,
                body=response_body,
                properties=pika.BasicProperties(delivery_mode=2),
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

        log.info("Build %s: published response to '%s' with status '%s'", build_id, response_queue, status)
        connection.add_callback_threadsafe(do_publish_and_ack)

    except concurrent.futures.TimeoutError:
        future.cancel()
        log.error("Build %s: task timed out after %ds, requeuing", build_id, timeout)
        buildlog.finish_build(build_id, project_name, job_name, "timeout")
        connection.add_callback_threadsafe(lambda: nack(requeue=True))

    except Exception:
        log.exception("Build %s: failed to process task", build_id)
        buildlog.finish_build(build_id, project_name, job_name, "error")
        connection.add_callback_threadsafe(lambda: nack(requeue=True))


def _on_message(
    ch: pika.adapters.blocking_connection.BlockingChannel,
    method: pika.spec.Basic.Deliver,
    _properties: pika.spec.BasicProperties,
    body: bytes,
) -> None:
    """Dispatch to worker thread immediately so connection thread can send heartbeats."""
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        log.error("Invalid JSON payload, rejecting message")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    source = payload.get("source")
    task_text = payload.get("task")
    project_name = payload.get("project-name")
    job_name = payload.get("job-name")

    if not source or not task_text or not project_name or not job_name:
        log.error("Payload missing required keys (source, task, project-name, job-name), rejecting")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    build_id = str(uuid.uuid4())
    container_id = socket.gethostname()
    log.info("Build %s: received task from '%s' for %s/%s", build_id, source, project_name, job_name)

    buildlog.start_build(build_id, project_name, job_name, container_id)
    record_task(task=task_text, result="(in progress)", processing_time_s=0.0, received_at=datetime.now(tz=timezone.utc))

    connection = ch.connection
    _task_executor.submit(_process_task, ch, method, connection, payload, build_id)


def _consume() -> None:
    cfg = get_editable()
    host = cfg.get("rabbitmq_host", "rabbitmq")
    user = cfg.get("rabbitmq_user", "guest")
    password = cfg.get("rabbitmq_password", "guest")
    try:
        connection = _connect(host, user, password)
    except RuntimeError:
        log.warning("RabbitMQ unavailable — running without task consumer")
        return
    channel = connection.channel()

    queue = _queue_name()
    channel.queue_declare(queue=queue, durable=True, arguments={"x-queue-type": "quorum"})

    routing_key = cfg.get("agent-name", "agent-dagster")
    channel.queue_bind(queue=queue, exchange="tasks.x", routing_key=routing_key)
    log.info("Bound '%s' to tasks.x with routing key '%s'", queue, routing_key)

    channel.basic_qos(prefetch_count=4)
    channel.basic_consume(queue=queue, on_message_callback=_on_message)

    log.info("Consuming from queue '%s'", queue)
    channel.start_consuming()


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_config()
    cfg = get_editable()
    redis_client = redis_lib.Redis(
        host=os.environ.get("REDIS_HOST") or cfg.get("redis_host", "cache"),
        port=int(os.environ.get("REDIS_PORT") or cfg.get("redis_port", 6379)),
        decode_responses=True,
    )
    init_log_redis(redis_client)
    init_task_redis(redis_client)
    buildlog.init_redis(redis_client)
    load_history()
    start_heartbeat("agent")
    consumer = threading.Thread(target=_consume, daemon=True)
    consumer.start()
    yield


app = FastAPI(title="agent-dagster", lifespan=lifespan)
app.include_router(router)
