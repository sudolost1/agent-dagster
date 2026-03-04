# agent-dagster/app

Application code for the Dagster pipeline builder agent. Contains the agent loop, Dagster-specific tools, and build logging. Shared modules (config, logbuffer, registry, tasklog) are identical to agent-base.

## Files

| File | Purpose |
|------|---------|
| `agent.py` | `Agent` class — structured output parsing + write→validate→materialize loop via Dagster tools |
| `main.py` | FastAPI entrypoint, lifespan init, RabbitMQ consumer with build tracking, dual thread pools |
| `tools.py` | Four tools: test_connection (SQLAlchemy), write_pipeline, launch_run, get_run_status (Dagster GraphQL) |
| `schema.py` | Pydantic schemas: `JobStatus`, `JobOutputItem`, `AgentOutput` for the build lifecycle |
| `buildlog.py` | Redis-backed build log (identical to agent-airflow) |
| `api.py` | FastAPI router — base endpoints plus `/build-logs` |
| `config.py` | YAML config loader (identical to agent-base) |
| `context.py` | Redis Streams reader (extends agent-base with `build_prefix_with_ids` and `get_new_entries`) |
| `logbuffer.py` | Redis log handler (identical to agent-base) |
| `registry.py` | Service registry heartbeat (identical to agent-base) |
| `tasklog.py` | Task completion log (identical to agent-base) |
