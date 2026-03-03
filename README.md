# agent-dagster

Specialized Dagster pipeline builder agent, forked from
[agent-base](https://github.com/sudolost1/agent-base). Receives ETL task
descriptions via RabbitMQ, uses an LLM to generate Dagster job code, writes it
to a persistent volume, launches a sample run, evaluates the result, and
reports back.

Uses [dagster-docker](https://docs.dagster.io/deployment/oss/deployment-options/docker)
with `DockerRunLauncher` for autoscaling job runs across containers.

## Two Operating Modes

### Agent mode

Starts the full Dagster stack **plus** the agent container listening for tasks
on RabbitMQ.

```bash
docker compose -f docker-compose.yml -f docker-compose.agent.yml up --build
```

- Agent API: `http://localhost:8000`
- Dagster UI: `http://localhost:3000`
- RabbitMQ Management: `http://localhost:15672`

### Dagster-only mode

Starts only the Dagster stack. No agent, no RabbitMQ. Previously generated
pipelines on the volume are available. Runs autoscale via `DockerRunLauncher`.

```bash
docker compose up --build
```

- Dagster UI: `http://localhost:3000`

## Task Format

### Required fields

```json
{
  "source": "pm",
  "task": "Pull from the users table, remove nulls, export to CSV",
  "project-name": "etl-users",
  "job-name": "clean-and-export"
}
```

### Full ETL task (recommended)

Connection details can live in the task payload or in a Redis context stream
the agent subscribes to. If the agent can't find what it needs in either
location, it reports `status: "pending"` with the missing details.

```json
{
  "source": "pm",
  "task": "Extract all rows from the users table updated in the last 7 days, normalize email addresses to lowercase, deduplicate by user_id, and load into the analytics.clean_users table",
  "project-name": "user-analytics",
  "job-name": "weekly-user-sync",
  "source_connection": {
    "type": "postgres",
    "host": "source-db.internal",
    "port": 5432,
    "database": "production",
    "schema": "public",
    "table": "users",
    "credentials_key": "prod-pg-readonly"
  },
  "target_connection": {
    "type": "postgres",
    "host": "warehouse.internal",
    "port": 5432,
    "database": "warehouse",
    "schema": "analytics",
    "table": "clean_users",
    "credentials_key": "warehouse-rw"
  },
  "options": {
    "incremental_key": "updated_at",
    "lookback_days": 7,
    "dedup_key": "user_id",
    "write_mode": "upsert"
  }
}
```

## Response Protocol

Every response has the same four-key envelope:

```json
{
  "task": { "...original task echoed back..." },
  "response": {
    "status": "complete|pending|failed",
    "summary": "Human-readable description of what happened",
    "build_id": "uuid",
    "run_id": "dagster-run-id (on complete/failed)",
    "missing": ["source_connection (on pending)"]
  },
  "source": "agent-dagster",
  "container_id": "hostname"
}
```

Use `build_id` with the `/build-logs` API to inspect the full step trace.

## HTTP API

Inherits all endpoints from agent-base, plus:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/build-logs` | List recent builds (all projects) |
| GET | `/build-logs/{project}/{job}` | List builds for a specific job |
| GET | `/build-logs/{project}/{job}/{build_id}` | Full step history for a build |
| GET | `/config` | List all editable config values |
| GET | `/log` | Application log |
| GET | `/context` | Redis context stream entries |
| GET | `/queue/count` | Pending messages and consumer count |
| GET | `/queue/log` | Completed task log |
| GET | `/{key}` | Get a single config value |
| PUT | `/{key}` | Update a single config value |

## Agent Tools

The LLM has access to these tools during the build loop:

| Tool | Description |
|------|-------------|
| `test_connection` | Test a database connection (postgres supported) |
| `write_pipeline` | Validate syntax and write a Dagster job file to the volume |
| `launch_run` | Launch a Dagster job via GraphQL |
| `get_run_status` | Poll a Dagster run's status and stats |

Plus any MCP tools configured in `config.yaml`.

## Dockerfiles

| File | Purpose |
|------|---------|
| `Dockerfile.agent` | Agent container (FastAPI + RabbitMQ + Dagster libs) |
| `Dockerfile.dagster` | Dagster host processes (webserver + daemon) |
| `Dockerfile.code` | Code location gRPC server + run container image |

## Project Structure

```
agent-dagster/
├── app/
│   ├── main.py          # FastAPI + RabbitMQ consumer + build orchestration
│   ├── api.py           # HTTP API router (includes /build-logs)
│   ├── agent.py         # Extended build loop with local tools + context refresh
│   ├── tools.py         # Local tool definitions for the LLM
│   ├── buildlog.py      # Redis-backed per-build step logging
│   ├── config.py        # Config loader / persistence
│   ├── context.py       # Redis context streams (with incremental refresh)
│   ├── logbuffer.py     # App log ring buffer (Redis-backed)
│   ├── registry.py      # Service heartbeat to Redis
│   ├── tasklog.py       # Completed task history log
│   └── schema.py        # Pydantic output schema
├── dagster/
│   ├── definitions.py   # Dynamic code location (auto-discovers jobs)
│   ├── dagster.yaml     # Dagster instance config (DockerRunLauncher)
│   └── workspace.yaml   # Code location gRPC endpoint
├── config.yaml
├── Dockerfile.agent
├── Dockerfile.dagster
├── Dockerfile.code
├── docker-compose.yml         # Dagster-only base
├── docker-compose.agent.yml   # Agent overlay
├── Pipfile
├── PLAN.md
└── README.md
```
