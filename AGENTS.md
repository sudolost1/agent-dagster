# agent-dagster

Dagster pipeline builder agent. Uses structured output (Pydantic schemas) to generate dlt pipelines, writes them to a shared volume, triggers Dagster validation/materialization runs via GraphQL, and iterates on LLM feedback.

## Architecture

Unlike agent-airflow's LLMCompiler, this agent uses a simpler parse→write→validate→re-prompt loop. The LLM returns `AgentOutput` with items in `pending`/`drafted`/`done` states. Drafted items are written as pipeline files, validated via Dagster runs, and the agent re-prompts with results until all items are `done` or retries are exhausted.

## Files

| File | Purpose |
|------|---------|
| `config.yaml` | Runtime config: LLM endpoint, Dagster GraphQL URL, pipelines dir, system prompt |
| `Dockerfile.agent` | Agent container build |
| `Dockerfile.code` | Dagster code-location container build |
| `Dockerfile.dagster` | Dagster webserver/daemon container build |
| `Pipfile` | Python dependencies |
| `app/` | Agent application code (see `app/AGENTS.md`) |
| `dagster/` | Dagster runtime config (see `dagster/AGENTS.md`) |
