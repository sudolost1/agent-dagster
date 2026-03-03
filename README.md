# agent-dagster

Specialized agent forked from
[agent-base](https://github.com/sudolost1/agent-base). Consumes task messages
from RabbitMQ, processes them with an OpenAI-compatible LLM (with optional MCP
tool-calling), and publishes the response back to the sender's queue.

See the [agent-base README](https://github.com/sudolost1/agent-base#readme) for
full documentation on the message protocol, HTTP API, and project structure.

## Quick Start

```bash
cp .env.example .env

# Run with Docker Compose (starts RabbitMQ + Redis + agent-dagster)
docker compose up --build

# Or run locally
pipenv install
pipenv run uvicorn app.main:app --reload
```
