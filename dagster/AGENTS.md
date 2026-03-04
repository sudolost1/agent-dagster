# agent-dagster/dagster

Dagster runtime configuration files. Loaded by the Dagster webserver and code-location containers.

## Files

| File | Purpose |
|------|---------|
| `definitions.py` | Code location module — dynamically discovers `@job` definitions from the pipelines volume |
| `dagster.yaml` | Instance config — Postgres storage, DockerRunLauncher with shared volumes, QueuedRunCoordinator |
| `workspace.yaml` | Tells webserver to load code from `code-location:4000` gRPC server |
