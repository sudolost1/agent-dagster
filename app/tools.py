"""Dagster tool implementations. Four tools: test_connection (SQLAlchemy), write_pipeline, launch_run, get_run_status (Dagster GraphQL)."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import urllib.request
from pathlib import Path

from app.config import get_editable

log = logging.getLogger(__name__)

PIPELINES_DIR_DEFAULT = "/opt/dagster/app/pipelines"
TEST_CONNECTION_TIMEOUT = 15


def _pipelines_dir() -> Path:
    """Return the configured pipelines volume path."""
    return Path(get_editable().get("pipelines_dir", PIPELINES_DIR_DEFAULT))


def _graphql_url() -> str:
    """Return the Dagster GraphQL endpoint URL from config."""
    return get_editable().get("dagster_graphql_url", "http://dagster-webserver:3000/graphql")


# ---------------------------------------------------------------------------
# Tool: test_connection
# ---------------------------------------------------------------------------
# Uses SQLAlchemy with connection strings in the same format as dlt sql_database.
# This avoids custom handlers per DB type; any SQLAlchemy-supported backend works.

# SQLAlchemy driver names (same as dlt sql_database). Requires: postgres->psycopg2, mysql->pymysql.
_DRIVER_MAP = {
    "postgres": "postgresql+psycopg2",
    "postgresql": "postgresql+psycopg2",
    "mysql": "mysql+pymysql",
}


def _connection_string(
    type: str, host: str, port: int, database: str, user: str, password: str
) -> str:
    """Build a SQLAlchemy connection string from component parts."""
    from urllib.parse import quote_plus

    driver = _DRIVER_MAP.get(type.lower() if type else "", "")
    if not driver:
        raise ValueError(f"Unsupported connection type: {type}. Supported: {list(_DRIVER_MAP.keys())}")
    safe_password = quote_plus(password)
    return f"{driver}://{user}:{safe_password}@{host}:{port}/{database}"


def _do_test_connection(type: str, host: str, port: int, database: str, user: str, password: str) -> str:
    """Execute a test query against the database. Returns JSON result string."""
    from sqlalchemy import create_engine, text

    url = _connection_string(type, host, port, database, user, password)
    engine = create_engine(url, connect_args={"connect_timeout": 10})
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return json.dumps({
        "success": True,
        "message": f"Connected to {type}://{host}:{port}/{database}",
    })


def test_connection(type: str, host: str, port: int, database: str, user: str, password: str) -> str:
    """Validate database connectivity via SQLAlchemy test query."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(_do_test_connection, type, host, port, database, user, password)
        try:
            return future.result(timeout=TEST_CONNECTION_TIMEOUT)
        except concurrent.futures.TimeoutError:
            return json.dumps({
                "success": False,
                "message": f"Connection attempt timed out after {TEST_CONNECTION_TIMEOUT}s (host={host}, port={port})",
            })
        except Exception as exc:
            return json.dumps({"success": False, "message": str(exc)})


TEST_CONNECTION_SCHEMA = {
    "type": "function",
    "function": {
        "name": "test_connection",
        "description": "Test a database connection (SQLAlchemy/dlt-compatible). Supports postgres, postgresql, mysql. Returns success or error.",
        "parameters": {
            "type": "object",
            "properties": {
                "type": {"type": "string", "description": "Database type: 'postgres', 'postgresql', or 'mysql'"},
                "host": {"type": "string"},
                "port": {"type": "integer"},
                "database": {"type": "string"},
                "user": {"type": "string"},
                "password": {"type": "string"},
            },
            "required": ["type", "host", "port", "database", "user", "password"],
        },
    },
}


# ---------------------------------------------------------------------------
# Tool: write_pipeline
# ---------------------------------------------------------------------------

def write_pipeline(project_name: str, job_name: str, code: str) -> str:
    """Syntax-check and write a Python pipeline file to the shared volume."""
    safe_project = project_name.replace("/", "_").replace("..", "_")
    safe_job = job_name.replace("/", "_").replace("..", "_")

    try:
        compile(code, f"{safe_job}.py", "exec")
    except SyntaxError as exc:
        return json.dumps({"success": False, "error": f"Syntax error at line {exc.lineno}: {exc.msg}"})

    base = _pipelines_dir() / safe_project
    base.mkdir(parents=True, exist_ok=True)

    init_file = base / "__init__.py"
    if not init_file.exists():
        init_file.write_text("")

    target = base / f"{safe_job}.py"
    target.write_text(code)
    log.info("Wrote pipeline to %s", target)
    return json.dumps({"success": True, "path": str(target)})


WRITE_PIPELINE_SCHEMA = {
    "type": "function",
    "function": {
        "name": "write_pipeline",
        "description": (
            "Validate Python syntax and write a Dagster job file to the pipelines volume. "
            "Returns the file path on success or the syntax error."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "project_name": {"type": "string", "description": "Project subdirectory name"},
                "job_name": {"type": "string", "description": "Job module filename (without .py)"},
                "code": {"type": "string", "description": "Full Python source code for the Dagster job"},
            },
            "required": ["project_name", "job_name", "code"],
        },
    },
}


# ---------------------------------------------------------------------------
# Tool: launch_run
# ---------------------------------------------------------------------------

def _graphql_request(query: str, variables: dict | None = None) -> dict:
    """Send a GraphQL request to the Dagster webserver."""
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(
        _graphql_url(),
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def launch_run(
    project_name: str,
    job_name: str,
    *,
    run_config: dict | None = None,
) -> str:
    """Trigger a Dagster job run via GraphQL launchRun mutation."""
    safe_job = job_name.replace("-", "_")
    query = """
    mutation LaunchRun($selector: JobOrPipelineSelector!, $runConfigData: RunConfigData) {
      launchRun(executionParams: {
        selector: $selector
        runConfigData: $runConfigData
      }) {
        __typename
        ... on LaunchRunSuccess {
          run { runId status }
        }
        ... on PythonError {
          message
        }
        ... on RunConfigValidationInvalid {
          errors { message }
        }
      }
    }
    """
    variables: dict = {
        "selector": {
            "repositoryLocationName": "code-location",
            "repositoryName": "__repository__",
            "jobName": safe_job,
        },
        "runConfigData": run_config or {},
    }
    try:
        data = _graphql_request(query, variables)
        result = data.get("data", {}).get("launchRun", {})
        typename = result.get("__typename", "")
        if typename == "LaunchRunSuccess":
            run = result["run"]
            return json.dumps({"success": True, "run_id": run["runId"], "status": run["status"]})
        errors = result.get("errors", [result.get("message", "Unknown error")])
        return json.dumps({"success": False, "error": str(errors)})
    except Exception as exc:
        return json.dumps({"success": False, "error": str(exc)})


LAUNCH_RUN_SCHEMA = {
    "type": "function",
    "function": {
        "name": "launch_run",
        "description": "Launch a Dagster job run via GraphQL. Returns the run_id on success.",
        "parameters": {
            "type": "object",
            "properties": {
                "project_name": {"type": "string", "description": "Project name (for context)"},
                "job_name": {"type": "string", "description": "Dagster job name to launch"},
            },
            "required": ["project_name", "job_name"],
        },
    },
}


# ---------------------------------------------------------------------------
# Tool: get_run_status
# ---------------------------------------------------------------------------

def get_run_status(run_id: str) -> str:
    """Poll a Dagster run's status and logs via GraphQL."""
    query = """
    query RunStatus($runId: ID!) {
      runOrError(runId: $runId) {
        __typename
        ... on Run {
          runId
          status
          stats {
            ... on RunStatsSnapshot {
              stepsSucceeded
              stepsFailed
              startTime
              endTime
            }
          }
        }
        ... on RunNotFoundError {
          message
        }
        ... on PythonError {
          message
        }
      }
    }
    """
    try:
        data = _graphql_request(query, {"runId": run_id})
        result = data.get("data", {}).get("runOrError", {})
        typename = result.get("__typename", "")
        if typename == "Run":
            return json.dumps({
                "run_id": result["runId"],
                "status": result["status"],
                "stats": result.get("stats", {}),
            })
        return json.dumps({"error": result.get("message", "Unknown error")})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


GET_RUN_STATUS_SCHEMA = {
    "type": "function",
    "function": {
        "name": "get_run_status",
        "description": "Query the status of a Dagster run by run_id. Returns status, step counts, and timing.",
        "parameters": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "The Dagster run ID"},
            },
            "required": ["run_id"],
        },
    },
}


# ---------------------------------------------------------------------------
# Registry: all local tools
# ---------------------------------------------------------------------------

LOCAL_TOOLS: dict[str, callable] = {
    "test_connection": test_connection,
    "write_pipeline": write_pipeline,
    "launch_run": launch_run,
    "get_run_status": get_run_status,
}

LOCAL_TOOL_SCHEMAS: list[dict] = [
    TEST_CONNECTION_SCHEMA,
    WRITE_PIPELINE_SCHEMA,
    LAUNCH_RUN_SCHEMA,
    GET_RUN_STATUS_SCHEMA,
]
