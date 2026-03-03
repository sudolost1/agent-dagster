"""Dynamic Dagster code location.

Walks the pipelines volume directory, imports every Python module it finds,
and collects all @job-decorated objects into a single Definitions.
This file is used by the code-server (gRPC) and by DockerRunLauncher
containers.
"""
from __future__ import annotations

import importlib.util
import logging
import os
import sys
from pathlib import Path

from dagster import Definitions, JobDefinition

log = logging.getLogger(__name__)

PIPELINES_DIR = os.environ.get("PIPELINES_DIR", "/opt/dagster/app/pipelines")


def _discover_jobs(base_dir: str) -> list[JobDefinition]:
    jobs: list[JobDefinition] = []
    base = Path(base_dir)
    if not base.is_dir():
        log.warning("Pipelines directory %s does not exist", base_dir)
        return jobs

    for py_file in sorted(base.rglob("*.py")):
        if py_file.name.startswith("_"):
            continue
        module_name = py_file.stem
        spec = importlib.util.spec_from_file_location(module_name, py_file)
        if spec is None or spec.loader is None:
            continue
        try:
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if isinstance(attr, JobDefinition):
                    jobs.append(attr)
                    log.info("Discovered job '%s' from %s", attr.name, py_file)
        except Exception:
            log.exception("Failed to load module %s", py_file)

    return jobs


defs = Definitions(jobs=_discover_jobs(PIPELINES_DIR))
