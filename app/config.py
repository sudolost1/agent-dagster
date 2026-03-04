"""YAML config manager. Thread-safe loader with runtime-editable keys. Identical to agent-base."""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

import yaml

_CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "config.yaml"))
_lock = threading.Lock()

_config: dict[str, Any] = {}


def load_config(path: Path | None = None) -> dict[str, Any]:
    """Load config.yaml from disk into the module-level cache."""
    global _config
    path = path or _CONFIG_PATH
    with open(path) as f:
        _config = yaml.safe_load(f)
    return _config


def get_config() -> dict[str, Any]:
    """Return full config dict, loading from disk on first access."""
    if not _config:
        load_config()
    return _config


def get_editable() -> dict[str, Any]:
    """Return a copy of the editable config section."""
    return dict(get_config().get("editable", {}))


def update_editable(key: str, value: Any) -> dict[str, Any]:
    """Update a single editable key and persist to disk."""
    cfg = get_config()
    if key not in cfg.get("editable", {}):
        raise KeyError(f"'{key}' is not an editable config key")
    with _lock:
        cfg["editable"][key] = value
        _persist(cfg)
    return get_editable()


def _persist(cfg: dict[str, Any]) -> None:
    """Write current config dict back to config.yaml."""
    with open(_CONFIG_PATH, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False)
