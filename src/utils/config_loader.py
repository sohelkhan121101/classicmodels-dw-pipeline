"""
config_loader.py
-----------------
Single place responsible for reading:
  - secrets.yaml   (credentials, paths)
  - table_config.json (static per-table load rules)

Keeping this separate means every other module just calls
`load_secrets()` / `load_table_config()` instead of re-implementing
file I/O everywhere - single source of truth for config access.
"""

import json
import yaml
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def load_secrets(path: str = None) -> dict:
    path = path or os.path.join(PROJECT_ROOT, "config", "secrets.yaml")
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_table_config(path: str = None) -> dict:
    path = path or os.path.join(PROJECT_ROOT, "metadata", "table_config.json")
    with open(path, "r") as f:
        return json.load(f)


def get_active_tables(table_config: dict) -> list:
    return [t for t, cfg in table_config.items() if cfg.get("is_active", True)]


def resolve_path(relative_path: str) -> str:
    """Resolve a path from secrets.yaml relative to project root."""
    return os.path.join(PROJECT_ROOT, relative_path)
