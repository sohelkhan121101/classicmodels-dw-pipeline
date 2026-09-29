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
import os

import yaml

# Default: two levels up from src/utils/config_loader.py -> project root.
# Override with CLASSICMODELS_PROJECT_ROOT if the pipeline is run from
# somewhere that layout doesn't hold (a different mount path in Docker,
# a packaged install, etc).
PROJECT_ROOT = os.environ.get(
    "CLASSICMODELS_PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
)


def _default_secrets_path() -> str:
    return os.environ.get(
        "CLASSICMODELS_SECRETS_PATH",
        os.path.join(PROJECT_ROOT, "config", "secrets.yaml"),
    )


def _default_table_config_path() -> str:
    return os.environ.get(
        "CLASSICMODELS_TABLE_CONFIG_PATH",
        os.path.join(PROJECT_ROOT, "metadata", "table_config.json"),
    )


def load_secrets(path: str = None) -> dict:
    path = path or _default_secrets_path()
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"secrets.yaml not found at '{path}'. "
            f"Copy config/secrets.yaml.example to config/secrets.yaml and fill in your "
            f"real credentials, or set CLASSICMODELS_SECRETS_PATH to point at your config file."
        )
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_table_config(path: str = None) -> dict:
    path = path or _default_table_config_path()
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"table_config.json not found at '{path}'. "
            f"Set CLASSICMODELS_TABLE_CONFIG_PATH to point at your config file if it "
            f"lives somewhere other than metadata/table_config.json."
        )
    with open(path, "r") as f:
        return json.load(f)


def get_active_tables(table_config: dict) -> list:
    return [t for t, cfg in table_config.items() if cfg.get("is_active", True)]


def resolve_path(relative_path: str) -> str:
    """Resolve a path from secrets.yaml relative to PROJECT_ROOT (itself
    overridable via CLASSICMODELS_PROJECT_ROOT - see module docstring)."""
    return os.path.join(PROJECT_ROOT, relative_path)
