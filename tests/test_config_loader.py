"""
test_config_loader.py
----------------------
Unit tests for src/utils/config_loader.py

We don't touch the real project config/metadata files here - each test
writes its OWN temporary yaml/json file (via pytest's tmp_path fixture)
so tests stay isolated and don't break if someone edits secrets.yaml later.
"""

import json
import yaml
import pytest

from src.utils.config_loader import load_secrets, load_table_config, get_active_tables, resolve_path


def test_load_secrets_reads_yaml_correctly(tmp_path):
    secrets_content = {
        "mysql": {"source": {"host": "localhost", "port": 3306}},
        "paths": {"raw": "data/raw"},
    }
    secrets_file = tmp_path / "secrets.yaml"
    secrets_file.write_text(yaml.dump(secrets_content))

    result = load_secrets(path=str(secrets_file))

    assert result["mysql"]["source"]["host"] == "localhost"
    assert result["paths"]["raw"] == "data/raw"


def test_load_table_config_reads_json_correctly(tmp_path):
    config_content = {
        "orders": {"load_type": "incremental", "primary_key": ["orderNumber"]}
    }
    config_file = tmp_path / "table_config.json"
    config_file.write_text(json.dumps(config_content))

    result = load_table_config(path=str(config_file))

    assert "orders" in result
    assert result["orders"]["load_type"] == "incremental"


def test_get_active_tables_filters_inactive_tables():
    table_config = {
        "orders": {"is_active": True},
        "employees": {"is_active": True},
        "old_table": {"is_active": False},
    }

    active = get_active_tables(table_config)

    assert "orders" in active
    assert "employees" in active
    assert "old_table" not in active
    assert len(active) == 2


def test_get_active_tables_defaults_to_active_when_flag_missing():
    # is_active key missing entirely -> should still be treated as active
    table_config = {"productlines": {}}

    active = get_active_tables(table_config)

    assert active == ["productlines"]


def test_resolve_path_returns_absolute_path():
    result = resolve_path("data/raw")

    assert result.endswith("data/raw")
    import os
    assert os.path.isabs(result)
