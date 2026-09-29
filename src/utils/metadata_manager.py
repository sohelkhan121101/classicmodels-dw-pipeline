"""
metadata_manager.py
--------------------
All reads/writes to our JSON-based metadata store live here:

    metadata/watermark_state.json   -> per-table incremental load pointer
    metadata/schema_registry.json   -> per-table last-known schema (for schema evolution detection)
    logs/pipeline_run_log.jsonl     -> append-only run history (JSON Lines)

"""

import json
import os
import platform
from datetime import datetime, timezone

IS_WINDOWS = platform.system() == "Windows"

if IS_WINDOWS:
    LOCK_SH = LOCK_EX = LOCK_UN = None

    def _flock(f, mode):
        pass  # no-op on Windows
else:
    import fcntl
    LOCK_SH, LOCK_EX, LOCK_UN = fcntl.LOCK_SH, fcntl.LOCK_EX, fcntl.LOCK_UN

    def _flock(f, mode):
        fcntl.flock(f, mode)


class MetadataManager:
    def __init__(self, watermark_file: str, schema_registry_file: str, run_log_file: str):
        self.watermark_file = watermark_file
        self.schema_registry_file = schema_registry_file
        self.run_log_file = run_log_file
        os.makedirs(os.path.dirname(run_log_file), exist_ok=True)

    def _read_json(self, path):
        if not os.path.exists(path):
            return {}
        with open(path, "r") as f:
            _flock(f, LOCK_SH)
            try:
                data = json.load(f)
            finally:
                _flock(f, LOCK_UN)
        return data

    def _write_json(self, path, data):
        tmp_path = path + ".tmp"
        with open(tmp_path, "w") as f:
            _flock(f, LOCK_EX)
            try:
                json.dump(data, f, indent=2, default=str)
            finally:
                _flock(f, LOCK_UN)
        os.replace(tmp_path, path)

    def get_watermark(self, table_name: str):
        state = self._read_json(self.watermark_file)
        return state.get(table_name, {}).get("last_watermark_value")

    def update_watermark(self, table_name: str, new_value, status: str, records_processed: int):
        state = self._read_json(self.watermark_file)
        state.setdefault(table_name, {})
        state[table_name]["last_watermark_value"] = new_value
        state[table_name]["last_run_status"] = status
        state[table_name]["last_run_timestamp"] = datetime.now(timezone.utc).isoformat()
        state[table_name]["records_processed"] = records_processed
        self._write_json(self.watermark_file, state)

    def mark_failed(self, table_name: str):
        state = self._read_json(self.watermark_file)
        state.setdefault(table_name, {})
        state[table_name]["last_run_status"] = "FAILED"
        state[table_name]["last_run_timestamp"] = datetime.now(timezone.utc).isoformat()
        self._write_json(self.watermark_file, state)

    def get_known_schema(self, table_name: str):
        registry = self._read_json(self.schema_registry_file)
        return registry.get(table_name)

    def update_schema(self, table_name: str, schema_dict: dict, version: int):
        registry = self._read_json(self.schema_registry_file)
        registry[table_name] = {
            "schema": schema_dict,
            "version": version,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write_json(self.schema_registry_file, registry)

    def log_run(self, table_name, layer, status, records_success=0,
                records_quarantined=0, error_message=None,
                start_time=None, end_time=None):
        record = {
            "table_name": table_name,
            "layer": layer,
            "status": status,
            "records_success": records_success,
            "records_quarantined": records_quarantined,
            "error_message": error_message,
            "start_time": start_time.isoformat() if start_time else None,
            "end_time": (end_time or datetime.now(timezone.utc)).isoformat(),
        }
        with open(self.run_log_file, "a") as f:
            _flock(f, LOCK_EX)
            try:
                f.write(json.dumps(record, default=str) + "\n")
            finally:
                _flock(f, LOCK_UN)
