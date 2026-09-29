"""
reset_pipeline.py
--------------------
Resets DYNAMIC pipeline state so the next run behaves like a brand-new,
first-ever run. Does NOT touch config/secrets.yaml or
metadata/table_config.json - those are static configuration, not state.

Resets:
  - metadata/watermark_state.json  -> {}
  - metadata/schema_registry.json  -> {}
  - data/raw, data/archive, data/curated, data/quarantine  -> removed (local folders)
  - logs/*.jsonl                   -> removed
  - Gold star-schema tables in MySQL ("classicmodels_dw") -> TRUNCATEd, since
    Gold now lives in the Dockerized MySQL warehouse, not a local folder.

Usage:
    python reset_pipeline.py                # reset everything (state + local data + logs + gold tables)
    python reset_pipeline.py --state-only   # only reset watermark/schema json,
                                             # keep raw/curated/logs on disk and gold tables in MySQL
"""

import argparse
import json
import os
import shutil

from sqlalchemy import text

from src.utils.config_loader import load_secrets, resolve_path
from src.warehouse.mysql_manager import MySQLManager, TABLE_CREATE_ORDER


def reset_state_files(secrets: dict):
    watermark_file = resolve_path(secrets["metadata"]["watermark_file"])
    schema_registry_file = resolve_path(secrets["metadata"]["schema_registry_file"])

    for f in (watermark_file, schema_registry_file):
        os.makedirs(os.path.dirname(f), exist_ok=True)
        with open(f, "w") as fh:
            json.dump({}, fh)
        print(f"reset -> {f}")


def reset_data_and_logs(secrets: dict):
    # "archive" is optional - some setups (like this one) don't keep a
    # separate archive copy, so its key may not exist in secrets.yaml at
    # all. Every other key here is skipped the same way if it's missing,
    # rather than crashing on a KeyError.
    for key in ("raw", "archive", "curated", "quarantine"):
        if key not in secrets["paths"]:
            print(f"skipped -> '{key}' not configured in secrets.yaml (paths), nothing to reset")
            continue
        path = resolve_path(secrets["paths"][key])
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"removed -> {path}")
        os.makedirs(path, exist_ok=True)

    logs_dir = resolve_path(secrets["paths"]["logs"])
    if os.path.exists(logs_dir):
        for fname in os.listdir(logs_dir):
            if fname.endswith(".jsonl"):
                fpath = os.path.join(logs_dir, fname)
                os.remove(fpath)
                print(f"removed -> {fpath}")
    os.makedirs(logs_dir, exist_ok=True)


def reset_gold_tables(secrets: dict):
    """Truncates every Gold star-schema table in MySQL (classicmodels_dw),
    rather than deleting local files - Gold now lives in the Docker MySQL
    warehouse so a fresh run can rebuild every dim/fact from scratch."""
    mysql_gold_cfg = secrets["warehouse"]["mysql"]
    dw = MySQLManager(mysql_gold_cfg)
    dw.ensure_schema()
    with dw.engine.begin() as conn:
        # FK constraints now exist between the star-schema tables (so ERD
        # tools like DBeaver can draw them) - truncating one at a time in
        # TABLE_CREATE_ORDER would still fail against a live FK, so checks
        # are turned off for this reset pass only.
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for table in TABLE_CREATE_ORDER:
            conn.execute(text(f"TRUNCATE TABLE `{table}`"))
            print(f"truncated -> {mysql_gold_cfg['database']}.{table}")
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))


def main():
    parser = argparse.ArgumentParser(description="Reset pipeline state for a fresh run")
    parser.add_argument("--state-only", action="store_true",
                         help="Only reset watermark/schema JSON; keep raw/curated/gold data and logs on disk")
    args = parser.parse_args()

    secrets = load_secrets()

    reset_state_files(secrets)
    if not args.state_only:
        reset_data_and_logs(secrets)
        try:
            reset_gold_tables(secrets)
        except Exception as e:
            print(f"WARNING: could not reset gold tables in MySQL ({e}). "
                  f"Is the Docker MySQL container running?")

    print("\nDone. table_config.json and secrets.yaml were NOT touched (static config).")


if __name__ == "__main__":
    main()
