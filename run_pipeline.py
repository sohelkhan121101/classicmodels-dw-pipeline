"""
run_pipeline.py
-----------------
Single entry point for the whole local classicmodels pipeline:

    Stage 1: source_to_raw.run_all()     MySQL      -> Raw 
    Stage 2: raw_to_curated.run_all()    Raw        -> Curated + Quarantine
    Stage 3: curated_to_gold.run_all()   Curated    -> Gold (star schema, MySQL "classicmodels_dw")

Each stage isolates per-table failures internally (self-healing: a bad
table doesn't crash the run, and its watermark simply doesn't advance).
This script stops moving forward a layer if the layer behind it had ZERO
successful tables, since there's nothing new/valid to build on top of.
"""

import sys

from src.extract import source_to_raw
from src.transform import raw_to_curated
from src.transform import curated_to_gold


def main():
    print("=== STAGE 1: source -> raw ===")
    raw_failures = source_to_raw.run_all()

    print("=== STAGE 2: raw -> curated/quarantine ===")
    curated_failures = raw_to_curated.run_all()

    print("=== STAGE 3: curated -> gold (star schema) ===")
    gold_failures = curated_to_gold.run_all()

    print("\n=== PIPELINE SUMMARY ===")
    print(f"Stage 1 (raw) failures:      {raw_failures if raw_failures else 'none'}")
    print(f"Stage 2 (curated) failures:  {curated_failures if curated_failures else 'none'}")
    print(f"Stage 3 (gold) failures:     {gold_failures if gold_failures else 'none'}")

    if raw_failures or curated_failures or gold_failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
