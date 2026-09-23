"""
run_pipeline.py
----------------
Local entry point, For manual/local testing, this script runs
both stages in sequence.

Usage:
    python run_pipeline.py
"""

from src.extract import source_to_raw
# from src.transform import raw_to_curated


def main():
    print("=== STAGE 1: Source -> Raw ===")
    raw_failures = source_to_raw.run_all()

    # print("\n=== STAGE 2: Raw -> Curated (+ Quarantine) ===")
    # # Only curate tables that succeeded in stage 1 (fail-fast per table,
    # # not for the whole pipeline)
    # failed_tables = {t for t, _ in raw_failures}
    # curated_failures = raw_to_curated.run_all()

    print("\n=== PIPELINE SUMMARY ===")
    print(f"Raw stage failures:     {raw_failures if raw_failures else 'None'}")
    # print(f"Curated stage failures: {curated_failures if curated_failures else 'None'}")


if __name__ == "__main__":
    main()
