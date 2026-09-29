import os
from datetime import datetime

# --- SCD Type 2: the "still current" sentinel end-date ---------------------
# Every "current" dimension row gets this as row_end_date until it's
# superseded. 9999-12-31 is the industry-standard Kimball convention.
SCD2_OPEN_END_DATE_STR = os.environ.get("CLASSICMODELS_SCD2_OPEN_END_DATE", "9999-12-31")
SCD2_OPEN_END_DATE = datetime.strptime(SCD2_OPEN_END_DATE_STR, "%Y-%m-%d").date()

# --- Incremental extraction: "beginning of time" fallback ------------------
# Used when a table has never been loaded before (no watermark recorded
# yet) and its lookback-window logic needs a base date to subtract from.
LOOKBACK_BASE_DATE = os.environ.get("CLASSICMODELS_LOOKBACK_BASE_DATE", "1900-01-01")

# --- Default filename for the human-readable activity log ------------------
# (the JSONL run-history file location itself IS already fully
# config-driven via secrets.yaml -> metadata.run_log_file; this is the
# separate rotating ActivityLogger stream, and only its filename - not
# its directory, which already comes from secrets.yaml -> paths.logs -
# was previously a hardcoded function default.)
DEFAULT_ACTIVITY_LOG_FILE = os.environ.get("CLASSICMODELS_ACTIVITY_LOG_FILE", "pipeline.jsonl")

# --- Date parsing format used consistently across the pipeline -------------
DATE_FORMAT = os.environ.get("CLASSICMODELS_DATE_FORMAT", "%Y-%m-%d")
