"""
logger.py
---------
Custom structured JSON logger for the pipeline.

Every log record is a single JSON object with a FIXED set of fields, so
downstream tools (Grafana, jq, pandas) can parse logs without regex:

    timestamp        -> ISO8601 UTC time the log was emitted
    source           -> which layer/table/component emitted it (e.g. "orders" / "raw_extractor")
    who              -> the process/user context (getpass.getuser() + hostname), i.e. "who ran this"
    activity         -> short name of the activity being logged (e.g. "extract_full_load")
    status           -> START | SUCCESS | FAILED | WARNING | INFO
    duration_seconds -> filled only on completion logs (SUCCESS/FAILED), null on START
    message          -> human readable detail
    extra            -> free-form dict for anything activity-specific (row counts etc.)

Logs are written to:
    1. A rotating local file (logs/pipeline.jsonl) - one JSON object per line (JSONL)
    2. stdout (so Airflow task logs also capture it)
"""

import json
import logging
import socket
import getpass
import os
from datetime import datetime, timezone

from src.utils.constants import DEFAULT_ACTIVITY_LOG_FILE


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = getattr(record, "payload", None)
        if payload is None:
            # fallback for plain logger.info("string") calls
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": record.name,
                "who": f"{getpass.getuser()}@{socket.gethostname()}",
                "activity": "generic_log",
                "status": record.levelname,
                "duration_seconds": None,
                "message": record.getMessage(),
                "extra": {},
            }
        return json.dumps(payload, default=str)


def get_logger(log_dir: str = "logs", log_file: str = DEFAULT_ACTIVITY_LOG_FILE) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("classicmodels_pipeline")

    if logger.handlers:      # avoid duplicate handlers on repeated calls
        return logger

    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(os.path.join(log_dir, log_file))
    file_handler.setFormatter(JsonFormatter())

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(JsonFormatter())

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


class ActivityLogger:
    """
    Convenience wrapper so calling code doesn't have to build the payload dict
    by hand every time. Usage:

        alog = ActivityLogger(logger, source="orders")
        alog.start("extract_incremental")
        ... do work ...
        alog.success("extract_incremental", extra={"records": 245})
        # or on failure:
        alog.failed("extract_incremental", error="connection refused")
    """

    def __init__(self, logger: logging.Logger, source: str):
        self.logger = logger
        self.source = source
        self.who = f"{getpass.getuser()}@{socket.gethostname()}"
        self._start_times = {}

    def _emit(self, activity, status, message="", extra=None, duration=None):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": self.source,
            "who": self.who,
            "activity": activity,
            "status": status,
            "duration_seconds": duration,
            "message": message,
            "extra": extra or {},
        }
        level = logging.ERROR if status == "FAILED" else (
            logging.WARNING if status == "WARNING" else logging.INFO
        )
        self.logger.log(level, "", extra={"payload": payload})

    def start(self, activity, message="starting", extra=None):
        self._start_times[activity] = datetime.now(timezone.utc)
        self._emit(activity, "START", message=message, extra=extra, duration=None)

    def success(self, activity, message="completed successfully", extra=None):
        duration = self._elapsed(activity)
        self._emit(activity, "SUCCESS", message=message, extra=extra, duration=duration)

    def failed(self, activity, error, extra=None):
        duration = self._elapsed(activity)
        extra = extra or {}
        extra["error"] = str(error)
        self._emit(activity, "FAILED", message="activity failed", extra=extra, duration=duration)

    def warning(self, activity, message, extra=None):
        self._emit(activity, "WARNING", message=message, extra=extra, duration=None)

    def _elapsed(self, activity):
        start = self._start_times.get(activity)
        if start is None:
            return None
        return round((datetime.now(timezone.utc) - start).total_seconds(), 3)
