"""
Centralized logging configuration for BZA Keywords Expand.

Usage:
    from logger import get_logger
    logger = get_logger(__name__)
    logger.info("message", extra={"job_id": "xxx", "theme_id": "xxx"})

Log file: logs/expansion.log  (rotates daily at midnight, keeps 30 days)
Console:  INFO+ to stdout
File:     DEBUG+ to file
"""

import logging
import os
from logging.handlers import TimedRotatingFileHandler

# Always resolve relative to this file, works under uvicorn --reload child processes
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOGS_DIR, "expansion.log")


# ---------- Formatter ----------

class ContextFormatter(logging.Formatter):
    """Prepend [job_id] [theme_id] to every message when present in extra."""
    FMT     = "%(asctime)s %(levelname)-8s %(name)-20s %(context)s%(message)s"
    DATEFMT = "%Y-%m-%d %H:%M:%S"

    def format(self, record: logging.LogRecord) -> str:
        parts = []
        job_id   = getattr(record, "job_id",   None)
        theme_id = getattr(record, "theme_id", None)
        if job_id:
            parts.append(f"[job:{job_id}]")
        if theme_id:
            parts.append(f"[theme:{theme_id}]")
        record.context = " ".join(parts) + " " if parts else ""
        return super().format(record)


# ---------- Module-level singleton handlers ----------
# Built once per process; survive logger.getLogger() being called multiple times.

_file_handler: logging.Handler | None    = None
_console_handler: logging.Handler | None = None


def _build_file_handler() -> logging.Handler:
    h = TimedRotatingFileHandler(
        LOG_FILE,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
        delay=False,          # open the file immediately so we can confirm it's writable
    )
    h.suffix = "%Y-%m-%d"
    h.setFormatter(ContextFormatter(fmt=ContextFormatter.FMT, datefmt=ContextFormatter.DATEFMT))
    h.setLevel(logging.DEBUG)
    return h


def _build_console_handler() -> logging.Handler:
    h = logging.StreamHandler()
    h.setFormatter(ContextFormatter(fmt=ContextFormatter.FMT, datefmt=ContextFormatter.DATEFMT))
    h.setLevel(logging.INFO)
    return h


def get_logger(name: str) -> logging.Logger:
    """Return a named logger with file + console handlers (idempotent)."""
    global _file_handler, _console_handler

    # Initialise handlers once per process
    if _file_handler is None:
        _file_handler = _build_file_handler()
    if _console_handler is None:
        _console_handler = _build_console_handler()

    logger = logging.getLogger(name)

    # Attach handlers only once (guard against multiple calls)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        logger.addHandler(_file_handler)
        logger.addHandler(_console_handler)
        logger.propagate = False   # don't bubble up to root logger / uvicorn

    return logger
