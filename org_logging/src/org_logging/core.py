from __future__ import annotations

import logging
import os
from contextvars import ContextVar
from typing import Any, Dict, Optional

from .json_format import JsonFormatter
from .spark import try_get_pyspark_logger


# Context that will be injected into every log line
_CONTEXT: ContextVar[Dict[str, Any]] = ContextVar("_ORG_LOGGING_CONTEXT", default={})


def bind_context(**fields: Any) -> None:
    """
    Bind context fields (project/env/run_id/task/etc.) that will be included in every log record.
    Call once at job start, and optionally per task.
    """
    ctx = dict(_CONTEXT.get())
    ctx.update({k: v for k, v in fields.items() if v is not None})
    _CONTEXT.set(ctx)


def clear_context() -> None:
    """Clear any bound context."""
    _CONTEXT.set({})


class ContextFilter(logging.Filter):
    """
    Injects bound context + enforces a convention:
      - extra structured fields should be passed as keys prefixed with 'x_'.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _CONTEXT.get()
        for k, v in ctx.items():
            # Don't overwrite existing attrs
            if not hasattr(record, k):
                setattr(record, k, v)
        return True


def _configure_python_logger(logger: logging.Logger) -> None:
    """
    Configure standard logging with JSON output. Safe to call multiple times.
    """
    if getattr(logger, "_org_logging_configured", False):
        return

    level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)
    logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(ContextFilter())
    logger.addHandler(handler)

    # Prevent double logging if root handlers exist
    logger.propagate = False

    setattr(logger, "_org_logging_configured", True)


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger that:
      - Uses PySparkLogger when available (Spark runtime)
      - Otherwise uses standard logging with JSON formatter
      - Always injects bound context into log records
    """
    spark_logger = try_get_pyspark_logger(name)
    if spark_logger is not None:
        # PySparkLogger is a logging.Logger subclass; add filter for context injection.
        # Safe: filter is no-op if already present.
        spark_logger.addFilter(ContextFilter())
        return spark_logger

    logger = logging.getLogger(name)
    _configure_python_logger(logger)
    return logger
