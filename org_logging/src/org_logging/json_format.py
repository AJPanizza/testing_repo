from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict


# Fields from LogRecord that are noisy or not stable across runtimes
_SKIP = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JsonFormatter(logging.Formatter):
    """
    JSON formatter that emits a stable schema.
    Conventions:
      - Use record.<standard_fields> for fixed metadata (project/env/run_id/etc.)
      - Use extra fields prefixed with 'x_' for event-specific structured fields
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": _utc_now(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }

        # Include exception info (if any) in a structured way
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        # Include bound context fields (they are set as attributes by ContextFilter)
        # Anything not in _SKIP and not starting with '_' is allowed,
        # but we encourage event extras as x_*.
        for k, v in record.__dict__.items():
            if k in _SKIP or k.startswith("_"):
                continue
            if k in payload:
                continue
            # Keep it: includes context fields and x_* event fields
            payload[k] = v

        return json.dumps(payload, default=str)
