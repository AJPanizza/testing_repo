from __future__ import annotations

from typing import Optional


def try_get_pyspark_logger(name: str):
    """
    Try to return PySparkLogger.getLogger(name) if available.
    Returns None if pyspark (or the logger module) isn't present.
    """
    try:
        from pyspark.logger import PySparkLogger  # type: ignore

        return PySparkLogger.getLogger(name)
    except Exception:
        return None
