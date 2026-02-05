"""
To use the logger in your project, import the functions as follows:

from org_logging import bind_context, clear_context, get_logger

def test_logger_runs_without_spark():
    clear_context()
    bind_context(project="demo", env="test", run_id="123")
    log = get_logger("demo")
    log.info("hello", extra={"x_table": "main.customers", "x_rows": 10})
"""

from .core import bind_context, clear_context, get_logger

__all__ = ["get_logger", "bind_context", "clear_context"]
