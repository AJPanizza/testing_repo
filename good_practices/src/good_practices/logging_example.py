from org_logging import bind_context, get_logger

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.runtime import dbutils

bind_context(
    project="sfmc_segments",
    env="prod",
    run_id=dbutils.jobs.taskValues.get("run_id", "run_id", default="unknown") if "dbutils" in globals() else None,
    task_key="main",
)

log = get_logger(__name__)
log.info("starting pipeline", extra={"x_source": "aurora", "x_table": "usermgmt.user"})
