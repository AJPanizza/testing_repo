"""This file configures pytest.

This file is in the root since it can be used for tests in any place in this
project, including tests under resources/.
"""

from __future__ import annotations

import csv
import json
import os
import pathlib
import sys
import types
from contextlib import contextmanager
from typing import Optional, TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_LOCAL_SPARK: Optional["SparkSession"] = None


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--compute",
        action="store",
        default=os.environ.get("TEST_COMPUTE", "databricks"),
        choices=["databricks", "local"],
        help="Select compute backend: databricks or local (env: TEST_COMPUTE).",
    )


@pytest.fixture()
def spark(request: pytest.FixtureRequest) -> SparkSession:
    """Provide a SparkSession fixture for tests.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    compute = request.config.getoption("--compute")
    if compute == "local":
        return _get_local_spark()
    return _get_databricks_spark()


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Example usage:

        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    from databricks.sdk import WorkspaceClient

    conf = WorkspaceClient().config
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
    print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
    print(f"  see {url} for manual configuration", file=sys.stdout)

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session."""
    with _allow_stderr_output(config):
        compute = config.getoption("--compute")
        if compute == "local":
            _bootstrap_local_runtime()
            return

        _enable_fallback_compute()

        # Initialize Spark session eagerly, so it is available even when
        # SparkSession.builder.getOrCreate() is used. For DB Connect 15+,
        # we validate version compatibility with the remote cluster.
        if hasattr(_get_databricks_builder(), "validateSession"):
            _get_databricks_builder().validateSession().getOrCreate()
        else:
            _get_databricks_builder().getOrCreate()


def _get_databricks_builder():
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder


def _get_databricks_spark() -> SparkSession:
    return _get_databricks_builder().getOrCreate()


def _get_local_spark() -> SparkSession:
    try:
        from pyspark.sql import SparkSession as SparkSessionRuntime
    except ImportError as exc:
        raise ImportError(
            "PySpark is required for local tests. Install it and rerun with --compute=local."
        ) from exc

    global _LOCAL_SPARK
    if _LOCAL_SPARK is None:
        _LOCAL_SPARK = (
            SparkSessionRuntime.builder.master("local[*]")
            .appName("good_practices_tests")
            .getOrCreate()
        )
    return _LOCAL_SPARK


def _bootstrap_local_runtime() -> None:
    spark_session = _get_local_spark()
    try:
        import databricks.sdk.runtime as runtime  # type: ignore

        runtime.spark = spark_session
        return
    except Exception:
        pass

    runtime = types.ModuleType("databricks.sdk.runtime")
    runtime.spark = spark_session
    sys.modules.setdefault("databricks", types.ModuleType("databricks"))
    sys.modules.setdefault("databricks.sdk", types.ModuleType("databricks.sdk"))
    sys.modules["databricks.sdk.runtime"] = runtime
    sys.modules["databricks.sdk"].runtime = runtime
