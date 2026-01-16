# tests/conftest.py
import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[2]")  # no cluster needed
        .config("spark.sql.shuffle.partitions", "1")
        .appName("unit-tests")
        .getOrCreate()
    )


def assertDataFrameEqualIgnoreColumnOrder(actual_df, expected_df):
    assertDataFrameEqual(
        actual_df.select(sorted(actual_df.columns)),
        expected_df.select(sorted(expected_df.columns)),
    )
