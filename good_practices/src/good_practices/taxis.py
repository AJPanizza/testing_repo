from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame


def find_all_taxis() -> DataFrame:
    """Find all taxi data."""
    return spark.read.table("samples.nyctaxi.trips")


def count_taxis(df: DataFrame) -> int:
    """Count all taxi records."""
    return df.count()


def filter_taxis_by_vendor_id(df: DataFrame, vendor_id: str) -> DataFrame:
    """Filter taxis by vendor ID."""
    return df.filter(df.vendor_id == vendor_id)
