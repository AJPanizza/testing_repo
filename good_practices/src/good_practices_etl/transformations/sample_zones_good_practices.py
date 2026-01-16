from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum as spark_sum

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.runtime import spark
# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dp.table
def sample_zones_good_practices():
    # Read from the "sample_trips" table, then sum all the fares
    return (
        spark.read.table("sample_trips_good_practices")
        .groupBy(col("pickup_zip"))
        .agg(spark_sum("fare_amount").alias("total_fare"))
    )
