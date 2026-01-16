from pyspark import pipelines as dp
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk.runtime import spark

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dp.table
def sample_trips_good_practices():
    return spark.read.table("samples.nyctaxi.trips")
