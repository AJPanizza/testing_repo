# Databricks notebook source
# Auto Loader - Triggered execution for batch processing
# Configuration using dbutils widgets for flexibility

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Create dbutils widgets for configuration
dbutils.widgets.text("catalog", "samples", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")
dbutils.widgets.text("export_volume", "default", "Export Volume")

# Get configuration values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
export_volume = dbutils.widgets.get("export_volume")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

volume_path = f"/Volumes/{catalog}/{schema}/{export_volume}"
date = datetime.now().strftime("%Y-%m-%d")

print(f"Configuration:")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")

# COMMAND ----------

main_table = spark.table("main_table")
segment_rows = spark.table("segments").collect()


for segment in segment_rows:
    res = (
        main_table
        .where(array_contains(col("list_of_segments"), segment.segment))
        .drop("list_of_segments")
    )
    print(f"Segment: {segment.segment}")
    res.toPandas().to_csv(f"{volume_path}/{segment.segment}{date}.csv", index=False)

