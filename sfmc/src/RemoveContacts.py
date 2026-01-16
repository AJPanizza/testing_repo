# Databricks notebook source
# DBTITLE 1,Configuration
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Create dbutils widgets for configuration
dbutils.widgets.text("catalog", "samples", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")
dbutils.widgets.text("volume", "autoloader_volume", "Volume Name")

# Get configuration values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

# Set catalog and Schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Volume paths
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
checkpoint_base_path = f"/Volumes/{catalog}/{schema}/checkpoints"
schema_base_path = f"/Volumes/{catalog}/{schema}/{volume}/schemas"

print(f"Configuration:")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Volume: {volume}")
print(f"Volume Path: {volume_path}")

remove_table = "bronze_remove"

# COMMAND ----------

# DBTITLE 1,Create table
# Create remove data table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {remove_table} (
  email STRING,
  source_file STRING,
  ingestion_timestamp TIMESTAMP,
  file_modification_time TIMESTAMP
)
""")

# COMMAND ----------

# DBTITLE 1,Remove
remove_source_path = f"{volume_path}/*/remove/*.csv"
remove_checkpoint_path = f"{checkpoint_base_path}/remove_stream"
remove_schema_path = schema_base_path+"/remove_schema"
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"Processing REMOVE files from: {remove_source_path}")
print(f"Batch ID: {batch_id}")


remove_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", remove_schema_path)
    
    # CSV-specific options
    .option("header", "true")
    .option("inferSchema", "true")

    .load(remove_source_path)
    .select("email")
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("file_modification_time", col("_metadata.file_modification_time"))
)

# Process with triggered execution (availableNow)
remove_query = (remove_df
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", remove_checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True) 
    .toTable(remove_table)
)

# Wait for completion
remove_query.awaitTermination()
print("✓ REMOVE files processed successfully")
