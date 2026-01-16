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

print(f"Configuration:")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Volume: {volume}")
print(f"Volume Path: {volume_path}")

add_table = "bronze_add"

# COMMAND ----------

# DBTITLE 1,Create Table and define schema
add_files_schema = StructType([
    StructField("Name", StringType(), True),
    StructField("email", StringType(), False),
    StructField("DOB", DateType(), True),
    StructField("occupation", StringType(), True),
    StructField("phone_number", StringType(), True)
])


# Create add data table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {add_table} (
  Name STRING,
  email STRING,
  DOB DATE,
  occupation STRING,
  phone_number STRING,
  source_file STRING,
  ingestion_timestamp TIMESTAMP,
  file_modification_time TIMESTAMP
)
""")

# COMMAND ----------

# DBTITLE 1,Add new arriving records
add_source_path = f"{volume_path}/*/add/*.csv"
add_checkpoint_path = f"{checkpoint_base_path}/add_stream"
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"Processing ADD files from: {add_source_path}")
print(f"Batch ID: {batch_id}")

add_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .schema(add_files_schema)
    .option("cloudFiles.maxFilesPerTrigger", "1000")
    
    # CSV-specific options
    .option("header", "true")
    .option("inferSchema", "true")
    
    .load(add_source_path)
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("file_modification_time", col("_metadata.file_modification_time"))
)

# Process with triggered execution (availableNow)
add_query = (add_df
    .writeStream
    .outputMode("append")
    .option("checkpointLocation", add_checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True) 
    .toTable(add_table)
)

# Wait for completion
add_query.awaitTermination()
print("✓ ADD files processed successfully")

