# Databricks notebook source
# DBTITLE 1,Configuration
# Auto Loader - Triggered execution for batch processing
# Configuration using dbutils widgets for flexibility

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Create dbutils widgets for configuration
dbutils.widgets.text("catalog", "agustin_training_catalog", "Catalog Name")
dbutils.widgets.text("schema", "dev_agustin_panizza_sfmc", "Schema Name")
dbutils.widgets.text("volume", "mock_files", "Volume Name")

# Get configuration values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Volume paths
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
checkpoint_base_path = f"/Volumes/{catalog}/{schema}/checkpoints"
schema_base_path = f"/Volumes/{catalog}/{schema}/schemas"

print(f"Configuration:")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Volume: {volume}")
print(f"Volume Path: {volume_path}")



# COMMAND ----------

# DBTITLE 1,Upload historic_data to bronze
file_location = f"{volume_path}/people_data_historic.csv"

historic_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(file_location)
)

historic_df.write.mode("overwrite").saveAsTable("historic_data")

# COMMAND ----------

# DBTITLE 1,Upload Historic Data
contact_table = "contact_silver"

schema = StructType(
    [
        StructField('Name', StringType(), True),
        StructField('email', StringType(), True),
        StructField('DOB', DateType(), True),
        StructField('occupation', StringType(), True),
        StructField('phone_number', StringType(), True),
        StructField('random_id', StringType(), True),
        StructField('list_keys', StringType(), True)
    ]
)

spark.sql(f"""
CREATE TABLE {contact_table} (
    Name STRING,
    email STRING NOT NULL PRIMARY KEY,
    DOB DATE,
    occupation STRING,
    phone_number STRING,
    random_id STRING NOT NULL,
    list_keys STRING
)
""")

spark.sql(f"""
INSERT OVERWRITE {contact_table}
SELECT * FROM historic_data
""")

