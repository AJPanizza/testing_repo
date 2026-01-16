# Databricks notebook source
# DBTITLE 1,Configuration
# Auto Loader - Triggered execution for batch processing
# Configuration using dbutils widgets for flexibility

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Create dbutils widgets for configuration
dbutils.widgets.text("catalog", "samples", "Catalog Name")
dbutils.widgets.text("schema", "default", "Schema Name")

# Get configuration values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")


print(f"Configuration:")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE main_table AS (
# MAGIC   SELECT cont.*, seg.list_of_segments FROM contact_silver cont
# MAGIC   LEFT JOIN (
# MAGIC     SELECT email, array_agg(segment) as list_of_segments
# MAGIC     FROM contact_segment_table
# MAGIC     WHERE is_active=True
# MAGIC     GROUP BY email
# MAGIC     ) seg ON cont.email = seg.email
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE segments AS
# MAGIC   SELECT DISTINCT segment from contact_segment_table
# MAGIC   
