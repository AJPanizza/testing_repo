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

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# Volume paths
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
checkpoint_base_path = f"/Volumes/{catalog}/{schema}/checkpoints"
schema_base_path = f"/Volumes/{catalog}/{schema}/checkpoints/schemas"

print(f"Configuration:")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Volume: {volume}")
print(f"Volume Path: {volume_path}")

add_table = "bronze_add"
remove_table = "bronze_remove"

# COMMAND ----------

# DBTITLE 1,Add segment
from pyspark.sql.functions import split, element_at, col, row_number
from pyspark.sql import Window
from delta.tables import DeltaTable

contact_segment_checkpoint = f"{checkpoint_base_path}/contact_segment"
contact_segment_table = "contact_segment_table"

# Create historical contact data
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {contact_segment_table} (
  email STRING,
  segment STRING,
  file_modification_time TIMESTAMP,
  is_active BOOLEAN
) USING DELTA
""")

bronze_df = (spark.readStream
  .table(add_table)
)

bronze_df = bronze_df.select(
        "email",
        "file_modification_time",
        element_at(split("source_file", "/"), -3).alias("segment")
    )

target_columns = ["email", "segment"] 

def upsert_to_contact_segment(microbatch_df, _):
    w = Window.partitionBy("email", "segment").orderBy(col("file_modification_time").desc())
    latest = (microbatch_df
              .withColumn("rn", row_number().over(w))
              .filter("rn = 1")
              .drop("rn"))

    tgt = DeltaTable.forName(spark, contact_segment_table)
    
    # Insert values: insert s.email and s.segment
    insert_vals = {'email': col('s.email'), 'segment': col('s.segment'), "file_modification_time": col('s.file_modification_time'), "is_active": lit(True)}

    (tgt.alias("t")
        .merge(latest.alias("s"), "t.email = s.email AND t.segment = s.segment")
        .whenNotMatchedInsert(values=insert_vals)
        .whenMatchedUpdate(condition="t.file_modification_time < s.file_modification_time", set=insert_vals)
        .execute())

writer = (bronze_df.writeStream
  .foreachBatch(upsert_to_contact_segment)
  .option("checkpointLocation", contact_segment_checkpoint)
  .trigger(availableNow=True)
)

q = writer.start()
q.awaitTermination()

# COMMAND ----------

# DBTITLE 1,Soft delete segment
from pyspark.sql.functions import split, element_at, col, row_number
from pyspark.sql import Window
from delta.tables import DeltaTable

contact_segment_table = "contact_segment_table"
contact_segment_rm_checkpoint = f"{checkpoint_base_path}/contact_segment_remove"

bronze_remove_df = (spark.readStream
  .table(remove_table)
)

bronze_remove_df = bronze_remove_df.select(
        "email",
        "file_modification_time",
        element_at(split("source_file", "/"), -3).alias("segment")
    )

target_columns = ["email", "segment"] 

def remove_from_contact_segment(microbatch_df, _):
    w = Window.partitionBy("email", "segment").orderBy(col("file_modification_time").desc())
    latest = (microbatch_df
              .withColumn("rn", row_number().over(w))
              .filter("rn = 1")
              .drop("rn"))

    tgt = DeltaTable.forName(spark, contact_segment_table)
    
    # Insert soft delete values: delete s.email and s.segment
    delete_vals = {'email': col('s.email'), 'segment': col('s.segment'), "file_modification_time": col('s.file_modification_time'), "is_active": lit(False)}
    update_cond = "t.file_modification_time < s.file_modification_time"

    (tgt.alias("t")
        .merge(latest.alias("s"), "t.email = s.email AND t.segment = s.segment")
        .whenMatchedUpdate(condition=update_cond, set=delete_vals)
        .execute())

writer = (bronze_remove_df.writeStream
  .foreachBatch(remove_from_contact_segment)
  .option("checkpointLocation", contact_segment_rm_checkpoint)
  .trigger(availableNow=True)
)

q = writer.start()
q.awaitTermination()
