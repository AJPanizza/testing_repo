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
contact_table = "contact_silver"


# COMMAND ----------

# DBTITLE 1,Upsert contact
# create the scd silver table
from pyspark.sql import Window
from delta.tables import DeltaTable
from pyspark.sql.functions import col, row_number, when, udf
from pyspark.sql.types import StringType
import random, string

contact_checkpoint_path = f"{checkpoint_base_path}/contact"

# Add random_id to the columns
all_columns = ["Name", "DOB", "occupation", "phone_number", "random_id"]
target_columns = ["Name", "DOB", "occupation", "phone_number"]

random_ids = spark.table(contact_table).select("random_id").collect()
existing_ids = [value.random_id for value in random_ids]

# UDF for generating random_id
def id_generator(size=15, chars=string.ascii_uppercase + string.ascii_lowercase + string.digits):
    while True:
        random_id = ''.join(random.choice(chars) for _ in range(size))
        if random_id not in existing_ids:
            existing_ids.append(random_id)
            return random_id

generate_random_id_udf = udf(id_generator, StringType())

def upsert_to_contacts(microbatch_df, _):
    # pick latest per id inside the microbatch
    w = Window.partitionBy("email").orderBy(col("file_modification_time").desc())
    latest = (
        microbatch_df
        .withColumn("rn", row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    # Add random_id only for new inserts
    latest_with_id = latest.withColumn("random_id", generate_random_id_udf())

    tgt = DeltaTable.forName(spark, "contact_silver")

    # Update condition: only update if source has non-null values that are different from target
    update_conditions = []
    for c in target_columns:
        update_conditions.append(f"(s.{c} IS NOT NULL AND (t.{c} IS NULL OR t.{c} != s.{c}))")
    update_cond = " OR ".join(update_conditions) if update_conditions else "false"

    # Update set: only update columns where source is not null, keep existing random_id
    update_set = {c: when(col(f"s.{c}").isNotNull(), col(f"s.{c}")).otherwise(col(f"t.{c}")) for c in target_columns}
    update_set["random_id"] = col("t.random_id")

    # Insert values: use source values, including email as the key, and new random_id
    insert_vals = {c: col(f"s.{c}") for c in ["email"] + target_columns}
    insert_vals["random_id"] = col("s.random_id")

    (
        tgt.alias("t")
        .merge(latest_with_id.alias("s"), "t.email = s.email")
        .whenMatchedUpdate(condition=update_cond, set=update_set)
        .whenNotMatchedInsert(values=insert_vals)
        .execute()
    )

bronze_df = (
    spark.readStream
    .table(add_table)
)

writer = (
    bronze_df.writeStream
    .foreachBatch(upsert_to_contacts)
    .option("checkpointLocation", contact_checkpoint_path)
    .trigger(availableNow=True)
)

q = writer.start()
q.awaitTermination()
