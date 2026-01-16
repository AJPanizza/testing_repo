# Databricks notebook source
catalog = "agustin_training_catalog"
schema = "dev_agustin_panizza_sfmc"
root_path = f"/Volumes/{catalog}/{schema}"

mock_volume = f"{root_path}/mock_files"
load_volume = f"{root_path}/files"



# COMMAND ----------

for segment in ["blue", "green", "red"]:
    dbutils.fs.mkdirs(f"{load_volume}/{segment}")
    dbutils.fs.mkdirs(f"{load_volume}/{segment}/add")
    dbutils.fs.mkdirs(f"{load_volume}/{segment}/remove")


# COMMAND ----------

dbutils.fs.cp(f"{mock_volume}/people_data_04.csv", f"{load_volume}/red/add")
