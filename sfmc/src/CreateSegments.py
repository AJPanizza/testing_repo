# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE agustin_training_catalog.dev_agustin_panizza_sfmc.segments
# MAGIC AS
# MAGIC SELECT segment
# MAGIC FROM VALUES
# MAGIC   ('blue'),
# MAGIC   ('green'),
# MAGIC   ('red') AS data(segment)
