# Databricks notebook source
# MAGIC %md
# MAGIC # Validaciones
# MAGIC
# MAGIC Pensemos en validaciones que se pueden hacer sobre las tablas.
# MAGIC
# MAGIC ### bronze_add
# MAGIC - Not null: email, file_source, file_modification
# MAGIC
# MAGIC ### bronze_remove
# MAGIC - Not null: emails, file_source, file_modification
# MAGIC
# MAGIC ### contacts_silver
# MAGIC - PK: email
# MAGIC ### contacts_segment
# MAGIC
# MAGIC ### main_table
# MAGIC
