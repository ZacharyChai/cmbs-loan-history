# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Creates the schema and the volume that hold the cached filings and the Delta tables.
# MAGIC Safe to re-run. The catalog must already exist; on Free Edition that is `workspace`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cmbs")
dbutils.widgets.text("volume", "filings")
catalog, schema, volume = (dbutils.widgets.get(w) for w in ("catalog", "schema", "volume"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
print(f"volume path: /Volumes/{catalog}/{schema}/{volume}")
