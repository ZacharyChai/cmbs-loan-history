# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Creates the schema and the volume that hold the cached filings and the Delta tables,
# MAGIC and copies the published-snapshot reference file into the volume, from the Git
# MAGIC folder's own `tests/fixtures/`, so `04_checks` can find it without a separate manual
# MAGIC upload. Safe to re-run. The catalog must already exist; on Free Edition that is
# MAGIC `workspace`.

# COMMAND ----------

import os

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cmbs")
dbutils.widgets.text("volume", "filings")
catalog, schema, volume = (dbutils.widgets.get(w) for w in ("catalog", "schema", "volume"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
print(f"volume path: /Volumes/{catalog}/{schema}/{volume}")

# COMMAND ----------

reference_dir = f"/Volumes/{catalog}/{schema}/{volume}/reference"
dbutils.fs.mkdirs(reference_dir)
source = os.path.abspath(os.path.join(os.getcwd(), "..", "tests", "fixtures", "june_2026_published_snapshot.csv"))
dbutils.fs.cp(f"file:{source}", f"{reference_dir}/june_2026_published_snapshot.csv")
print(f"copied the published-snapshot reference to {reference_dir}/june_2026_published_snapshot.csv")
