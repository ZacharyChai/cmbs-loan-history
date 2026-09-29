# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze
# MAGIC Reads the asset-data XML with Spark (one row per `<assets>` element) and appends it to
# MAGIC `bronze_absee_assets` as raw strings, with the source file, CIK, accession, form and
# MAGIC filing date. Only filings that are not in the table yet are read, so a re-run appends
# MAGIC nothing twice and the monthly job loads just the new month.

# COMMAND ----------

import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
import pipeline

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cmbs")
dbutils.widgets.text("volume", "filings")
catalog, schema, volume = (dbutils.widgets.get(w) for w in ("catalog", "schema", "volume"))
cache = f"/Volumes/{catalog}/{schema}/{volume}/cache"
bronze_table = f"{catalog}.{schema}.bronze_absee_assets"

# COMMAND ----------

manifest = spark.read.option("header", True).csv(f"{cache}/filing_manifest.csv")
todo = pipeline.new_manifest_rows(spark, manifest, bronze_table)
rows = todo.select("cik", "accession", "xml_file").collect()
print(f"{manifest.count()} filings in the manifest, {len(rows)} not in bronze yet")

# COMMAND ----------

if rows:
    paths = [f"{cache}/filings/{r.cik}/{r.accession}/{r.xml_file}" for r in rows]
    raw = pipeline.read_asset_xml(spark, paths)
    bronze = pipeline.to_bronze(spark, raw, todo)
    pipeline.append_bronze(bronze, bronze_table)

# COMMAND ----------

display(spark.sql(f"""
    SELECT cik, count(DISTINCT accession) AS filings, count(*) AS asset_records
    FROM {bronze_table} GROUP BY cik ORDER BY cik
"""))
