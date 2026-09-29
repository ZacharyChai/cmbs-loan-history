# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Silver
# MAGIC Types the bronze rows, keeps real loans (assetNumber such as `12` or `7A`; hyphenated
# MAGIC or dotted numbers are property children), and MERGEs on
# MAGIC `(deal_id, loan_id, reporting_period_end)`. A re-filed or amended month replaces the
# MAGIC original; an older filing never overwrites a newer one. Delta CHECK constraints reject
# MAGIC rows with a missing key or a negative balance.

# COMMAND ----------

import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
import pipeline

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cmbs")
catalog, schema = (dbutils.widgets.get(w) for w in ("catalog", "schema"))
bronze_table = f"{catalog}.{schema}.bronze_absee_assets"
silver_table = f"{catalog}.{schema}.silver_loan_month"

# COMMAND ----------

silver = pipeline.build_silver(spark.table(bronze_table))
counts = pipeline.merge_silver(spark, silver, silver_table)
print(counts)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {silver_table}").select("version", "timestamp", "operation", "operationMetrics"))
