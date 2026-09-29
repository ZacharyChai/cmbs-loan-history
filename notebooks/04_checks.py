# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Checks
# MAGIC Fails the job on: duplicate keys; a balance roll-forward outside one cent; any
# MAGIC difference from the published June 2026 snapshot (71 asset records, 66 loans, 49 active
# MAGIC real estate, same balance, same status, defeasance flag and property type per loan).
# MAGIC Results are appended to `dq_check_results`. The month-to-month continuity diagnostic
# MAGIC is written to `dq_balance_continuity` and does not fail the job.

# COMMAND ----------

import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
import checks
from config import DEALS
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cmbs")
dbutils.widgets.text("volume", "filings")
catalog, schema, volume = (dbutils.widgets.get(w) for w in ("catalog", "schema", "volume"))
t = lambda name: f"{catalog}.{schema}.{name}"
published = f"/Volumes/{catalog}/{schema}/{volume}/reference/june_2026_published_snapshot.csv"

# COMMAND ----------

results = checks.run_checks(
    spark, t("bronze_absee_assets"), t("silver_loan_month"), published,
    [d["tie_out_accession"] for d in DEALS],
)
(
    spark.createDataFrame([(r.name, r.passed, r.detail) for r in results], "check string, passed boolean, detail string")
    .withColumn("run_at", F.current_timestamp())
    .write.format("delta").mode("append").saveAsTable(t("dq_check_results"))
)

# COMMAND ----------

continuity = checks.continuity_report(spark.table(t("silver_loan_month")))
continuity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("dq_balance_continuity"))
print(f"continuity diagnostic: {continuity.count()} consecutive-month breaks (not a failing check)")

# COMMAND ----------

# Raises, and so fails the job task, if any check failed.
checks.raise_on_failure(results)
