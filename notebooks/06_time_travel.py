# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Time travel
# MAGIC `silver_loan_month` is queried at the version before the latest month's MERGE, and
# MAGIC again at the current version, using `VERSION AS OF`. The difference is exactly the
# MAGIC rows that MERGE inserted or updated in the most recent run of notebook 03.
# MAGIC
# MAGIC This is not a substitute for running the pipeline monthly. It shows what one Delta
# MAGIC table already holds after that pipeline has run a few times.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cmbs")
catalog, schema = (dbutils.widgets.get(w) for w in ("catalog", "schema"))
table = f"{catalog}.{schema}.silver_loan_month"

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {table}").select(
    "version", "timestamp", "operation", "operationMetrics"
)
display(history)

# COMMAND ----------

latest_version = spark.sql(f"DESCRIBE HISTORY {table}").selectExpr("max(version)").first()[0]
if latest_version == 0:
    print("Only one version exists so far; run the pipeline again on a later month "
          "to see a before/after comparison.")
else:
    before = spark.sql(f"SELECT * FROM {table} VERSION AS OF {latest_version - 1}")
    after = spark.table(table)
    print(f"version {latest_version - 1}: {before.count()} rows")
    print(f"version {latest_version} (current): {after.count()} rows")
    new_or_changed = after.exceptAll(before)
    print(f"rows inserted or updated by the latest run: {new_or_changed.count()}")
    display(new_or_changed.select("deal_id", "loan_id", "reporting_period_end", "accession",
                                   "current_balance").orderBy("reporting_period_end", "loan_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta time travel vs. ALFRED vintages
# MAGIC
# MAGIC Both let you ask what was known as of some point, but they answer a different
# MAGIC question. `VERSION AS OF` in this table answers "what did **this table** hold right
# MAGIC after a given pipeline run" -- it is this project's own write history, and a bug fixed
# MAGIC today rewrites every future version going forward, not the past ones. An ALFRED
# MAGIC vintage in `bridge-pipeline` answers "what did **the publisher** report as of a given
# MAGIC real-world date", which is a fact about the source series itself and does not change
# MAGIC no matter how many times the pipeline that stores it is rerun.
