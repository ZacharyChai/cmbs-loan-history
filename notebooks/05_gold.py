# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Gold
# MAGIC Two gold tables, both keyed by `(deal_id, reporting_period_end)`:
# MAGIC
# MAGIC - `gold_monthly_panel`: one row per property type, with active balance, defeased and
# MAGIC   paid-off loan counts, DSCR distribution, occupancy, and delinquency status. Answers
# MAGIC   when the pool moved and which property types drove it.
# MAGIC - `gold_pool_summary`: the whole-pool roll-up for the same grain, without a property
# MAGIC   type split, so the headline number does not need summing the panel.
# MAGIC
# MAGIC Delinquency buckets follow the standard CREFC payment status codes; see `src/gold.py`
# MAGIC for the caveat on where that mapping comes from.
# MAGIC
# MAGIC Overwrites both tables in full each run: rebuilding a few thousand rows from silver
# MAGIC costs nothing, and it means a fixed bug in gold.py is reflected everywhere immediately,
# MAGIC with no incremental state of its own to go stale.

# COMMAND ----------

import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
import gold

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cmbs")
catalog, schema = (dbutils.widgets.get(w) for w in ("catalog", "schema"))
t = lambda name: f"{catalog}.{schema}.{name}"

# COMMAND ----------

silver = spark.table(t("silver_loan_month"))
panel = gold.build_gold_panel(silver)
panel.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_monthly_panel"))

summary = gold.build_gold_pool_summary(silver)
summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_pool_summary"))

print(f"gold_monthly_panel: {panel.count()} rows; gold_pool_summary: {summary.count()} rows")

# COMMAND ----------

display(spark.sql(f"""
    SELECT reporting_period_end, deal_id, active_loan_count, active_balance, dscr_avg
    FROM {t('gold_pool_summary')}
    ORDER BY reporting_period_end DESC, deal_id
    LIMIT 12
"""))
