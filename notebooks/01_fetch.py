# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Fetch
# MAGIC Lists every ABS-EE and ABS-EE/A filing for both deals from EDGAR and downloads the
# MAGIC asset-data XML into the volume. Files already in the volume are never fetched again.
# MAGIC
# MAGIC Needs outbound internet, which Free Edition allows only after LinkedIn verification.
# MAGIC Without it, run `python src/fetch_history.py` locally and upload with
# MAGIC `python src/stage_upload.py`, then skip this notebook.
# MAGIC
# MAGIC Set `refresh_lists` to `true` when a new month has been filed.

# COMMAND ----------

import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "cmbs")
dbutils.widgets.text("volume", "filings")
dbutils.widgets.dropdown("refresh_lists", "false", ["false", "true"])
catalog, schema, volume = (dbutils.widgets.get(w) for w in ("catalog", "schema", "volume"))

# config.py reads this when it is first imported, so set it before the import below.
os.environ["CMBS_CACHE_DIR"] = f"/Volumes/{catalog}/{schema}/{volume}/cache"

import fetch_history

args = ["--refresh-lists"] if dbutils.widgets.get("refresh_lists") == "true" else []
try:
    fetch_history.main(args)
except SystemExit as e:
    # fetch_history exits non-zero when a filing failed to download. Make that fail the task.
    if e.code:
        raise RuntimeError("Some filings failed to download; see filings_failed.csv in the cache.")
