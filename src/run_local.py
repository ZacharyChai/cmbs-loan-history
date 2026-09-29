"""Run bronze and silver locally on the cached filings, then print what came out.

    python src/run_local.py --june     # only the two June 2026 tie-out filings
    python src/run_local.py            # every cached filing

This is how the pipeline is checked before it goes to Databricks. Tables are written to
a temporary local warehouse and deleted afterwards.
"""

import argparse
import csv
import os
import shutil
import sys
import tempfile

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import checks  # noqa: E402
import gold  # noqa: E402
import pipeline  # noqa: E402
from config import DEALS  # noqa: E402
from local_spark import get_spark  # noqa: E402

MANIFEST = "data/cache/filing_manifest.csv"


def load_manifest_rows(june_only: bool):
    rows = list(csv.DictReader(open(MANIFEST)))
    if june_only:
        tie = {d["tie_out_accession"] for d in DEALS}
        rows = [r for r in rows if r["accession"] in tie]
    for r in rows:
        r["path"] = "file:" + os.path.abspath(r["local_path"])
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--june", action="store_true")
    args = ap.parse_args()

    wh = tempfile.mkdtemp(prefix="cmbs_local_")
    spark = get_spark(wh)
    try:
        rows = load_manifest_rows(args.june)
        manifest = spark.createDataFrame(
            [(r["accession"], r["form"], r["filing_date"]) for r in rows],
            "accession string, form string, filing_date string",
        )
        raw = pipeline.read_asset_xml(spark, [r["path"] for r in rows])
        bronze = pipeline.to_bronze(spark, raw, manifest)
        pipeline.append_bronze(bronze, "bronze_absee_assets")
        print("bronze rows:", spark.table("bronze_absee_assets").count())
        silver = pipeline.build_silver(spark.table("bronze_absee_assets"))
        counts = pipeline.merge_silver(spark, silver, "silver_loan_month")
        print("silver merge:", counts)
        s = spark.table("silver_loan_month")
        results = checks.run_checks(
            spark, "bronze_absee_assets", "silver_loan_month",
            os.path.abspath("tests/fixtures/june_2026_published_snapshot.csv"),
            [d["tie_out_accession"] for d in DEALS],
        )
        for r in results:
            print(r.line())
        cont = checks.continuity_report(s)
        print("continuity diagnostic:", cont.count(), "consecutive-month breaks across",
              cont.select("deal_id", "loan_id").distinct().count(), "loans")
        panel = gold.build_gold_panel(s)
        panel.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_monthly_panel")
        print("gold panel rows:", panel.count(), "columns:", len(panel.columns))
        panel.filter("reporting_period_end = date'2026-06-08'").orderBy(F.desc("active_balance")).show(10, truncate=False)
        summary = gold.build_gold_pool_summary(s)
        print("--- pool summary, last 6 months ---")
        summary.orderBy(F.desc("reporting_period_end")).limit(6).orderBy("reporting_period_end").show(truncate=False)
        print("silver rows:", s.count(), "| loans in latest month:")
        s.filter("reporting_period_end = date'2026-06-08'").groupBy("deal_id").count().show()
        s.filter("reporting_period_end = date'2026-06-08' and loan_status='active' and not is_defeased") \
            .selectExpr("count(*) as active_re", "round(sum(current_balance),2) as balance").show()
    finally:
        spark.stop()
        shutil.rmtree(wh, ignore_errors=True)


if __name__ == "__main__":
    main()
