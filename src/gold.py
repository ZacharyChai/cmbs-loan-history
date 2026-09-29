"""Gold: one monthly panel row per (deal_id, reporting_period_end, property_type).

Answers the question this project exists to answer: how did the pool reach 44.6 percent
Elevated or Acute, when did loans pay off or defease, and when did DSCR slide.

Delinquency buckets follow the standard CREFC Investor Reporting Package loan payment
status codes (0 current, 1 to 4 the 30/60/90/120-day bands, 5 performing matured
balloon, A and B current-but-late). These are the codes used industry-wide, not
something this filing's own schema defines, so verify against a servicer's IRP glossary
before relying on the bucket boundaries for anything beyond this panel.
"""

import os
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DELINQUENCY_BUCKET = {
    "0": "current", "A": "current (grace period)", "B": "current (late, in grace)",
    "1": "30-59 days delinquent", "2": "60-89 days delinquent",
    "3": "90-119 days delinquent", "4": "120+ days delinquent",
    "5": "performing matured balloon",
}


def build_gold_panel(silver: DataFrame) -> DataFrame:
    delinq_map = F.create_map(*[x for kv in DELINQUENCY_BUCKET.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    s = silver.withColumn(
        "delinquency_bucket",
        F.coalesce(F.try_element_at(delinq_map, "payment_status_code"), F.lit("unknown / blank")),
    )
    active = s.filter("loan_status = 'active' AND NOT is_defeased")

    counts = s.groupBy("deal_id", "reporting_period_end", "property_type").agg(
        F.count("*").alias("loan_count"),
        F.sum(F.when(F.col("loan_status") == "active", F.col("current_balance")).otherwise(0.0))
        .alias("_all_active_balance"),  # includes defeased; active_balance below excludes them
        F.sum(F.when((F.col("loan_status") == "active") & ~F.col("is_defeased"), F.col("current_balance")).otherwise(0.0))
        .alias("active_balance"),
        F.sum(F.when((F.col("loan_status") == "active") & ~F.col("is_defeased"), 1).otherwise(0)).alias("active_loan_count"),
        F.sum(F.when(F.col("is_defeased") & (F.col("loan_status") == "active"), 1).otherwise(0)).alias("defeased_loan_count"),
        F.sum(F.when(F.col("is_defeased") & (F.col("loan_status") == "active"), F.col("current_balance")).otherwise(0.0))
        .alias("defeased_balance"),
        F.sum(F.when(F.col("loan_status") == "paid_off", 1).otherwise(0)).alias("paid_off_loan_count"),
    ).drop("_all_active_balance")

    dscr = active.groupBy("deal_id", "reporting_period_end", "property_type").agg(
        F.round(F.avg("dscr"), 3).alias("dscr_avg"),
        F.round(F.expr("percentile_approx(dscr, 0.5)"), 3).alias("dscr_median"),
        F.round(F.min("dscr"), 3).alias("dscr_min"),
        F.round(F.max("dscr"), 3).alias("dscr_max"),
        F.sum(F.when(F.col("dscr").isNull(), 1).otherwise(0)).alias("dscr_missing_count"),
        F.round(F.avg("occupancy"), 4).alias("occupancy_avg"),
        F.round(F.expr("percentile_approx(occupancy, 0.5)"), 4).alias("occupancy_median"),
    )

    delinq = (
        active.groupBy("deal_id", "reporting_period_end", "property_type")
        .pivot("delinquency_bucket", list(dict.fromkeys(DELINQUENCY_BUCKET.values())) + ["unknown / blank"])
        .count()
        .na.fill(0)
    )
    delinq_cols = [c for c in delinq.columns if c not in ("deal_id", "reporting_period_end", "property_type")]
    for c in delinq_cols:
        delinq = delinq.withColumnRenamed(c, "delinquent_count_" + c.replace(" ", "_").replace("(", "").replace(")", "").replace(",", "").replace("+", "plus").replace("/", "or"))

    panel = (
        counts.join(dscr, ["deal_id", "reporting_period_end", "property_type"], "left")
        .join(delinq, ["deal_id", "reporting_period_end", "property_type"], "left")
    )
    for c in panel.columns:
        if c.startswith("delinquent_count_"):
            panel = panel.na.fill({c: 0})
    return panel.orderBy("deal_id", "reporting_period_end", "property_type")


def build_gold_pool_summary(silver: DataFrame) -> DataFrame:
    """One row per (deal_id, reporting_period_end): the whole-pool numbers the property-type
    panel rolls up from, so a reader does not have to sum the panel to get the headline."""
    active = silver.filter("loan_status = 'active' AND NOT is_defeased")
    return (
        silver.groupBy("deal_id", "reporting_period_end")
        .agg(F.count("*").alias("loan_count"))
        .join(
            active.groupBy("deal_id", "reporting_period_end").agg(
                F.count("*").alias("active_loan_count"),
                F.sum("current_balance").alias("active_balance"),
                F.round(F.avg("dscr"), 3).alias("dscr_avg"),
            ),
            ["deal_id", "reporting_period_end"], "left",
        )
        .na.fill({"active_loan_count": 0, "active_balance": 0.0})
        .orderBy("deal_id", "reporting_period_end")
    )
