"""Data checks on the silver table. Any failed check fails the job.

    1. duplicate_keys      no two silver rows share (deal_id, loan_id, reporting_period_end)
    2. rollforward         per loan and month, beginning balance minus scheduled principal,
                           unscheduled principal, other principal adjustment and the loss
                           newly realized that month equals the ending scheduled balance
    3. tie-out             the June 2026 filings reproduce the published cre-credit-risk
                           snapshot: 71 asset records, 66 loans, 49 active real estate,
                           the same balance, and the same status, defeasance flag and
                           property type on every loan

continuity_report() is a diagnostic, not a check. The filings themselves report a
beginning balance that does not match the prior month's ending balance for some loans;
see the README. It is written out so the size of the problem is visible, not hidden.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Balances are in cents, so a residual larger than one cent is not rounding.
ROLLFORWARD_TOLERANCE = 0.01

# The published snapshot counts, from the cre-credit-risk README and loans.db.
EXPECTED_JUNE = {"asset_records": 71, "loans": 66, "active_real_estate": 49}
BALANCE_TOLERANCE = 0.01


@dataclass
class Result:
    name: str
    passed: bool
    detail: str

    def line(self) -> str:
        return f"{'PASS' if self.passed else 'FAIL'}  {self.name}: {self.detail}"


class CheckFailure(Exception):
    pass


# --- 1. duplicate keys ------------------------------------------------------------
def duplicate_keys(silver: DataFrame) -> Result:
    dupes = (
        silver.groupBy("deal_id", "loan_id", "reporting_period_end").count().filter("count > 1")
    )
    n = dupes.count()
    return Result("duplicate_keys", n == 0, f"{n} keys appear more than once, {silver.count()} rows")


# --- 2. balance roll-forward ------------------------------------------------------
def with_realized_loss_increment(silver: DataFrame) -> DataFrame:
    """The filings report realized loss as a running total that repeats every month after
    the loan liquidates. Only the change since the prior month belongs in that month's
    roll-forward. A loan's first row has no prior month to difference against, so its
    increment is zero."""
    w = Window.partitionBy("deal_id", "loan_id").orderBy("reporting_period_end")
    cum = F.coalesce(F.col("realized_loss_cumulative"), F.lit(0.0))
    return silver.withColumn("realized_loss_increment", F.coalesce(cum - F.lag(cum).over(w), F.lit(0.0)))


def rollforward_residuals(silver: DataFrame) -> DataFrame:
    s = with_realized_loss_increment(silver)
    zero = lambda c: F.coalesce(F.col(c), F.lit(0.0))  # noqa: E731
    return s.withColumn(
        "residual",
        F.col("beginning_balance")
        - zero("scheduled_principal")
        - zero("unscheduled_principal")
        - zero("other_principal_adjustment")
        - F.col("realized_loss_increment")
        - F.col("end_scheduled_balance"),
    )


def rollforward(silver: DataFrame, tolerance: float = ROLLFORWARD_TOLERANCE) -> Result:
    r = rollforward_residuals(silver)
    missing = r.filter(F.col("beginning_balance").isNull() | F.col("end_scheduled_balance").isNull()).count()
    testable = r.filter(F.col("beginning_balance").isNotNull() & F.col("end_scheduled_balance").isNotNull())
    # Amounts are in cents; rounding to cents keeps float noise (100 - 10 - 89.99 is
    # 0.010000000000005 in binary) from failing a residual of exactly one cent.
    testable = testable.withColumn("residual", F.round("residual", 2))
    bad = testable.filter(F.abs("residual") > tolerance)
    n_bad = bad.count()
    n = testable.count()
    worst = testable.agg(F.max(F.abs("residual"))).first()[0] or 0.0
    detail = (
        f"{n_bad} of {n} loan-months outside ${tolerance:.2f}, largest residual ${worst:,.2f}; "
        f"{missing} rows lack a beginning or ending balance"
    )
    return Result("rollforward", n_bad == 0 and missing == 0, detail)


# --- diagnostic: month-to-month continuity ----------------------------------------
def continuity_report(silver: DataFrame, tolerance: float = 1.0) -> DataFrame:
    """Consecutive-month pairs where this month's beginning balance is not last month's
    ending balance. Not a pass/fail check: the mismatch is in the filings."""
    w = Window.partitionBy("deal_id", "loan_id").orderBy("reporting_period_end")
    month = F.trunc("reporting_period_end", "month")
    s = (
        silver.withColumn("_prev_end", F.lag("end_scheduled_balance").over(w))
        .withColumn("_prev_month", F.lag(month).over(w))
        .withColumn("_prev_accession", F.lag("accession").over(w))
    )
    consecutive = F.months_between(month, F.col("_prev_month")) == 1
    return (
        s.filter(consecutive & F.col("_prev_end").isNotNull() & F.col("beginning_balance").isNotNull())
        .withColumn("gap", F.col("beginning_balance") - F.col("_prev_end"))
        .filter(F.abs("gap") > tolerance)
        .select("deal_id", "loan_id", "reporting_period_end", "_prev_end", "beginning_balance", "gap",
                "original_balance", "loan_structure", "accession")
        .withColumnRenamed("_prev_end", "prior_month_end_balance")
    )


# --- 3. tie-out to the published snapshot -----------------------------------------
def load_published(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", True).csv(path)
        .select(
            "deal_id", "loan_id",
            F.col("current_balance").cast("double").alias("current_balance"),
            "loan_status",
            (F.lower("is_defeased") == "true").alias("is_defeased"),
            "property_type",
        )
    )


def tie_out(
    spark: SparkSession,
    bronze: DataFrame,
    silver: DataFrame,
    published: DataFrame,
    tie_accessions: list[str],
) -> list[Result]:
    results = []
    records = bronze.filter(F.col("accession").isin(tie_accessions)).count()
    results.append(Result(
        "tie_out: asset records", records == EXPECTED_JUNE["asset_records"],
        f"{records} <assets> records in the two June filings, expected {EXPECTED_JUNE['asset_records']}",
    ))

    june = silver.filter(F.col("accession").isin(tie_accessions))
    loans = june.count()
    results.append(Result(
        "tie_out: loans", loans == EXPECTED_JUNE["loans"],
        f"{loans} loans, expected {EXPECTED_JUNE['loans']} "
        f"({records - loans} of the {records} records are property children)",
    ))

    active = june.filter("loan_status = 'active' AND NOT is_defeased")
    n_active = active.count()
    results.append(Result(
        "tie_out: active real estate", n_active == EXPECTED_JUNE["active_real_estate"],
        f"{n_active} active loans excluding defeased and paid off, expected {EXPECTED_JUNE['active_real_estate']}",
    ))

    balance = active.agg(F.sum("current_balance")).first()[0] or 0.0
    pub_active = published.filter("loan_status = 'active' AND NOT is_defeased")
    pub_balance = pub_active.agg(F.sum("current_balance")).first()[0] or 0.0
    results.append(Result(
        "tie_out: active balance", abs(balance - pub_balance) <= BALANCE_TOLERANCE,
        f"${balance:,.2f} against published ${pub_balance:,.2f}",
    ))

    j = (
        june.alias("s").join(published.alias("p"), ["deal_id", "loan_id"], "full_outer")
        .select(
            "deal_id", "loan_id",
            F.col("s.current_balance").alias("s_bal"), F.col("p.current_balance").alias("p_bal"),
            F.col("s.loan_status").alias("s_status"), F.col("p.loan_status").alias("p_status"),
            F.col("s.is_defeased").alias("s_def"), F.col("p.is_defeased").alias("p_def"),
            F.col("s.property_type").alias("s_type"), F.col("p.property_type").alias("p_type"),
        )
    )
    differs = j.filter(
        F.col("s_status").isNull() | F.col("p_status").isNull()
        | (F.col("s_status") != F.col("p_status"))
        | (F.col("s_def") != F.col("p_def"))
        | (F.col("s_type") != F.col("p_type"))
        | (F.abs(F.coalesce(F.col("s_bal"), F.lit(0.0)) - F.coalesce(F.col("p_bal"), F.lit(0.0))) > BALANCE_TOLERANCE)
    )
    n_diff = differs.count()
    sample = ", ".join(f"{r.deal_id}/{r.loan_id}" for r in differs.limit(5).collect())
    results.append(Result(
        "tie_out: per loan", n_diff == 0,
        f"{n_diff} loans differ from the published snapshot in balance, status, defeasance or property type"
        + (f" (e.g. {sample})" if n_diff else ""),
    ))
    return results


def run_checks(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    published_path: str,
    tie_accessions: list[str],
) -> list[Result]:
    bronze = spark.table(bronze_table)
    silver = spark.table(silver_table)
    published = load_published(spark, published_path)
    results = [duplicate_keys(silver), rollforward(silver)]
    results += tie_out(spark, bronze, silver, published, tie_accessions)
    return results


def raise_on_failure(results: list[Result]) -> None:
    for r in results:
        print(r.line())
    failed = [r for r in results if not r.passed]
    if failed:
        raise CheckFailure(f"{len(failed)} of {len(results)} checks failed: "
                           + "; ".join(r.name for r in failed))
