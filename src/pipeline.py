"""Bronze and silver steps for the ABS-EE loan history.

Bronze keeps every <assets> element from every filing as raw strings, with the file it
came from. Silver is typed, holds one row per (deal_id, loan_id, reporting_period_end),
and is written with MERGE so a re-filed or amended month replaces the original.

The parsing rules follow src/parse_absee.py in github.com/ZacharyChai/cre-credit-risk:
the loan-level balance and DSCR fallback chains, the defeasance test, and property type
taken from the first <property> element (or from the children of a multi-property loan).
One rule differs on purpose: a real loan is an assetNumber like "12" or "7A", and
anything with a hyphen or a dot ("4-001", "4.01") is a property child. The 2017-05
GS6 filing numbers its property children with a dot, which the hyphen-only rule counts
as loans.

Everything here uses the DataFrame API and SQL functions that also run on Databricks
serverless: try_cast and try_to_timestamp instead of cast and to_date, because ANSI mode
raises on a malformed value where the plain functions return null.
"""

import os
import sys

from pyspark.errors import AnalysisException, IllegalArgumentException
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DEALS  # noqa: E402
from schema import LOAN_TAGS, bronze_schema  # noqa: E402

KEY = ["deal_id", "loan_id", "reporting_period_end"]

# A loan is digits with an optional trailing letter ("12", "7A"). Children of a
# multi-property loan are "<loan>-<n>" or "<loan>.<n>".
LOAN_NUMBER_REGEX = r"^\d+[A-Za-z]?$"
CHILD_NUMBER_REGEX = r"^(\d+)[-.]\d+$"

CIK_TO_DEAL = {d["cik"]: d["deal_id"] for d in DEALS}

PROPERTY_TYPE = {
    "MF": "Multifamily", "RT": "Retail", "OF": "Office", "IN": "Industrial",
    "WH": "Warehouse", "MU": "Mixed Use", "LO": "Lodging", "SS": "Self Storage",
    "MH": "Manufactured Housing", "HC": "Health Care", "SE": "Defeased (Securities)",
    "98": "Other", "OT": "Other", "NA": "Unknown", "": "Unknown",
}


# --- bronze ---------------------------------------------------------------------
def read_asset_xml(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Read the asset-data XML files, one row per <assets> element, all strings."""
    reader = spark.read.format("xml").schema(bronze_schema()).option("rowTag", "assets")
    try:
        df = reader.load(paths)
    except IllegalArgumentException:
        # The open-source spark-xml package used for local runs takes one comma-separated
        # path. Databricks' built-in reader takes a list, like any file source.
        df = reader.load(",".join(paths))
    return df.withColumn("source_file", _source_file_column(df))


def _source_file_column(df: DataFrame):
    """Databricks exposes the file through _metadata.file_path (input_file_name() is not
    supported on Unity Catalog compute). The plain XML package used for local tests has
    no _metadata column, so fall back to input_file_name() there."""
    try:
        df.select("_metadata.file_path")
        return F.col("_metadata.file_path")
    except AnalysisException:
        return F.input_file_name()


def to_bronze(spark: SparkSession, raw: DataFrame, manifest: DataFrame) -> DataFrame:
    """Attach cik, accession, form and filing date to the raw rows.

    cik and accession come from the file path (.../filings/<cik>/<accession>/<file>.xml),
    form and filing_date from the filing manifest, joined on accession.
    """
    path = r"/filings/(\d+)/(\d{10}-\d{2}-\d{6})/"
    with_keys = (
        raw.withColumn("cik", F.regexp_extract("source_file", path, 1))
        .withColumn("accession", F.regexp_extract("source_file", path, 2))
    )
    meta = manifest.select(
        "accession",
        F.col("form").alias("filing_form"),
        F.col("filing_date").alias("filing_date"),
    ).dropDuplicates(["accession"])
    return (
        with_keys.join(F.broadcast(meta), "accession", "left")
        .withColumn("ingested_at", F.current_timestamp())
    )


def new_manifest_rows(spark: SparkSession, manifest: DataFrame, bronze_table: str) -> DataFrame:
    """Manifest rows whose accession is not in bronze yet, so a re-run appends nothing
    twice and the monthly job only loads the new month."""
    if not spark.catalog.tableExists(bronze_table):
        return manifest
    loaded = spark.table(bronze_table).select("accession").distinct()
    return manifest.join(loaded, "accession", "left_anti")


def append_bronze(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("append").saveAsTable(table)


# --- silver ---------------------------------------------------------------------
def _num(col: str):
    """Double from a raw string column: blank or malformed becomes null."""
    return F.expr(f"try_cast(nullif(trim(`{col}`), '') as double)")


def _date(col: str):
    """Date from EDGAR's MM-DD-YYYY string: blank or malformed becomes null."""
    return F.expr(f"cast(try_to_timestamp(nullif(trim(`{col}`), ''), 'MM-dd-yyyy') as date)")


def _txt(col: str):
    return F.trim(F.coalesce(F.col(col), F.lit("")))


def _flag(col: str):
    return F.lower(_txt(col)) == "true"


def build_silver(bronze: DataFrame) -> DataFrame:
    """Typed loan-month rows from bronze. Children of multi-property loans are used only
    to fill in the parent's property type."""
    p = F.expr("try_element_at(property, 1)")
    base = (
        bronze
        .withColumn("_p", p)
        .withColumn("loan_id", _txt("assetNumber"))
        .withColumn("_is_loan", F.col("loan_id").rlike(LOAN_NUMBER_REGEX))
        .withColumn("_parent", F.regexp_extract("loan_id", CHILD_NUMBER_REGEX, 1))
        .withColumn("_child_ptc", F.nullif(F.trim(F.coalesce(F.col("_p.propertyTypeCode"), F.lit(""))), F.lit("")))
    )

    kids = (
        base.filter((F.col("_parent") != "") & F.col("_child_ptc").isNotNull())
        .groupBy("source_file", F.col("_parent").alias("loan_id"))
        .agg(F.collect_set("_child_ptc").alias("_kid_codes"))
    )

    loans = base.filter("_is_loan").join(kids, ["source_file", "loan_id"], "left")

    flat = "_p."
    ptc_raw = F.trim(F.coalesce(F.col(flat + "propertyTypeCode"), F.lit("")))
    defeased_code = F.trim(F.coalesce(F.col(flat + "DefeasedStatusCode"), F.lit("")))
    prop_name = F.lower(F.trim(F.coalesce(F.col(flat + "propertyName"), F.lit(""))))
    is_defeased = (ptc_raw == "SE") | defeased_code.isin("F", "P") | (prop_name == "defeased")
    kid_n = F.size(F.coalesce(F.col("_kid_codes"), F.array()))
    ptc = (
        F.when(ptc_raw != "", ptc_raw)
        .when(~is_defeased & (kid_n == 1), F.col("_kid_codes")[0])
        .when(~is_defeased & (kid_n > 1), F.lit("MU"))
        .otherwise(ptc_raw)
    )
    ptc = F.when(is_defeased & ptc.isin("", "SE"), F.lit("SE")).otherwise(ptc)

    # Property fields are nested; lift the ones the chains below use into plain columns.
    lifted = loans
    for name in (
        "mostRecentDebtServiceCoverageNetCashFlowpercentage",
        "debtServiceCoverageNetCashFlowSecuritizationPercentage",
        "mostRecentDebtServiceCoverageNetOperatingIncomePercentage",
        "debtServiceCoverageNetOperatingIncomeSecuritizationPercentage",
        "mostRecentNetOperatingIncomeAmount",
        "netOperatingIncomeSecuritizationAmount",
        "mostRecentPhysicalOccupancyPercentage",
        "physicalOccupancySecuritizationPercentage",
        "valuationSecuritizationAmount",
        "valuationSecuritizationDate",
    ):
        lifted = lifted.withColumn("p_" + name, F.col(flat + name))

    dscr_cols = [
        ("p_mostRecentDebtServiceCoverageNetCashFlowpercentage", "most_recent_ncf"),
        ("p_debtServiceCoverageNetCashFlowSecuritizationPercentage", "securitization_ncf"),
        ("p_mostRecentDebtServiceCoverageNetOperatingIncomePercentage", "most_recent_noi"),
        ("p_debtServiceCoverageNetOperatingIncomeSecuritizationPercentage", "securitization_noi"),
    ]
    dscr = F.coalesce(*[_num(c) for c, _ in dscr_cols])
    dscr_basis = F.coalesce(
        *[F.when(_num(c).isNotNull(), F.lit(label)) for c, label in dscr_cols], F.lit("none")
    )

    cur_bal = F.coalesce(
        _num("reportPeriodEndActualBalanceAmount"),
        _num("reportPeriodEndScheduledLoanBalanceAmount"),
        _num("scheduledPrincipalBalanceSecuritizationAmount"),
    )
    noi = F.coalesce(_num("p_mostRecentNetOperatingIncomeAmount"), _num("p_netOperatingIncomeSecuritizationAmount"))
    occ = F.coalesce(_num("p_mostRecentPhysicalOccupancyPercentage"), _num("p_physicalOccupancySecuritizationPercentage"))
    valuation = _num("p_valuationSecuritizationAmount")
    coupon = F.coalesce(_num("reportPeriodInterestRatePercentage"), _num("interestRateSecuritizationPercentage"))

    type_map = F.create_map(*[x for kv in PROPERTY_TYPE.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])

    typed = (
        lifted
        .withColumn("deal_id", F.coalesce(*[F.when(F.col("cik") == k, F.lit(v)) for k, v in CIK_TO_DEAL.items()]))
        .withColumn("reporting_period_begin", _date("reportingPeriodBeginningDate"))
        .withColumn("reporting_period_end", _date("reportingPeriodEndDate"))
        .withColumn("filing_date", F.col("filing_date").cast("date"))
        .withColumn("form", F.col("filing_form"))
        .withColumn("_ptc", ptc)
        .withColumn("current_balance", cur_bal)
        .withColumn("dscr", dscr)
        .withColumn("dscr_basis", dscr_basis)
        .withColumn("noi", noi)
        .withColumn("valuation", valuation)
        .withColumn("is_defeased", is_defeased)
    )

    silver = typed.select(
        "deal_id",
        "loan_id",
        "reporting_period_begin",
        "reporting_period_end",
        "accession",
        "form",
        "filing_date",
        "source_file",
        F.col("primaryServicerName").alias("servicer"),
        F.col("originatorName").alias("originator"),
        _date("originationDate").alias("origination_date"),
        _date("maturityDate").alias("maturity_date"),
        _num("originalLoanAmount").alias("original_balance"),
        _num("reportPeriodBeginningScheduleLoanBalanceAmount").alias("beginning_balance"),
        _num("scheduledPrincipalAmount").alias("scheduled_principal"),
        _num("unscheduledPrincipalCollectedAmount").alias("unscheduled_principal"),
        _num("otherPrincipalAdjustmentAmount").alias("other_principal_adjustment"),
        _num("reportPeriodEndScheduledLoanBalanceAmount").alias("end_scheduled_balance"),
        _num("reportPeriodEndActualBalanceAmount").alias("end_actual_balance"),
        _num("scheduledPrincipalBalanceSecuritizationAmount").alias("securitization_balance"),
        "current_balance",
        _num("realizedLossToTrustAmount").alias("realized_loss_cumulative"),
        F.round(coupon * 100, 4).alias("coupon_pct"),
        _flag("interestOnlyIndicator").alias("io_flag"),
        _flag("balloonIndicator").alias("balloon_flag"),
        F.col("loanStructureCode").alias("loan_structure"),
        F.col("loanStructureCode").isin("PP", "A1").alias("is_pari_passu"),
        F.col("paymentStatusLoanCode").alias("payment_status_code"),
        F.coalesce(_num("NumberProperties").cast("int"), F.lit(1)).alias("num_properties"),
        F.col("_ptc").alias("property_type_code"),
        F.coalesce(
            F.try_element_at(type_map, F.col("_ptc")),
            F.when(F.col("_ptc") != "", F.col("_ptc")),
            F.lit("Unknown"),
        ).alias("property_type"),
        F.col("_p.propertyState").alias("state"),
        F.col("_p.propertyCity").alias("city"),
        F.col("noi"),
        occ.alias("occupancy"),
        F.col("dscr"),
        F.col("dscr_basis"),
        F.col("valuation"),
        _date("p_valuationSecuritizationDate").alias("valuation_date"),
        F.when(F.col("current_balance").isNotNull() & (F.col("current_balance") != 0) & F.col("valuation").isNotNull() & (F.col("valuation") != 0),
               F.round(F.col("current_balance") / F.col("valuation"), 4)).alias("ltv"),
        F.when(F.col("noi").isNotNull() & (F.col("noi") != 0) & F.col("current_balance").isNotNull() & (F.col("current_balance") != 0),
               F.round(F.col("noi") / F.col("current_balance"), 4)).alias("debt_yield"),
        "is_defeased",
        F.when(F.col("current_balance").isNull() | (F.col("current_balance") == 0), F.lit("paid_off"))
        .otherwise(F.lit("active")).alias("loan_status"),
        F.col("liquidationPrepaymentCode").alias("liquidation_prepayment_code"),
    )
    return silver


def dedupe_latest_filing(df: DataFrame) -> DataFrame:
    """One row per key. When a month was filed twice (an amendment, or a re-file), keep
    the later filing. MERGE requires this: a source with two rows for one key fails."""
    w = Window.partitionBy(*KEY).orderBy(F.col("filing_date").desc(), F.col("accession").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# --- silver table: MERGE and constraints ---------------------------------------
SILVER_CONSTRAINTS = {
    "loan_id_present": "loan_id IS NOT NULL AND loan_id <> ''",
    "period_end_present": "reporting_period_end IS NOT NULL",
    "deal_known": "deal_id IN (" + ", ".join(f"'{d['deal_id']}'" for d in DEALS) + ")",
    "balance_not_negative": "current_balance IS NULL OR current_balance >= 0",
}


def merge_silver(spark: SparkSession, silver: DataFrame, table: str) -> dict:
    """Write silver, replacing a month's row when a later filing covers the same key.

    Returns counts: how many rows were inserted, updated, and left as they were.
    """
    from delta.tables import DeltaTable

    src = dedupe_latest_filing(silver)
    if not spark.catalog.tableExists(table):
        src.write.format("delta").saveAsTable(table)
        for name, expr in SILVER_CONSTRAINTS.items():
            spark.sql(f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expr})")
        return {"inserted": src.count(), "updated": 0}

    target = DeltaTable.forName(spark, table)
    on = " AND ".join(f"t.{k} = s.{k}" for k in KEY)
    newer = "s.filing_date > t.filing_date OR (s.filing_date = t.filing_date AND s.accession > t.accession)"
    (
        target.alias("t").merge(src.alias("s"), on)
        .whenMatchedUpdateAll(condition=newer)
        .whenNotMatchedInsertAll()
        .execute()
    )
    m = target.history(1).select("operationMetrics").first()[0]
    return {
        "inserted": int(m.get("numTargetRowsInserted", 0)),
        "updated": int(m.get("numTargetRowsUpdated", 0)),
    }
