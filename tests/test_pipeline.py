"""Bronze and silver rules on small hand-built filings."""

import pytest

from conftest import load, loan

GS6 = "1704459"
ACC1 = "0001111111-26-000001"
ACC2 = "0001111111-26-000002"
ACC3 = "0001111111-26-000003"


def rows(spark, table):
    return {(r.loan_id): r for r in spark.table(table).collect()}


def test_dotted_and_hyphenated_children_are_not_loans(spark, tables):
    b, s = tables
    kids = [
        loan("2", property={"propertyTypeCode": ""}),
        loan("2-001", property={"propertyTypeCode": "OF"}),
        loan("2.02", property={"propertyTypeCode": "OF"}),
        loan("7A"),
        loan("1"),
    ]
    load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", kids)])
    assert set(rows(spark, s)) == {"1", "2", "7A"}
    assert spark.table(b).count() == 5  # bronze keeps every <assets> record


def test_parent_property_type_comes_from_children(spark, tables):
    b, s = tables
    assets = [
        loan("2", property={"propertyTypeCode": ""}),
        loan("2-001", property={"propertyTypeCode": "OF"}),
        loan("2-002", property={"propertyTypeCode": "OF"}),
        loan("3", property={"propertyTypeCode": ""}),
        loan("3-001", property={"propertyTypeCode": "OF"}),
        loan("3-002", property={"propertyTypeCode": "RT"}),
    ]
    load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", assets)])
    r = rows(spark, s)
    assert r["2"].property_type == "Office"
    assert r["3"].property_type == "Mixed Use"  # children disagree


@pytest.mark.parametrize("prop, code", [
    ({"propertyTypeCode": "SE"}, "SE"),
    ({"propertyTypeCode": "", "propertyName": "Defeased"}, "SE"),
    # Flagged defeased by status code but carrying a real property code: the reference rule
    # in cre-credit-risk normalizes only a blank or SE code, so the code stays. The
    # defeasance flag, not the property type, is what identifies these loans.
    ({"propertyTypeCode": "OF", "DefeasedStatusCode": "F"}, "OF"),
    ({"propertyTypeCode": "OF", "DefeasedStatusCode": "P"}, "OF"),
])
def test_defeasance_is_detected_three_ways(spark, tables, prop, code):
    b, s = tables
    load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", [loan("1", property=prop)])])
    r = rows(spark, s)["1"]
    assert r.is_defeased is True
    assert r.property_type_code == code


def test_blank_and_malformed_values_become_null_without_raising(spark, tables):
    """Spark runs with ANSI mode on here, as on Databricks serverless, where a bad cast or
    date parse raises instead of returning null."""
    b, s = tables
    bad = loan(
        "1", maturityDate="13-45-2026", originationDate="", originalLoanAmount="n/a",
        realizedLossToTrustAmount="", reportPeriodEndActualBalanceAmount="",
    )
    load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", [bad])])
    r = rows(spark, s)["1"]
    assert r.maturity_date is None and r.origination_date is None
    assert r.original_balance is None and r.realized_loss_cumulative is None
    assert r.current_balance == 90.0  # falls back from actual to scheduled ending balance


def test_balance_and_dscr_fallback_chains_and_status(spark, tables):
    b, s = tables
    assets = [
        loan("1", property={"propertyTypeCode": "OF",
                            "debtServiceCoverageNetCashFlowSecuritizationPercentage": "1.40",
                            "valuationSecuritizationAmount": "200.00"}),
        loan("2", reportPeriodEndActualBalanceAmount="", reportPeriodEndScheduledLoanBalanceAmount="0.00",
             scheduledPrincipalBalanceSecuritizationAmount="", property={"propertyTypeCode": "RT"}),
    ]
    load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", assets)])
    r = rows(spark, s)
    assert r["1"].dscr == 1.40 and r["1"].dscr_basis == "securitization_ncf"
    assert r["1"].ltv == 0.45 and r["1"].coupon_pct == 5.0
    assert r["1"].loan_status == "active"
    assert r["2"].loan_status == "paid_off" and r["2"].dscr is None and r["2"].dscr_basis == "none"


def test_merge_amendment_replaces_the_original(spark, tables):
    b, s = tables
    original = [loan("1", reportPeriodEndScheduledLoanBalanceAmount="90.00")]
    amended = [loan("1", reportPeriodEndScheduledLoanBalanceAmount="85.00",
                    reportPeriodEndActualBalanceAmount="85.00", scheduledPrincipalAmount="15.00")]
    first = load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", original)])
    second = load(spark, b, s, [(GS6, ACC2, "ABS-EE/A", "2026-06-10", amended)])
    assert first == {"inserted": 1, "updated": 0}
    assert second == {"inserted": 0, "updated": 1}
    r = rows(spark, s)["1"]
    assert spark.table(s).count() == 1
    assert (r.current_balance, r.accession, r.form) == (85.0, ACC2, "ABS-EE/A")
    assert spark.table(b).count() == 2  # bronze keeps both filings


def test_merge_rerun_changes_nothing(spark, tables):
    b, s = tables
    filing = (GS6, ACC1, "ABS-EE", "2026-05-20", [loan("1")])
    load(spark, b, s, [filing])
    again = load(spark, b, s, [filing])
    assert again == {"inserted": 0, "updated": 0}
    assert spark.table(s).count() == 1


def test_merge_older_filing_does_not_overwrite_a_newer_one(spark, tables):
    b, s = tables
    newer = [loan("1", reportPeriodEndScheduledLoanBalanceAmount="85.00", reportPeriodEndActualBalanceAmount="85.00")]
    older = [loan("1")]
    load(spark, b, s, [(GS6, ACC2, "ABS-EE/A", "2026-06-10", newer)])
    result = load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", older)])
    assert result == {"inserted": 0, "updated": 0}
    assert rows(spark, s)["1"].accession == ACC2


def test_two_filings_of_one_month_in_a_single_batch_keep_the_later(spark, tables):
    """MERGE fails if the source has two rows for one key, so the batch is deduplicated."""
    b, s = tables
    original = [loan("1")]
    amended = [loan("1", reportPeriodEndScheduledLoanBalanceAmount="85.00", reportPeriodEndActualBalanceAmount="85.00")]
    load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", original),
                       (GS6, ACC2, "ABS-EE/A", "2026-06-10", amended)])
    r = rows(spark, s)["1"]
    assert spark.table(s).count() == 1 and r.accession == ACC2


def test_a_new_month_is_added_next_to_the_old_one(spark, tables):
    b, s = tables
    load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", [loan("1")])])
    result = load(spark, b, s, [(GS6, ACC3, "ABS-EE", "2026-06-20", [loan("1", end="06-06-2026", begin="05-06-2026")])])
    assert result == {"inserted": 1, "updated": 0}
    assert spark.table(s).count() == 2


def test_check_constraints_reject_bad_rows(spark, tables):
    b, s = tables
    load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", [loan("1")])])
    negative = [loan("1", end="06-06-2026", begin="05-06-2026",
                     reportPeriodEndActualBalanceAmount="-5.00", reportPeriodEndScheduledLoanBalanceAmount="-5.00")]
    with pytest.raises(Exception, match="(?i)constraint|violat"):
        load(spark, b, s, [(GS6, ACC2, "ABS-EE", "2026-06-20", negative)])
    assert spark.table(s).count() == 1


def test_new_manifest_rows_skips_what_bronze_already_holds(spark, tables):
    import pipeline
    b, s = tables
    load(spark, b, s, [(GS6, ACC1, "ABS-EE", "2026-05-20", [loan("1")])])
    manifest = spark.createDataFrame(
        [(ACC1, "ABS-EE", "2026-05-20"), (ACC2, "ABS-EE", "2026-06-20")],
        "accession string, form string, filing_date string",
    )
    todo = [r.accession for r in pipeline.new_manifest_rows(spark, manifest, b).collect()]
    assert todo == [ACC2]
    assert pipeline.new_manifest_rows(spark, manifest, "no_such_table").count() == 2
