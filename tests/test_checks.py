"""Each check must pass on good data and fail, naming the problem, on bad data."""

from datetime import date

import pytest

import checks

SILVER_SCHEMA = (
    "deal_id string, loan_id string, reporting_period_end date, accession string, "
    "beginning_balance double, scheduled_principal double, unscheduled_principal double, "
    "other_principal_adjustment double, end_scheduled_balance double, "
    "realized_loss_cumulative double, original_balance double, loan_structure string"
)


def silver(spark, rows):
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def row(loan, period, begin, sched, unsched, end, loss=None, acc="A1", deal="D1"):
    return (deal, loan, period, acc, begin, sched, unsched, None, end, loss, 100.0, "WL")


def test_duplicate_keys_passes_when_keys_are_unique(spark):
    df = silver(spark, [row("1", date(2026, 5, 6), 100.0, 10.0, 0.0, 90.0),
                        row("1", date(2026, 6, 6), 90.0, 10.0, 0.0, 80.0)])
    assert checks.duplicate_keys(df).passed


def test_duplicate_keys_fails_on_a_repeated_key(spark):
    df = silver(spark, [row("1", date(2026, 5, 6), 100.0, 10.0, 0.0, 90.0, acc="A1"),
                        row("1", date(2026, 5, 6), 100.0, 10.0, 0.0, 90.0, acc="A2")])
    result = checks.duplicate_keys(df)
    assert not result.passed and "1 keys" in result.detail


def test_rollforward_passes_when_balances_reconcile(spark):
    df = silver(spark, [row("1", date(2026, 5, 6), 100.0, 10.0, 0.0, 90.0),
                        row("2", date(2026, 5, 6), 50.0, 0.5, 49.5, 0.0)])
    assert checks.rollforward(df).passed


def test_rollforward_fails_when_a_balance_does_not_reconcile(spark):
    df = silver(spark, [row("1", date(2026, 5, 6), 100.0, 10.0, 0.0, 90.0),
                        row("2", date(2026, 5, 6), 100.0, 10.0, 0.0, 85.0)])
    result = checks.rollforward(df)
    assert not result.passed
    assert "1 of 2 loan-months" in result.detail and "$5.00" in result.detail


def test_rollforward_allows_one_cent_of_rounding_but_not_two(spark):
    ok = silver(spark, [row("1", date(2026, 5, 6), 100.00, 10.00, 0.0, 89.99)])
    bad = silver(spark, [row("1", date(2026, 5, 6), 100.00, 10.00, 0.0, 89.98)])
    assert checks.rollforward(ok).passed
    assert not checks.rollforward(bad).passed


def test_rollforward_fails_when_a_balance_is_missing_instead_of_skipping_it(spark):
    df = silver(spark, [row("1", date(2026, 5, 6), None, 10.0, 0.0, 90.0)])
    result = checks.rollforward(df)
    assert not result.passed and "1 rows lack" in result.detail


def test_realized_loss_counts_once_even_though_it_repeats_every_month(spark):
    """Loan pays off 30 of 100 with a 20 loss in May; the running loss stays 20 in June."""
    df = silver(spark, [
        row("1", date(2026, 4, 6), 100.0, 0.0, 0.0, 100.0, loss=None),
        row("1", date(2026, 5, 6), 100.0, 10.0, 70.0, 0.0, loss=20.0),
        row("1", date(2026, 6, 6), 0.0, None, None, 0.0, loss=20.0),
    ])
    assert checks.rollforward(df).passed


def test_a_loans_first_row_has_no_loss_increment(spark):
    df = silver(spark, [row("1", date(2026, 6, 6), 0.0, None, None, 0.0, loss=20.0)])
    assert checks.rollforward(df).passed


def test_continuity_flags_consecutive_months_only(spark):
    df = silver(spark, [
        row("1", date(2026, 3, 6), 100.0, 0.0, 0.0, 100.0),
        row("1", date(2026, 4, 6), 70.0, 0.0, 0.0, 70.0),   # begins 30 below March's end: flagged
        row("1", date(2026, 6, 6), 40.0, 0.0, 0.0, 40.0),   # May is missing: not compared
        row("2", date(2026, 3, 6), 100.0, 0.0, 0.0, 100.0),
        row("2", date(2026, 4, 6), 100.0, 0.0, 0.0, 100.0),  # chains cleanly
    ])
    flagged = checks.continuity_report(df).collect()
    assert [(r.loan_id, r.gap) for r in flagged] == [("1", -30.0)]


# --- tie-out ---------------------------------------------------------------------
@pytest.fixture
def small_expected(monkeypatch):
    monkeypatch.setitem(checks.EXPECTED_JUNE, "asset_records", 3)
    monkeypatch.setitem(checks.EXPECTED_JUNE, "loans", 2)
    monkeypatch.setitem(checks.EXPECTED_JUNE, "active_real_estate", 1)


def tie_frames(spark, published_type="Office", silver_balance=90.0):
    bronze = spark.createDataFrame([("T1",), ("T1",), ("T1",), ("OTHER",)], "accession string")
    s = spark.createDataFrame(
        [("D1", "1", "T1", silver_balance, "active", False, "Office"),
         ("D1", "2", "T1", 0.0, "paid_off", False, "Retail"),
         ("D1", "9", "OTHER", 5.0, "active", False, "Office")],
        "deal_id string, loan_id string, accession string, current_balance double, "
        "loan_status string, is_defeased boolean, property_type string",
    )
    p = spark.createDataFrame(
        [("D1", "1", 90.0, "active", False, published_type),
         ("D1", "2", 0.0, "paid_off", False, "Retail")],
        "deal_id string, loan_id string, current_balance double, loan_status string, "
        "is_defeased boolean, property_type string",
    )
    return bronze, s, p


def test_tie_out_passes_when_silver_matches_the_published_snapshot(spark, small_expected):
    bronze, s, p = tie_frames(spark)
    results = checks.tie_out(spark, bronze, s, p, ["T1"])
    assert all(r.passed for r in results), [r.line() for r in results]


def test_tie_out_fails_and_names_the_loan_when_a_property_type_differs(spark, small_expected):
    bronze, s, p = tie_frames(spark, published_type="Retail")
    failed = [r for r in checks.tie_out(spark, bronze, s, p, ["T1"]) if not r.passed]
    assert [r.name for r in failed] == ["tie_out: per loan"]
    assert "D1/1" in failed[0].detail


def test_tie_out_fails_when_the_balance_moves(spark, small_expected):
    bronze, s, p = tie_frames(spark, silver_balance=91.0)
    failed = {r.name for r in checks.tie_out(spark, bronze, s, p, ["T1"]) if not r.passed}
    assert {"tie_out: active balance", "tie_out: per loan"} <= failed


def test_tie_out_fails_when_a_tie_out_filing_was_superseded(spark, small_expected):
    """If an amendment replaced the June rows, silver holds none with the June accession."""
    bronze, s, p = tie_frames(spark)
    failed = {r.name for r in checks.tie_out(spark, bronze, s, p, ["MISSING"]) if not r.passed}
    assert "tie_out: loans" in failed


def test_raise_on_failure_raises_and_names_the_failed_checks():
    ok = checks.Result("a", True, "fine")
    bad = checks.Result("b", False, "broken")
    checks.raise_on_failure([ok])
    with pytest.raises(checks.CheckFailure, match="1 of 2 checks failed: b"):
        checks.raise_on_failure([ok, bad])
