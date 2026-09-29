"""Gold panel aggregation on hand-built silver rows."""

from datetime import date

import gold

SILVER_SCHEMA = (
    "deal_id string, loan_id string, reporting_period_end date, property_type string, "
    "current_balance double, dscr double, occupancy double, loan_status string, "
    "is_defeased boolean, payment_status_code string"
)


def silver(spark, rows):
    return spark.createDataFrame(rows, SILVER_SCHEMA)


def r(loan, ptype, bal, dscr, occ, status="active", defeased=False, pay="0", period=date(2026, 6, 6)):
    return ("D1", loan, period, ptype, bal, dscr, occ, status, defeased, pay)


def by_type(panel, ptype):
    return {row.property_type: row for row in panel.collect()}[ptype]


def test_panel_sums_active_balance_and_counts_by_property_type(spark):
    df = silver(spark, [
        r("1", "Office", 100.0, 1.5, 0.9),
        r("2", "Office", 200.0, 2.0, 0.8),
        r("3", "Retail", 50.0, 1.2, 0.95),
    ])
    panel = gold.build_gold_panel(df)
    off = by_type(panel, "Office")
    assert (off.loan_count, off.active_loan_count, off.active_balance) == (2, 2, 300.0)
    assert by_type(panel, "Retail").active_balance == 50.0


def test_defeased_and_paid_off_loans_are_excluded_from_active_balance(spark):
    df = silver(spark, [
        r("1", "Office", 100.0, 1.5, 0.9),
        r("2", "Office", 80.0, None, None, defeased=True),
        r("3", "Office", 0.0, None, None, status="paid_off"),
    ])
    panel = gold.build_gold_panel(df)
    off = by_type(panel, "Office")
    assert off.loan_count == 3
    assert (off.active_loan_count, off.active_balance) == (1, 100.0)
    assert (off.defeased_loan_count, off.defeased_balance) == (1, 80.0)
    assert off.paid_off_loan_count == 1


def test_dscr_and_occupancy_stats_ignore_defeased_and_paid_off_rows(spark):
    """A defeased or paid-off loan's stale dscr must not pull the active average."""
    df = silver(spark, [
        r("1", "Office", 100.0, 1.0, 0.5),
        r("2", "Office", 100.0, 3.0, 0.9),
        r("3", "Office", 50.0, 9.0, 0.1, defeased=True),
    ])
    off = by_type(gold.build_gold_panel(df), "Office")
    assert off.dscr_avg == 2.0 and off.dscr_min == 1.0 and off.dscr_max == 3.0
    assert off.occupancy_avg == 0.7


def test_missing_dscr_is_counted_not_silently_dropped(spark):
    df = silver(spark, [r("1", "Office", 100.0, None, 0.9), r("2", "Office", 100.0, 2.0, 0.8)])
    off = by_type(gold.build_gold_panel(df), "Office")
    assert off.dscr_missing_count == 1 and off.dscr_avg == 2.0


def test_delinquency_buckets_map_the_known_codes_and_group_the_rest_as_unknown(spark):
    df = silver(spark, [
        r("1", "Office", 100.0, 1.0, 0.9, pay="0"),
        r("2", "Office", 100.0, 1.0, 0.9, pay="3"),
        r("3", "Office", 100.0, 1.0, 0.9, pay="A"),
        r("4", "Office", 100.0, 1.0, 0.9, pay=""),
        r("5", "Office", 100.0, 1.0, 0.9, pay="Z"),  # a code the mapping does not know
    ])
    row = by_type(gold.build_gold_panel(df), "Office").asDict()
    assert row["delinquent_count_current"] == 1
    assert row["delinquent_count_90-119_days_delinquent"] == 1
    assert row["delinquent_count_current_grace_period"] == 1
    assert row["delinquent_count_unknown_or_blank"] == 2  # blank code and the unrecognized "Z"


def test_delinquency_and_dscr_are_scoped_to_active_loans_only(spark):
    """A defeased or paid-off loan does not add to any delinquency bucket."""
    df = silver(spark, [
        r("1", "Office", 100.0, 1.0, 0.9, pay="0"),
        r("2", "Office", 80.0, None, None, defeased=True, pay="0"),
        r("3", "Office", 0.0, None, None, status="paid_off", pay="3"),
    ])
    row = by_type(gold.build_gold_panel(df), "Office").asDict()
    assert row["delinquent_count_current"] == 1
    assert row["delinquent_count_90-119_days_delinquent"] == 0


def test_panel_has_one_row_per_month_per_property_type(spark):
    df = silver(spark, [
        r("1", "Office", 100.0, 1.0, 0.9, period=date(2026, 5, 6)),
        r("1", "Office", 90.0, 1.1, 0.9, period=date(2026, 6, 6)),
    ])
    panel = gold.build_gold_panel(df)
    assert panel.count() == 2
    assert sorted(row.reporting_period_end for row in panel.collect()) == [date(2026, 5, 6), date(2026, 6, 6)]


def test_pool_summary_rolls_up_every_property_type_for_a_month(spark):
    df = silver(spark, [
        r("1", "Office", 100.0, 1.0, 0.9),
        r("2", "Retail", 200.0, 3.0, 0.8),
        r("3", "Office", 50.0, None, None, defeased=True),
    ])
    row = gold.build_gold_pool_summary(df).collect()[0]
    assert (row.loan_count, row.active_loan_count, row.active_balance) == (3, 2, 300.0)
    assert row.dscr_avg == 2.0
