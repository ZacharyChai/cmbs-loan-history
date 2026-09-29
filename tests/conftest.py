"""Shared fixtures: a local Spark session, and a builder for small ABS-EE test filings."""

import itertools
import os

import pytest

import pipeline
from local_spark import get_spark

NS = "http://www.sec.gov/edgar/document/absee/cmbs/assetdata"
_counter = itertools.count()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    session = get_spark(str(tmp_path_factory.mktemp("warehouse")))
    yield session
    session.stop()


@pytest.fixture
def tables(spark):
    """Unique bronze and silver table names per test, dropped afterwards."""
    n = next(_counter)
    names = (f"bronze_t{n}", f"silver_t{n}")
    yield names
    for t in names:
        spark.sql(f"DROP TABLE IF EXISTS {t}")


def _asset_xml(a: dict) -> str:
    """One <assets> element. Keys are XML tag names; 'property' is a dict of property tags."""
    body = "".join(f"<{k}>{v}</{k}>" for k, v in a.items() if k != "property")
    prop = a.get("property")
    if prop is not None:
        body += "<property>" + "".join(f"<{k}>{v}</{k}>" for k, v in prop.items()) + "</property>"
    return f"<assets>{body}</assets>"


def loan(number, end="05-06-2026", begin="04-06-2026", **kw):
    """A loan-level record with the fields the roll-forward and typing need."""
    a = {
        "assetNumber": number,
        "reportingPeriodBeginningDate": begin,
        "reportingPeriodEndDate": end,
        "reportPeriodBeginningScheduleLoanBalanceAmount": "100.00",
        "scheduledPrincipalAmount": "10.00",
        "reportPeriodEndScheduledLoanBalanceAmount": "90.00",
        "reportPeriodEndActualBalanceAmount": "90.00",
        "reportPeriodInterestRatePercentage": "0.05",
        "loanStructureCode": "WL",
        "property": {"propertyTypeCode": "OF", "propertyName": "Tower", "propertyState": "NY"},
    }
    prop_over = kw.pop("property", None)
    a.update(kw)
    if prop_over is not None:
        a["property"] = prop_over
    return a


def write_filing(root, cik, accession, assets):
    """Write <root>/filings/<cik>/<accession>/exh_102.xml and return its file: path."""
    folder = os.path.join(str(root), "filings", cik, accession)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, "exh_102.xml")
    with open(path, "w") as fh:
        fh.write(f'<?xml version="1.0"?><assetData xmlns="{NS}">'
                 + "".join(_asset_xml(a) for a in assets) + "</assetData>")
    return "file:" + path


def load(spark, bronze_table, silver_table, filings):
    """filings: list of (cik, accession, form, filing_date, assets). Runs bronze then silver
    for exactly those filings and returns the merge counts."""
    paths, meta = [], []
    for cik, acc, form, fdate, assets in filings:
        paths.append(write_filing(load.root, cik, acc, assets))
        meta.append((acc, form, fdate))
    manifest = spark.createDataFrame(meta, "accession string, form string, filing_date string")
    raw = pipeline.read_asset_xml(spark, paths)
    pipeline.append_bronze(pipeline.to_bronze(spark, raw, manifest), bronze_table)
    silver = pipeline.build_silver(
        spark.table(bronze_table).filter(
            "accession in (" + ",".join(f"'{f[1]}'" for f in filings) + ")"
        )
    )
    return pipeline.merge_silver(spark, silver, silver_table)


@pytest.fixture(autouse=True)
def _filings_root(tmp_path):
    load.root = tmp_path
    yield
