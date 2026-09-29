"""Checks against the real cached filings. Skipped when data/cache is not present."""

import csv
import os
import xml.etree.ElementTree as ET

import pytest

import checks
import pipeline
import schema
from config import DEALS

MANIFEST = "data/cache/filing_manifest.csv"
PUBLISHED = os.path.abspath("tests/fixtures/june_2026_published_snapshot.csv")

pytestmark = pytest.mark.real_data
have_cache = os.path.exists(MANIFEST)


@pytest.mark.skipif(not have_cache, reason="no cached filings")
def test_schema_lists_every_tag_found_in_the_cached_filings():
    local = lambda t: t.rsplit("}", 1)[-1]  # noqa: E731
    loan_tags, prop_tags = set(), set()
    for r in csv.DictReader(open(MANIFEST)):
        for a in ET.parse(r["local_path"]).getroot().iter():
            if local(a.tag) != "assets":
                continue
            for c in a:
                if local(c.tag) == "property":
                    prop_tags.update(local(x.tag) for x in c)
                else:
                    loan_tags.add(local(c.tag))
    assert loan_tags <= set(schema.LOAN_TAGS), sorted(loan_tags - set(schema.LOAN_TAGS))
    assert prop_tags <= set(schema.PROPERTY_TAGS), sorted(prop_tags - set(schema.PROPERTY_TAGS))


@pytest.mark.skipif(not have_cache, reason="no cached filings")
def test_june_2026_filings_tie_out_to_the_published_snapshot(spark, tables):
    bronze_t, silver_t = tables
    tie = [d["tie_out_accession"] for d in DEALS]
    rows = [r for r in csv.DictReader(open(MANIFEST)) if r["accession"] in tie]
    assert len(rows) == 2
    manifest = spark.createDataFrame(
        [(r["accession"], r["form"], r["filing_date"]) for r in rows],
        "accession string, form string, filing_date string",
    )
    raw = pipeline.read_asset_xml(spark, ["file:" + os.path.abspath(r["local_path"]) for r in rows])
    pipeline.append_bronze(pipeline.to_bronze(spark, raw, manifest), bronze_t)
    pipeline.merge_silver(spark, pipeline.build_silver(spark.table(bronze_t)), silver_t)
    results = checks.tie_out(
        spark, spark.table(bronze_t), spark.table(silver_t), checks.load_published(spark, PUBLISHED), tie
    )
    assert all(r.passed for r in results), [r.line() for r in results if not r.passed]
