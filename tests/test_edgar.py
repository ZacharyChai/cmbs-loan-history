import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import edgar  # noqa: E402

SUBMISSIONS = {
    "filings": {
        "recent": {
            "accessionNumber": ["0001-26-000003", "0001-26-000002", "0001-26-000001", "0001-25-000009"],
            "form": ["ABS-EE", "10-D", "ABS-EE/A", "ABS-EE"],
            "filingDate": ["2026-06-23", "2026-06-23", "2026-06-30", "2025-01-20"],
            "reportDate": ["", "2026-06-08", "", ""],
            "primaryDocument": ["a.htm", "b.htm", "c.htm", "d.htm"],
        },
        "files": [{"name": "CIK0000000001-submissions-001.json"}],
    }
}

OLD_PAGE = {
    "accessionNumber": ["0001-17-000001"],
    "form": ["ABS-EE"],
    "filingDate": ["2017-05-15"],
}

ASSET_XML = b"""<?xml version="1.0"?>
<assetData xmlns="http://www.sec.gov/edgar/document/absee/cmbs/assetdata">
  <assets><assetNumber>1</assetNumber><reportingPeriodEndDate>06-08-2026</reportingPeriodEndDate></assets>
  <assets><assetNumber>2</assetNumber><reportingPeriodEndDate>06-08-2026</reportingPeriodEndDate></assets>
  <assets><assetNumber>2-001</assetNumber></assets>
  <assets><assetNumber>2-002</assetNumber></assets>
  <assets><assetNumber>3A</assetNumber></assets>
</assetData>"""


def test_parse_submissions_reads_parallel_arrays():
    rows = edgar.parse_submissions(SUBMISSIONS)
    assert len(rows) == 4
    assert rows[0] == {
        "accession": "0001-26-000003", "form": "ABS-EE", "filing_date": "2026-06-23",
        "report_date": "", "primary_document": "a.htm",
    }


def test_parse_submissions_reads_an_older_page_without_optional_fields():
    rows = edgar.parse_submissions(OLD_PAGE)
    assert rows == [{
        "accession": "0001-17-000001", "form": "ABS-EE", "filing_date": "2017-05-15",
        "report_date": "", "primary_document": "",
    }]


def test_keep_abs_ee_keeps_original_and_amendment_only():
    kept = edgar.keep_abs_ee(edgar.parse_submissions(SUBMISSIONS))
    assert [r["form"] for r in kept] == ["ABS-EE", "ABS-EE/A", "ABS-EE"]


def test_xml_candidates_prefers_102_and_ignores_other_files():
    index = {"directory": {"item": [
        {"name": "exh_103.xml"}, {"name": "primary.htm"}, {"name": "exh_102.xml"}, {"name": "x.txt"},
    ]}}
    assert edgar.xml_candidates(index) == ["exh_102.xml", "exh_103.xml"]


def test_root_localname_ignores_namespace_and_bad_input():
    assert edgar.root_localname(ASSET_XML) == "assetData"
    assert edgar.root_localname(b"<a><b/></a>") == "a"
    # A truncated HTML error page reports its own root, which is never "assetData".
    assert edgar.root_localname(b"<html><body>error") == "html"
    assert edgar.root_localname(b"") == ""


def test_summarize_asset_xml_separates_records_from_real_loans():
    info = edgar.summarize_asset_xml(ASSET_XML)
    assert info["assets_blocks"] == 5
    assert info["real_loans"] == 3  # "1", "2" and "3A"; the two hyphenated rows are property children
    assert info["reporting_period_end"] == "2026-06-08"


def test_cached_get_never_refetches_an_existing_file(tmp_path):
    dest = tmp_path / "a.xml"
    dest.write_bytes(b"cached")

    class Boom:
        def get(self, *a, **k):
            raise AssertionError("network used for a cached file")

    assert edgar.cached_get("https://example.invalid/a.xml", str(dest), session=Boom()) == b"cached"
