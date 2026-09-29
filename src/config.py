"""Shared settings.

The deal registry, User-Agent and request delay follow src/config.py in
github.com/ZacharyChai/cre-credit-risk, which this project extends from one
monthly snapshot to the full monthly history.
"""

import os

# EDGAR requires a descriptive User-Agent on every request.
USER_AGENT = "cre-credit-risk research project zachchainy@gmail.com"

# Do not set a Host header: this project calls both www.sec.gov and data.sec.gov.
HEADERS = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}

# EDGAR fair access is 10 requests per second. Sleep this long after every network call.
REQUEST_DELAY_SECONDS = 0.3

# Every monthly asset-data filing is one of these forms.
FORMS = ("ABS-EE", "ABS-EE/A")

# Cache root. Filings are immutable, so a file that exists is never fetched again. Only
# the filing lists are refreshable, with --refresh-lists. Locally this is the gitignored
# data/cache folder; the Databricks fetch notebook sets CMBS_CACHE_DIR to a volume path.
CACHE_DIR = os.environ.get("CMBS_CACHE_DIR", "data/cache")

# tie_out_accession is the June 2026 filing that the cre-credit-risk snapshot was built
# from. The tie-out check reproduces that snapshot from this accession.
DEALS = [
    {
        "deal_id": "GS_2017-GS6",
        "name": "GS Mortgage Securities Trust 2017-GS6",
        "cik": "1704459",
        "tie_out_accession": "0001888524-26-010803",
    },
    {
        "deal_id": "GS_2017-GS7",
        "name": "GS Mortgage Securities Trust 2017-GS7",
        "cik": "1710765",
        "tie_out_accession": "0001888524-26-010863",
    },
]
