"""Step 1: list every ABS-EE filing for both deals and cache the asset-data XML.

    python src/fetch_history.py --list-only     # write the manifest of filings, download nothing
    python src/fetch_history.py                 # also download and validate the XML
    python src/fetch_history.py --limit 3       # only the first 3 filings per deal (smoke test)
    python src/fetch_history.py --refresh-lists # re-read the filing lists (a new month was filed)

Writes data/cache/filing_manifest.csv with one row per asset-data XML file. Filings that
have no asset-data XML are written to data/cache/filings_without_xml.csv so a gap is
visible instead of silently dropped.
"""

import argparse
import csv
import os
import sys
from collections import Counter

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edgar  # noqa: E402
from config import CACHE_DIR, DEALS  # noqa: E402

MANIFEST = os.path.join(CACHE_DIR, "filing_manifest.csv")
MISSING = os.path.join(CACHE_DIR, "filings_without_xml.csv")
LIST_ONLY_MANIFEST = os.path.join(CACHE_DIR, "filing_list.csv")
FAILED = os.path.join(CACHE_DIR, "filings_failed.csv")

FIELDS = [
    "deal_id", "cik", "accession", "form", "filing_date", "report_date",
    "xml_file", "reporting_period_end", "assets_blocks", "real_loans",
    "bytes", "sha256", "local_path",
]


def write_csv(path, fields, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def summarize_listing(deal, filings):
    forms = Counter(f["form"] for f in filings)
    dates = [f["filing_date"] for f in filings]
    months = Counter(d[:7] for d in dates)
    multi = {m: n for m, n in months.items() if n > 1}
    print(f"{deal['deal_id']} (CIK {deal['cik']}): {len(filings)} filings, "
          f"{dict(forms)}, {min(dates)} to {max(dates)}")
    if multi:
        print(f"  months with more than one filing: {multi}")
    tie = deal["tie_out_accession"]
    print(f"  tie-out accession {tie} listed: {any(f['accession'] == tie for f in filings)}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--list-only", action="store_true")
    ap.add_argument("--refresh-lists", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="first N filings per deal, 0 = all")
    args = ap.parse_args(argv)

    listing, manifest, missing, failed = [], [], [], []
    for deal in DEALS:
        filings = edgar.list_filings(deal["cik"], refresh=args.refresh_lists)
        summarize_listing(deal, filings)
        if args.limit:
            filings = filings[: args.limit]
        for f in filings:
            base = {"deal_id": deal["deal_id"], "cik": deal["cik"], **f}
            listing.append(base)
            if args.list_only:
                continue
            try:
                found = edgar.fetch_asset_xml(deal["cik"], f["accession"])
            except requests.RequestException as exc:
                # Keep going: everything already downloaded stays cached, and a re-run
                # picks up only what is missing.
                failed.append({**base, "error": str(exc)[:200]})
                print(f"  FAILED {f['accession']} {f['filing_date']}: {exc}")
                continue
            if not found:
                missing.append(base)
                print(f"  no asset-data XML: {f['accession']} {f['form']} {f['filing_date']}")
            for info in found:
                manifest.append({**base, **info})

    write_csv(LIST_ONLY_MANIFEST, ["deal_id", "cik", "accession", "form", "filing_date",
                                   "report_date", "primary_document"], listing)
    print(f"wrote {LIST_ONLY_MANIFEST} ({len(listing)} rows)")
    if args.list_only:
        return

    write_csv(MANIFEST, FIELDS, manifest)
    write_csv(MISSING, ["deal_id", "cik", "accession", "form", "filing_date", "report_date"], missing)
    write_csv(FAILED, ["deal_id", "cik", "accession", "form", "filing_date", "error"], failed)
    print(f"wrote {MANIFEST} ({len(manifest)} XML files), {MISSING} ({len(missing)} filings), "
          f"{FAILED} ({len(failed)} filings)")
    for deal in DEALS:
        rows = [m for m in manifest if m["deal_id"] == deal["deal_id"]]
        blocks = Counter(m["reporting_period_end"][:7] for m in rows)
        dup = {k: v for k, v in blocks.items() if v > 1}
        print(f"{deal['deal_id']}: {len(rows)} XML files, {len(blocks)} distinct reporting months"
              + (f", months in more than one file: {dup}" if dup else ""))

    if failed:
        print(f"{len(failed)} filings failed to download. Re-run to fetch only those.")
        sys.exit(1)


if __name__ == "__main__":
    main()
