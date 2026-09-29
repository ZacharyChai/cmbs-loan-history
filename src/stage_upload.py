"""Stage the cached filings for upload to a Unity Catalog volume.

    python src/stage_upload.py

Copies only the asset-data XML files (not index.json or the other exhibits), the filing
manifest and the published-snapshot reference into data/upload/, laid out the way the
notebooks expect under /Volumes/<catalog>/<schema>/<volume>/:

    cache/filing_manifest.csv
    cache/filings/<cik>/<accession>/<xml file>
    reference/june_2026_published_snapshot.csv

Then upload with the Databricks CLI, which you configure yourself (see the printed
commands). Use this route when outbound internet from Databricks is not enabled yet.
"""

import csv
import os
import shutil

MANIFEST = "data/cache/filing_manifest.csv"
REFERENCE = "tests/fixtures/june_2026_published_snapshot.csv"
OUT = "data/upload"


def main():
    rows = list(csv.DictReader(open(MANIFEST)))
    shutil.rmtree(OUT, ignore_errors=True)
    for r in rows:
        dest = os.path.join(OUT, "cache", "filings", r["cik"], r["accession"], r["xml_file"])
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.copyfile(r["local_path"], dest)
    shutil.copyfile(MANIFEST, os.path.join(OUT, "cache", "filing_manifest.csv"))
    os.makedirs(os.path.join(OUT, "reference"), exist_ok=True)
    shutil.copyfile(REFERENCE, os.path.join(OUT, "reference", os.path.basename(REFERENCE)))
    size = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(OUT) for f in fs)
    print(f"staged {len(rows)} XML files, {size / 1e6:.1f} MB, in {OUT}/")
    print("Upload (check `databricks fs cp --help` for your CLI version):")
    print("  databricks fs cp -r data/upload/cache     dbfs:/Volumes/<catalog>/<schema>/<volume>/cache")
    print("  databricks fs cp -r data/upload/reference dbfs:/Volumes/<catalog>/<schema>/<volume>/reference")


if __name__ == "__main__":
    main()
