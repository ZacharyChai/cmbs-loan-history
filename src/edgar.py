"""List a deal's ABS-EE filings and cache their asset-data XML locally.

Pure helpers (parse_submissions, xml_candidates, summarize_asset_xml) do no I/O so they
can be tested without the network. The functions that touch EDGAR go through
cached_get, which never re-fetches a file that already exists.
"""

import hashlib
import io
import json
import os
import time
import xml.etree.ElementTree as ET

import requests

from config import CACHE_DIR, FORMS, HEADERS, REQUEST_DELAY_SECONDS

SUBMISSIONS_URL = "https://data.sec.gov/submissions/{name}"
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{name}"


# --- pure helpers ---------------------------------------------------------------
def acc_nodash(accession: str) -> str:
    return accession.replace("-", "")


def parse_submissions(doc: dict) -> list[dict]:
    """Turn one submissions JSON block into rows.

    EDGAR stores filings as parallel arrays (form[i], accessionNumber[i], ...). This
    accepts both the top-level document (rows under filings.recent) and the older
    pages listed under filings.files (rows at the top level).
    """
    block = doc.get("filings", {}).get("recent", doc)
    forms = block.get("form", [])
    rows = []
    for i, form in enumerate(forms):
        rows.append(
            {
                "accession": block["accessionNumber"][i],
                "form": form,
                "filing_date": block["filingDate"][i],
                "report_date": block.get("reportDate", [""] * len(forms))[i],
                "primary_document": block.get("primaryDocument", [""] * len(forms))[i],
            }
        )
    return rows


def keep_abs_ee(rows: list[dict], forms=FORMS) -> list[dict]:
    return [r for r in rows if r["form"] in forms]


def xml_candidates(index_doc: dict) -> list[str]:
    """XML file names in a filing folder. Names containing 102 sort first, since the
    loan-level exhibit is EX-102, but every .xml is checked by its root element."""
    names = [
        item["name"]
        for item in index_doc.get("directory", {}).get("item", [])
        if item.get("name", "").lower().endswith(".xml")
    ]
    return sorted(names, key=lambda n: ("102" not in n, n))


def root_localname(data: bytes) -> str:
    """Local name of the XML root element, ignoring any namespace. '' if unparseable."""
    try:
        for _event, elem in ET.iterparse(io.BytesIO(data), events=("start",)):
            return elem.tag.rsplit("}", 1)[-1]
    except ET.ParseError:
        return ""
    return ""


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _mdy_to_iso(value: str) -> str:
    """EDGAR dates in the asset XML are MM-DD-YYYY."""
    parts = value.strip().split("-")
    if len(parts) == 3 and len(parts[2]) == 4:
        return f"{parts[2]}-{parts[0]}-{parts[1]}"
    return value.strip()


def summarize_asset_xml(data: bytes) -> dict:
    """Counts used to sanity-check a file before it goes to bronze.

    assets_blocks counts every <assets> element. real_loans counts those whose
    assetNumber has no hyphen (the same rule as parse_absee.py in cre-credit-risk);
    hyphenated numbers are property children of a multi-property loan.
    """
    root = ET.fromstring(data)
    blocks = [e for e in root.iter() if _local(e.tag) == "assets"]
    real = 0
    period_end = ""
    for a in blocks:
        number = ""
        for child in a:
            name = _local(child.tag)
            if name == "assetNumber":
                number = (child.text or "").strip()
            elif name == "reportingPeriodEndDate" and not period_end:
                period_end = _mdy_to_iso(child.text or "")
        if number and "-" not in number:
            real += 1
    return {"assets_blocks": len(blocks), "real_loans": real, "reporting_period_end": period_end}


# --- network + cache ------------------------------------------------------------
def cached_get(url: str, dest: str, refresh: bool = False, session=None) -> bytes:
    """Return the bytes at url, from dest if it exists, else download and store them."""
    if os.path.exists(dest) and not refresh:
        with open(dest, "rb") as fh:
            return fh.read()
    get = (session or requests).get
    # EDGAR answers 503 for individual URLs now and then even under the rate limit,
    # and the same URL works a little later. Back off 2, 4, 8, 16, 30, 30 seconds.
    for attempt in range(6):
        resp = get(url, headers=HEADERS, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(2 ** (attempt + 1), 30))
            continue
        resp.raise_for_status()
        break
    else:
        resp.raise_for_status()
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    tmp = dest + ".part"
    with open(tmp, "wb") as fh:
        fh.write(resp.content)
    os.replace(tmp, dest)
    time.sleep(REQUEST_DELAY_SECONDS)
    return resp.content


def list_filings(cik: str, refresh: bool = False, session=None) -> list[dict]:
    """All ABS-EE and ABS-EE/A filings for a CIK, oldest first.

    Reads the top-level submissions document and every older page it points to.
    """
    padded = f"CIK{int(cik):010d}"
    top_name = f"{padded}.json"
    top_path = os.path.join(CACHE_DIR, "submissions", top_name)
    top = json.loads(cached_get(SUBMISSIONS_URL.format(name=top_name), top_path, refresh, session))
    rows = parse_submissions(top)
    for page in top.get("filings", {}).get("files", []):
        page_path = os.path.join(CACHE_DIR, "submissions", page["name"])
        doc = json.loads(cached_get(SUBMISSIONS_URL.format(name=page["name"]), page_path, refresh, session))
        rows.extend(parse_submissions(doc))
    rows = keep_abs_ee(rows)
    return sorted(rows, key=lambda r: (r["filing_date"], r["accession"]))


def fetch_asset_xml(cik: str, accession: str, session=None) -> list[dict]:
    """Cache the folder index and every XML candidate for one filing, then return the
    files whose root element is <assetData>, with their counts."""
    folder = os.path.join(CACHE_DIR, "filings", str(int(cik)), accession)
    nd = acc_nodash(accession)
    index_url = ARCHIVE_URL.format(cik=int(cik), acc_nodash=nd, name="index.json")
    index_doc = json.loads(cached_get(index_url, os.path.join(folder, "index.json"), session=session))
    found = []
    for name in xml_candidates(index_doc):
        url = ARCHIVE_URL.format(cik=int(cik), acc_nodash=nd, name=name)
        path = os.path.join(folder, name)
        data = cached_get(url, path, session=session)
        if root_localname(data) != "assetData":
            continue
        info = summarize_asset_xml(data)
        info.update(
            {
                "xml_file": name,
                "local_path": path,
                "bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
        )
        found.append(info)
    return found
