# Deploying the monthly job

`monthly_refresh.json` chains the five notebooks into one Databricks job:
fetch -> bronze -> silver -> checks -> gold. A failed check fails the `checks` task,
which stops `gold` from running on data that hasn't passed.

## Before creating it

1. Update every `notebook_path` in `monthly_refresh.json` if your Git folder isn't at
   `/Workspace/Users/zachchainy@gmail.com/cmbs-loan-history` (check the path in the
   workspace file browser). This is the Databricks workspace login, which is separate
   from the SEC EDGAR contact email in `src/config.py`.
2. `catalog`, `schema` and `volume` in `base_parameters` default to `workspace`, `cmbs`
   and `filings`, matching `notebooks/00_setup.py`. Change both files together if you
   used different names.

## Create it

```bash
databricks jobs create --json @jobs/monthly_refresh.json
```

The job is created **paused** (`schedule.pause_status: PAUSED`), on purpose: run it once
manually first (`databricks jobs run-now --job-id <id>`, or the Run now button in the
UI) and check the results before it runs unattended.

## Unpause it

```bash
databricks jobs update --job-id <id> --json '{"new_settings": {"schedule": {"pause_status": "UNPAUSED"}}}'
```

Or flip the toggle on the job's page in the UI.

## Schedule

The 28th of each month at 14:00 America/New_York (18:00-19:00 UTC depending on DST).
Chosen from the filing dates in `data/cache/filing_manifest.csv`: both deals file
between the 20th and the 30th, most often the 26th, so the 28th catches the month's
filing in the same run for the large majority of months. A filing that lands after the
28th is picked up by the following month's run instead, since `fetch` always re-lists
every filing, not just the newest one.

## Free Edition limits that shape this job

Free Edition allows up to 5 concurrent job tasks; this job runs 5 tasks in a strict
chain, one at a time, so it never approaches that limit. All five notebooks run on
serverless compute (no cluster is specified in the job JSON), which is the only compute
type Free Edition offers.
