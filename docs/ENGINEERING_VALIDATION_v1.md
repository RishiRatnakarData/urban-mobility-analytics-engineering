# Engineering validation and execution boundaries

## Local incrementality — strengthened

The local pandas/Parquet pipeline now supports repeatable CSV or Parquet input batches with `--input`, in addition to `--sample`. It maintains trip-grain Silver and refreshes only the affected date partitions in Gold. An update to an existing trip ID can change its pickup date: the pipeline identifies **both the old and new dates** and refreshes both. A date that becomes empty is removed from Gold rather than left stale.

`to_silver` now filters invalid rows before choosing the last *valid* record for duplicate IDs and drops missing IDs and non-finite required numerical metrics. Invalid-only batches fail before table writes. A source fingerprint, source filename, record reconciliation, newly observed IDs, previously matched IDs, refreshed dates, and validation results are reported in `artifacts/run_summary.json`; run history is appended locally to `artifacts/run_history.jsonl`. These outputs are excluded from Git.

The quality gate includes the six original local tests plus **four additional cross-layer assertions**: unique Gold grain, reconciled trip counts, reconciled revenue, and finite Gold metrics. The legacy `quality_checks()` function remains available for existing consumers.

> **Local/runtime boundaries:** The local pipeline replaces Silver and Gold files using temporary Parquet files and `os.replace`, but those two replacements are **not a multi-table transaction**. Cloud-grade ACID semantics remain the responsibility of Delta Lake in the separately maintained Databricks notebook. Run audit history is intended for local observability, not production logging.

## Prior Databricks evidence — retained, not rerun

The original repository documents four cloud executions on September 10, 2026, including February reruns, a controlled missing-file failure/recovery, and January arriving after February. Those verified historical runs produced **7,052,769 Bronze**, **6,560,019 Silver**, and **13,884 Gold** records, with eight of eight Databricks checks passing. These results are not new performance measurements from this local upgrade. See [Databricks execution](databricks_execution.md) and [machine-readable reference](evidence/cloud_execution_reference.json).

## Test coverage

- Original local transform tests remain intact.
- Added tests cover idempotent repeats, corrective updates crossing pickup dates, historical late arrivals, invalid duplicates, missing IDs, non-finite values, quality-gate failures, uniqueness, source rejection, and run-history persistence.
- CI still compiles the Databricks notebook and runs the local pipeline and dbt models without cloud credentials. The updated CI explicitly checks all ten local quality gates.

## Source limitations and future deployment

The `databricks/01_medallion_notebook.py` notebook was **not modified by this package**. Cloud MERGE, multi-table failure recovery, access controls, alerting, orchestration, and incremental source discovery require dedicated live-environment validation before production use. The current local dbt incremental mart uses a 62-day lookback, which does **not** guarantee arbitrary-age corrections outside that window; the local pandas Gold refresh uses explicit changed-date tracking instead. These two execution paths are not equivalent in that edge case.

The Power BI screenshots are previously captured reports; they are not newly rendered from the small local fixture. All quantitative claims about millions of records are attributed to the original Databricks run notes.
