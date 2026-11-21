# Local pipeline observability — data-quality and run-health summary

## Purpose and scope

The existing Databricks cloud notebook already writes quality and run-log Delta tables, and the local pipeline already writes `artifacts/run_summary.json`, `run_history.jsonl`, and `data_quality_results.csv`. This add-on makes a **single read-only local summary** from those persisted artifacts, plus the current local Silver and Gold Parquet snapshots. It does not add a scheduler, cloud monitor, hosted dashboard, database, or Databricks execution claim.

## Run (Python 3.12)

```powershell
python -m pip install -r requirements.txt
python -m src.pipeline --sample
python -m src.observability --out artifacts/observability.json
python -m pytest -q tests/test_observability.py
```

Optional warning thresholds (**disabled by default** for historical 2025 NYC TLC trip data):

```powershell
python -m src.observability --max-data-age-days 30 --max-run-age-hours 24 --out artifacts/observability.json
```

These thresholds are useful when monitoring **current** pipeline operations; they would obviously flag a historical research batch if applied indiscriminately. The pickup timestamp is stored without an explicit time-zone offset, so data age is reported at **calendar-day** granularity rather than assuming UTC. Pipeline-run age uses timezone-aware UTC.

## Report fields

| Area | Reported from actual persisted data |
|---|---|
| Latest input batch | Bronze batch rows, valid unique trips, rejected/duplicate row count (from existing upsert accounting) |
| Curated tables | Silver and Gold row counts from Parquet, cross-checked against the latest successful summary |
| Data quality | Test totals and failed check names from the existing quality CSV |
| Nulls and duplicates | Missing Silver identifiers/dates/zones; duplicate Silver trip IDs and Gold date-zone keys |
| Reconciliation | Gold trip-count total against observed Silver rows |
| Freshness | Latest trip pickup date and most recent successful local run age |
| Duration | Newly measured elapsed seconds of `run_batch`; unavailable for pre-upgrade histories |
| Attempt status | Latest **CLI-instrumented** attempted run's outcome, separately from any preceding successful snapshot |
| Warning summary | Exact failed checks, missing outputs, older timestamps, absent prior duration, and optional threshold warnings |

On future **local CLI runs**, `src/pipeline.py` records elapsed wall-clock time in `run_summary.json` and an independent `artifacts/last_attempt.json` with either `success` or `failed` plus the exception class on failure. The latter prevents a failed latest CLI attempt from being misrepresented by an older success snapshot. Invocations of `run_batch()` directly from Python, killed processes, and historical runs predating the upgrade are not guaranteed to have this attempt marker; the report explicitly says when latest-attempt status is unknown. Exceptions still propagate, and normal failed-batch protection remains in place.

## Status interpretation

- `passed` — all persisted local snapshot checks passed and the newest recorded CLI attempt succeeded.
- `failed_validation` — a failed latest CLI attempt, a failed quality check, corrupted key/reconciliation data, or missing required persisted outputs.
- `snapshot_passed_latest_attempt_unknown` — the snapshot checks passed but no reliable latest CLI attempt result was recorded.

A `passed` status is **not** an uptime, latency SLA, Delta Lake, or production reliability claim. Warnings may coexist with passed validation. Any historical September 2026 cloud-run row counts and durations remain exactly as previously reported; this local add-on makes no new Databricks measurements.
