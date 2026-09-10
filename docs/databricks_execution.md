# Databricks execution

The PySpark notebook in `databricks/01_medallion_notebook.py` was executed on Databricks Free Edition against official NYC TLC Yellow Taxi Parquet files for January and February 2025.

## Runtime objects

The notebook creates five Delta tables in `workspace.urban_mobility`:

| Table | Purpose |
|---|---|
| `bronze_taxi_trips` | Source-aligned records with ingestion metadata |
| `silver_taxi_trips` | Validated, deduplicated trips with derived features |
| `gold_daily_zone_metrics` | Daily pickup-zone metrics for reporting |
| `data_quality_results` | Persisted results for each pipeline check |
| `pipeline_run_log` | Append-only run metadata and row counts |

Source files are stored in the managed volume `/Volumes/workspace/urban_mobility/landing/`.

## Verified scenarios

| Scenario | Bronze rows | Silver rows | Gold rows | Quality | Refreshed dates | Duration |
|---|---:|---:|---:|---:|---:|---:|
| February initial load | 3,577,543 | 3,306,910 | 6,668 | 8/8 | 30 | 86.66 s |
| February idempotent rerun | 3,577,543 | 3,306,910 | 6,668 | 8/8 | 30 | 163.55 s |
| Recovery after missing-source test | 3,577,543 | 3,306,910 | 6,668 | 8/8 | 30 | 149.60 s |
| January processed after February | 7,052,769 | 6,560,019 | 13,884 | 8/8 | 33 | 150.97 s |

## Idempotent rerun

The February source file was processed twice. Bronze, Silver, and Gold business-table counts remained unchanged on the second run, and both `trip_id_unique` and `gold_grain_unique` passed. Audit history remained append-only.

## Controlled recovery

The source path was intentionally changed to a nonexistent file. The notebook raised `FileNotFoundError` before any table write. After the correct path was restored, the run returned to the verified February counts and all eight quality checks passed.

## Late-arriving data

January was processed after February to exercise historical partition refreshes. The final Silver table retained:

- January 2025: **3,253,109 rows**
- February 2025: **3,306,910 rows**

The run rebuilt 33 affected Gold date partitions and produced 13,884 daily-zone rows without removing the previously loaded February data.

## Data-quality checks

The Databricks pipeline persists these checks:

- Bronze is nonempty
- Silver is nonempty
- Trip IDs are complete
- Trip IDs are unique
- Pickup and drop-off locations are complete
- Trip distances are positive
- Trip durations are positive
- Gold date and pickup-zone grain is unique

## Power BI exports

The Gold metrics and latest quality results are small enough to export as curated CSV files for Power BI Desktop. Raw trip-level data remains in Delta Lake and is not loaded into the report.
