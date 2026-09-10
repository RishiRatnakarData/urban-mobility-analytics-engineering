# Databricks Free Edition build checklist

Complete this checklist before describing the project as executed on Databricks. The local
Python/dbt build is independently reproducible and does not prove cloud execution.

## 1. Create the free workspace

1. Sign up for [Databricks Free Edition](https://login.databricks.com/).
2. Open the workspace. Free Edition provides serverless compute and default storage; no Azure
   subscription is required.
3. Do not enter credentials, tokens, workspace IDs, or personal email addresses in this repository.

## 2. Obtain the public source data

1. Open the [NYC TLC trip-record data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
2. Download Yellow Taxi Parquet for **February 2025**.
3. Keep the official filename `yellow_tripdata_2025-02.parquet`.
4. Record its download URL, file size, and source month in the README claims ledger.

NYC is the source geography, while the repository title remains the more reusable **Urban Mobility
Analytics Engineering Platform**.

## 3. Import and prepare the notebook

1. In Databricks, open **Workspace** and choose **Create > Import**.
2. Import `databricks/01_medallion_notebook.py` as a source notebook.
3. Connect the notebook to the available serverless compute.
4. Run only the first setup cell. It prints the active catalog and an upload path resembling:

   ```text
   /Volumes/<your-catalog>/urban_mobility/landing/yellow_tripdata_2025-02.parquet
   ```

5. In **Catalog**, open that catalog, then `urban_mobility > Volumes > landing`.
6. Upload `yellow_tripdata_2025-02.parquet` without renaming it.

## 4. Execute and validate

1. Run all notebook cells.
2. Save the final JSON summary and screenshots of:
   - Bronze, Silver, and Gold row counts.
   - Eight passing data-quality checks.
   - The latest rows in `pipeline_run_log`.
   - A sample of `gold_daily_zone_metrics`.
3. In Catalog Explorer, confirm these five Delta tables exist:
   - `bronze_taxi_trips`
   - `silver_taxi_trips`
   - `gold_daily_zone_metrics`
   - `data_quality_results`
   - `pipeline_run_log`

## 5. Prove idempotency

1. Run all cells again with the same February source file.
2. Confirm Bronze, Silver, and Gold row counts are unchanged.
3. Confirm `trip_id_unique` and `gold_grain_unique` still pass.
4. It is expected that the audit tables gain another run: business tables remain stable while run
   history is append-only.

## 6. Prove controlled failure and recovery

1. Temporarily change `SOURCE_FILENAME` to `missing.parquet`.
2. Run the preflight cell directly below the upload checkpoint.
3. Capture the expected `FileNotFoundError`; the check occurs before table writes.
4. Restore `SOURCE_FILENAME = "yellow_tripdata_2025-02.parquet"`.
5. Run all cells and capture the successful recovery summary.

## 7. Demonstrate a late-arriving partition

1. Download and upload `yellow_tripdata_2025-01.parquet` from the same official TLC page.
2. Change `SOURCE_FILENAME` to `yellow_tripdata_2025-01.parquet`.
3. Run all cells after February has already been processed.
4. Confirm January rows are inserted into Silver and January Gold partitions are created/refreshed.
5. Confirm February Gold results remain present and all eight checks pass.
6. Save before/after counts and the `date_partitions_refreshed` value in the claims ledger.

This deliberately processes an older month after a newer one. Delta MERGE prevents duplicate trip
keys, while partition-scoped Gold replacement updates only dates present in the incoming batch.

## 8. Build Power BI evidence

1. Download the two files produced under `landing/exports/`:
   - `gold_daily_zone_metrics.csv`
   - `data_quality_results_latest.csv`
2. Follow `docs/power_bi.md` in Power BI Desktop.
3. Validate card totals against Databricks before taking screenshots.
4. Store cropped screenshots in `docs/images/`; exclude account, catalog, and workspace identifiers.

## 9. Finish the evidence ledger

Update the README with the actual run date, source months, file sizes, row counts, durations,
idempotency result, late-arrival result, failure/recovery result, and Power BI screenshot. Do not
replace placeholders with estimates.

## Free Edition boundary

This portfolio uses a notebook-driven pipeline because Free Edition is serverless and quota-limited.
A production deployment would add scheduled jobs, environment promotion, secrets, access policies,
monitoring alerts, and cost controls. Do not claim those production controls unless implemented.
