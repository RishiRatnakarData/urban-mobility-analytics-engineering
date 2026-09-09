# Microsoft Fabric build - exact checklist

Complete this before listing Microsoft Fabric or PySpark for the project.

1. Create a Fabric-enabled workspace named `rr-portfolio-dev`.
2. Create a Lakehouse named `nyc_mobility_lh`.
3. Download one small monthly Yellow Taxi Parquet file from the official NYC TLC trip-record page.
4. In the Lakehouse, upload it to `Files/landing/yellow_tripdata.parquet`.
5. Create a PySpark notebook named `nb_medallion_taxi` and attach the Lakehouse.
6. Paste `fabric/01_medallion_notebook.py` into logical cells at each `Cell` marker.
7. Run all cells. Capture Bronze, Silver, Gold, and data-quality row counts.
8. Rerun the notebook and confirm stable row counts and no duplicate trip keys.
9. Create a Data Pipeline named `pl_nyc_taxi` that runs the notebook. Add a parameter for source path or month.
10. Configure a failure path and retry policy. Deliberately use a bad path once, capture the failure, repair it, and document recovery.
11. Connect Power BI to the Gold Delta table or SQL analytics endpoint.
12. Create report pages: Operations Overview, Zone Performance, Time Patterns, and Data Quality.
13. Add your actual Fabric screenshots under `docs/images/` with sensitive workspace identifiers cropped.
14. Update the README claims ledger with exact run date, source month, row counts, duration, and test results.

Optional advanced work:

- Create a Warehouse serving layer and run dbt against Fabric using the official adapter instructions.
- Connect the Fabric workspace to Git only if the tenant and item types support it.
- Create dev/test workspaces and document deployment-pipeline rules without claiming production deployment.

