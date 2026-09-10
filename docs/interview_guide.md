# Project reasoning and interview guide

## Five-minute walkthrough

1. **Problem:** Mobility teams need trustworthy daily demand and revenue metrics by pickup zone.
2. **Source and grain:** Official TLC Parquet begins at trip grain; the title stays urban/general
   because the engineering pattern transfers to other cities.
3. **Architecture:** Bronze preserves source records, Silver validates unique trips, and Gold serves
   daily zone metrics. Audit tables preserve quality and run evidence.
4. **Reliability:** deterministic hashes plus Delta MERGE make reruns idempotent; invalid records fail
   quality rules; only incoming date partitions are refreshed in Gold.
5. **Extension:** January is processed after February to demonstrate late-arriving historical data.
6. **Boundary:** CI proves the local pandas/dbt contract. Screenshots and run logs separately prove
   Databricks execution; neither proves a production deployment.

## Questions you should be able to answer

### Why keep Bronze if Silver is already clean?

Bronze preserves source values, lineage, and ingestion metadata. If a Silver rule is wrong, the
pipeline can be corrected and replayed without downloading or silently altering the source again.

### What makes this pipeline idempotent?

Bronze uses a deterministic source-record hash and inserts only unseen rows. Silver uses a
deterministic trip key with Delta MERGE. Gold replaces only the affected date partitions. Repeating
the same input therefore leaves business-table counts and keys stable.

### How are late arrivals handled?

An older incoming month is merged into Silver by trip key. The notebook derives the dates touched
by that batch and recomputes those Gold partitions from the complete Silver table, leaving unrelated
dates intact.

### Why use dbt after PySpark?

PySpark performs scalable validation and trip-level feature engineering. dbt expresses the serving
contract, tests column assumptions and mart grain, and produces reviewable SQL lineage. In this
portfolio, dbt runs locally on DuckDB so reviewers can verify it without a cloud account.

### What does CI prove?

CI proves Python behavior, the local sample pipeline, notebook syntax, and the DuckDB dbt build on a
clean runner. It does not prove Databricks authentication, serverless availability, Delta execution,
workspace permissions, or Power BI results.

### What would change in production?

Use scheduled Lakeflow Jobs, Auto Loader or managed ingestion, checkpointed streaming where needed,
quarantine tables, alerting, secrets, least-privilege catalog permissions, dev/test/prod environments,
and cost/performance tests. These are recommendations, not claims about this repository.

## Key limitations

- The deterministic key can theoretically collide when separate trips share the selected fields.
- A month-at-a-time notebook is a portfolio-scale batch, not a streaming service.
- Taxi trips represent one mobility mode and may reflect geographic and service-access bias.
- Power BI uses exported aggregate CSV files in the free path rather than a production live connection.
