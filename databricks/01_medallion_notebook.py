# Databricks notebook source
# MAGIC %md
# MAGIC # Urban Mobility Medallion Pipeline
# MAGIC Run this notebook on Databricks after uploading one official NYC TLC Yellow Taxi
# MAGIC Parquet file to the managed volume created in the setup cell. The implementation
# MAGIC uses Delta MERGE for idempotent ingestion and refreshes Gold only for dates touched
# MAGIC by the incoming batch, including late-arriving historical records.

# COMMAND ----------

import json
import time
import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = spark.sql("SELECT current_catalog()").first()[0]
SCHEMA = "urban_mobility"
VOLUME = "landing"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")

SOURCE_FILENAME = "yellow_tripdata_2025-02.parquet"
SOURCE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{SOURCE_FILENAME}"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_taxi_trips"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_taxi_trips"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_zone_metrics"
DQ_TABLE = f"{CATALOG}.{SCHEMA}.data_quality_results"
RUN_TABLE = f"{CATALOG}.{SCHEMA}.pipeline_run_log"
EXPORT_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/exports"

print({"catalog": CATALOG, "schema": SCHEMA, "upload_to": SOURCE_PATH})

# COMMAND ----------
# MAGIC %md
# MAGIC **Upload checkpoint:** after running the setup cell above, upload the Parquet file
# MAGIC to the printed path. This preflight intentionally stops before any table writes when
# MAGIC the source is missing. Use it for the documented controlled-failure test.

# COMMAND ----------

source_parent = SOURCE_PATH.rsplit("/", 1)[0]
source_name = SOURCE_PATH.rsplit("/", 1)[1]
source_files = {item.name.rstrip("/") for item in dbutils.fs.ls(source_parent)}
if source_name not in source_files:
    raise FileNotFoundError(
        f"Source file not found at {SOURCE_PATH}. Upload the official TLC Parquet file and rerun."
    )

run_id = str(uuid.uuid4())
started = time.perf_counter()
checked_at = datetime.now(timezone.utc)

source = spark.read.parquet(SOURCE_PATH)
column_lookup = {name.lower(): name for name in source.columns}
required = {
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pulocationid",
    "dolocationid",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
}
missing = sorted(required - set(column_lookup))
if missing:
    raise ValueError(f"Missing required TLC columns: {missing}")


def source_column(name):
    return F.col(column_lookup[name.lower()])


source_payload = F.struct(*[F.col(name) for name in sorted(source.columns)])
incoming_bronze = (
    source
    .withColumn("source_record_id", F.sha2(F.to_json(source_payload), 256))
    .withColumn("source_file", F.lit(SOURCE_PATH))
    .withColumn("source_month", F.regexp_extract(F.lit(SOURCE_PATH), r"(\d{4}-\d{2})", 1))
    .withColumn("ingested_at_utc", F.lit(checked_at).cast("timestamp"))
    .withColumn("batch_id", F.lit(run_id))
    .dropDuplicates(["source_record_id"])
)

if spark.catalog.tableExists(BRONZE_TABLE):
    (
        DeltaTable.forName(spark, BRONZE_TABLE)
        .alias("target")
        .merge(incoming_bronze.alias("source"), "target.source_record_id = source.source_record_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    incoming_bronze.write.format("delta").saveAsTable(BRONZE_TABLE)

# COMMAND ----------

trip_key_columns = [
    source_column("vendorid").cast("string"),
    source_column("tpep_pickup_datetime").cast("string"),
    source_column("tpep_dropoff_datetime").cast("string"),
    source_column("pulocationid").cast("string"),
    source_column("dolocationid").cast("string"),
]
trip_key = F.sha2(
    F.concat_ws("|", *[F.coalesce(value, F.lit("<null>")) for value in trip_key_columns]),
    256,
)

silver_candidates = (
    incoming_bronze
    .select(
        trip_key.alias("trip_id"),
        F.col("source_record_id"),
        F.col("source_file"),
        F.col("source_month"),
        F.col("ingested_at_utc"),
        source_column("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        source_column("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        source_column("pulocationid").cast("int").alias("pickup_location_id"),
        source_column("dolocationid").cast("int").alias("dropoff_location_id"),
        source_column("passenger_count").cast("int").alias("passenger_count"),
        source_column("trip_distance").cast("double").alias("trip_distance"),
        source_column("fare_amount").cast("double").alias("fare_amount"),
        source_column("tip_amount").cast("double").alias("tip_amount"),
    )
    .filter(F.col("pickup_datetime").isNotNull())
    .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
    .filter(F.col("pickup_location_id").isNotNull() & F.col("dropoff_location_id").isNotNull())
    .filter((F.col("trip_distance") > 0) & (F.col("fare_amount") >= 0))
    .withColumn(
        "row_number",
        F.row_number().over(
            Window.partitionBy("trip_id").orderBy(
                F.col("ingested_at_utc").desc(), F.col("source_record_id").desc()
            )
        ),
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
    .withColumn("trip_minutes", (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0)
    .withColumn("revenue", F.col("fare_amount") + F.coalesce(F.col("tip_amount"), F.lit(0.0)))
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
    .withColumn("is_peak", F.col("pickup_hour").isin([7, 8, 9, 16, 17, 18, 19]).cast("int"))
)

if spark.catalog.tableExists(SILVER_TABLE):
    (
        DeltaTable.forName(spark, SILVER_TABLE)
        .alias("target")
        .merge(silver_candidates.alias("source"), "target.trip_id = source.trip_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_candidates.write.format("delta").partitionBy("pickup_date").saveAsTable(SILVER_TABLE)

# COMMAND ----------


def aggregate_gold(frame):
    return (
        frame.groupBy("pickup_date", "pickup_location_id")
        .agg(
            F.count("trip_id").alias("trip_count"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
            F.round(F.avg("is_peak"), 4).alias("peak_trip_share"),
        )
    )


silver = spark.table(SILVER_TABLE)
affected_dates = [row.pickup_date for row in silver_candidates.select("pickup_date").distinct().collect()]

if spark.catalog.tableExists(GOLD_TABLE):
    for affected_date in affected_dates:
        date_text = affected_date.isoformat()
        date_gold = aggregate_gold(silver.filter(F.col("pickup_date") == F.lit(affected_date)))
        (
            date_gold.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"pickup_date = '{date_text}'")
            .saveAsTable(GOLD_TABLE)
        )
else:
    aggregate_gold(silver).write.format("delta").partitionBy("pickup_date").saveAsTable(GOLD_TABLE)

gold = spark.table(GOLD_TABLE)

# COMMAND ----------

bronze_count = spark.table(BRONZE_TABLE).count()
silver_count = silver.count()
gold_count = gold.count()
duplicate_trips = silver.groupBy("trip_id").count().filter(F.col("count") > 1).count()
duplicate_gold_keys = (
    gold.groupBy("pickup_date", "pickup_location_id").count().filter(F.col("count") > 1).count()
)
checks = [
    (run_id, "bronze_nonempty", bronze_count > 0, bronze_count),
    (run_id, "silver_nonempty", silver_count > 0, silver_count),
    (run_id, "trip_id_complete", silver.filter(F.col("trip_id").isNull()).count() == 0, silver.filter(F.col("trip_id").isNull()).count()),
    (run_id, "trip_id_unique", duplicate_trips == 0, duplicate_trips),
    (run_id, "positive_distance", silver.filter(F.col("trip_distance") <= 0).count() == 0, silver.filter(F.col("trip_distance") <= 0).count()),
    (run_id, "positive_duration", silver.filter(F.col("trip_minutes") <= 0).count() == 0, silver.filter(F.col("trip_minutes") <= 0).count()),
    (run_id, "locations_complete", silver.filter(F.col("pickup_location_id").isNull() | F.col("dropoff_location_id").isNull()).count() == 0, silver.filter(F.col("pickup_location_id").isNull() | F.col("dropoff_location_id").isNull()).count()),
    (run_id, "gold_grain_unique", duplicate_gold_keys == 0, duplicate_gold_keys),
]
dq = (
    spark.createDataFrame(checks, ["run_id", "check_name", "passed", "observed"])
    .withColumn("checked_at_utc", F.lit(checked_at).cast("timestamp"))
)
dq.write.format("delta").mode("append").saveAsTable(DQ_TABLE)

failed_checks = dq.filter(~F.col("passed")).count()
if failed_checks:
    raise ValueError(f"{failed_checks} critical data-quality checks failed")

duration_seconds = round(time.perf_counter() - started, 2)
run_log = spark.createDataFrame(
    [(run_id, checked_at, SOURCE_PATH, bronze_count, silver_count, gold_count, len(affected_dates), duration_seconds, "SUCCEEDED")],
    ["run_id", "run_utc", "source_path", "bronze_rows", "silver_rows", "gold_rows", "date_partitions_refreshed", "duration_seconds", "status"],
)
run_log.write.format("delta").mode("append").saveAsTable(RUN_TABLE)

# COMMAND ----------
# MAGIC %md
# MAGIC Export the small Gold aggregate and current run's quality evidence for Power BI Desktop.

# COMMAND ----------

dbutils.fs.mkdirs(EXPORT_DIR)
gold.orderBy("pickup_date", "pickup_location_id").toPandas().to_csv(
    f"{EXPORT_DIR}/gold_daily_zone_metrics.csv", index=False
)
dq.orderBy("check_name").toPandas().to_csv(
    f"{EXPORT_DIR}/data_quality_results_latest.csv", index=False
)

summary = {
    "run_id": run_id,
    "run_utc": checked_at.isoformat(),
    "source_path": SOURCE_PATH,
    "bronze_rows": bronze_count,
    "silver_rows": silver_count,
    "gold_rows": gold_count,
    "quality_checks_passed": len(checks) - failed_checks,
    "quality_checks_total": len(checks),
    "date_partitions_refreshed": len(affected_dates),
    "duration_seconds": duration_seconds,
}
print(json.dumps(summary, indent=2))
display(dq.orderBy("check_name"))
display(gold.orderBy(F.col("pickup_date").desc(), F.col("trip_count").desc()).limit(20))
