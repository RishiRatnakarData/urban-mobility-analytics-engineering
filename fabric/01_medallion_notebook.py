# Fabric notebook source. Paste cells into a Fabric PySpark notebook after
# uploading a Parquet source file to Files/landing/yellow_tripdata.parquet.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE = "Files/landing/yellow_tripdata.parquet"
BRONZE_TABLE = "bronze_taxi_trips"
SILVER_TABLE = "silver_taxi_trips"
GOLD_TABLE = "gold_daily_zone_metrics"
DQ_TABLE = "data_quality_results"

# Cell 1 - idempotent Bronze ingestion with source metadata.
raw = (
    spark.read.parquet(SOURCE)
    .withColumn("source_file", F.input_file_name())
    .withColumn("ingested_at_utc", F.current_timestamp())
)
raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_TABLE)

# Cell 2 - normalize NYC TLC field names and create a deterministic trip key.
bronze = spark.table(BRONZE_TABLE)
silver = (
    bronze
    .select(
        F.sha2(F.concat_ws("|", F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime"), F.col("PULocationID"), F.col("DOLocationID"), F.col("fare_amount")), 256).alias("trip_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("passenger_count").cast("int"),
        F.col("trip_distance").cast("double"),
        F.col("fare_amount").cast("double"),
        F.col("tip_amount").cast("double"),
        F.col("total_amount").cast("double"),
    )
    .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
    .filter((F.col("trip_distance") > 0) & (F.col("fare_amount") >= 0))
    .withColumn("row_number", F.row_number().over(Window.partitionBy("trip_id").orderBy(F.col("pickup_datetime").desc())))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .withColumn("trip_minutes", (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0)
    .withColumn("is_peak", F.hour("pickup_datetime").isin([7, 8, 9, 16, 17, 18, 19]).cast("int"))
)
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TABLE)

# Cell 3 - persisted data-quality evidence. Fail the notebook if critical checks fail.
silver_df = spark.table(SILVER_TABLE)
checks = [
    ("bronze_nonempty", bronze.count() > 0, bronze.count()),
    ("silver_nonempty", silver_df.count() > 0, silver_df.count()),
    ("trip_id_complete", silver_df.filter(F.col("trip_id").isNull()).count() == 0, silver_df.filter(F.col("trip_id").isNull()).count()),
    ("trip_id_unique", silver_df.groupBy("trip_id").count().filter(F.col("count") > 1).count() == 0, silver_df.groupBy("trip_id").count().filter(F.col("count") > 1).count()),
    ("positive_distance", silver_df.filter(F.col("trip_distance") <= 0).count() == 0, silver_df.filter(F.col("trip_distance") <= 0).count()),
    ("positive_duration", silver_df.filter(F.col("trip_minutes") <= 0).count() == 0, silver_df.filter(F.col("trip_minutes") <= 0).count()),
]
dq = spark.createDataFrame(checks, ["check_name", "passed", "failed_or_observed_rows"]).withColumn("checked_at_utc", F.current_timestamp())
dq.write.format("delta").mode("append").saveAsTable(DQ_TABLE)
if dq.filter(~F.col("passed")).count() > 0:
    raise ValueError("Critical data-quality checks failed")

# Cell 4 - Gold serving table.
gold = (
    silver_df.groupBy("pickup_date", "pickup_location_id")
    .agg(
        F.count("trip_id").alias("trip_count"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_minutes"), 2).alias("avg_trip_minutes"),
        F.round(F.avg("is_peak"), 4).alias("peak_trip_share"),
    )
)
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_TABLE)

# Cell 5 - capture values for README claims ledger.
display(dq.orderBy("check_name"))
display(spark.sql(f"SELECT COUNT(*) AS bronze_rows FROM {BRONZE_TABLE}"))
display(spark.sql(f"SELECT COUNT(*) AS silver_rows FROM {SILVER_TABLE}"))
display(spark.sql(f"SELECT COUNT(*) AS gold_rows FROM {GOLD_TABLE}"))

