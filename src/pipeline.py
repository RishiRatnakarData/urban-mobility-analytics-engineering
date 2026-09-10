"""Runnable local medallion contract for the Databricks implementation."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.transform import quality_checks, refresh_gold_for_dates, to_silver, upsert_silver

ROOT = Path(__file__).resolve().parents[1]


def run_sample() -> None:
    source = ROOT / "data" / "sample" / "taxi_sample.csv"
    raw = pd.read_csv(source)
    bronze_path = ROOT / "data" / "bronze" / "taxi_sample.parquet"
    silver_path = ROOT / "data" / "silver" / "trips.parquet"
    gold_path = ROOT / "data" / "gold" / "daily_zone_metrics.parquet"
    for path in [bronze_path, silver_path, gold_path]:
        path.parent.mkdir(parents=True, exist_ok=True)
    raw.to_parquet(bronze_path, index=False)
    incoming_silver = to_silver(raw)
    if silver_path.exists():
        current_silver = pd.read_parquet(silver_path)
        for column in ["pickup_datetime", "dropoff_datetime"]:
            current_silver[column] = pd.to_datetime(current_silver[column])
        current_silver["pickup_date"] = pd.to_datetime(current_silver["pickup_date"]).dt.date
    else:
        current_silver = pd.DataFrame()
    silver = upsert_silver(current_silver, raw)
    checks = quality_checks(raw, silver)
    if not checks["passed"].all():
        raise ValueError(f"Data quality failure:\n{checks}")
    if gold_path.exists():
        current_gold = pd.read_parquet(gold_path)
        current_gold["pickup_date"] = pd.to_datetime(current_gold["pickup_date"]).dt.date
    else:
        current_gold = pd.DataFrame()
    affected_dates = sorted(incoming_silver["pickup_date"].unique())
    gold = refresh_gold_for_dates(current_gold, silver, affected_dates)
    silver.to_parquet(silver_path, index=False)
    gold.to_parquet(gold_path, index=False)
    artifacts = ROOT / "artifacts"
    artifacts.mkdir(exist_ok=True)
    checks.to_csv(artifacts / "data_quality_results.csv", index=False)
    summary = {
        "run_utc": datetime.now(UTC).isoformat(),
        "mode": "sample-local",
        "bronze_rows": len(raw),
        "silver_rows": len(silver),
        "gold_rows": len(gold),
        "quality_checks_passed": int(checks["passed"].sum()),
        "date_partitions_refreshed": len(affected_dates),
    }
    (artifacts / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true", required=True)
    parser.parse_args()
    run_sample()


if __name__ == "__main__":
    main()

