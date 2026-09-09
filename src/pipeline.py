"""Runnable local medallion pipeline; Fabric implementation lives in fabric/."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.transform import quality_checks, to_gold, to_silver

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
    silver = to_silver(raw)
    checks = quality_checks(raw, silver)
    if not checks["passed"].all():
        raise ValueError(f"Data quality failure:\n{checks}")
    gold = to_gold(silver)
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

