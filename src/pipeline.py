"""Local medallion pipeline with idempotent upserts, cross-layer checks, and lineage."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

from src.lineage import partition_labels, upsert_counts
from src.quality_contract import validated_quality_checks
from src.transform import affected_dates_for_upsert, refresh_gold_for_dates, to_silver, upsert_silver

ROOT = Path(__file__).resolve().parents[1]


def load_source(path: Path) -> pd.DataFrame:
    """Load CSV or Parquet from a local batch. Never mutate the source file."""
    if not path.is_file():
        raise FileNotFoundError(f"Input file does not exist: {path}")
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    raise ValueError("Input must be a CSV or Parquet file")


def _read_silver(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    for column in ["pickup_datetime", "dropoff_datetime"]:
        frame[column] = pd.to_datetime(frame[column])
    frame["pickup_date"] = pd.to_datetime(frame["pickup_date"]).dt.date
    return frame


def _read_gold(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    frame["pickup_date"] = pd.to_datetime(frame["pickup_date"]).dt.date
    return frame


def _fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_parquet_replacement(frame: pd.DataFrame, destination: Path) -> None:
    """Write to temporary file before atomic single-file replacement."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    staged = destination.with_name(destination.stem + f".{uuid4().hex}.tmp.parquet")
    try:
        frame.to_parquet(staged, index=False)
        os.replace(staged, destination)
    finally:
        staged.unlink(missing_ok=True)


def run_batch(source: Path, *, sample: bool = False) -> dict:
    """Run one local batch; return a reproducible accounting of changed date partitions."""
    source = source.resolve()
    raw = load_source(source)
    if raw.empty:
        raise ValueError("Empty input batch; no tables were modified")

    silver_path = ROOT / "data" / "silver" / "trips.parquet"
    gold_path = ROOT / "data" / "gold" / "daily_zone_metrics.parquet"
    current_silver = _read_silver(silver_path)
    current_gold = _read_gold(gold_path)
    incoming_silver = to_silver(raw)
    if incoming_silver.empty:
        raise ValueError("No valid trips in incoming batch; no tables were modified")
    silver = upsert_silver(current_silver, raw)
    affected_dates = affected_dates_for_upsert(current_silver, incoming_silver)
    gold = refresh_gold_for_dates(current_gold, silver, affected_dates)

    checks = validated_quality_checks(raw, silver, gold)
    if not checks["passed"].all():
        failed = checks.loc[~checks["passed"], ["check_name", "observed"]]
        raise ValueError(f"Cross-layer data quality gate failed before writes:\n{failed}")

    # Generate and verify both staged Parquet files before replacing final paths.
    for output in [silver_path, gold_path]:
        output.parent.mkdir(parents=True, exist_ok=True)
    silver_staged = silver_path.with_name(f"trips.{uuid4().hex}.tmp.parquet")
    gold_staged = gold_path.with_name(f"daily_zone_metrics.{uuid4().hex}.tmp.parquet")
    try:
        silver.to_parquet(silver_staged, index=False)
        gold.to_parquet(gold_staged, index=False)
        # Local Parquet file replacement is not a multi-table ACID transaction.
        os.replace(silver_staged, silver_path)
        os.replace(gold_staged, gold_path)
    finally:
        silver_staged.unlink(missing_ok=True)
        gold_staged.unlink(missing_ok=True)

    digest = _fingerprint(source)
    bronze_path = (
        ROOT / "data" / "bronze" / "taxi_sample.parquet"
        if sample else ROOT / "data" / "bronze" / f"batch_{digest[:16]}.parquet"
    )
    if not bronze_path.exists():
        _write_parquet_replacement(raw, bronze_path)

    artifacts = ROOT / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    checks.to_csv(artifacts / "data_quality_results.csv", index=False)
    summary = {
        "run_utc": datetime.now(UTC).isoformat(),
        "mode": "sample-local" if sample else "local-input",
        "source_file_name": source.name,
        "source_sha256": digest,
        "source_byte_count": source.stat().st_size,
        "bronze_batch_rows": len(raw),
        "silver_rows": len(silver),
        "gold_rows": len(gold),
        "quality_checks_passed": int(checks["passed"].sum()),
        "quality_checks_total": len(checks),
        "date_partitions_refreshed": len(affected_dates),
        "affected_dates": partition_labels(affected_dates),
        "upsert": upsert_counts(current_silver, incoming_silver, len(raw)),
    }
    (artifacts / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    with (artifacts / "run_history.jsonl").open("a", encoding="utf-8") as history:
        history.write(json.dumps(summary) + "\n")
    print(json.dumps(summary, indent=2))
    return summary


def run_sample() -> None:
    run_batch(ROOT / "data" / "sample" / "taxi_sample.csv", sample=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Local NYC taxi medallion batch")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sample", action="store_true")
    source.add_argument("--input", type=Path, help="CSV or Parquet batch")
    args = parser.parse_args()
    if args.sample:
        run_sample()
    else:
        run_batch(args.input)


if __name__ == "__main__":
    main()
