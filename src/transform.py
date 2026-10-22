"""Shared local Bronze -> Silver -> Gold transformations.

The Databricks/Delta implementation remains independently versioned in databricks/.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

REQUIRED_COLUMNS = {
    "trip_id", "pickup_datetime", "dropoff_datetime", "pickup_zone",
    "dropoff_zone", "passenger_count", "trip_distance", "fare_amount", "tip_amount",
}


def to_silver(raw: pd.DataFrame) -> pd.DataFrame:
    """Keep the latest valid occurrence per trip ID, then derive analytics features."""
    missing = sorted(REQUIRED_COLUMNS - set(raw.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    df = raw.copy()
    for column in ["pickup_datetime", "dropoff_datetime"]:
        df[column] = pd.to_datetime(df[column], errors="coerce")
    for column in ["passenger_count", "trip_distance", "fare_amount", "tip_amount"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.replace([np.inf, -np.inf], np.nan)
    # Filter before deduplicating: a later invalid duplicate must not remove a valid trip.
    df = df.dropna(subset=[
        "trip_id", "pickup_datetime", "dropoff_datetime", "pickup_zone",
        "dropoff_zone", "trip_distance", "fare_amount",
    ])
    df = df[df["trip_id"].astype(str).str.strip().ne("")]
    df = df[
        (df["dropoff_datetime"] > df["pickup_datetime"])
        & (df["trip_distance"] > 0)
        & (df["fare_amount"] >= 0)
    ]
    df = df.drop_duplicates(subset="trip_id", keep="last").copy()
    df["trip_minutes"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds() / 60
    df["revenue"] = df["fare_amount"] + df["tip_amount"].fillna(0)
    df["pickup_date"] = df["pickup_datetime"].dt.date
    df["pickup_hour"] = df["pickup_datetime"].dt.hour
    df["is_peak"] = df["pickup_hour"].isin([7, 8, 9, 16, 17, 18, 19]).astype(int)
    return df.sort_values(["pickup_datetime", "trip_id"]).reset_index(drop=True)


def quality_checks(raw: pd.DataFrame, silver: pd.DataFrame) -> pd.DataFrame:
    """Preserved six-check interface for the existing tests and local consumers."""
    checks = [
        ("bronze_nonempty", len(raw) > 0, len(raw)),
        ("silver_nonempty", len(silver) > 0, len(silver)),
        ("trip_id_unique", silver["trip_id"].is_unique, int(silver["trip_id"].duplicated().sum())),
        ("positive_distance", silver["trip_distance"].gt(0).all(), int(silver["trip_distance"].le(0).sum())),
        ("positive_duration", silver["trip_minutes"].gt(0).all(), int(silver["trip_minutes"].le(0).sum())),
        (
            "zones_complete",
            silver[["pickup_zone", "dropoff_zone"]].notna().all().all(),
            int(silver[["pickup_zone", "dropoff_zone"]].isna().sum().sum()),
        ),
    ]
    return pd.DataFrame(checks, columns=["check_name", "passed", "observed"])


def to_gold(silver: pd.DataFrame) -> pd.DataFrame:
    return (
        silver.groupby(["pickup_date", "pickup_zone"], as_index=False)
        .agg(
            trip_count=("trip_id", "count"),
            total_revenue=("revenue", "sum"),
            avg_distance=("trip_distance", "mean"),
            avg_trip_minutes=("trip_minutes", "mean"),
            peak_trip_share=("is_peak", "mean"),
        )
        .sort_values(["pickup_date", "trip_count"], ascending=[True, False])
        .reset_index(drop=True)
    )


def upsert_silver(current: pd.DataFrame, incoming_raw: pd.DataFrame) -> pd.DataFrame:
    """Insert new trips and replace existing IDs with their latest valid record."""
    incoming = to_silver(incoming_raw)
    if current.empty:
        return incoming
    combined = pd.concat([current, incoming], ignore_index=True)
    return (
        combined.drop_duplicates(subset="trip_id", keep="last")
        .sort_values(["pickup_datetime", "trip_id"])
        .reset_index(drop=True)
    )


def affected_dates_for_upsert(
    current_silver: pd.DataFrame, incoming_silver: pd.DataFrame,
) -> list[object]:
    """Include old *and* new dates of replaced IDs to prevent stale Gold rows."""
    if incoming_silver.empty:
        return []
    affected = set(incoming_silver["pickup_date"].dropna().tolist())
    if not current_silver.empty:
        previous = current_silver[
            current_silver["trip_id"].isin(incoming_silver["trip_id"])
        ]
        affected.update(previous["pickup_date"].dropna().tolist())
    return sorted(affected)


def refresh_gold_for_dates(
    current_gold: pd.DataFrame,
    silver: pd.DataFrame,
    affected_dates: list[object],
) -> pd.DataFrame:
    """Recompute touched partitions, dropping old dates that become empty."""
    dates = set(affected_dates)
    if not dates:
        return current_gold.copy()
    refreshed = to_gold(silver[silver["pickup_date"].isin(dates)])
    if current_gold.empty:
        return refreshed
    unchanged = current_gold[~current_gold["pickup_date"].isin(dates)]
    return (
        pd.concat([unchanged, refreshed], ignore_index=True)
        .sort_values(["pickup_date", "trip_count"], ascending=[True, False])
        .reset_index(drop=True)
    )
