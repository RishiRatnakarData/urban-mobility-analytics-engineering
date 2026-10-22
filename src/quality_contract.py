"""Cross-layer quality checks for the local medallion pipeline."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from src.transform import quality_checks


def validated_quality_checks(
    raw: pd.DataFrame, silver: pd.DataFrame, gold: pd.DataFrame,
) -> pd.DataFrame:
    """Extend baseline checks with Gold grain and Bronze/Silver/Gold reconciliation."""
    baseline = quality_checks(raw, silver)
    duplicate_gold = int(gold.duplicated(subset=["pickup_date", "pickup_zone"]).sum())
    count_total = int(gold["trip_count"].sum()) if not gold.empty else 0
    silver_count = len(silver)
    silver_revenue = float(silver["revenue"].sum())
    gold_revenue = float(gold["total_revenue"].sum())
    tolerance = 1e-8 * max(1.0, abs(silver_revenue))
    metrics = [
        ("gold_grain_unique", duplicate_gold == 0, duplicate_gold),
        ("gold_trip_counts_reconciled", count_total == silver_count, count_total - silver_count),
        (
            "gold_revenue_reconciled",
            math.isclose(gold_revenue, silver_revenue, rel_tol=1e-8, abs_tol=tolerance),
            round(gold_revenue - silver_revenue, 6),
        ),
        (
            "gold_metrics_finite",
            bool(np.isfinite(gold[["trip_count", "total_revenue", "avg_distance", "avg_trip_minutes", "peak_trip_share"]]
                 .apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float)).all()),
            int((~np.isfinite(gold[["trip_count", "total_revenue", "avg_distance", "avg_trip_minutes", "peak_trip_share"]]
                .apply(pd.to_numeric, errors="coerce").to_numpy(dtype=float))).sum()),
        ),
    ]
    return pd.concat(
        [baseline, pd.DataFrame(metrics, columns=["check_name", "passed", "observed"])],
        ignore_index=True,
    )
