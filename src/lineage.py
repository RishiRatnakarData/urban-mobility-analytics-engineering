"""Small, auditable local batch lineage and upsert accounting."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


def upsert_counts(current: pd.DataFrame, incoming: pd.DataFrame, raw_rows: int) -> dict:
    """Count inserted and replaced IDs; replacements include unchanged reruns."""
    current_ids: set = set() if current.empty else set(current["trip_id"])
    incoming_ids: set = set(incoming["trip_id"])
    return {
        "incoming_bronze_rows": int(raw_rows),
        "incoming_valid_unique_trips": len(incoming_ids),
        "incoming_filtered_or_duplicate_rows": int(raw_rows - len(incoming_ids)),
        "new_trip_ids": len(incoming_ids - current_ids),
        "existing_trip_ids_matched": len(incoming_ids & current_ids),
        "matching_id_definition": "existing IDs matched; does not imply changed values",
    }


def partition_labels(dates: Iterable) -> list[str]:
    return sorted({str(value) for value in dates})
