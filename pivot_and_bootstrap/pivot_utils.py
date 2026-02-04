from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, Tuple, Dict, List

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


# ----------------------------
# Column detection utilities
# ----------------------------

def find_pickup_datetime_col(columns: List[str]) -> str:
    """
    Detect pickup datetime column name (case-insensitive).
    """
    candidates = [
        "tpep_pickup_datetime",
        "pickup_datetime",
        "lpep_pickup_datetime",
    ]
    lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    raise KeyError("Pickup datetime column not found")


def find_pickup_location_col(columns: List[str]) -> str:
    """
    Detect pickup location column name.
    """
    candidates = [
        "pulocationid",
        "pickup_location_id",
        "pickup_location",
    ]
    lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    raise KeyError("Pickup location column not found")


def infer_taxi_type_from_path(file_path: str | Path) -> str:
    """
    Infer taxi type (yellow, green, fhv, hvfhv) from file path.
    """
    path = str(file_path).lower()
    for taxi in ["yellow", "green", "fhv", "hvfhv"]:
        if taxi in path:
            return taxi
    return "unknown"


# ----------------------------
# Month inference
# ----------------------------

def infer_month_from_path(file_path: str | Path) -> Optional[Tuple[int, int]]:
    """
    Infer (year, month) from path patterns like:
    - yellow_tripdata_2023-01.parquet
    - year=2023/month=01/
    """
    path = str(file_path)

    # Case 1: Hive-style partitions year=YYYY/month=MM
    m = re.search(r"year=(20\d{2})/month=(0[1-9]|1[0-2])", path)
    if m:
        return int(m.group(1)), int(m.group(2))

    # Case 2: Filename-based YYYY-MM
    m = re.search(r"(20\d{2})[-_/](0[1-9]|1[0-2])", path)
    if m:
        return int(m.group(1)), int(m.group(2))

    return None


# ----------------------------
# Pivoting
# ----------------------------

def pivot_counts_date_taxi_type_location(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot counts into wide format:
    index = (taxi_type, date, pickup_place)
    columns = hour_0 ... hour_23
    """
    grouped = (
        pdf
        .groupby(["taxi_type", "date", "pickup_place", "hour"])
        .size()
        .rename("count")
        .reset_index()
    )

    pivoted = (
        grouped
        .pivot_table(
            index=["taxi_type", "date", "pickup_place"],
            columns="hour",
            values="count",
            fill_value=0,
        )
    )

    pivoted.columns = [f"hour_{int(h)}" for h in pivoted.columns]
    pivoted = pivoted.reset_index()

    return pivoted


# ----------------------------
# Cleanup
# ----------------------------

def cleanup_low_count_rows(
    df: pd.DataFrame,
    min_rides: int = 50,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Drop rows where total rides across all hour columns < min_rides.
    """
    hour_cols = [c for c in df.columns if c.startswith("hour_")]
    totals = df[hour_cols].sum(axis=1)

    keep_mask = totals >= min_rides
    dropped = int((~keep_mask).sum())

    cleaned = df.loc[keep_mask].reset_index(drop=True)

    stats = {
        "rows_dropped_low_count": dropped,
        "rows_kept": int(keep_mask.sum()),
    }

    return cleaned, stats
