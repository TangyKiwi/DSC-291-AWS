import pandas as pd
import numpy as np
import pytest

from pivot_utils import (
    find_pickup_datetime_col,
    find_pickup_location_col,
    infer_month_from_path,
    pivot_counts_date_taxi_type_location,
    cleanup_low_count_rows,
)


# ---------------------------------------------------
# Column detection
# ---------------------------------------------------

def test_find_pickup_datetime_col():
    cols = ["VendorID", "tpep_pickup_datetime", "fare_amount"]
    assert find_pickup_datetime_col(cols) == "tpep_pickup_datetime"


def test_find_pickup_location_col():
    cols = ["PULocationID", "DOLocationID"]
    assert find_pickup_location_col(cols) == "PULocationID"


# ---------------------------------------------------
# Month inference
# ---------------------------------------------------

def test_infer_month_from_path_filename():
    path = "yellow_tripdata_2023-01.parquet"
    assert infer_month_from_path(path) == (2023, 1)


def test_infer_month_from_path_partitioned():
    path = "s3://bucket/year=2022/month=11/data.parquet"
    assert infer_month_from_path(path) == (2022, 11)


def test_infer_month_from_path_none():
    path = "s3://bucket/data.parquet"
    assert infer_month_from_path(path) is None


# ---------------------------------------------------
# Pivot correctness
# ---------------------------------------------------

def test_pivot_counts_basic():
    df = pd.DataFrame({
        "taxi_type": ["yellow", "yellow", "yellow"],
        "date": [pd.to_datetime("2023-01-01").date()] * 3,
        "pickup_place": [1, 1, 1],
        "hour": [0, 1, 1],
    })

    pivoted = pivot_counts_date_taxi_type_location(df)

    assert "hour_0" in pivoted.columns
    assert "hour_1" in pivoted.columns
    assert pivoted.loc[0, "hour_0"] == 1
    assert pivoted.loc[0, "hour_1"] == 2


# ---------------------------------------------------
# Cleanup logic
# ---------------------------------------------------

def test_cleanup_low_count_rows_drops():
    df = pd.DataFrame({
        "taxi_type": ["yellow"],
        "date": [pd.to_datetime("2023-01-01").date()],
        "pickup_place": [1],
        "hour_0": [10],
        "hour_1": [5],
    })

    cleaned, stats = cleanup_low_count_rows(df, min_rides=50)

    assert len(cleaned) == 0
    assert stats["rows_dropped_low_count"] == 1


def test_cleanup_low_count_rows_keeps():
    df = pd.DataFrame({
        "taxi_type": ["yellow"],
        "date": [pd.to_datetime("2023-01-01").date()],
        "pickup_place": [1],
        "hour_0": [30],
        "hour_1": [25],
    })

    cleaned, stats = cleanup_low_count_rows(df, min_rides=50)

    assert len(cleaned) == 1
    assert stats["rows_kept"] == 1
