from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

import pandas as pd

from pivot_utils import infer_month_from_path
from io_utils import discover_parquet_files
from partition_optimization import parse_size, find_optimal_partition_size
from pivot_utils import (
    find_pickup_datetime_col,
    find_pickup_location_col,
    infer_taxi_type_from_path,
    pivot_counts_date_taxi_type_location,
    cleanup_low_count_rows,
)

import dask.dataframe as dd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------
# Single-file processing
# ---------------------------------------------------

def process_single_file(
    file_path: str,
    intermediate_dir: Path,
    min_rides: int,
) -> Dict[str, int]:
    """
    Process one Parquet file into an intermediate pivoted Parquet file.
    """
    stats = defaultdict(int)

    expected_month = infer_month_from_path(file_path)
    taxi_type = infer_taxi_type_from_path(file_path)

    ddf = dd.read_parquet(file_path)
    stats["input_rows"] += int(ddf.shape[0].compute())

    pdf = ddf.compute()

    pickup_dt_col = find_pickup_datetime_col(pdf.columns.tolist())
    pickup_loc_col = find_pickup_location_col(pdf.columns.tolist())

    pdf["pickup_datetime"] = pd.to_datetime(pdf[pickup_dt_col], errors="coerce")
    bad_parse = pdf["pickup_datetime"].isna().sum()
    stats["bad_parse_rows"] += int(bad_parse)
    pdf = pdf.dropna(subset=["pickup_datetime"])

    pdf["date"] = pdf["pickup_datetime"].dt.date
    pdf["hour"] = pdf["pickup_datetime"].dt.hour
    pdf["pickup_place"] = pdf[pickup_loc_col]
    pdf["taxi_type"] = taxi_type

    if expected_month is not None:
        y, m = expected_month
        mismatch = (
            (pdf["pickup_datetime"].dt.year != y) |
            (pdf["pickup_datetime"].dt.month != m)
        )
        stats["month_mismatch_rows"] += int(mismatch.sum())

    pivoted = pivot_counts_date_taxi_type_location(pdf)

    cleaned, cleanup_stats = cleanup_low_count_rows(
        pivoted,
        min_rides=min_rides,
    )

    for k, v in cleanup_stats.items():
        stats[k] += v

    out_path = intermediate_dir / f"{Path(file_path).stem}_pivot.parquet"
    cleaned.to_parquet(out_path, index=False)

    stats["output_rows"] += len(cleaned)

    return stats


# ---------------------------------------------------
# Combine all intermediate files
# ---------------------------------------------------

def combine_into_wide_table(
    intermediate_dir: Path,
    output_path: Path,
) -> int:
    """
    Combine all intermediate Parquet files into one wide table.
    """
    files = list(intermediate_dir.glob("*.parquet"))
    dfs = [pd.read_parquet(p) for p in files]

    combined = pd.concat(dfs, ignore_index=True)
    hour_cols = [c for c in combined.columns if c.startswith("hour_")]

    final = (
        combined
        .groupby(["taxi_type", "date", "pickup_place"], as_index=False)[hour_cols]
        .sum()
    )

    final.to_parquet(output_path, index=False)
    return len(final)


# ---------------------------------------------------
# CLI
# ---------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Taxi pivot pipeline")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--min-rides", type=int, default=50)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--partition-size", type=str, default=None)
    parser.add_argument("--skip-partition-optimization", action="store_true")
    parser.add_argument("--keep-intermediate", action="store_true")

    args = parser.parse_args()

    start_time = time.time()

    input_dir = args.input_dir
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    intermediate_dir = output_dir / "intermediate"
    intermediate_dir.mkdir(exist_ok=True)

    files = discover_parquet_files(input_dir)

    # Group by (year, month)
    files_by_month: Dict[Tuple[int, int], List[str]] = defaultdict(list)
    for f in files:
        ym = infer_month_from_path(f)
        if ym is not None:
            files_by_month[ym].append(f)

    global_stats = defaultdict(int)

    for (year, month), month_files in sorted(files_by_month.items()):
        logger.info("Processing %04d-%02d (%d files)", year, month, len(month_files))

        for f in month_files:
            stats = process_single_file(
                f,
                intermediate_dir=intermediate_dir,
                min_rides=args.min_rides,
            )
            for k, v in stats.items():
                global_stats[k] += v

    final_output = output_dir / "taxi_wide_table.parquet"
    final_rows = combine_into_wide_table(intermediate_dir, final_output)

    global_stats["final_output_rows"] = final_rows
    global_stats["runtime_seconds"] = int(time.time() - start_time)

    logger.info("Pipeline complete")
    for k, v in global_stats.items():
        logger.info("%s: %s", k, v)

    if not args.keep_intermediate:
        for p in intermediate_dir.glob("*.parquet"):
            p.unlink()


if __name__ == "__main__":
    main()
