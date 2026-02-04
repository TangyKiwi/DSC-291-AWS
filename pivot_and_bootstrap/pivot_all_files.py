from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

import pandas as pd
import dask
import dask.dataframe as dd
from dask import delayed, compute

from io_utils import discover_parquet_files, get_filesystem
from pivot_utils import (
    find_pickup_datetime_col,
    find_pickup_location_col,
    infer_taxi_type_from_path,
    infer_month_from_path,
    pivot_counts_date_taxi_type_location,
    cleanup_low_count_rows,
)
from partition_optimization import (
    parse_size,
    find_optimal_partition_size,
)

import dask.dataframe as dd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------
# Single-file processing
# ---------------------------------------------------
@delayed
def process_single_file(
    file_path: str,
    intermediate_dir: Path,
    min_rides: int,
    partition_size: str | None,
) -> Dict[str, int]:
    """
    Process one Parquet file into an intermediate pivoted Parquet file.
    """
    stats = defaultdict(int)

    expected_month = infer_month_from_path(file_path)
    taxi_type = infer_taxi_type_from_path(file_path)

    ddf = dd.read_parquet(file_path, storage_options={'anon': True})

    if partition_size is not None:
        ddf = ddf.repartition(partition_size=partition_size)

    stats["input_rows"] += int(ddf.shape[0].compute())

    pickup_dt_col = find_pickup_datetime_col(ddf.columns.tolist())
    pickup_loc_col = find_pickup_location_col(ddf.columns.tolist())

    ddf["pickup_datetime"] = dd.to_datetime(ddf[pickup_dt_col], errors="coerce")
    bad_parse = ddf["pickup_datetime"].isna().sum().compute()
    stats["bad_parse_rows"] += int(bad_parse)
    ddf = ddf.dropna(subset=["pickup_datetime"])

    ddf["date"] = ddf["pickup_datetime"].dt.date
    ddf["hour"] = ddf["pickup_datetime"].dt.hour
    ddf["pickup_place"] = ddf[pickup_loc_col]
    ddf["taxi_type"] = taxi_type

    if expected_month is not None:
        y, m = expected_month
        mismatch = (
            (ddf["pickup_datetime"].dt.year != y) |
            (ddf["pickup_datetime"].dt.month != m)
        )
        stats["month_mismatch_rows"] += int(mismatch.sum().compute())

    pdf = ddf.compute()

    pivoted = pivot_counts_date_taxi_type_location(pdf)

    cleaned, cleanup_stats = cleanup_low_count_rows(
        pivoted,
        min_rides=min_rides,
    )

    for k, v in cleanup_stats.items():
        stats[k] += v
    stats["output_rows"] += len(cleaned)

    out_path = intermediate_dir / f"{Path(file_path).stem}_pivot.parquet"
    cleaned.to_parquet(out_path, index=False)

    return stats

# ---------------------------------------------------
# Month processing
# ---------------------------------------------------
@delayed
def process_month(
    files,
    intermediate_dir,
    min_rides,
    partition_size,
):
    """
    Schedule all files in a month in parallel.
    """
    file_tasks = [
        process_single_file(
            file_path=f,
            intermediate_dir=intermediate_dir,
            min_rides=min_rides,
            partition_size=partition_size,
        )
        for f in files
    ]

    file_stats = merge_stats_dicts(file_tasks)
    return file_stats


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

@delayed
def merge_stats_dicts(stats_list):
    merged = defaultdict(int)
    for stats in stats_list:
        for k, v in stats.items():
            merged[k] += int(v)
    return dict(merged)

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

    if args.workers > 1:
        from dask.distributed import Client, LocalCluster
        cluster = LocalCluster(n_workers=args.workers, threads_per_worker=1)
        client = Client(cluster)
        logger.info("Using Dask distributed with %d workers", args.workers)
    else:
        client = None
        logger.info("Using single-threaded Dask")

    input_dir = args.input_dir
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    intermediate_dir = output_dir / "intermediate"
    intermediate_dir.mkdir(exist_ok=True)

    files = discover_parquet_files(input_dir)
    logger.info("Discovered %d Parquet files", len(files))

    # Group by (year, month)
    files_by_month: Dict[Tuple[int, int], List[str]] = defaultdict(list)
    for f in files:
        ym = infer_month_from_path(f)
        if ym:
            files_by_month[ym].append(f)

    month_tasks = []

    for (year, month), month_files in sorted(files_by_month.items()):
        logger.info("Scheduling %04d-%02d (%d files)", year, month, len(month_files))

        partition_size = args.partition_size

        if not args.skip_partition_optimization and partition_size is None:
            fs, fs_path = get_filesystem(month_files[0])
            candidate_sizes = [
                parse_size(s) for s in ["64MB", "128MB", "256MB"]    
            ]
            optimal = find_optimal_partition_size(
                fs_path,
                candidate_sizes=candidate_sizes,
                max_memory_usage=2 * 1024**3,  # 2 GB
                filesystem=fs
            )
            partition_size = f"{optimal}B"
            logger.info("Optimal partition size for %04d-%02d: %s", year, month, partition_size)

        task = process_month(
            files=month_files,
            intermediate_dir=intermediate_dir,
            min_rides=args.min_rides,
            partition_size=partition_size
        )
        month_tasks.append(task)

    month_stats = compute(*month_tasks)
    final_stats = merge_stats_dicts(month_stats).compute()  

    final_output = output_dir / "taxi_wide_table.parquet"
    final_rows = combine_into_wide_table(intermediate_dir, final_output)

    final_stats["final_output_rows"] = final_rows
    final_stats["runtime_seconds"] = int(time.time() - start_time)

    logger.info("Pipeline complete")
    for k, v in final_stats.items():
        logger.info("%s: %s", k, v)

    if not args.keep_intermediate:
        for p in intermediate_dir.glob("*.parquet"):
            p.unlink()

    if client is not None:
        if args.verbose:
            logger.info("Shutting down Dask client")
        client.close()

if __name__ == "__main__":
    main()
