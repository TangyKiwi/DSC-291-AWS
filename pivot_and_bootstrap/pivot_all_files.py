from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

import pyarrow.parquet as pq
import dask.dataframe as dd
from dask import delayed, compute

from io_utils import discover_parquet_files, get_filesystem, is_s3_path
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import geopandas as gpd
from shapely.geometry import Point

taxi_zones = gpd.read_file("taxi_zones.shx")
taxi_zones = taxi_zones.set_crs(epsg=2263).to_crs(epsg=4326)
sindex = taxi_zones.sindex

def find_pickup_place(lat, lon):
    point = Point(lon, lat)
    for idx in sindex.intersection(point.bounds):
        if taxi_zones.geometry.iloc[idx].covers(point):
            return idx + 1
    return None

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
    Process one Parquet file into an intermediate cleaned Parquet file.
    """
    logger = logging.getLogger(__name__)
    stats = defaultdict(int)

    logger.info("[START file] %s", file_path)

    expected_month = infer_month_from_path(file_path)
    taxi_type = infer_taxi_type_from_path(file_path)

    schema = pq.read_schema(file_path, filesystem=get_filesystem(file_path))

    # ddf = dd.read_parquet(file_path, storage_options={'anon': True} if is_s3_path(file_path) else None)

    # if partition_size is not None:
    #     ddf = ddf.repartition(partition_size=partition_size)

    # input_rows = int(ddf.shape[0].compute())
    # stats["input_rows"] += input_rows
    # logger.info(
    #     "[READ file] %s rows=%d partitions=%d",
    #     file_path,
    #     input_rows,
    #     ddf.npartitions,
    # )

    pickup_dt_col = find_pickup_datetime_col(schema.names)
    pickup_loc_col = find_pickup_location_col(schema.names)

    read_cols = [pickup_dt_col]
    lat_col, lon_col = None, None
    if pickup_loc_col:
        read_cols.append(pickup_loc_col)
    else:
        pickup_loc_col = 'pickup_place'
        schema_cols = {col.lower(): col for col in schema.names}
        if 'start_lat' in schema_cols and 'start_lon' in schema_cols:
            lat_col = schema_cols['start_lat']
            lon_col = schema_cols['start_lon']
        elif 'pickup_latitude' in schema_cols and 'pickup_longitude' in schema_cols:
            lat_col = schema_cols['pickup_latitude']
            lon_col = schema_cols['pickup_longitude']
        read_cols.extend([lat_col, lon_col])

    ddf = dd.read_parquet(file_path, columns=read_cols, storage_options={'anon': True} if is_s3_path(file_path) else None)
    input_rows = len(ddf)
    stats["input_rows"] += input_rows

    if partition_size is not None:
        ddf = ddf.repartition(partition_size=partition_size)

    if lat_col and lon_col:
        ddf['pickup_place'] = ddf.apply(lambda row: find_pickup_place(row[lat_col], row[lon_col]), axis=1, meta=('pickup_place', 'int64'))
        ddf.drop(columns=[lat_col, lon_col])

    ddf[pickup_dt_col] = dd.to_datetime(ddf[pickup_dt_col], errors="coerce")
    y, m = expected_month
    mismatch_mask = (ddf[pickup_dt_col].dt.year != y) | (ddf[pickup_dt_col].dt.month != m)
    mismatch_count = mismatch_mask.sum()
    stats["month_mismatch_rows"] += mismatch_count
    ddf = ddf[~mismatch_mask]

    ddf = ddf.assign(
        date=ddf[pickup_dt_col].dt.date,
        hour=ddf[pickup_dt_col].dt.hour,
        pickup_place=ddf[pickup_loc_col],
        taxi_type=taxi_type,
    )

    agg = ddf.groupby(["taxi_type", "date", "pickup_place", "hour"]).size().to_frame("count").reset_index()
    pivoted = pivot_counts_date_taxi_type_location(agg)
    cleaned, cleanup_stats = cleanup_low_count_rows(pivoted, min_rides)

    for k, v in cleanup_stats.items():
        stats[k] += v
    stats["output_rows"] += len(cleaned)

    out_path = f"{intermediate_dir}/{Path(file_path).stem}_pivot.parquet"

    logger.info("Writing intermediate parquet to %s", out_path)

    cleaned.to_parquet(
        out_path,
        storage_options={"anon": True} if is_s3_path(out_path) else None,
    )

    # ddf["pickup_datetime"] = dd.to_datetime(ddf[pickup_dt_col], errors="coerce")
    # bad_parse = ddf["pickup_datetime"].isna().sum().compute()
    # stats["bad_parse_rows"] += int(bad_parse)
    # ddf = ddf.dropna(subset=["pickup_datetime"])

    # ddf = ddf.assign(
    #     date=ddf["pickup_datetime"].dt.date,
    #     hour=ddf["pickup_datetime"].dt.hour,
    #     pickup_place=ddf[pickup_loc_col],
    #     taxi_type=taxi_type,
    # )

    # if expected_month is not None:
    #     y, m = expected_month
    #     mismatch = int((
    #         (ddf["pickup_datetime"].dt.year != y) |
    #         (ddf["pickup_datetime"].dt.month != m)
    #     ).sum().compute())
    #     stats["month_mismatch_rows"] += mismatch
    
    #     logger.info(
    #         "[AGG file] %s bad_parse=%d month_mismatch=%d",
    #         file_path,
    #         bad_parse,
    #         stats["month_mismatch_rows"],
    #     )

    # pdf = ddf.compute()
    # pivoted = pivot_counts_date_taxi_type_location(pdf)
    # cleaned, cleanup_stats = cleanup_low_count_rows(pivoted, min_rides)

    # for k, v in cleanup_stats.items():
    #     stats[k] += v
    # stats["output_rows"] += len(cleaned)

    # out_path = f"{intermediate_dir}/{Path(file_path).stem}_pivot.parquet"

    # logger.info("Writing intermediate parquet to %s", out_path)

    # cleaned.to_parquet(
    #     out_path,
    #     storage_options={"anon": True} if is_s3_path(out_path) else None,
    # )

    # logger.info(
    #     "[DONE file] %s output_rows=%d dropped_low_count=%d",
    #     file_path,
    #     stats["output_rows"],
    #     stats["rows_dropped_low_count"],
    # )

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
    logger = logging.getLogger(__name__)
    logger.info(
        "[START month] %d files partition_size=%s",
        len(files),
        partition_size,
    )

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
    logger.info("[DONE month] %d files complete", len(files))
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
    logger.info("Reading intermediates from %s", intermediate_dir)

    ddf = dd.read_parquet(
        intermediate_dir,
        storage_options={"anon": True} if is_s3_path(intermediate_dir) else None,
    )

    hour_cols = [c for c in ddf.columns if c.startswith("hour_")]

    final = (
        ddf
        .groupby(["taxi_type", "date", "pickup_place"])[hour_cols]
        .sum()
        .reset_index()
    )

    logger.info("Writing final output to %s", output_path)

    final.to_parquet(
        output_path,
        write_index=False,
        storage_options={"anon": True} if is_s3_path(output_path) else None,
    )

    return final.shape[0].compute()

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
        client.run_on_scheduler(
            lambda: logging.getLogger("distributed").setLevel(logging.INFO)
        )
        logger.info("Using Dask distributed with %d workers", args.workers)
    else:
        client = None
        logger.info("Using single-threaded Dask")

    input_dir = args.input_dir
    output_dir = args.output_dir
    
    fs = get_filesystem(args.output_dir)

    if fs.protocol == "file":
        logger.info("Creating local output directory at %s", output_dir)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        intermediate_dir = output_dir / "intermediate"
        final_output = output_dir / "taxi_wide_table.parquet"
        logger.info("Creating local intermediate directory at %s", intermediate_dir)
        intermediate_dir.mkdir(exist_ok=True)
    else:
        logger.info("Using remote output directory at %s", output_dir)
        intermediate_dir = f"{output_dir}intermediate"
        final_output = f"{output_dir}taxi_wide_table.parquet"
        fs.mkdirs(intermediate_dir, exist_ok=True)

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
            fs = get_filesystem(month_files[0])
            candidate_sizes = [
                parse_size(s) for s in ["64MB", "128MB", "256MB"]    
            ]
            optimal = find_optimal_partition_size(
                month_files[0],
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

    final_rows = combine_into_wide_table(intermediate_dir, final_output)

    final_stats["final_output_rows"] = final_rows
    final_stats["runtime_seconds"] = int(time.time() - start_time)

    logger.info("Pipeline complete")
    for k, v in final_stats.items():
        logger.info("%s: %s", k, v)

    if not args.keep_intermediate:
        fs = get_filesystem(intermediate_dir)

        logger.info("Removing intermediate files from %s", intermediate_dir)

        for path in fs.ls(intermediate_dir):
            fs.rm(path, recursive=True)

        try:
            fs.rmdir(intermediate_dir)
        except Exception:
            pass

    if client is not None:
        client.close()

if __name__ == "__main__":
    main()
