from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
import logging
import time
import psutil
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from dask import delayed, compute
from tqdm import tqdm

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
geom_array = taxi_zones.geometry.values
index_array = taxi_zones.index.values

# ---------------------------------------------------
# Memory Tracking 
# ---------------------------------------------------

def process_single_file_rss(file_path: str,
    intermediate_dir: Path,
    min_rides: int
) -> Dict[str, int]:
    process = psutil.Process()
    peak_rss = 0

    def track_peak(mem_val):
        nonlocal peak_rss
        if mem_val > peak_rss:
            peak_rss = mem_val
    
    stats = process_single_file(file_path, intermediate_dir, min_rides)
    track_peak(process.memory_info().rss)
    stats["peak_rss"] = peak_rss
    return stats

# ---------------------------------------------------
# Single-file processing
# ---------------------------------------------------
def process_single_file(
    file_path: str,
    intermediate_dir: Path,
    min_rides: int
) -> Dict[str, int]:
    """
    Process one Parquet file into an intermediate cleaned Parquet file.
    """
    logger = logging.getLogger("pivot_pipeline.process_single_file")
    stats = defaultdict(int)

    logger.info("[START file] %s", file_path)

    expected_month = infer_month_from_path(file_path)
    taxi_type = infer_taxi_type_from_path(file_path)

    schema = pq.read_schema(file_path, filesystem=get_filesystem(file_path))

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

    df = pd.read_parquet(
        file_path, 
        columns=read_cols, 
        engine="pyarrow",
        storage_options={'anon': True} if is_s3_path(file_path) else None,
    )
    input_rows = len(df)
    stats["input_rows"] = input_rows
    logger.info(
        "[READ file] %s rows=%d",
        file_path,
        input_rows
    )

    if lat_col and lon_col:
        logger.info("Performing spatial join to find pickup zones for %s", file_path)
        pickup_loc_col = 'pickup_place'
        
        gdf_points = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
            crs=taxi_zones.crs
        )
        joined = gpd.sjoin(gdf_points, taxi_zones, how="left", predicate="within")
        joined = joined.reset_index()
        joined = joined.drop_duplicates(subset=['index'])
        df['pickup_place'] = joined['index_right'].add(1)
        df = df.drop(columns=[lat_col, lon_col])

        logger.info("Spatial join complete for %s", file_path)
        schema_error = df['pickup_place'].isna().sum()
        stats["schema_error_rows"] = int(schema_error)
        logger.info("Missing pickup zones after spatial join: %d", schema_error)

    df[pickup_dt_col] = pd.to_datetime(df[pickup_dt_col], errors="coerce")
    y, m = expected_month
    mismatch_mask = (df[pickup_dt_col].dt.year != y) | (df[pickup_dt_col].dt.month != m)
    mismatch_count = mismatch_mask.sum()
    stats["month_mismatch_rows"] = int(mismatch_count)
    df = df[~mismatch_mask]

    # drop rows with any na, final catch
    before_dropna = len(df)
    df = df.dropna(subset=[pickup_dt_col, pickup_loc_col])
    after_dropna = len(df)
    stats["dropped_na_rows"] = before_dropna - after_dropna

    df = df.assign(
        date=df[pickup_dt_col].dt.date,
        hour=df[pickup_dt_col].dt.hour,
        pickup_place=df[pickup_loc_col].astype('int64'),
        taxi_type=taxi_type,
    )

    # logger.info("Current df rows after datetime and location processing: %d", len(df))

    agg = df.groupby(["taxi_type", "date", "pickup_place", "hour"]).size().reset_index(name="count")
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
    logger.info("Reading intermediates from %s", intermediate_dir)

    ddf = pd.read_parquet(
        intermediate_dir,
        engine="pyarrow",
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
        index=False,
        storage_options={"anon": True} if is_s3_path(output_path) else None,
    )

    return final.shape[0]

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
    start_time = time.perf_counter()

    logging.basicConfig(
        level=getattr(logging, "INFO", logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logger = logging.getLogger("pivot_pipeline")

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

    final_stats = defaultdict(int)

    all_files = [f for month_files in files_by_month.values() for f in month_files]
    logger.info("Processing %d files grouped into %d months with %d workers", len(all_files), len(files_by_month), args.workers)

    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        results = list(tqdm(executor.map(process_single_file_rss, all_files, [intermediate_dir]*len(all_files), [args.min_rides]*len(all_files)), total=len(all_files)))

        for stats in results:
            for k, v in stats.items():
                final_stats[k] += int(v)

    final_rows = combine_into_wide_table(intermediate_dir, final_output)
    final_stats["final_output_rows"] = final_rows
    final_stats["runtime_seconds"] = time.perf_counter() - start_time
    final_stats["peak_rss_gb"] = final_stats["peak_rss"] / 1024**3

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

if __name__ == "__main__":
    main()
