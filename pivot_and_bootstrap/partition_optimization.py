from __future__ import annotations

import re
import time
import logging
from typing import Iterable, Optional

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def parse_size(size_str: str) -> int:
    """
    Parse human-readable size strings like '200MB', '1.5GB' → bytes.
    """
    m = re.match(r"([\d.]+)\s*(KB|MB|GB)", size_str.upper())
    if not m:
        raise ValueError(f"Invalid size string: {size_str}")

    value = float(m.group(1))
    unit = m.group(2)

    multiplier = {
        "KB": 1024,
        "MB": 1024 ** 2,
        "GB": 1024 ** 3,
    }[unit]

    return int(value * multiplier)


def find_optimal_partition_size(
    parquet_path: str,
    candidate_sizes: Iterable[int],
    max_memory_usage: int,
    filesystem=None,
) -> int:
    """
    Benchmark read times for candidate partition sizes.
    Select fastest size under memory constraint.
    """
    best_size = None
    best_time = float("inf")

    for size in candidate_sizes:
        try:
            start = time.time()
            pq.ParquetFile(
                parquet_path,
                filesystem=filesystem,
            ).iter_batches(batch_size=size)
            elapsed = time.time() - start

            if elapsed < best_time:
                best_time = elapsed
                best_size = size

        except MemoryError:
            logger.warning("Memory error at size %s", size)
            continue

    if best_size is None:
        raise RuntimeError("No viable partition size found")

    return best_size
