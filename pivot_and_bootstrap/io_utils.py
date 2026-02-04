from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List

import fsspec

logger = logging.getLogger(__name__)


# ----------------------------
# Path helpers
# ----------------------------

def is_s3_path(path: str | Path) -> bool:
    """
    Return True if the path is an S3 URI.
    """
    return str(path).startswith("s3://")


def get_storage_options(path: str | Path) -> Dict:
    """
    Return storage options for fsspec based on path.
    Uses anonymous access for public S3 buckets.
    """
    if is_s3_path(path):
        return {"anon": True}
    return {}


def get_filesystem(path: str | Path):
    """
    Return an fsspec filesystem for local or S3 paths.
    """
    if is_s3_path(path):
        return fsspec.filesystem("s3", anon=True)
    return fsspec.filesystem("file")


# ----------------------------
# Parquet discovery
# ----------------------------

def discover_parquet_files(input_path: str | Path) -> List[str]:
    """
    Recursively discover all .parquet files from a local directory or S3 prefix.
    Returns a sorted list of fully-qualified paths.
    """
    input_path = str(input_path)
    fs = get_filesystem(input_path)

    if is_s3_path(input_path):
        # Remove scheme for fs.walk
        stripped = input_path.replace("s3://", "", 1)
        base = stripped.rstrip("/")
        files = []

        for root, _, filenames in fs.walk(base):
            for name in filenames:
                if name.endswith(".parquet"):
                    files.append(f"s3://{root}/{name}")

    else:
        base = Path(input_path)
        if not base.exists():
            raise FileNotFoundError(f"Input path does not exist: {input_path}")

        files = [
            str(p)
            for p in base.rglob("*.parquet")
            if p.is_file()
        ]

    files.sort()

    if not files:
        logger.warning("No Parquet files found under %s", input_path)

    return files