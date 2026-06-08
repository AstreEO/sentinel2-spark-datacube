"""
Load Sentinel-2 B04/B08 bands into a Spark DataFrame.

Each zip archive becomes one Spark partition — one worker per tile,
8 tiles processed in parallel on local[8].

    manifest.json  →  RDD[(zip_path, date)]  →  flatMap(_iter_tile)
                                                  ↓  (8 workers in parallel)
                                              Spark DataFrame
                                              (date, row, col, red, nir, ndvi)
"""

from __future__ import annotations

import json
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Iterator

import numpy as np
from rasterio.enums import Resampling

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    FloatType, IntegerType, StringType, StructField, StructType,
)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

SCHEMA = StructType([
    StructField("date", StringType(),  False),
    StructField("row",  IntegerType(), False),
    StructField("col",  IntegerType(), False),
    StructField("red",  FloatType(),   True),
    StructField("nir",  FloatType(),   True),
    StructField("ndvi", FloatType(),   True),
])

READ_DOWNSAMPLE = 10   # 10 m native → 100 m


def _find_band(zf: zipfile.ZipFile, band: str) -> str:
    pattern = re.compile(rf".*IMG_DATA/R10m/.*_{band}_10m\.jp2$")
    matches = [n for n in zf.namelist() if pattern.match(n)]
    if not matches:
        matches = [n for n in zf.namelist() if f"_{band}_" in n and n.endswith(".jp2")]
    if not matches:
        raise FileNotFoundError(f"Band {band} not found in zip.")
    return matches[0]


def _iter_tile(record: tuple[str, str]) -> Iterator[tuple]:
    """
    Spark worker function — runs in a worker process, one per tile.
    Opens the zip, extracts B04 (Red) + B08 (NIR), computes NDVI,
    yields one row per valid pixel.
    """
    import rasterio

    zip_path, date = record
    with zipfile.ZipFile(zip_path) as zf:
        b04_name = _find_band(zf, "B04")
        b08_name = _find_band(zf, "B08")
        with tempfile.TemporaryDirectory() as tmp:
            b04_path = zf.extract(b04_name, tmp)
            b08_path = zf.extract(b08_name, tmp)
            with rasterio.open(b04_path) as ds4, rasterio.open(b08_path) as ds8:
                h, w = ds4.height, ds4.width
                out_h, out_w = h // READ_DOWNSAMPLE, w // READ_DOWNSAMPLE
                red = ds4.read(1, out_shape=(out_h, out_w),
                               resampling=Resampling.average).astype(np.float32)
                nir = ds8.read(1, out_shape=(out_h, out_w),
                               resampling=Resampling.average).astype(np.float32)

    denom = nir + red
    with np.errstate(invalid="ignore", divide="ignore"):
        ndvi = np.where(denom > 0, (nir - red) / denom, np.nan).astype(np.float32)

    mask = (red > 0) & (nir > 0)
    rows_idx, cols_idx = np.where(mask)
    for r, c in zip(rows_idx, cols_idx):
        yield (
            date,
            int(r * READ_DOWNSAMPLE),
            int(c * READ_DOWNSAMPLE),
            float(red[r, c]),
            float(nir[r, c]),
            float(ndvi[r, c]),
        )


def _to_linux_path(path: str) -> str:
    """Convert a Windows path to its WSL2 /mnt/<drive>/... equivalent."""
    import re
    m = re.match(r'^([A-Za-z]):[/\\](.*)', path)
    if m:
        drive = m.group(1).lower()
        rest  = m.group(2).replace('\\', '/')
        return f'/mnt/{drive}/{rest}'
    return path  # already a Linux path


def _resolve_path(path: str) -> str:
    """
    Resolve a tile zip path for the current execution environment.

    Two cases:
    - Kubernetes executor pod  → SPARK_DATA_DIR=/data/raw is injected via
      spark.executorEnv in spark_session.py.  The PVC is mounted flat, so
      only the filename is needed.
    - WSL2 local[8] mode       → convert Windows C:\\... path to /mnt/c/...
    """
    import os
    from pathlib import PurePosixPath, PureWindowsPath

    data_dir = os.environ.get("SPARK_DATA_DIR")
    if data_dir:
        # Extract the filename regardless of whether the stored path is
        # Windows (C:\...) or POSIX (/mnt/c/...) format.
        try:
            filename = PureWindowsPath(path).name
        except Exception:
            filename = PurePosixPath(path).name
        return str(Path(data_dir) / filename)

    # Local WSL2: convert Windows path separators
    return _to_linux_path(path)


def load_datacube(spark: SparkSession) -> DataFrame:
    manifest_path = RAW_DIR / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found at {manifest_path}.")

    entries = json.loads(manifest_path.read_text())
    records = [(_resolve_path(e["path"]), e["date"]) for e in entries]

    print(f"  Parallelising {len(entries)} tiles across {spark.sparkContext.defaultParallelism} cores")
    rdd = spark.sparkContext.parallelize(records, numSlices=len(records))
    rdd = rdd.flatMap(_iter_tile)
    df  = spark.createDataFrame(rdd, schema=SCHEMA)
    df.cache()

    n = df.count()
    print(f"  Total pixel-observations: {n:,}")
    return df
