"""
Aggregate NDVI statistics per geographic zone using pixel-coordinate bounding boxes
derived from the Sentinel-2 T33TTG tile CRS (EPSG:32633).
"""

from __future__ import annotations

import math
from pathlib import Path
import json

import numpy as np
import pandas as pd
from pyproj import Transformer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# T33TTG origin and 10 m pixel size (UTM zone 33N)
TILE_ORIGIN_X = 300_000.0   # metres easting
TILE_ORIGIN_Y = 4_700_040.0  # metres northing (top-left)
PIXEL_SIZE = 10.0            # 10 m/pixel at 10 m bands

ZONES = {
    "Centro Storico":  {"lon": 12.492, "lat": 41.893, "radius_km": 3},
    "Villa Borghese":  {"lon": 12.492, "lat": 41.914, "radius_km": 2},
    "Castelli Romani": {"lon": 12.700, "lat": 41.750, "radius_km": 5},
    "Agro Romano":     {"lon": 12.350, "lat": 41.850, "radius_km": 5},
}

_wgs84_to_utm33 = Transformer.from_crs("EPSG:4326", "EPSG:32633", always_xy=True)


def _zone_pixel_bounds(zone: dict) -> tuple[int, int, int, int]:
    """Return (row_min, row_max, col_min, col_max) for a zone circle's bounding box."""
    cx, cy = _wgs84_to_utm33.transform(zone["lon"], zone["lat"])
    r = zone["radius_km"] * 1000

    col_min = int((cx - r - TILE_ORIGIN_X) / PIXEL_SIZE)
    col_max = int((cx + r - TILE_ORIGIN_X) / PIXEL_SIZE)
    # rows increase downward
    row_min = int((TILE_ORIGIN_Y - (cy + r)) / PIXEL_SIZE)
    row_max = int((TILE_ORIGIN_Y - (cy - r)) / PIXEL_SIZE)

    return max(0, row_min), max(0, row_max), max(0, col_min), max(0, col_max)


def zone_stats(df: DataFrame) -> DataFrame:
    """
    Return a DataFrame with NDVI statistics per (zone, date).
    Uses rectangular pixel bounding boxes — fast for Spark filter pushdown.
    """
    frames = []
    for name, zone in ZONES.items():
        row_min, row_max, col_min, col_max = _zone_pixel_bounds(zone)
        filtered = (
            df.filter(
                (F.col("row") >= row_min) & (F.col("row") <= row_max) &
                (F.col("col") >= col_min) & (F.col("col") <= col_max)
            )
            .withColumn("zone", F.lit(name).cast(StringType()))
            .groupBy("zone", "date")
            .agg(
                F.mean("ndvi").alias("ndvi_mean"),
                F.stddev("ndvi").alias("ndvi_std"),
                F.expr("percentile(ndvi, 0.25)").alias("ndvi_p25"),
                F.expr("percentile(ndvi, 0.75)").alias("ndvi_p75"),
                F.count("ndvi").alias("pixel_count"),
            )
        )
        frames.append(filtered)

    result = frames[0]
    for f in frames[1:]:
        result = result.union(f)

    return result.orderBy("zone", "date")


def save_zone_stats(df: DataFrame, out_dir: Path) -> pd.DataFrame:
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_csv(out_dir / "zone_stats.csv", index=False)
    return pdf
