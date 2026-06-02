"""
NDVI computation helpers and data cube assembly.

The data cube is a 3-D structure: (row × col × time).
After loading, this module coerces the DataFrame into a pivot-ready form
and exports a dense numpy array for visualisation.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def summarise_by_date(df: DataFrame) -> DataFrame:
    """Return per-date NDVI statistics (mean, std, p25, p75)."""
    return df.groupBy("date").agg(
        F.mean("ndvi").alias("ndvi_mean"),
        F.stddev("ndvi").alias("ndvi_std"),
        F.expr("percentile(ndvi, 0.25)").alias("ndvi_p25"),
        F.expr("percentile(ndvi, 0.75)").alias("ndvi_p75"),
        F.count("ndvi").alias("pixel_count"),
    ).orderBy("date")


def build_dense_cube(df: DataFrame, downsample: int = 10) -> tuple[np.ndarray, list[str]]:
    """
    Aggregate pixels into a spatial grid and return a 3-D numpy array
    shaped (rows, cols, time) plus sorted date labels.

    downsample: aggregate every N pixels into one cell (reduces memory).
    """
    df_ds = df.withColumn("row_ds", (F.col("row") / downsample).cast("int")) \
              .withColumn("col_ds", (F.col("col") / downsample).cast("int"))

    agg = df_ds.groupBy("date", "row_ds", "col_ds").agg(
        F.mean("ndvi").alias("ndvi_mean")
    )

    pdf: pd.DataFrame = agg.toPandas()
    dates = sorted(pdf["date"].unique())
    date_idx = {d: i for i, d in enumerate(dates)}

    max_row = int(pdf["row_ds"].max()) + 1
    max_col = int(pdf["col_ds"].max()) + 1
    cube = np.full((max_row, max_col, len(dates)), np.nan, dtype=np.float32)

    for _, row in pdf.iterrows():
        t = date_idx[row["date"]]
        cube[int(row["row_ds"]), int(row["col_ds"]), t] = row["ndvi_mean"]

    return cube, dates
