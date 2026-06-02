"""
End-to-end Sentinel-2 NDVI Data Cube pipeline.

Architecture
------------
Step 1 — Tile extraction (driver, sequential)
    rasterio opens each .zip archive, extracts B04 (Red) and B08 (NIR)
    at 100 m resolution, computes NDVI pixel-wise and writes a flat
    Parquet file: ~9.6 M rows × (date, row, col, red, nir, ndvi).

Step 2 — Global NDVI statistics (Spark, 8 parallel partitions)
    Spark reads the Parquet file and fans out a groupBy(date).agg()
    across 8 threads via Catalyst + Tungsten.  No Python workers
    involved: the aggregation runs entirely on the JVM.

Step 3 — Zone aggregation (Spark, 8 parallel partitions)
    Four geographic zones (Centro Storico, Villa Borghese, Castelli
    Romani, Agro Romano) are expressed as rectangular pixel bounding
    boxes.  Spark pushes the filter predicates into the Parquet scan
    (predicate pushdown) before the shuffle, minimising I/O.

Step 4 — Dense cube assembly (driver)
    After a spatial groupBy().agg() for block-averaging, toPandas()
    pulls the reduced result back to the driver where it is reshaped
    into a (1098, 1098, 8) float32 numpy array — the spatio-temporal
    data cube.

Step 5 — Visualisations (driver)
    Matplotlib maps, Plotly interactive HTML, animated GIF.
"""

import time
from pathlib import Path

import numpy as np

from pipeline.spark_session import get_spark
from pipeline.loader import load_datacube, SCHEMA
from pipeline.ndvi import summarise_by_date, build_dense_cube
from pipeline.aggregator import zone_stats, save_zone_stats
from viz.timeseries import plot_timeseries
from viz.seasonal_maps import plot_seasonal_maps
from viz.gif_export import export_gif
from viz.datacube_3d import plot_3d_cube

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def _banner(step: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {step}")
    print(f"{'─' * 60}")


def main():
    t0 = time.time()

    # ── Spark session ────────────────────────────────────────────────
    spark = get_spark()
    sc = spark.sparkContext
    print(f"\nSpark version : {spark.version}")
    print(f"Master        : {sc.master}")
    print(f"Default cores : {sc.defaultParallelism}")
    print(f"Spark UI      : {sc.uiWebUrl}")

    # ── Step 1: Load ─────────────────────────────────────────────────
    _banner("[1/5] Building Spark DataFrame from Sentinel-2 tiles")
    df = load_datacube(spark)
    print(f"\n  Schema  : {df.schema.simpleString()}")
    print(f"  Columns : {df.columns}")
    print(f"  Partitions (default parallelism = {sc.defaultParallelism})")

    # ── Step 2: Global NDVI statistics ───────────────────────────────
    _banner("[2/5] Global NDVI statistics — groupBy(date).agg() on Spark")
    print("  Spark executes partial aggregations on 8 partitions in parallel,")
    print("  then merges results.  All arithmetic runs on the JVM (Tungsten).")
    t2 = time.time()
    summary = summarise_by_date(df)
    summary.show(truncate=False)
    summary.toPandas().to_csv(PROCESSED_DIR / "global_stats.csv", index=False)
    print(f"  Elapsed: {time.time() - t2:.1f}s  →  saved global_stats.csv")

    # ── Step 3: Zone time series ──────────────────────────────────────
    _banner("[3/5] Zone NDVI aggregation — filter pushdown + groupBy(zone, date)")
    print("  Spark pushes rectangular bounding-box filters into the Parquet")
    print("  scan (predicate pushdown), reading only relevant row groups.")
    t3 = time.time()
    zone_df = zone_stats(df)
    pdf_zones = save_zone_stats(zone_df, PROCESSED_DIR)
    print(pdf_zones.pivot(index="date", columns="zone", values="ndvi_mean")
          .round(3).to_string())
    print(f"\n  Elapsed: {time.time() - t3:.1f}s  →  saved zone_stats.csv")

    # ── Step 4: Dense numpy cube ──────────────────────────────────────
    _banner("[4/5] Assembling dense spatio-temporal data cube (numpy)")
    print("  groupBy(date, row_ds, col_ds).agg(mean(ndvi)) on Spark,")
    print("  then toPandas() → reshape to (rows, cols, time) array.")
    t4 = time.time()
    cube, dates = build_dense_cube(df, downsample=10)
    np.save(PROCESSED_DIR / "ndvi_cube.npy", cube)
    (PROCESSED_DIR / "cube_dates.txt").write_text("\n".join(dates))
    mem_mb = cube.nbytes / 1e6
    print(f"\n  Cube shape : {cube.shape}  ({len(dates)} dates)")
    print(f"  dtype      : {cube.dtype}")
    print(f"  Memory     : {mem_mb:.1f} MB")
    print(f"  Elapsed    : {time.time() - t4:.1f}s  →  saved ndvi_cube.npy")

    df.unpersist()
    spark.stop()

    # ── Step 5: Visualisations ────────────────────────────────────────
    _banner("[5/5] Rendering visualisations")
    t5 = time.time()
    plot_timeseries(PROCESSED_DIR / "zone_stats.csv", PROCESSED_DIR)
    plot_seasonal_maps(cube, dates, PROCESSED_DIR)
    export_gif(cube, dates, PROCESSED_DIR / "ndvi_cycle.gif")
    plot_3d_cube(cube, dates, PROCESSED_DIR)
    print(f"\n  Elapsed: {time.time() - t5:.1f}s")

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    print(f"  Pipeline complete in {time.time() - t0:.0f}s")
    print(f"  Outputs → data/processed/")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()
