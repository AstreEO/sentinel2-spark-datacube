import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[8]")
        .appName("Sentinel2-NDVI-DataCube")
        .config("spark.driver.memory", "16g")
        .config("spark.executor.memory", "8g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
