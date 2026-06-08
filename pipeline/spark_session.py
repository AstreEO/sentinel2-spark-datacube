"""
Spark session factory — supports both local and Kubernetes modes.

Local (default):
    spark = get_spark()                  # master=local[8]

Kubernetes (client mode, driver on WSL2, executors in kind):
    export SPARK_MASTER=k8s://https://127.0.0.1:PORT
    export SPARK_K8S_IMAGE=sentinel2-spark:latest
    export SPARK_DRIVER_HOST=<WSL2-IP reachable from kind pods>
    export SPARK_EXECUTORS=2
    spark = get_spark()

The submit script (scripts/spark_submit_k8s.sh) sets all env vars
automatically before calling run_pipeline.py.
"""

import os
import sys

from pyspark.sql import SparkSession

# Workers must use the same Python executable as the driver so that
# pickle-serialised closures (RDD functions, UDFs) are compatible.
os.environ.setdefault("PYSPARK_PYTHON",       sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# ── Spark configs injected only in k8s mode ───────────────────────────────────
# These tell Spark to mount the spark-data-pvc (which contains the Sentinel-2
# zip archives) at /data/raw inside every executor pod.
# The loader then reads SPARK_DATA_DIR=/data/raw to resolve file paths.
_K8S_EXECUTOR_CONFIGS: dict[str, str] = {
    "spark.kubernetes.namespace":                                   "spark-jobs",
    "spark.kubernetes.authenticate.driver.serviceAccountName":      "spark-driver",
    # ── PVC volume mount in executor pods ─────────────────────────────────────
    "spark.kubernetes.executor.volumes.persistentVolumeClaim"
        ".spark-data.mount.path":                                   "/data/raw",
    "spark.kubernetes.executor.volumes.persistentVolumeClaim"
        ".spark-data.options.claimName":                            "spark-data-pvc",
    "spark.kubernetes.executor.volumes.persistentVolumeClaim"
        ".spark-data.mount.readOnly":                               "true",
    # ── Tell Python workers inside executor pods where zips live ───────────────
    "spark.executorEnv.SPARK_DATA_DIR":                             "/data/raw",
    # ── Cosmetics ──────────────────────────────────────────────────────────────
    "spark.kubernetes.driver.pod.name":                             "sentinel2-driver",
    "spark.kubernetes.executor.label.app":                          "sentinel2-ndvi",
}


def get_spark(
    master:    str | None = None,
    image:     str | None = None,
    executors: int | None = None,
) -> SparkSession:
    """
    Build (or retrieve) the global SparkSession.

    Parameters are resolved in this order: explicit argument → env var → default.

    Parameters
    ----------
    master : str | None
        Spark master URL.
        Env var: SPARK_MASTER.  Default: "local[8]".
        k8s example: "k8s://https://127.0.0.1:37151"
    image : str | None
        Docker image tag for k8s executor pods.
        Env var: SPARK_K8S_IMAGE.  Default: "sentinel2-spark:latest".
        Ignored in local mode.
    executors : int | None
        Number of executor pods to request (k8s mode only).
        Env var: SPARK_EXECUTORS.  Default: 2.
    """
    master    = master    or os.environ.get("SPARK_MASTER",     "local[8]")
    executors = executors or int(os.environ.get("SPARK_EXECUTORS", "2"))
    is_k8s    = master.startswith("k8s://")

    builder = (
        SparkSession.builder
        .master(master)
        .appName("Sentinel2-NDVI-DataCube")
        .config("spark.driver.memory",   "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
    )

    if is_k8s:
        image = image or os.environ.get("SPARK_K8S_IMAGE", "sentinel2-spark:latest")
        builder = builder.config("spark.kubernetes.container.image", image)
        builder = builder.config("spark.executor.instances", str(executors))

        for key, val in _K8S_EXECUTOR_CONFIGS.items():
            builder = builder.config(key, val)

        # Driver host: WSL2 IP must be routable from inside kind pod network.
        # Auto-detected by spark_submit_k8s.sh and passed via SPARK_DRIVER_HOST.
        driver_host = os.environ.get("SPARK_DRIVER_HOST")
        if driver_host:
            builder = builder.config("spark.driver.host", driver_host)

    return builder.getOrCreate()
