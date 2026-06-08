# ────────────────────────────────────────────────────────────────────────────
# Sentinel-2 NDVI DataCube — Spark executor / driver image
#
# Python 3.12 · OpenJDK 17 · Apache Spark 3.5.1 · rasterio 1.3
#
# Build:  docker build -t sentinel2-spark:latest .
# Load:   kind load docker-image sentinel2-spark:latest --name cern-lab
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim

LABEL org.opencontainers.image.title="sentinel2-spark" \
      org.opencontainers.image.description="Spark executor for Sentinel-2 NDVI pipeline" \
      org.opencontainers.image.source="https://github.com/AstreEO/sentinel2-spark-datacube"

# ── 1. System dependencies ───────────────────────────────────────────────────
#    - openjdk-17: Spark JVM (driver/executor process)
#    - libgdal-dev + gdal-bin: native GDAL libraries required by rasterio
#    - procps: ps/kill used by Spark's process management
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        libgdal-dev \
        gdal-bin \
        wget \
        procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── 2. Apache Spark 3.5.1 ───────────────────────────────────────────────────
ARG SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN wget -q \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# ── 3. Python dependencies ───────────────────────────────────────────────────
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ── 4. Pipeline code ─────────────────────────────────────────────────────────
#    Executor workers deserialise Spark closures that reference pipeline
#    modules (loader._iter_tile, ndvi.*, aggregator.*).  The modules must
#    exist on the executor's PYTHONPATH at the same package paths.
WORKDIR /app
COPY pipeline/ ./pipeline/
COPY viz/      ./viz/
# manifest.json is only read by the driver (client mode), but included for
# completeness / future cluster-mode support.
COPY data/raw/manifest.json ./data/raw/manifest.json

ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# ── 5. Spark k8s entrypoint ──────────────────────────────────────────────────
#    Spark's Kubernetes backend uses this entrypoint to start both the driver
#    (cluster mode) and each executor pod.  The CMD is overridden at runtime
#    by Spark with the appropriate class and arguments.
ENTRYPOINT ["/opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh"]
CMD [""]
