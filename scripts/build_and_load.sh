#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────────
# build_and_load.sh
#
# Builds the Spark executor Docker image and loads it into the kind cluster.
# Must be run from the repository root (or any directory — uses absolute paths).
#
# The image contains:
#   - Python 3.12 + all pipeline dependencies (rasterio, pyspark, numpy, …)
#   - OpenJDK 17 + Apache Spark 3.5.1
#   - pipeline/ and viz/ packages (needed by executor worker processes)
#
# Usage:
#   scripts/build_and_load.sh
#   SPARK_IMAGE_TAG=v1.2 KIND_CLUSTER=my-cluster scripts/build_and_load.sh
# ────────────────────────────────────────────────────────────────────────────
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-cern-lab}"
IMAGE_NAME="sentinel2-spark"
IMAGE_TAG="${SPARK_IMAGE_TAG:-latest}"
FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(realpath "${SCRIPT_DIR}/..")"

echo "image   : ${FULL_IMAGE}"
echo "cluster : ${CLUSTER_NAME}"
echo "root    : ${ROOT}"
echo ""

# ── Build ─────────────────────────────────────────────────────────────────────
echo "── docker build ─────────────────────────────────────────"
docker build --tag "${FULL_IMAGE}" "${ROOT}"
echo ""

# ── Load into kind ────────────────────────────────────────────────────────────
echo "── kind load docker-image ───────────────────────────────"
kind load docker-image "${FULL_IMAGE}" --name "${CLUSTER_NAME}"
echo ""

# ── Verify presence on all nodes ──────────────────────────────────────────────
echo "── image present on nodes ───────────────────────────────"
all_ok=true
for node in $(kind get nodes --name "${CLUSTER_NAME}"); do
    result=$(docker exec "${node}" crictl images 2>/dev/null \
             | grep "${IMAGE_NAME}" | awk '{print $2}' | head -1 || true)
    if [[ -n "${result}" ]]; then
        echo "  ✓ ${node}  (tag: ${result})"
    else
        echo "  ✗ ${node}  — image not found"
        all_ok=false
    fi
done

echo ""
if $all_ok; then
    echo "✓ ${FULL_IMAGE} is ready in cluster '${CLUSTER_NAME}'."
else
    echo "WARNING: image missing on some nodes. Try re-running this script." >&2
    exit 1
fi
