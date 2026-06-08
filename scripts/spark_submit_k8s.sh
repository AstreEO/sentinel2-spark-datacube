#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────────
# spark_submit_k8s.sh
#
# Submits the Sentinel-2 NDVI pipeline to a Spark-on-Kubernetes cluster.
# Runs in CLIENT mode: the driver executes locally (WSL2), executor pods
# are created in the kind cluster inside namespace spark-jobs.
#
# Prerequisites (run once):
#   kubectl apply -f k8s/namespace.yaml
#   kubectl apply -f k8s/rbac.yaml
#   kubectl apply -f k8s/spark-data-pv.yaml
#   scripts/copy_data_to_kind.sh    # populate hostPath PV on worker nodes
#   scripts/build_and_load.sh       # build + load Docker image into kind
#
# Usage:
#   scripts/spark_submit_k8s.sh
#   SPARK_EXECUTORS=4 scripts/spark_submit_k8s.sh
#   SPARK_EXECUTORS=8 SPARK_IMAGE_TAG=v2 scripts/spark_submit_k8s.sh
# ────────────────────────────────────────────────────────────────────────────
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-cern-lab}"
IMAGE="${SPARK_K8S_IMAGE:-sentinel2-spark:latest}"
EXECUTORS="${SPARK_EXECUTORS:-2}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(realpath "${SCRIPT_DIR}/..")"

# ── 1. Resolve k8s API server URL ────────────────────────────────────────────
K8S_API=$(kubectl config view \
    --context "kind-${CLUSTER_NAME}" \
    --minify \
    -o jsonpath='{.clusters[0].cluster.server}')

echo "k8s API : ${K8S_API}"

# ── 2. Resolve driver host IP ─────────────────────────────────────────────────
# In WSL2, executor pods (Docker containers) reach the host via the
# docker-bridge / kind network IP.  Auto-detect: prefer the IP on the
# same subnet as the kind nodes.
if [[ -z "${SPARK_DRIVER_HOST:-}" ]]; then
    KIND_NODE_IP=$(kubectl get nodes \
        --context "kind-${CLUSTER_NAME}" \
        -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
    SUBNET_PREFIX=$(echo "${KIND_NODE_IP}" | cut -d. -f1-3)
    DRIVER_HOST=$(ip route get "${KIND_NODE_IP}" 2>/dev/null \
        | grep -oP 'src \K[0-9.]+' | head -1 || hostname -I | awk '{print $1}')
else
    DRIVER_HOST="${SPARK_DRIVER_HOST}"
fi

echo "driver  : ${DRIVER_HOST}"
echo "image   : ${IMAGE}"
echo "workers : ${EXECUTORS} executor pod(s)"
echo ""

# ── 3. Export env vars read by spark_session.get_spark() ─────────────────────
export SPARK_MASTER="k8s://${K8S_API}"
export SPARK_K8S_IMAGE="${IMAGE}"
export SPARK_DRIVER_HOST="${DRIVER_HOST}"
export SPARK_EXECUTORS="${EXECUTORS}"

# ── 4. Run pipeline ───────────────────────────────────────────────────────────
cd "${ROOT}"
echo "── Launching pipeline ────────────────────────────────────"
python run_pipeline.py

echo ""
echo "✓ Pipeline complete. Outputs in data/processed/"
