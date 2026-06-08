#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────────────────
# copy_data_to_kind.sh
#
# Copies Sentinel-2 zip archives into every kind worker node so that the
# hostPath PersistentVolume at /sentinel2-data is populated.
#
# Run this once after the kind cluster is up (or when new tiles are added).
# It is idempotent: re-running only overwrites files that already exist.
#
# Alternative (new cluster): use kind extraMounts in k8s/kind-config.yaml
# to bind-mount the directory directly — no docker cp needed.
#
# Usage:
#   scripts/copy_data_to_kind.sh
#   KIND_CLUSTER=my-cluster scripts/copy_data_to_kind.sh
# ────────────────────────────────────────────────────────────────────────────
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER:-cern-lab}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/../data/raw"
DATA_DIR="$(realpath "${DATA_DIR}")"

echo "cluster : ${CLUSTER_NAME}"
echo "source  : ${DATA_DIR}"
echo ""

# Get worker node container names (exclude control-plane)
mapfile -t workers < <(kind get nodes --name "${CLUSTER_NAME}" | grep -v control-plane)

if [[ ${#workers[@]} -eq 0 ]]; then
    echo "ERROR: no worker nodes found in cluster '${CLUSTER_NAME}'" >&2
    exit 1
fi

for node in "${workers[@]}"; do
    echo "── node: ${node} ──────────────────────────"
    docker exec "${node}" mkdir -p /sentinel2-data

    zip_count=0
    for zip in "${DATA_DIR}"/*.zip; do
        [[ -f "${zip}" ]] || continue
        filename="$(basename "${zip}")"
        printf "  copying %-70s" "${filename}"
        docker cp "${zip}" "${node}:/sentinel2-data/${filename}"
        echo "✓"
        (( zip_count++ ))
    done

    if [[ ${zip_count} -eq 0 ]]; then
        echo "  WARNING: no .zip files found in ${DATA_DIR}"
    else
        echo "  ${zip_count} tile(s) copied"
    fi
done

echo ""
echo "✓ Done. /sentinel2-data is ready on all ${#workers[@]} worker node(s)."
