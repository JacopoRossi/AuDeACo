#!/bin/bash
# ============================================================================
#  test-performance-choreography-pe.sh
#
#  Bring up the whole choreography-mqueue system:
#    1. Read the test manifest (DSL)
#    2. Build a Docker image for every task defined in the manifest
#       (each one shares the same task-wrapper Dockerfile)
#    3. Build the deploy-manager image via docker compose
#    4. Clean stale POSIX message queues
#    5. Start the stack (redis + deploy-manager + task containers)
#    6. Follow container logs until you press Ctrl+C
#
#  Usage:
#       ./test-performance-choreography-pe.sh [path/to/manifest.yaml]
#
#  If no manifest is provided, the default one in tests/test_0_code is used.
# ============================================================================

set -euo pipefail

# --- Resolve script directory so the script can be launched from anywhere ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# --- Resolve the manifest path ---------------------------------------------
CONTEXT="tests/test_0_code/"
MANIFEST_PATH="${1:-tests/test_0_code/manifest.yaml}"

if [ ! -f "$MANIFEST_PATH" ]; then
    echo "Error: Manifest file not found at $MANIFEST_PATH"
    exit 1
fi

echo "[INFO] Using manifest: $MANIFEST_PATH"

# --- Extract unique image names from the manifest --------------------------
IMAGES=$(grep -E '^[[:space:]]*-?[[:space:]]*image:' "$MANIFEST_PATH" \
         | sed -E 's/.*image:[[:space:]]*"?([^"]*)"?/\1/' \
         | sort -u)

if [ -z "$IMAGES" ]; then
    echo "[WARN] No 'image:' entries found in $MANIFEST_PATH"
fi

# --- Cleanup any previous run ----------------------------------------------
echo "[INFO] Stopping previous stack (if any)..."
docker compose down --remove-orphans || true

# Force-remove the fixed-name infrastructure containers in case a previous
# run left them behind under a different compose project.
for c in redis deploy-manager; do
    docker rm -f "$c" 2>/dev/null || true
done

# Force-remove any leftover task containers named after the manifest images.
for img in $IMAGES; do
    docker rm -f "$img" 2>/dev/null || true
done

# --- Build one Docker image per task using the task-wrapper Dockerfile ----
docker build -t jeffadac/task-wrapper:latest -f services/task-wrapper/Dockerfile .
python3 sdk/image-builder/src/main.py -f $MANIFEST_PATH -c $CONTEXT
# for img in $IMAGES; do
#     echo "[INFO] Building task image: $img"
#     docker build -t "$img" -f services/task-wrapper/Dockerfile .
# done

# --- Build infrastructure services (deploy-manager, etc.) -----------------
echo "[INFO] Building infrastructure services..."
docker compose build

# --- Remove stale POSIX message queues ------------------------------------
if [ -d /dev/mqueue ]; then
    echo "[INFO] Cleaning /dev/mqueue/*"
    rm -f /dev/mqueue/* 2>/dev/null || sudo rm -f /dev/mqueue/* 2>/dev/null || true
fi

# --- Start the whole stack -------------------------------------------------
echo "[INFO] Starting the choreography stack..."
docker compose up --build

echo ""
echo "[INFO] System is up. Containers:"
docker ps --format "  - {{.Names}}  ({{.Image}})"

echo ""
echo "[INFO] Following task container logs. Press Ctrl+C to stop."
echo "----------------------------------------------------------------"

# Follow logs of every task container that is currently running.
# When the user hits Ctrl+C we tear the stack down cleanly.
trap 'echo; echo "[INFO] Shutting down..."; docker compose stop; exit 0' INT TERM

for img in $IMAGES; do
    if docker ps --format '{{.Names}}' | grep -q "^${img}$"; then
        docker logs -f "$img" &
    fi
done

# Wait for any background log follower to terminate
wait
