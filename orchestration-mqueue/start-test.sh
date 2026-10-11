#!/bin/bash
#
# test-performance-choreography-pe.sh
# ------------------------------------
# Convenience launcher for the RealTime Microservices stack (choreography
# with process-engine implementation, message-queue based).
#
# What this script does:
#   1. Reads the task images declared in tests/test_0_code/manifest.yaml
#   2. Builds one Docker image per task using the task-wrapper Dockerfile
#   3. Tears down any previous run and (re)builds & starts the core stack
#      (redis, deploy-manager, execution-manager) via docker compose
#   4. Streams the execution-manager logs to stdout until it terminates
#   5. Stops every container that was started
#
# Usage:
#   ./test-performance-choreography-pe.sh
#
# Notes:
#   - Run from the project root (where docker-compose.yml lives).
#   - Requires: Docker + Docker Compose plugin, bash, grep, sed.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CONTEXT="tests/test_0_code/"
MANIFEST_PATH="tests/test_0_code/manifest.yaml"
TASK_WRAPPER_DOCKERFILE="services/task-wrapper/Dockerfile"

# ---------------------------------------------------------------------------
# 1. Sanity checks
# ---------------------------------------------------------------------------
if [ ! -f "$MANIFEST_PATH" ]; then
    echo "Error: Manifest file not found at $MANIFEST_PATH"
    exit 1
fi

if [ ! -f "$TASK_WRAPPER_DOCKERFILE" ]; then
    echo "Error: task-wrapper Dockerfile not found at $TASK_WRAPPER_DOCKERFILE"
    exit 1
fi

# ---------------------------------------------------------------------------
# 2. Extract the unique task image names from the manifest
# ---------------------------------------------------------------------------
IMAGES=$(grep -E '^[[:space:]]*-?[[:space:]]*image:' "$MANIFEST_PATH" \
         | sed -E 's/.*image:[[:space:]]*"?([^"]*)"?/\1/' \
         | sort -u)

if [ -z "$IMAGES" ]; then
    echo "Warning: No task images found in $MANIFEST_PATH"
fi

# ---------------------------------------------------------------------------
# 3. Cleanup any previous run
# ---------------------------------------------------------------------------
echo "==> Stopping any previous stack ..."
docker compose down --remove-orphans 2>/dev/null || true
# Remove fixed-name containers that may have been left orphaned by a previous
# run started under a different Compose project name.
docker rm -f redis deploy-manager execution-manager 2>/dev/null || true
for img in $IMAGES; do
    docker rm -f "$img" 2>/dev/null || true
done

# ---------------------------------------------------------------------------
# 4. Build one task-wrapper image per task declared in the manifest
# ---------------------------------------------------------------------------
docker build -t jeffadac/task-wrapper:latest -f services/task-wrapper/Dockerfile .
python3 sdk/image-builder/src/main.py -f $MANIFEST_PATH -c $CONTEXT

# for img in $IMAGES; do
#     echo "==> Building task image: $img"
#     docker build -t "$img" -f "$TASK_WRAPPER_DOCKERFILE" .
# done

# ---------------------------------------------------------------------------
# 5. Bring the core stack up (redis + deploy-manager + execution-manager)
# ---------------------------------------------------------------------------
echo "==> Starting core stack ..."
docker compose up --build -d

# Wait for the execution-manager container to be visible to Docker.
EXEC_CONTAINER=""
for _ in {1..10}; do
    EXEC_CONTAINER=$(docker compose ps -q execution-manager 2>/dev/null || true)
    if [ -n "$EXEC_CONTAINER" ]; then
        break
    fi
    sleep 0.2
done
if [ -z "$EXEC_CONTAINER" ]; then
    EXEC_CONTAINER="execution-manager"
fi

# ---------------------------------------------------------------------------
# 6. Follow execution-manager logs until it exits
# ---------------------------------------------------------------------------
echo "==> Streaming execution-manager logs (Ctrl+C to stop)"
docker logs -f "$EXEC_CONTAINER" || true

# ---------------------------------------------------------------------------
# 7. Cleanup
# ---------------------------------------------------------------------------
echo "==> Shutting down stack ..."
docker compose stop
for img in $IMAGES; do
    docker stop "$img" 2>/dev/null || true
done

echo "==> Done."
