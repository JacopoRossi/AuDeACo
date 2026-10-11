#!/bin/bash
# -----------------------------------------------------------------------------
# test-performance-choreography-pe.sh
#
# One-shot launcher for the choreography-zeromq real-time microservices stack.
#
# Steps:
#   1. Tear down any previous instance (containers + IPC volume).
#   2. Rebuild the task-wrapper base image.
#   3. Use the SDK image-builder to generate one Docker image per task
#      declared in tests/test_0_code/manifest.yaml.
#   4. Bring up docker-compose (redis + deploy-manager). The deploy-manager
#      then spawns the task containers dynamically and loads the schedule
#      into Redis.
#
# Usage:
#     ./test-performance-choreography-pe.sh [path/to/manifest_dir]
#
#   - With no argument the default test mission is used:
#         tests/test_0_code
#   - The argument, if given, must point to a directory that contains a
#     `manifest.yaml` and one source folder per task referenced in it.
# -----------------------------------------------------------------------------
set -e

# Resolve the directory this script lives in: this becomes the project root.
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Mission to deploy (can be overridden from the CLI).
MISSION_DIR="${1:-$PROJECT_DIR/tests/test_0_code}"

if [ ! -f "$MISSION_DIR/manifest.yaml" ]; then
    echo "ERROR: manifest.yaml not found in '$MISSION_DIR'" >&2
    exit 1
fi

# Discover the task aliases declared in the manifest so cleanup is generic.
TASK_NAMES=$(grep -E '^\s*-\s*alias:' "$MISSION_DIR/manifest.yaml" \
             | sed -E 's/.*alias:\s*"?([^"[:space:]]+)"?.*/\1/')

echo "=== [1/4] Cleanup ==="
cd "$PROJECT_DIR"
docker compose down --remove-orphans 2>/dev/null || true
# Force-remove containers that may be left over from a previous run
# (e.g. stopped containers or instances created outside this compose project).
for c in redis deploy-manager $TASK_NAMES; do
    docker rm -f "$c" 2>/dev/null || true
done
docker volume rm zmq_ipc 2>/dev/null || true
docker volume create zmq_ipc >/dev/null

echo "=== [2/4] Rebuild task-wrapper base image ==="
docker build -t jeffadac/task-wrapper:latest \
    -f services/task-wrapper/Dockerfile .

echo "=== [3/4] Build per-task images (SDK) ==="
cd "$MISSION_DIR"
python3 "$PROJECT_DIR/sdk/image-builder/src/main.py" -f manifest.yaml -c .

echo "=== [4/4] Start the system ==="
cd "$PROJECT_DIR"
docker compose up --build
