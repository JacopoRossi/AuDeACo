#!/usr/bin/env bash
#
# run-all.sh
#
# End-to-end build & run for the RealTime Microservices stack.
#
#   1. Build the task-wrapper base image
#   2. Build the per-task images (via the SDK image-builder)
#   3. Build deploy-manager and execution-manager
#   4. Start Redis
#   5. Run deploy-manager (provisions task-services and populates Redis)
#   6. Run execution-manager (executes the schedule via gRPC)
#   7. Cleanup
#
# Usage:
#   ./run-all.sh                # use task/manifest.yaml
#   ./run-all.sh path/to/m.yaml # use a custom manifest
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

TOTAL=8
step() { echo -e "\n${GREEN}▶ [$1/$TOTAL] $2${NC}\n"; }
fail() { echo -e "\n${RED}✖ Error at step $1: $2${NC}\n" >&2; exit 1; }

MANIFEST_FILE="task/manifest.yaml"
SOURCE_MANIFEST="${1:-$MANIFEST_FILE}"

if [ "$SOURCE_MANIFEST" != "$MANIFEST_FILE" ]; then
  if [ ! -f "$SOURCE_MANIFEST" ]; then
    fail 0 "Source manifest not found: $SOURCE_MANIFEST"
  fi
  cp "$SOURCE_MANIFEST" "$MANIFEST_FILE"
fi

echo -e "\n=============================================="
echo -e "${GREEN}    RealTime Microservices  -  $SOURCE_MANIFEST${NC}"
echo -e "==============================================\n"

# 0 — Pre-clean any stale containers
docker compose down >/dev/null 2>&1 || true
# Remove any stray fixed-name containers from previous runs (different compose project, etc.)
for c in redis deploy-manager execution-manager-grpc; do
  docker rm -f "$c" >/dev/null 2>&1 || true
done
ACTIVE_CONTAINERS=$(docker ps -aq --filter "name=task-service-" || true)
if [ -n "$ACTIVE_CONTAINERS" ]; then
  docker rm -f $ACTIVE_CONTAINERS >/dev/null 2>&1 || true
fi

# 1 — Build task-wrapper base image
step 1 "docker build task-wrapper"
docker build --no-cache -t jeffadac/realtime-microservices:task-wrapper \
  -f services/task-wrapper/Dockerfile . \
  || fail 1 "docker build task-wrapper"

# 2 — Build per-task images via SDK image-builder
step 2 "python3 image-builder (task manifest)"
python3 sdk/image-builder/src/main.py -f "$MANIFEST_FILE" -c task --no-cache \
  || fail 2 "python3 image-builder"

# 3 — Build deploy-manager
step 3 "docker compose build deploy-manager"
docker compose build deploy-manager \
  || fail 3 "docker compose build deploy-manager"

# 4 — Build execution-manager
step 4 "docker compose build execution-manager"
docker compose build execution-manager \
  || fail 4 "docker compose build execution-manager"

# 5 — Start Redis (background)
step 5 "docker compose up -d redis"
docker compose up -d redis \
  || fail 5 "docker compose up redis"

# 6 — Run deploy-manager (provisions task-services and populates Redis)
step 6 "docker compose up deploy-manager"
docker compose up deploy-manager \
  || fail 6 "docker compose up deploy-manager"

DEPLOY_EXIT=$(docker inspect deploy-manager --format '{{.State.ExitCode}}' 2>/dev/null || echo "missing")
if [ "$DEPLOY_EXIT" != "0" ]; then
  fail 6 "deploy-manager exited with code $DEPLOY_EXIT"
fi

# 7 — Run execution-manager (executes the schedule via gRPC)
step 7 "docker compose up execution-manager"
docker compose up execution-manager \
  || fail 7 "docker compose up execution-manager"

# 8 — Cleanup
step 8 "docker compose down + remove task-service-* containers"
docker compose down
ACTIVE_CONTAINERS=$(docker ps -aq --filter "name=task-service-" || true)
if [ -n "$ACTIVE_CONTAINERS" ]; then
  docker rm -f $ACTIVE_CONTAINERS
fi

echo -e "\n${GREEN}✔ All steps completed successfully.${NC}\n"
