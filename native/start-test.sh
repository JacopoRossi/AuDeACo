#!/bin/bash
# =============================================================================
# start-test.sh
#
# Builds the Execution Manager service and launches it.
# This script starts the whole system: it compiles the C sources via CMake
# and then runs the resulting binary, which executes the schedule defined
# in `services/execution-manager/src/main.c`.
#
# Usage:
#   ./start-test.sh
#
# Environment:
#   CMAKE   Override the cmake binary to use (default: auto-detected).
#
# Notes:
#   - Real-time scheduling (SCHED_FIFO) and `mlockall` require elevated
#     privileges. Run with `sudo` if you hit EPERM errors, or grant the
#     appropriate capabilities (e.g. CAP_SYS_NICE, CAP_IPC_LOCK).
# =============================================================================

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EM_DIR="$ROOT_DIR/services/execution-manager"
BUILD_DIR="$EM_DIR/build"
BIN="$BUILD_DIR/execution-manager"

# Resolve a working cmake: try $CMAKE, then common system locations, then PATH.
# This avoids picking up broken/wrong-arch binaries (e.g. ~/.local/bin/cmake).
find_cmake() {
    local candidates=("${CMAKE:-}" /usr/bin/cmake /usr/local/bin/cmake /bin/cmake)
    for c in "${candidates[@]}"; do
        if [[ -n "$c" && -x "$c" ]] && "$c" --version >/dev/null 2>&1; then
            echo "$c"; return 0
        fi
    done
    while IFS= read -r c; do
        if "$c" --version >/dev/null 2>&1; then echo "$c"; return 0; fi
    done < <(command -v -a cmake 2>/dev/null || true)
    return 1
}

CMAKE_BIN="$(find_cmake)" || {
    echo "[ERROR] No working 'cmake' found. Install cmake (>= 3.20) or set CMAKE=/path/to/cmake." >&2
    exit 1
}
echo "[*] Using cmake: $CMAKE_BIN ($("$CMAKE_BIN" --version | head -n1))"

echo "[*] Configuring Execution Manager (CMake)..."
"$CMAKE_BIN" -S "$EM_DIR" -B "$BUILD_DIR"

echo "[*] Building Execution Manager..."
"$CMAKE_BIN" --build "$BUILD_DIR"

echo "[*] Launching Execution Manager..."
echo "------------------------------------------------------------"
"$BIN"
echo "------------------------------------------------------------"
echo "[*] Execution Manager terminated."
