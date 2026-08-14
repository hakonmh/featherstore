#!/usr/bin/env bash
set -uo pipefail

failed=0

run_suite() {
    local name="$1"
    local marker="$2"
    uv run pytest -m "$marker" tests/
    local code=$?

    if [ "$code" -eq 5 ]; then
        echo "[test:all] Skipped $name (no tests collected)"
    elif [ "$code" -ne 0 ]; then
        echo "[test:all] $name failed with exit code $code"
        failed=1
    fi
}

run_suite unit "not integration and not e2e"
run_suite integration "integration"
run_suite e2e "e2e"

exit "$failed"
