#!/usr/bin/env bash
set -uo pipefail

failed=0

declare -A suites=(
    [unit]="not integration and not e2e"
    [integration]="integration"
    [e2e]="e2e"
)

for suite in unit integration e2e; do
    uv run pytest -m "${suites[$suite]}" tests/
    code=$?

    if [ "$code" -eq 5 ]; then
        echo "[test:all] Skipped $suite (no tests collected)"
    elif [ "$code" -ne 0 ]; then
        echo "[test:all] $suite failed with exit code $code"
        failed=1
    fi
done

exit "$failed"
