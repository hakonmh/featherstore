$failed = $false

$suites = @(
    @{name = 'unit'; marker = 'not integration and not e2e'},
    @{name = 'integration'; marker = 'integration'},
    @{name = 'e2e'; marker = 'e2e'}
)

foreach ($suite in $suites) {
    & uv run pytest -m $suite.marker tests/
    $code = $LASTEXITCODE

    if ($code -eq 5) {
        Write-Host "[test:all] Skipped $($suite.name) (no tests collected)"
    } elseif ($code -ne 0) {
        Write-Host "[test:all] $($suite.name) failed with exit code $code"
        $failed = $true
    }
}

if ($failed) {
    exit 1
}
