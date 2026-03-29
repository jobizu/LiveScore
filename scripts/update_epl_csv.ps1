$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot '.venv\Scripts\python.exe'
$scraper = Join-Path $projectRoot 'scripts\scrape_epl_2526.py'
$output = Join-Path $projectRoot 'data\epl_2025_2026.csv'

if (-not (Test-Path $pythonExe)) {
    throw "Python virtual environment not found at: $pythonExe"
}

& $pythonExe $scraper --output $output
Write-Host "EPL CSV updated: $output"
