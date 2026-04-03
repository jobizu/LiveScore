$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $projectRoot '.venv\Scripts\python.exe'
$scraper = Join-Path $projectRoot 'scripts\scrape_livescore_epl_fixtures_2526.py'
$output = Join-Path $projectRoot 'data\epl_livescore_fixtures_2025_2026.csv'

if (-not (Test-Path $pythonExe)) {
    throw "Python virtual environment not found at: $pythonExe"
}

& $pythonExe $scraper --output $output
Write-Host "LiveScore EPL fixtures CSV updated: $output"
