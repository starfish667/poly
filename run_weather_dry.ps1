$ErrorActionPreference = "Stop"
$env:PYTHONUNBUFFERED = "1"

$interval = if ($env:POLYBOT_WEATHER_INTERVAL) { $env:POLYBOT_WEATHER_INTERVAL } elseif ($env:POLYBOT_INTERVAL) { $env:POLYBOT_INTERVAL } else { "15" }
$activeInterval = if ($env:POLYBOT_WEATHER_ACTIVE_INTERVAL) { $env:POLYBOT_WEATHER_ACTIVE_INTERVAL } elseif ($env:POLYBOT_ACTIVE_INTERVAL) { $env:POLYBOT_ACTIVE_INTERVAL } else { "0.5" }
$maxConcurrency = if ($env:POLYBOT_WEATHER_MAX_CONCURRENCY) { $env:POLYBOT_WEATHER_MAX_CONCURRENCY } elseif ($env:POLYBOT_MAX_CONCURRENCY) { $env:POLYBOT_MAX_CONCURRENCY } else { "6" }
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logFile = "logs\weather-dry-$timestamp.log"

conda run --no-capture-output -n poly python -u weather_monitor.py --interval $interval --active-interval $activeInterval --max-concurrency $maxConcurrency --log-file $logFile
