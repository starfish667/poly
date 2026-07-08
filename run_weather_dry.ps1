$ErrorActionPreference = "Stop"
$env:PYTHONUNBUFFERED = "1"

$interval = if ($env:POLYBOT_WEATHER_INTERVAL) { $env:POLYBOT_WEATHER_INTERVAL } elseif ($env:POLYBOT_INTERVAL) { $env:POLYBOT_INTERVAL } else { "15" }
$activeInterval = if ($env:POLYBOT_WEATHER_ACTIVE_INTERVAL) { $env:POLYBOT_WEATHER_ACTIVE_INTERVAL } elseif ($env:POLYBOT_ACTIVE_INTERVAL) { $env:POLYBOT_ACTIVE_INTERVAL } else { "0.5" }
$maxConcurrency = if ($env:POLYBOT_WEATHER_MAX_CONCURRENCY) { $env:POLYBOT_WEATHER_MAX_CONCURRENCY } elseif ($env:POLYBOT_MAX_CONCURRENCY) { $env:POLYBOT_MAX_CONCURRENCY } else { "6" }
$priceWebsocket = if ($env:POLYBOT_PRICE_WEBSOCKET) { $env:POLYBOT_PRICE_WEBSOCKET } else { "1" }
$priceWebsocketMaxAge = if ($env:POLYBOT_PRICE_WEBSOCKET_MAX_AGE) { $env:POLYBOT_PRICE_WEBSOCKET_MAX_AGE } else { "10" }
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$logFile = "logs\weather-dry-$timestamp.log"

$priceArgs = @()
if ($priceWebsocket -ne "0") {
    $priceArgs += "--price-websocket"
    $priceArgs += "--price-websocket-max-age"
    $priceArgs += $priceWebsocketMaxAge
}

conda run --no-capture-output -n poly python -u weather_monitor.py --interval $interval --active-interval $activeInterval --max-concurrency $maxConcurrency --log-file $logFile @priceArgs
