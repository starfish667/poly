$ErrorActionPreference = "Stop"

if (-not $env:POLYMARKET_PRIVATE_KEY) {
    throw "POLYMARKET_PRIVATE_KEY is not set."
}

if (-not $env:POLYMARKET_WALLET_ADDRESS) {
    throw "POLYMARKET_WALLET_ADDRESS is not set."
}

$env:POLYBOT_ENABLE_LIVE = "1"
$env:PYTHONUNBUFFERED = "1"
$env:EARNINGS_SEC_LOOKAHEAD_DAYS = "2"

$interval = if ($env:POLYBOT_INTERVAL) { $env:POLYBOT_INTERVAL } else { "15" }
$activeInterval = if ($env:POLYBOT_ACTIVE_INTERVAL) { $env:POLYBOT_ACTIVE_INTERVAL } else { "3" }
$maxConcurrency = if ($env:POLYBOT_MAX_CONCURRENCY) { $env:POLYBOT_MAX_CONCURRENCY } else { "4" }
conda run --no-capture-output -n poly python -u monitor.py --watchlist watchlist.json --interval $interval --active-interval $activeInterval --max-concurrency $maxConcurrency --live
