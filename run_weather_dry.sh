#!/usr/bin/env bash
set -euo pipefail

export PYTHONUNBUFFERED=1

INTERVAL="${POLYBOT_WEATHER_INTERVAL:-${POLYBOT_INTERVAL:-15}}"
ACTIVE_INTERVAL="${POLYBOT_WEATHER_ACTIVE_INTERVAL:-${POLYBOT_ACTIVE_INTERVAL:-0.5}}"
MAX_CONCURRENCY="${POLYBOT_WEATHER_MAX_CONCURRENCY:-${POLYBOT_MAX_CONCURRENCY:-6}}"
LOG_FILE="logs/weather-dry-$(date -u +%Y%m%d-%H%M%S).log"

conda run --no-capture-output -n poly python -u weather_monitor.py \
  --interval "$INTERVAL" \
  --active-interval "$ACTIVE_INTERVAL" \
  --max-concurrency "$MAX_CONCURRENCY" \
  --log-file "$LOG_FILE"
