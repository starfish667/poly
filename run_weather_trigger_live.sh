#!/usr/bin/env bash
set -euo pipefail

export PYTHONUNBUFFERED=1

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

if [[ -z "${POLYMARKET_PRIVATE_KEY:-}" ]]; then
  echo "POLYMARKET_PRIVATE_KEY is not set." >&2
  exit 1
fi

if [[ -z "${POLYMARKET_WALLET_ADDRESS:-}" ]]; then
  echo "POLYMARKET_WALLET_ADDRESS is not set." >&2
  exit 1
fi

export POLYBOT_ENABLE_LIVE=1

CITY_COUNT="${POLYBOT_TRIGGER_CITY_COUNT:-8}"
POLL_INTERVAL="${POLYBOT_TRIGGER_INTERVAL:-0.5}"
DISCOVERY_INTERVAL="${POLYBOT_TRIGGER_DISCOVERY_INTERVAL:-60}"
MAX_EVENT_VOLUME="${POLYBOT_TRIGGER_MAX_EVENT_VOLUME:-50000}"
MAX_EVENT_LIQUIDITY="${POLYBOT_TRIGGER_MAX_EVENT_LIQUIDITY:-}"
MIN_LIQUIDITY="${POLYBOT_TRIGGER_MIN_LIQUIDITY:-100}"
MAX_ENTRY_PRICE="${POLYBOT_TRIGGER_MAX_ENTRY_PRICE:-0.99}"
LIMIT_PRICE="${POLYBOT_TRIGGER_LIMIT_PRICE:-0.99}"
SIZE="${POLYBOT_TRIGGER_SIZE:-5}"
SOURCE="${POLYBOT_TRIGGER_SOURCE:-auto}"
PRICE_WEBSOCKET_MAX_AGE="${POLYBOT_TRIGGER_PRICE_WEBSOCKET_MAX_AGE:-10}"
PRICE_WAIT_SECONDS="${POLYBOT_TRIGGER_PRICE_WAIT_SECONDS:-2}"
LOG_FILE="logs/weather-trigger-live-$(date -u +%Y%m%d-%H%M%S).log"
PYTHON_BIN="${POLYBOT_PYTHON:-python}"

if [[ -z "${POLYBOT_PYTHON:-}" && -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
fi

ARGS=(
  --city-count "$CITY_COUNT"
  --poll-interval "$POLL_INTERVAL"
  --discovery-interval "$DISCOVERY_INTERVAL"
  --min-liquidity "$MIN_LIQUIDITY"
  --max-event-volume "$MAX_EVENT_VOLUME"
  --max-entry-price "$MAX_ENTRY_PRICE"
  --limit-price "$LIMIT_PRICE"
  --size "$SIZE"
  --source "$SOURCE"
  --price-websocket-max-age "$PRICE_WEBSOCKET_MAX_AGE"
  --price-wait-seconds "$PRICE_WAIT_SECONDS"
  --state weather_trigger_state.json
  --log-file "$LOG_FILE"
  --live
)

if [[ -n "$MAX_EVENT_LIQUIDITY" ]]; then
  ARGS+=(--max-event-liquidity "$MAX_EVENT_LIQUIDITY")
fi

"$PYTHON_BIN" -u weather_trigger_bot.py "${ARGS[@]}"
