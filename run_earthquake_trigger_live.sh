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

EVENT_URL="${POLYBOT_EARTHQUAKE_EVENT_URL:-https://polymarket.com/event/how-many-6pt5-or-above-earthquakes-july-6-july-12-20260702194353908}"
POLL_INTERVAL="${POLYBOT_EARTHQUAKE_INTERVAL:-0.5}"
MARKET_REFRESH_INTERVAL="${POLYBOT_EARTHQUAKE_MARKET_REFRESH_INTERVAL:-60}"
MIN_MAGNITUDE="${POLYBOT_EARTHQUAKE_MIN_MAGNITUDE:-}"
START_UTC="${POLYBOT_EARTHQUAKE_START_UTC:-}"
END_UTC="${POLYBOT_EARTHQUAKE_END_UTC:-}"
REVIEW_STATUS="${POLYBOT_EARTHQUAKE_REVIEW_STATUS:-all}"
SETTLEMENT_DELAY="${POLYBOT_EARTHQUAKE_SETTLEMENT_DELAY_SECONDS:-30}"
SETTLEMENT_UPDATE_MIN_AGE="${POLYBOT_EARTHQUAKE_SETTLEMENT_UPDATE_MIN_AGE_SECONDS:-30}"
MAX_NO_ENTRY_PRICE="${POLYBOT_EARTHQUAKE_MAX_NO_ENTRY_PRICE:-0.99}"
NO_LIMIT_PRICE="${POLYBOT_EARTHQUAKE_NO_LIMIT_PRICE:-0.99}"
NO_SIZE="${POLYBOT_EARTHQUAKE_NO_SIZE:-5}"
MAX_YES_ENTRY_PRICE="${POLYBOT_EARTHQUAKE_MAX_YES_ENTRY_PRICE:-0.99}"
YES_LIMIT_PRICE="${POLYBOT_EARTHQUAKE_YES_LIMIT_PRICE:-0.99}"
YES_SIZE="${POLYBOT_EARTHQUAKE_YES_SIZE:-5}"
PRICE_WEBSOCKET_MAX_AGE="${POLYBOT_EARTHQUAKE_PRICE_WEBSOCKET_MAX_AGE:-10}"
PRICE_WAIT_SECONDS="${POLYBOT_EARTHQUAKE_PRICE_WAIT_SECONDS:-2}"
LOG_FILE="logs/earthquake-trigger-live-$(date -u +%Y%m%d-%H%M%S).log"
PYTHON_BIN="${POLYBOT_PYTHON:-python}"

if [[ -z "${POLYBOT_PYTHON:-}" && -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
fi

ARGS=(
  --event-url "$EVENT_URL"
  --poll-interval "$POLL_INTERVAL"
  --market-refresh-interval "$MARKET_REFRESH_INTERVAL"
  --review-status "$REVIEW_STATUS"
  --settlement-delay-seconds "$SETTLEMENT_DELAY"
  --settlement-update-min-age-seconds "$SETTLEMENT_UPDATE_MIN_AGE"
  --max-no-entry-price "$MAX_NO_ENTRY_PRICE"
  --no-limit-price "$NO_LIMIT_PRICE"
  --no-size "$NO_SIZE"
  --max-yes-entry-price "$MAX_YES_ENTRY_PRICE"
  --yes-limit-price "$YES_LIMIT_PRICE"
  --yes-size "$YES_SIZE"
  --price-websocket-max-age "$PRICE_WEBSOCKET_MAX_AGE"
  --price-wait-seconds "$PRICE_WAIT_SECONDS"
  --state earthquake_trigger_state.json
  --log-file "$LOG_FILE"
  --live
)

if [[ -n "$MIN_MAGNITUDE" ]]; then
  ARGS+=(--min-magnitude "$MIN_MAGNITUDE")
fi

if [[ -n "$START_UTC" ]]; then
  ARGS+=(--start-utc "$START_UTC")
fi

if [[ -n "$END_UTC" ]]; then
  ARGS+=(--end-utc "$END_UTC")
fi

if [[ "${POLYBOT_EARTHQUAKE_TRADE_ON_START:-0}" == "1" ]]; then
  ARGS+=(--trade-on-start)
fi

"$PYTHON_BIN" -u earthquake_trigger_bot.py "${ARGS[@]}"
