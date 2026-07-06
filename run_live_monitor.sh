#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
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
export PYTHONUNBUFFERED=1
export EARNINGS_SEC_LOOKAHEAD_DAYS="${EARNINGS_SEC_LOOKAHEAD_DAYS:-2}"

if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x .venv/bin/python ]]; then
    PYTHON_BIN=".venv/bin/python"
  else
    PYTHON_BIN="python"
  fi
fi

exec "$PYTHON_BIN" -u monitor.py \
  --watchlist watchlist.json \
  --interval "${POLYBOT_INTERVAL:-15}" \
  --active-interval "${POLYBOT_ACTIVE_INTERVAL:-3}" \
  --max-concurrency "${POLYBOT_MAX_CONCURRENCY:-4}" \
  --live
