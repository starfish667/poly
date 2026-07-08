#!/usr/bin/env bash
set -euo pipefail

TARGET_USER="${POLYBOT_EC2_USER:-ec2-user}"
SESSION="${POLYBOT_EARTHQUAKE_TMUX_SESSION:-earthquake-trigger}"
MODE="${POLYBOT_EARTHQUAKE_TMUX_MODE:-live}"
ACTION="${POLYBOT_EARTHQUAKE_TMUX_ACTION:-restart}"
ATTACH="${POLYBOT_EARTHQUAKE_TMUX_ATTACH:-1}"
PROJECT_DIR="${POLYBOT_PROJECT_DIR:-}"
CURRENT_USER="$(id -un 2>/dev/null || printf unknown)"

find_project_dir() {
  if [[ -n "$PROJECT_DIR" ]]; then
    printf '%s\n' "$PROJECT_DIR"
    return
  fi

  if [[ "$CURRENT_USER" != "$TARGET_USER" && -f "/home/${TARGET_USER}/polymarket/earthquake_trigger_bot.py" ]]; then
    printf '/home/%s/polymarket\n' "$TARGET_USER"
    return
  fi

  if [[ "$CURRENT_USER" != "$TARGET_USER" && -f "/opt/polymarket/earthquake_trigger_bot.py" ]]; then
    printf '/opt/polymarket\n'
    return
  fi

  if [[ -f "./earthquake_trigger_bot.py" ]]; then
    pwd
    return
  fi

  if [[ -f "/home/${TARGET_USER}/polymarket/earthquake_trigger_bot.py" ]]; then
    printf '/home/%s/polymarket\n' "$TARGET_USER"
    return
  fi

  if [[ -f "/opt/polymarket/earthquake_trigger_bot.py" ]]; then
    printf '/opt/polymarket\n'
    return
  fi

  echo "Cannot find polymarket project. Set POLYBOT_PROJECT_DIR=/path/to/polymarket." >&2
  exit 1
}

install_tmux_if_missing() {
  if command -v tmux >/dev/null 2>&1; then
    return
  fi

  if [[ "$(id -u)" -ne 0 ]]; then
    echo "tmux is not installed. Run this script before switching user, or install tmux first." >&2
    exit 1
  fi

  if command -v dnf >/dev/null 2>&1; then
    dnf install -y tmux
    return
  fi

  if command -v yum >/dev/null 2>&1; then
    yum install -y tmux
    return
  fi

  if command -v apt-get >/dev/null 2>&1; then
    apt-get update
    apt-get install -y tmux
    return
  fi

  echo "tmux is not installed and no supported package manager was found." >&2
  exit 1
}

PROJECT_DIR="$(find_project_dir)"

if [[ "$(id -un)" != "$TARGET_USER" ]]; then
  install_tmux_if_missing
  if ! id "$TARGET_USER" >/dev/null 2>&1; then
    echo "User ${TARGET_USER} does not exist." >&2
    exit 1
  fi

  exec sudo -iu "$TARGET_USER" env \
    POLYBOT_PROJECT_DIR="$PROJECT_DIR" \
    POLYBOT_EC2_USER="$TARGET_USER" \
    POLYBOT_EARTHQUAKE_TMUX_SESSION="$SESSION" \
    POLYBOT_EARTHQUAKE_TMUX_MODE="$MODE" \
    POLYBOT_EARTHQUAKE_TMUX_ACTION="$ACTION" \
    POLYBOT_EARTHQUAKE_TMUX_ATTACH="$ATTACH" \
    bash -lc 'cd "$POLYBOT_PROJECT_DIR" && bash ./run_earthquake_trigger_tmux.sh'
fi

cd "$PROJECT_DIR"
mkdir -p logs

case "$MODE" in
  live)
    RUNNER="./run_earthquake_trigger_live.sh"
    ;;
  dry)
    RUNNER="./run_earthquake_trigger_dry.sh"
    ;;
  *)
    echo "POLYBOT_EARTHQUAKE_TMUX_MODE must be live or dry." >&2
    exit 1
    ;;
esac

if [[ ! -f "$RUNNER" ]]; then
  echo "Runner not found: ${PROJECT_DIR}/${RUNNER}" >&2
  exit 1
fi

if tmux has-session -t "$SESSION" 2>/dev/null; then
  case "$ACTION" in
    restart)
      tmux kill-session -t "$SESSION"
      ;;
    start)
      echo "tmux session ${SESSION} is already running."
      echo "Attach with: tmux attach -t ${SESSION}"
      exit 0
      ;;
    attach)
      exec tmux attach -t "$SESSION"
      ;;
    *)
      echo "POLYBOT_EARTHQUAKE_TMUX_ACTION must be restart, start, or attach." >&2
      exit 1
      ;;
  esac
fi

tmux new-session -d -s "$SESSION" -c "$PROJECT_DIR" "bash $RUNNER"

echo "Started earthquake trigger bot in tmux session: ${SESSION}"
echo "Mode: ${MODE}"
echo "Project: ${PROJECT_DIR}"
echo "Attach with: tmux attach -t ${SESSION}"

if [[ "$ATTACH" == "1" ]]; then
  exec tmux attach -t "$SESSION"
fi
