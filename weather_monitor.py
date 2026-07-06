from __future__ import annotations

import sys

import monitor


DEFAULT_ARGS = {
    "--watchlist": "weather_watchlist.json",
    "--state": "weather_monitor_state.json",
    "--interval": "15",
    "--active-interval": "0.5",
    "--max-concurrency": "6",
}


def ensure_default_args() -> None:
    for flag, value in reversed(DEFAULT_ARGS.items()):
        if flag not in sys.argv:
            sys.argv[1:1] = [flag, value]


if __name__ == "__main__":
    ensure_default_args()
    monitor.main()
