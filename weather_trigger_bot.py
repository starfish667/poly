from __future__ import annotations

import argparse
import asyncio
import sys
from decimal import Decimal
from pathlib import Path

from monitor import install_log_file
from polybot.weather_trigger import WeatherTriggerBot


def decimal_arg(value: str) -> Decimal:
    return Decimal(str(value))


def main() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except AttributeError:
        pass

    parser = argparse.ArgumentParser(
        description="Auto-discover same-day temperature markets and buy NO below observed highs."
    )
    parser.add_argument("--city-count", type=int, default=8)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--discovery-interval", type=float, default=60)
    parser.add_argument("--pages", type=int, default=4)
    parser.add_argument("--page-size", type=int, default=40)
    parser.add_argument("--lookahead-days", type=int, default=0)
    parser.add_argument("--min-liquidity", type=decimal_arg, default=Decimal("100"))
    parser.add_argument("--max-event-volume", type=decimal_arg, default=Decimal("50000"))
    parser.add_argument("--max-event-liquidity", type=decimal_arg)
    parser.add_argument("--max-entry-price", type=decimal_arg, default=Decimal("0.99"))
    parser.add_argument("--limit-price", type=decimal_arg, default=Decimal("0.99"))
    parser.add_argument("--size", type=decimal_arg, default=Decimal("5"))
    parser.add_argument("--state", default="weather_trigger_state.json")
    parser.add_argument("--source", choices=("auto", "aviationweather", "ecmwf"), default="auto")
    parser.add_argument("--weather-hours", type=int, default=72)
    parser.add_argument("--trade-on-first-observation", action="store_true")
    parser.add_argument("--log-file")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()

    if args.log_file:
        install_log_file(args.log_file)

    bot = WeatherTriggerBot(
        live=args.live,
        city_count=args.city_count,
        poll_interval=args.poll_interval,
        discovery_interval=args.discovery_interval,
        pages=args.pages,
        page_size=args.page_size,
        lookahead_days=args.lookahead_days,
        min_liquidity=args.min_liquidity,
        max_event_volume=args.max_event_volume,
        max_event_liquidity=args.max_event_liquidity,
        max_entry_price=args.max_entry_price,
        limit_price=args.limit_price,
        size=args.size,
        state_path=Path(args.state),
        source=args.source,
        weather_hours=args.weather_hours,
        trade_on_first_observation=args.trade_on_first_observation,
    )
    asyncio.run(bot.run_forever())


if __name__ == "__main__":
    main()
