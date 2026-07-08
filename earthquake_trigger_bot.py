from __future__ import annotations

import argparse
import asyncio
import sys
from decimal import Decimal
from pathlib import Path

from monitor import install_log_file
from polybot.earthquake_trigger import (
    DEFAULT_EVENT_URL,
    EarthquakeTriggerBot,
    parse_datetime_arg,
)


def decimal_arg(value: str) -> Decimal:
    return Decimal(str(value))


def main() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except AttributeError:
        pass

    parser = argparse.ArgumentParser(
        description=(
            "Earthquake count trigger bot for Polymarket. "
            "Buys NO on count buckets that become impossible, then buys final YES."
        )
    )
    parser.add_argument("--event-url", default=DEFAULT_EVENT_URL)
    parser.add_argument("--poll-interval", type=float, default=5)
    parser.add_argument("--market-refresh-interval", type=float, default=60)
    parser.add_argument("--state", default="earthquake_trigger_state.json")
    parser.add_argument("--min-magnitude", type=decimal_arg)
    parser.add_argument("--start-utc", type=parse_datetime_arg)
    parser.add_argument("--end-utc", type=parse_datetime_arg)
    parser.add_argument("--review-status", choices=("all", "reviewed"), default="all")
    parser.add_argument("--trade-on-start", action="store_true")
    parser.add_argument("--settlement-delay-seconds", type=float, default=30)
    parser.add_argument("--settlement-update-min-age-seconds", type=float, default=30)
    parser.add_argument("--max-no-entry-price", type=decimal_arg, default=Decimal("0.99"))
    parser.add_argument("--no-limit-price", type=decimal_arg, default=Decimal("0.99"))
    parser.add_argument("--no-size", type=decimal_arg, default=Decimal("5"))
    parser.add_argument("--max-yes-entry-price", type=decimal_arg, default=Decimal("0.99"))
    parser.add_argument("--yes-limit-price", type=decimal_arg, default=Decimal("0.99"))
    parser.add_argument("--yes-size", type=decimal_arg, default=Decimal("5"))
    parser.add_argument("--price-websocket-max-age", type=float, default=10)
    parser.add_argument("--price-wait-seconds", type=float, default=2)
    parser.add_argument("--log-file")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()

    if args.log_file:
        install_log_file(args.log_file)

    bot = EarthquakeTriggerBot(
        event_url=args.event_url,
        live=args.live,
        poll_interval=args.poll_interval,
        market_refresh_interval=args.market_refresh_interval,
        state_path=Path(args.state),
        min_magnitude=args.min_magnitude,
        start_utc=args.start_utc,
        end_utc=args.end_utc,
        review_status=args.review_status,
        trade_on_start=args.trade_on_start,
        settlement_delay_seconds=args.settlement_delay_seconds,
        settlement_update_min_age_seconds=args.settlement_update_min_age_seconds,
        max_no_entry_price=args.max_no_entry_price,
        no_limit_price=args.no_limit_price,
        no_size=args.no_size,
        max_yes_entry_price=args.max_yes_entry_price,
        yes_limit_price=args.yes_limit_price,
        yes_size=args.yes_size,
        price_websocket_max_age=args.price_websocket_max_age,
        price_wait_seconds=args.price_wait_seconds,
        once=args.once,
    )
    asyncio.run(bot.run_forever())


if __name__ == "__main__":
    main()
