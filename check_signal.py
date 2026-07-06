from __future__ import annotations

import argparse
import asyncio

from polymarket import AsyncPublicClient

from polybot.earnings import earnings_signal, parse_earnings_rule
from polybot.weather import weather_signal


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--strategy", choices=("weather", "earnings"), required=True)
    args = parser.parse_args()

    async with AsyncPublicClient() as client:
        market = await client.get_market(url=args.url)
        if args.strategy == "weather":
            signal = await weather_signal(market)
        else:
            print(parse_earnings_rule(market.question or "", market.description or ""))
            signal = await earnings_signal(market)
        print(signal)


if __name__ == "__main__":
    asyncio.run(main())
