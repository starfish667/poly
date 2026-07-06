from __future__ import annotations

import argparse
import asyncio

from polymarket import AsyncPublicClient


async def main_async(url: str) -> None:
    async with AsyncPublicClient() as client:
        market = await client.get_market(url=url)
        print(f"question: {market.question}")
        print(f"market_slug: {market.slug}")
        events = market.events or []
        print(f"events: {len(events)}")
        for event in events:
            print(f"event_slug: {event.slug}")
            event_obj = await client.get_event(url=f"https://polymarket.com/event/{event.slug}")
            print(f"event_markets: {len(event_obj.markets)}")
            for item in event_obj.markets:
                print(f"- {item.question} | https://polymarket.com/market/{item.slug}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect the Polymarket event for a weather market.")
    parser.add_argument("url")
    args = parser.parse_args()
    asyncio.run(main_async(args.url))


if __name__ == "__main__":
    main()
