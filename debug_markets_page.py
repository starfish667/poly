from __future__ import annotations

import asyncio

from polymarket import AsyncPublicClient


async def main() -> None:
    async with AsyncPublicClient() as client:
        markets = client.list_markets(
            closed=False,
            order="volume24hr",
            ascending=True,
            page_size=5,
        )
        page_no = 0
        async for page in markets:
            page_no += 1
            print("page", page_no, "items", len(page.items), "has_more", page.has_more)
            for market in page.items:
                print()
                print("question:", market.question)
                print("url:", f"https://polymarket.com/market/{market.slug}")
                print("state:", market.state)
                print("metrics:", market.metrics)
                print("prices:", market.prices)
                print("trading:", market.trading)
                print("resolution:", market.resolution)
                print("sports:", market.sports)
                print("tags:", [(tag.slug, tag.label) for tag in market.tags])
                print("events:", [(event.slug, event.title) for event in market.events])
                print("yes:", market.outcomes.yes)
                print("no:", market.outcomes.no)
            break


if __name__ == "__main__":
    asyncio.run(main())
