from __future__ import annotations

import argparse
import asyncio

from polymarket import AsyncPublicClient


async def main_async(url: str) -> None:
    async with AsyncPublicClient() as client:
        market = await client.get_market(url=url)
    print(f"question: {market.question}")
    print(f"slug: {market.slug}")
    print(f"description: {getattr(market, 'description', '')}")
    print(f"resolution_source: {getattr(market.resolution, 'source', None)}")
    print(f"yes_price: {market.outcomes.yes.price}")
    print(f"yes_token_id: {market.outcomes.yes.token_id}")
    print(f"no_price: {market.outcomes.no.price}")
    print(f"no_token_id: {market.outcomes.no.token_id}")
    print(f"spread: {getattr(market.prices, 'spread', None)}")
    print(f"liquidity: {getattr(market.metrics, 'liquidity_num', None) or getattr(market.metrics, 'liquidity', None)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect Polymarket YES/NO prices for one market.")
    parser.add_argument("url")
    args = parser.parse_args()
    asyncio.run(main_async(args.url))


if __name__ == "__main__":
    main()
