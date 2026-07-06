from __future__ import annotations

import asyncio

from polymarket import AsyncPublicClient


SLUGS = (
    "unty-quarterly-earnings-gaap-eps-07-14-2026-1pt44",
    "cag-quarterly-earnings-nongaap-eps-07-15-2026-0pt46",
    "pep-quarterly-earnings-nongaap-eps-07-09-2026-2pt21",
    "will-google-googl-q2-youtube-ads-revenue-be-above-10pt8b-20260703213709254",
    "khamenei-of-tweets-june-30-july-7-2026-20-24",
    "zelenskyy-of-tweets-july-7-july-14-2026-40-59",
    "highest-temperature-in-amsterdam-on-july-5-2026-21c",
    "wnba-ind-las-2026-07-05-assists-aja-wilson-2pt5",
    "atp-hurkacz-struff-2026-07-05-first-set-total-10pt5",
    "will-spacex-starship-flight-test-13-launch-by-august-31",
    "another-gta-vi-trailer-released-by-august-31-20260629164610050",
    "rocket-labs-neutron-rocket-launch-by-december-31-691",
)


async def main() -> None:
    async with AsyncPublicClient() as client:
        for slug in SLUGS:
            market = await client.get_market(slug=slug, include_tag=True)
            print("=" * 120)
            print(market.question)
            print("url:", f"https://polymarket.com/market/{market.slug}")
            print("yes/no:", market.outcomes.yes.price, market.outcomes.no.price)
            print("metrics:", market.metrics)
            print("state:", market.state)
            print("source:", market.resolution.source)
            print("description:", (market.description or "")[:1200].replace("\n", " "))


if __name__ == "__main__":
    asyncio.run(main())
