from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict

from polymarket import AsyncPublicClient

from find_small_crawlable_markets import Candidate, score_market


QUERY_BUCKETS: dict[str, tuple[str, ...]] = {
    "earnings_filings": ("earnings call", "earnings", "SEC filing", "8-K", "FDA approval"),
    "weather": ("hurricane", "weather", "temperature", "NOAA", "rain", "snow"),
    "social_media": ("tweets", "tweet", "X posts", "followers"),
    "media_charts": ("Spotify", "Billboard", "box office", "YouTube", "album", "song"),
    "project_releases": ("token launch", "airdrop", "mainnet", "GitHub release", "launch"),
    "sports_scores": ("MLB", "WNBA", "UFC", "tennis", "soccer score"),
    "legal_court": ("court", "verdict", "sentenced", "ruling"),
    "space": ("SpaceX", "Starship", "rocket launch", "NASA"),
}


async def collect_bucket(
    client: AsyncPublicClient,
    bucket: str,
    query: str,
    pages: int,
    page_size: int,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    paginator = client.search(
        q=query,
        events_status="active",
        sort="volume",
        page_size=page_size,
    )
    page_count = 0
    async for page in paginator:
        page_count += 1
        for result in page.items:
            for event in result.events:
                for market in event.markets:
                    candidate = score_market(market)
                    if candidate is None:
                        continue
                    candidates.append(candidate)
        if page_count >= pages:
            break
    print(f"{bucket:16s} query={query!r:18s} candidates={len(candidates)}")
    return candidates


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=2)
    parser.add_argument("--page-size", type=int, default=20)
    parser.add_argument("--top", type=int, default=8)
    args = parser.parse_args()

    by_bucket: dict[str, dict[str, Candidate]] = defaultdict(dict)
    async with AsyncPublicClient() as client:
        for bucket, queries in QUERY_BUCKETS.items():
            for query in queries:
                for candidate in await collect_bucket(client, bucket, query, args.pages, args.page_size):
                    old = by_bucket[bucket].get(candidate.question)
                    if old is None or candidate.score > old.score:
                        by_bucket[bucket][candidate.question] = candidate

    print()
    print("=" * 120)
    for bucket, items in by_bucket.items():
        ranked = sorted(items.values(), key=lambda item: item.score, reverse=True)
        print()
        print(f"[{bucket}] top {min(args.top, len(ranked))} / {len(ranked)}")
        for idx, item in enumerate(ranked[: args.top], start=1):
            print(f"{idx:02d}. score={item.score:.1f} {item.question}")
            print(f"    {item.url}")
            print(
                "    yes/no="
                f"{item.yes_price}/{item.no_price}"
                f" vol={item.volume} vol24={item.volume_24hr}"
                f" liq={item.liquidity} spread={item.spread}"
            )
            print(f"    end={item.end_date} why={', '.join(item.reasons)}")
            if item.warnings:
                print(f"    watch={', '.join(item.warnings)}")


if __name__ == "__main__":
    asyncio.run(main())
