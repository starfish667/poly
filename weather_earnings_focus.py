from __future__ import annotations

import argparse
import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from polymarket import AsyncPublicClient


WEATHER_QUERIES = (
    "highest temperature",
    "temperature",
    "Wunderground",
    "hurricane",
    "NOAA",
    "precipitation",
    "rainfall",
    "snowfall",
)

EARNINGS_QUERIES = (
    "quarterly earnings",
    "earnings EPS",
    "non-GAAP EPS",
    "GAAP EPS",
    "revenue above",
    "earnings materials",
    "SeekingAlpha",
)


@dataclass(frozen=True)
class Row:
    bucket: str
    score: float
    question: str
    url: str
    yes: Decimal | None
    no: Decimal | None
    volume: Decimal
    volume_24hr: Decimal
    liquidity: Decimal
    spread: Decimal | None
    end_date: datetime | None
    source: str
    trigger: str
    risks: str


def d(value: Decimal | None) -> Decimal:
    return value or Decimal("0")


def text(market: object) -> str:
    parts = [
        getattr(market, "question", "") or "",
        getattr(market, "description", "") or "",
        getattr(getattr(market, "resolution", None), "source", "") or "",
        getattr(market, "slug", "") or "",
    ]
    return " ".join(parts)


def event_url(market: object) -> str:
    events = getattr(market, "events", ()) or ()
    if events and getattr(events[0], "slug", None):
        return f"https://polymarket.com/event/{events[0].slug}"
    return f"https://polymarket.com/market/{getattr(market, 'slug', '')}"


def days_left(end_date: datetime | None) -> float:
    if end_date is None:
        return 9999
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    return (end_date - datetime.now(timezone.utc)).total_seconds() / 86400


def base_tradeable(market: object) -> bool:
    state = getattr(market, "state", None)
    outcomes = getattr(market, "outcomes", None)
    metrics = getattr(market, "metrics", None)
    if not state or not outcomes or not metrics:
        return False
    if getattr(state, "closed", None) or getattr(state, "archived", None):
        return False
    if getattr(state, "active", None) is False:
        return False
    if getattr(state, "accepting_orders", None) is False:
        return False
    if getattr(outcomes.yes, "price", None) is None or getattr(outcomes.no, "price", None) is None:
        return False
    volume = d(getattr(metrics, "volume_num", None) or getattr(metrics, "volume", None))
    liquidity = d(getattr(metrics, "liquidity_num", None) or getattr(metrics, "liquidity", None))
    if volume < Decimal("50") or liquidity < Decimal("50"):
        return False
    if volume > Decimal("300000") or liquidity > Decimal("70000"):
        return False
    return True


def classify_weather(market: object) -> Row | None:
    if not base_tradeable(market):
        return None
    blob = text(market)
    low = blob.lower()
    source = getattr(getattr(market, "resolution", None), "source", None) or ""
    source_low = source.lower()
    is_weather = (
        "wunderground.com/history/daily" in source_low
        or "noaa" in source_low
        or "weather.gov" in source_low
        or "nhc.noaa.gov" in source_low
        or "highest temperature" in low
        or "hurricane" in low
        or "precipitation" in low
        or "rainfall" in low
        or "snowfall" in low
    )
    if not is_weather:
        return None

    metrics = market.metrics
    prices = market.prices
    volume = d(metrics.volume_num or metrics.volume)
    volume_24hr = d(metrics.volume_24hr)
    liquidity = d(metrics.liquidity_num or metrics.liquidity)
    end_date = market.state.end_date
    spread = prices.spread

    score = 0.0
    score += 35 if "wunderground.com/history/daily" in source_low else 0
    score += 20 if "highest temperature" in low else 0
    score += 16 if 0 <= days_left(end_date) <= 3 else 0
    score += 8 if Decimal("100") <= volume_24hr <= Decimal("10000") else 0
    score += max(0, 12 - float(spread or 0) * 40)
    score += max(0, 12 - min(float(volume) / 25000, 12))
    if "hurricane" in low:
        score -= 8

    trigger = "poll Wunderground history page; resolve after next-date first datapoint publishes"
    if "hurricane" in low:
        trigger = "monitor NHC/NOAA advisories and official landfall/category records"
    risks = "fast bots on popular cities; Wunderground publication/revision timing"
    return Row(
        bucket="weather",
        score=score,
        question=market.question or "",
        url=event_url(market),
        yes=market.outcomes.yes.price,
        no=market.outcomes.no.price,
        volume=volume,
        volume_24hr=volume_24hr,
        liquidity=liquidity,
        spread=spread,
        end_date=end_date,
        source=source,
        trigger=trigger,
        risks=risks,
    )


def classify_earnings(market: object) -> Row | None:
    if not base_tradeable(market):
        return None
    blob = text(market)
    low = blob.lower()
    source = getattr(getattr(market, "resolution", None), "source", None) or ""
    is_earnings = (
        "earnings" in low
        and (
            "eps" in low
            or "revenue" in low
            or "official earnings materials" in low
            or "seekingalpha" in low
        )
    )
    if not is_earnings:
        return None

    metrics = market.metrics
    prices = market.prices
    volume = d(metrics.volume_num or metrics.volume)
    volume_24hr = d(metrics.volume_24hr)
    liquidity = d(metrics.liquidity_num or metrics.liquidity)
    end_date = market.state.end_date
    spread = prices.spread

    score = 0.0
    score += 30 if "eps" in low else 0
    score += 22 if "official earnings" in low or "official company earnings" in low else 0
    score += 16 if "seekingalpha.com" in source.lower() or "seekingalpha" in low else 0
    score += 18 if 0 <= days_left(end_date) <= 14 else 0
    score += 8 if Decimal("100") <= volume_24hr <= Decimal("5000") else 0
    score += max(0, 12 - float(spread or 0) * 45)
    score += max(0, 12 - min(float(volume) / 20000, 12))
    if re.search(r"\b(nflx|tsm|pep|googl|ms|blk)\b", low):
        score -= 7

    trigger = "monitor company IR/press release/SEC filing; parse EPS/revenue number immediately"
    risks = "pre-release expected move may reduce edge; SeekingAlpha fallback can lag or block scraping"
    return Row(
        bucket="earnings",
        score=score,
        question=market.question or "",
        url=event_url(market),
        yes=market.outcomes.yes.price,
        no=market.outcomes.no.price,
        volume=volume,
        volume_24hr=volume_24hr,
        liquidity=liquidity,
        spread=spread,
        end_date=end_date,
        source=source,
        trigger=trigger,
        risks=risks,
    )


async def collect(
    client: AsyncPublicClient,
    queries: tuple[str, ...],
    classifier,
    pages: int,
    page_size: int,
) -> list[Row]:
    rows: dict[str, Row] = {}
    for query in queries:
        paginator = client.search(q=query, events_status="active", sort="volume", page_size=page_size)
        count = 0
        async for page in paginator:
            count += 1
            for result in page.items:
                for event in result.events:
                    for market in event.markets:
                        row = classifier(market)
                        if row is None:
                            continue
                        old = rows.get(row.question)
                        if old is None or row.score > old.score:
                            rows[row.question] = row
            if count >= pages:
                break
    return sorted(rows.values(), key=lambda row: row.score, reverse=True)


def f(value: Decimal | None) -> str:
    return "-" if value is None else f"{float(value):.4g}"


def print_rows(title: str, rows: list[Row], top: int) -> None:
    print()
    print(title)
    print("=" * 120)
    for idx, row in enumerate(rows[:top], start=1):
        print(f"{idx:02d}. score={row.score:.1f} {row.question}")
        print(f"    {row.url}")
        print(
            f"    yes/no={f(row.yes)}/{f(row.no)}"
            f" vol={f(row.volume)} vol24={f(row.volume_24hr)}"
            f" liq={f(row.liquidity)} spread={f(row.spread)} end={row.end_date}"
        )
        print(f"    source={row.source or '(description only)'}")
        print(f"    trigger={row.trigger}")
        print(f"    risk={row.risks}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=3)
    parser.add_argument("--page-size", type=int, default=25)
    parser.add_argument("--top", type=int, default=12)
    args = parser.parse_args()

    async with AsyncPublicClient() as client:
        weather = await collect(client, WEATHER_QUERIES, classify_weather, args.pages, args.page_size)
        earnings = await collect(client, EARNINGS_QUERIES, classify_earnings, args.pages, args.page_size)

    print_rows("WEATHER", weather, args.top)
    print_rows("EARNINGS", earnings, args.top)


if __name__ == "__main__":
    asyncio.run(main())
