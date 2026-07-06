from __future__ import annotations

import argparse
import asyncio
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterable

from polymarket import AsyncPublicClient


CRAWLABLE_PATTERNS: tuple[tuple[str, str, int], ...] = (
    (r"\bearnings?\b|earnings call|conference call|guidance|eps|revenue", "earnings/transcript", 30),
    (r"\bsec\b|s-1|10-k|10-q|8-k|filing|edgar", "SEC/filing", 28),
    (r"\bfda\b|\bpdufa\b|clinical trial|phase [123]|drug approval", "FDA/clinical", 24),
    (r"\bweather\b|hurricane|storm|temperature|rainfall|snowfall|\brain\b|\bsnow\b|noaa|nws|precipitation", "weather/NOAA", 27),
    (r"\bearthquake\b|usgs", "USGS", 24),
    (r"\bbox office\b|domestic gross|billboard|spotify|youtube ads|twitch|netflix|imdb", "media ranking/API", 25),
    (r"\bepisode\b|season finale|love island|survivor|bachelor|big brother", "TV result/text", 22),
    (r"\btweet|tweets|x post|x posts|followers\b|instagram|tiktok", "social feed", 22),
    (r"\btoken\b|airdrop|mainnet|testnet|github release|app store|listing", "crypto/project official", 20),
    (r"\bscore\b|box score|game|match|ufc|mlb|wnba|nba|nhl|atp|wta|tennis|soccer|fifa|uefa", "sports score", 16),
    (r"\bcourt\b|ruling|injunction|verdict|sentenc|supreme court|appeals", "court docket/news", 18),
    (r"press release|official earnings|official account|official website|official video|official.*materials", "official announcement", 14),
)

BAD_PATTERNS: tuple[tuple[str, str, int], ...] = (
    (r"up or down|\bup/down\b", "5m price market, bot-heavy", -80),
    (r"\bbitcoin\b|\bbtc\b|\bethereum\b|\beth\b|\bxrp\b|\bdogecoin\b|\bdoge\b|\bsolana\b|\bsol\b|hyperliquid", "major crypto, bot-heavy", -45),
    (r"president|election winner|nominee|democratic nomination|republican nomination|mayor|governor|senate|house", "politics/long horizon", -26),
    (r"fed|fomc|cpi|inflation|rate cut|interest rate|jobs report|unemployment", "macro, crowded", -24),
    (r"by 2028|by 2029|by 2030|in 2028|in 2029|in 2030", "too long horizon", -18),
    (r"credible reporting|consensus of credible", "resolution may be ambiguous", -12),
)


@dataclass(frozen=True)
class Candidate:
    score: float
    question: str
    url: str
    yes_price: Decimal | None
    no_price: Decimal | None
    volume: Decimal
    volume_24hr: Decimal
    liquidity: Decimal
    spread: Decimal | None
    end_date: datetime | None
    source: str | None
    category: str | None
    tags: tuple[str, ...]
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]


def dec(value: Decimal | None) -> Decimal:
    return value if value is not None else Decimal("0")


def as_float(value: Decimal | None) -> float:
    return float(value or Decimal("0"))


def text_blob(market: object) -> str:
    pieces: list[str] = []
    for attr in ("question", "description", "category", "slug"):
        value = getattr(market, attr, None)
        if value:
            pieces.append(str(value))
    for tag in getattr(market, "tags", ()) or ():
        pieces.append(str(getattr(tag, "slug", "") or ""))
        pieces.append(str(getattr(tag, "label", "") or ""))
    for event in getattr(market, "events", ()) or ():
        pieces.append(str(getattr(event, "slug", "") or ""))
        pieces.append(str(getattr(event, "title", "") or ""))
    resolution = getattr(market, "resolution", None)
    if resolution is not None:
        pieces.append(str(getattr(resolution, "source", "") or ""))
    return " ".join(pieces).lower()


def has_match(patterns: Iterable[tuple[str, str, int]], blob: str) -> list[tuple[str, int]]:
    hits: list[tuple[str, int]] = []
    for pattern, label, score in patterns:
        if re.search(pattern, blob, flags=re.IGNORECASE):
            hits.append((label, score))
    return hits


def days_until(end_date: datetime | None) -> float | None:
    if end_date is None:
        return None
    now = datetime.now(timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    return (end_date - now).total_seconds() / 86400


def market_url(market: object) -> str:
    events = getattr(market, "events", ()) or ()
    if events and getattr(events[0], "slug", None):
        return f"https://polymarket.com/event/{events[0].slug}"
    return f"https://polymarket.com/market/{getattr(market, 'slug', '')}"


def score_market(market: object) -> Candidate | None:
    state = getattr(market, "state", None)
    outcomes = getattr(market, "outcomes", None)
    metrics = getattr(market, "metrics", None)
    prices = getattr(market, "prices", None)
    resolution = getattr(market, "resolution", None)
    sports = getattr(market, "sports", None)

    if not state or not outcomes or not metrics:
        return None
    if getattr(state, "closed", None) or getattr(state, "archived", None):
        return None
    if getattr(state, "active", None) is False:
        return None

    yes = getattr(outcomes, "yes", None)
    no = getattr(outcomes, "no", None)
    yes_price = getattr(yes, "price", None)
    no_price = getattr(no, "price", None)
    if yes_price is None or no_price is None:
        return None

    volume = dec(getattr(metrics, "volume_num", None) or getattr(metrics, "volume", None))
    volume_24hr = dec(getattr(metrics, "volume_24hr", None))
    liquidity = dec(getattr(metrics, "liquidity_num", None) or getattr(metrics, "liquidity", None))
    spread = getattr(prices, "spread", None) if prices else None
    end_date = getattr(state, "end_date", None)

    if volume < Decimal("50") or liquidity < Decimal("50"):
        return None
    if volume > Decimal("250000") or liquidity > Decimal("60000"):
        return None
    if yes_price <= Decimal("0.01") or no_price <= Decimal("0.01"):
        return None
    if spread is not None and spread > Decimal("0.50"):
        return None

    blob = text_blob(market)
    reasons: list[str] = []
    warnings: list[str] = []
    score = 0.0

    for label, points in has_match(CRAWLABLE_PATTERNS, blob):
        reasons.append(label)
        score += points
    for label, points in has_match(BAD_PATTERNS, blob):
        warnings.append(label)
        score += points

    source = getattr(resolution, "source", None) if resolution else None
    if source:
        reasons.append(f"source: {source[:80]}")
        score += 12
    else:
        warnings.append("no explicit resolution source")
        score -= 8

    if getattr(state, "enable_order_book", None) is True:
        score += 5
    elif getattr(state, "enable_order_book", None) is False:
        warnings.append("order book disabled in gamma state")
        score -= 18

    if getattr(state, "accepting_orders", None) is False:
        warnings.append("not accepting orders")
        score -= 30

    dte = days_until(end_date)
    if dte is None:
        warnings.append("no end date")
        score -= 6
    elif dte < -1:
        warnings.append("end date already passed")
        score -= 30
    elif dte <= 30:
        score += 18
    elif dte <= 120:
        score += 10
    elif dte > 365:
        warnings.append("long horizon")
        score -= 12

    vol = max(as_float(volume), 1.0)
    liq = max(as_float(liquidity), 1.0)
    score += max(0.0, 18.0 - math.log10(vol) * 3.2)
    score += max(0.0, 12.0 - math.log10(liq) * 2.2)
    if Decimal("100") <= volume_24hr <= Decimal("10000"):
        score += 8
    elif volume_24hr == 0:
        score -= 4
    if spread is not None:
        score += max(0.0, 10.0 - float(spread) * 35.0)

    if getattr(sports, "game_id", None):
        reasons.append(f"sports game id: {sports.game_id}")
        score += 7

    if not reasons:
        return None

    tags = tuple(
        (getattr(tag, "slug", None) or getattr(tag, "label", None) or "")
        for tag in (getattr(market, "tags", ()) or ())
    )

    return Candidate(
        score=score,
        question=getattr(market, "question", "") or "",
        url=market_url(market),
        yes_price=yes_price,
        no_price=no_price,
        volume=volume,
        volume_24hr=volume_24hr,
        liquidity=liquidity,
        spread=spread,
        end_date=end_date,
        source=source,
        category=getattr(market, "category", None),
        tags=tags,
        reasons=tuple(dict.fromkeys(reasons)),
        warnings=tuple(dict.fromkeys(warnings)),
    )


async def scan(max_pages: int, page_size: int) -> list[Candidate]:
    candidates: list[Candidate] = []
    seen_questions: set[str] = set()
    async with AsyncPublicClient() as client:
        markets = client.list_markets(
            closed=False,
            liquidity_num_min=50,
            liquidity_num_max=60000,
            volume_num_min=50,
            volume_num_max=250000,
            page_size=page_size,
        )
        page_count = 0
        total = 0
        async for page in markets:
            page_count += 1
            for market in page.items:
                total += 1
                candidate = score_market(market)
                if candidate is None:
                    continue
                key = candidate.question.lower()
                if key in seen_questions:
                    continue
                seen_questions.add(key)
                candidates.append(candidate)
            print(f"processed page {page_count}, total markets {total}, candidates {len(candidates)}")
            if page_count >= max_pages:
                break
    return sorted(candidates, key=lambda item: item.score, reverse=True)


def fmt_decimal(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"{float(value):.4g}"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=40)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--top", type=int, default=30)
    args = parser.parse_args()

    candidates = await scan(args.pages, args.page_size)
    print()
    print(f"TOP {min(args.top, len(candidates))} / {len(candidates)} candidates")
    print("=" * 120)
    for idx, item in enumerate(candidates[: args.top], start=1):
        print(f"{idx:02d}. score={item.score:.1f}  {item.question}")
        print(f"    url: {item.url}")
        print(
            "    price yes/no="
            f"{fmt_decimal(item.yes_price)}/{fmt_decimal(item.no_price)}"
            f"  vol={fmt_decimal(item.volume)}"
            f"  vol24={fmt_decimal(item.volume_24hr)}"
            f"  liq={fmt_decimal(item.liquidity)}"
            f"  spread={fmt_decimal(item.spread)}"
        )
        print(f"    end: {item.end_date}  category: {item.category}  tags: {', '.join(item.tags[:6])}")
        print(f"    why: {', '.join(item.reasons)}")
        if item.warnings:
            print(f"    watch: {', '.join(item.warnings)}")


if __name__ == "__main__":
    asyncio.run(main())
