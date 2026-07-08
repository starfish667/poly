from __future__ import annotations

import argparse
import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from polymarket import AsyncPublicClient

from polybot.markets import snapshot_from_market
from polybot.weather import (
    parse_temperature_rule,
    station_id_from_source,
    weather_local_tz,
)


WEATHER_QUERIES = (
    "highest temperature",
    "Wunderground",
    "temperature July",
)
TEMP_MARKET_SUFFIX = re.compile(
    r"-(?:neg)?\d+(?:pt\d+)?c(?:orbelow|orhigher)?$",
    flags=re.IGNORECASE,
)
SETTLED_PRICE = Decimal("0.995")


@dataclass
class WeatherEvent:
    event_slug: str
    source: str
    station_id: str
    timezone_name: str
    target_date: datetime.date
    city: str
    volume: Decimal
    liquidity: Decimal
    volume_24hr: Decimal
    market_count: int = 0
    open_market_count: int = 0

    @property
    def url(self) -> str:
        return f"https://polymarket.com/event/{self.event_slug}"


def dec(value: Decimal | None) -> Decimal:
    return value if value is not None else Decimal("0")


def event_slug_from_market_slug(slug: str) -> str | None:
    stripped = TEMP_MARKET_SUFFIX.sub("", slug)
    if stripped == slug:
        return None
    return stripped


def city_from_event_slug(event_slug: str) -> str:
    prefix = "highest-temperature-in-"
    if event_slug.startswith(prefix) and "-on-" in event_slug:
        raw_city = event_slug[len(prefix) :].split("-on-", 1)[0]
    else:
        raw_city = event_slug
    return " ".join(part.capitalize() for part in raw_city.split("-"))


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def active_window(target_date: datetime.date, local_tz: ZoneInfo) -> tuple[str, str]:
    local_start = datetime.combine(target_date, time.min, tzinfo=local_tz) - timedelta(minutes=15)
    local_end = datetime.combine(target_date + timedelta(days=1), time(hour=6), tzinfo=local_tz)
    return utc_iso(local_start), utc_iso(local_end)


def is_tradeable_weather_market(market: object) -> bool:
    state = getattr(market, "state", None)
    snapshot = snapshot_from_market(market)
    if not state:
        return False
    if getattr(state, "closed", None) or getattr(state, "archived", None):
        return False
    if getattr(state, "accepting_orders", None) is False:
        return False
    return "wunderground.com/history/daily" in snapshot.source.lower()


def is_open_price_pair(market: object) -> bool:
    snapshot = snapshot_from_market(market)
    yes_price = snapshot.yes_price
    no_price = snapshot.no_price
    return (
        yes_price is not None
        and no_price is not None
        and yes_price < SETTLED_PRICE
        and no_price < SETTLED_PRICE
    )


def event_from_market(market: object) -> WeatherEvent | None:
    if not is_tradeable_weather_market(market):
        return None
    snapshot = snapshot_from_market(market)
    event_slug = event_slug_from_market_slug(snapshot.slug)
    if event_slug is None:
        return None
    if not event_slug.startswith("highest-temperature-in-"):
        return None
    fallback_year = snapshot.end_date.year if snapshot.end_date else datetime.now(timezone.utc).year
    try:
        rule = parse_temperature_rule(
            snapshot.question,
            fallback_year=fallback_year,
            slug=snapshot.slug,
        )
        local_tz = weather_local_tz(snapshot.source)
        station_id = station_id_from_source(snapshot.source)
    except (ValueError, KeyError):
        return None
    return WeatherEvent(
        event_slug=event_slug,
        source=snapshot.source,
        station_id=station_id,
        timezone_name=local_tz.key,
        target_date=rule.date,
        city=city_from_event_slug(event_slug),
        volume=snapshot.volume,
        liquidity=snapshot.liquidity,
        volume_24hr=snapshot.volume_24hr,
        market_count=1,
        open_market_count=1 if is_open_price_pair(market) else 0,
    )


def merge_event(old: WeatherEvent, new: WeatherEvent) -> WeatherEvent:
    old.volume += new.volume
    old.liquidity += new.liquidity
    old.volume_24hr += new.volume_24hr
    old.market_count += 1
    old.open_market_count += new.open_market_count
    return old


def within_date_window(event: WeatherEvent, days: int) -> bool:
    utc_today = datetime.now(timezone.utc).date()
    if event.target_date < utc_today:
        return False
    local_today = datetime.now(timezone.utc).astimezone(ZoneInfo(event.timezone_name)).date()
    offset = (event.target_date - local_today).days
    return 0 <= offset <= days


async def collect_weather_events(
    client: AsyncPublicClient,
    *,
    pages: int,
    page_size: int,
) -> list[WeatherEvent]:
    events: dict[str, WeatherEvent] = {}
    seen_market_slugs: set[str] = set()
    for query in WEATHER_QUERIES:
        paginator = client.search(q=query, events_status="active", sort="volume", page_size=page_size)
        page_count = 0
        async for page in paginator:
            page_count += 1
            for result in page.items:
                for event in result.events:
                    for market in event.markets:
                        market_slug = getattr(market, "slug", "") or ""
                        if market_slug in seen_market_slugs:
                            continue
                        seen_market_slugs.add(market_slug)
                        weather_event = event_from_market(market)
                        if weather_event is None:
                            continue
                        old = events.get(weather_event.event_slug)
                        if old is None:
                            events[weather_event.event_slug] = weather_event
                        else:
                            merge_event(old, weather_event)
            if page_count >= pages:
                break
    return list(events.values())


async def verified_events(
    client: AsyncPublicClient,
    events: list[WeatherEvent],
    *,
    verify: bool,
) -> list[WeatherEvent]:
    if not verify:
        return events
    valid: list[WeatherEvent] = []
    for event in events:
        try:
            await client.get_event(url=event.url)
        except Exception:  # noqa: BLE001 - diagnostics for guessed event slugs.
            continue
        valid.append(event)
    return valid


def watch_item(
    event: WeatherEvent,
    *,
    size: Decimal,
    limit_price: Decimal,
    max_entry_price: Decimal,
    interval: float,
    active_interval: float,
) -> dict[str, object]:
    active_from, active_until = active_window(event.target_date, ZoneInfo(event.timezone_name))
    return {
        "name": f"{event.city} {event.target_date.isoformat()} Temperature",
        "strategy": "weather",
        "url": event.url,
        "limit_price": str(limit_price),
        "max_entry_price": str(max_entry_price),
        "size": str(size),
        "interval": interval,
        "active_interval": active_interval,
        "active_from_utc": active_from,
        "active_until_utc": active_until,
        "station_id": event.station_id,
        "timezone": event.timezone_name,
        "source": event.source,
    }


def rank(events: list[WeatherEvent], *, target_volume: Decimal) -> list[WeatherEvent]:
    return sorted(
        events,
        key=lambda event: (
            event.target_date,
            abs(event.volume - target_volume),
            -event.liquidity,
            event.city,
        ),
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Build a weather-only Polymarket watchlist.")
    parser.add_argument("--output", default="weather_watchlist.json")
    parser.add_argument("--pages", type=int, default=4)
    parser.add_argument("--page-size", type=int, default=40)
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--max-events", type=int, default=14)
    parser.add_argument("--min-liquidity", default="100")
    parser.add_argument("--max-volume", default="150000")
    parser.add_argument("--target-volume", default="40000")
    parser.add_argument("--size", default="5")
    parser.add_argument("--limit-price", default="0.99")
    parser.add_argument("--max-entry-price", default="0.99")
    parser.add_argument("--interval", type=float, default=15)
    parser.add_argument("--active-interval", type=float, default=0.5)
    parser.add_argument("--no-verify-events", action="store_true")
    args = parser.parse_args()

    min_liquidity = Decimal(args.min_liquidity)
    max_volume = Decimal(args.max_volume)

    async with AsyncPublicClient() as client:
        events = await collect_weather_events(
            client,
            pages=args.pages,
            page_size=args.page_size,
        )
        events = [
            event
            for event in events
            if within_date_window(event, args.days)
            and event.open_market_count > 0
            and event.liquidity >= min_liquidity
            and event.volume <= max_volume
        ]
        events = await verified_events(
            client,
            rank(events, target_volume=Decimal(args.target_volume))[: args.max_events],
            verify=not args.no_verify_events,
        )

    payload = [
        watch_item(
            event,
            size=Decimal(args.size),
            limit_price=Decimal(args.limit_price),
            max_entry_price=Decimal(args.max_entry_price),
            interval=args.interval,
            active_interval=args.active_interval,
        )
        for event in events
    ]
    output_path = Path(args.output)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"wrote {len(payload)} weather watch item(s) to {output_path}")
    for event in events:
        print(
            f"- {event.target_date} {event.city:14s} {event.station_id:4s} "
            f"{event.timezone_name:20s} markets={event.market_count:2d} "
            f"open={event.open_market_count:2d} "
            f"vol={event.volume} vol24={event.volume_24hr} liq={event.liquidity}"
        )


if __name__ == "__main__":
    asyncio.run(main())
