from __future__ import annotations

import asyncio
import csv
import gzip
import io
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import httpx
from polymarket import AsyncPublicClient

from polybot.markets import snapshot_from_market
from polybot.prices import PriceWebSocketCache
from polybot.trader import build_buy_plan
from polybot.types import TradePlan
from polybot.weather import (
    AVIATION_WEATHER_HEADERS,
    AVIATION_WEATHER_METAR_URL,
    TemperatureRule,
    Unit,
    aviation_observation_temp,
    aviation_observation_time,
    fetch_weather_com_history,
    historical_observation_temp,
    parse_temperature_rule,
    rounded_resolution_temperature,
    station_id_from_source,
    weather_local_tz,
)


StrategySource = Literal["auto", "aviationweather", "ecmwf"]

WEATHER_QUERIES = (
    "highest temperature",
    "Wunderground highest temperature",
    "temperature July",
)
TEMP_MARKET_SUFFIX = re.compile(
    r"-(?:neg)?\d+(?:pt\d+)?c(?:orbelow|orhigher)?$",
    flags=re.IGNORECASE,
)
ECMWF_OPEN_DATA_URL = "https://data.ecmwf.int/forecasts"
AVIATION_WEATHER_METAR_CACHE_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"


@dataclass
class WeatherSourceDecision:
    name: str
    latency_seconds: float | None
    min_interval_seconds: float
    reason: str


@dataclass
class WeatherEventCandidate:
    event_slug: str
    city: str
    source: str
    station_id: str
    timezone_name: str
    target_date: date
    volume: Decimal
    volume_24hr: Decimal
    liquidity: Decimal
    market_count: int = 0
    open_market_count: int = 0

    @property
    def url(self) -> str:
        return f"https://polymarket.com/event/{self.event_slug}"


@dataclass(frozen=True)
class ActionableNoMarket:
    question: str
    url: str
    slug: str
    token_id: str
    rule: TemperatureRule
    observed_high: Decimal
    rounded_high: Decimal
    event: WeatherEventCandidate

    @property
    def key(self) -> str:
        return f"{self.slug}:NO"


@dataclass
class TriggerState:
    fired: set[str]
    observed_maxima: dict[str, Decimal]
    checked_maxima: dict[str, Decimal]


@dataclass(frozen=True)
class TemperatureStats:
    source: str
    latest_by_unit: dict[Unit, Decimal]
    high_by_unit: dict[Unit, Decimal]
    latest_time_by_unit: dict[Unit, datetime]
    high_time_by_unit: dict[Unit, datetime]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def format_observed_time(observed_at: datetime | None, local_tz: ZoneInfo) -> str:
    if observed_at is None:
        return "unknown"
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    local_at = observed_at.astimezone(local_tz)
    return (
        f"{observed_at.astimezone(timezone.utc).isoformat(timespec='seconds')} UTC/"
        f"{local_at.isoformat(timespec='seconds')} local"
    )


def event_slug_from_market_slug(slug: str) -> str | None:
    stripped = TEMP_MARKET_SUFFIX.sub("", slug)
    return stripped if stripped != slug else None


def city_from_event_slug(event_slug: str) -> str:
    prefix = "highest-temperature-in-"
    if event_slug.startswith(prefix) and "-on-" in event_slug:
        raw_city = event_slug[len(prefix) :].split("-on-", 1)[0]
    else:
        raw_city = event_slug
    return " ".join(part.capitalize() for part in raw_city.split("-"))


def decimal_or_zero(value: Decimal | None) -> Decimal:
    return value if value is not None else Decimal("0")


def is_tradeable_weather_market(market: object) -> bool:
    state = getattr(market, "state", None)
    if state is None:
        return False
    if getattr(state, "closed", None) or getattr(state, "archived", None):
        return False
    if getattr(state, "active", None) is False:
        return False
    if getattr(state, "accepting_orders", None) is False:
        return False
    snapshot = snapshot_from_market(market)
    if "wunderground.com/history/daily" not in snapshot.source.lower():
        return False
    return snapshot.yes_token_id is not None and snapshot.no_token_id is not None


def is_open_price_pair(market: object, *, settled_price: Decimal = Decimal("0.995")) -> bool:
    snapshot = snapshot_from_market(market)
    return (
        snapshot.yes_price is not None
        and snapshot.no_price is not None
        and snapshot.yes_price < settled_price
        and snapshot.no_price < settled_price
    )


def event_from_market(market: object) -> WeatherEventCandidate | None:
    if not is_tradeable_weather_market(market):
        return None

    snapshot = snapshot_from_market(market)
    event_slug = event_slug_from_market_slug(snapshot.slug)
    if event_slug is None or not event_slug.startswith("highest-temperature-in-"):
        return None

    fallback_year = snapshot.end_date.year if snapshot.end_date else datetime.now(timezone.utc).year
    try:
        rule = parse_temperature_rule(
            snapshot.question,
            fallback_year=fallback_year,
            slug=snapshot.slug,
        )
        station_id = station_id_from_source(snapshot.source)
        local_tz = weather_local_tz(snapshot.source)
    except (ValueError, KeyError) as error:
        print(f"[{utc_now()}] skip weather market {snapshot.slug}: {error}")
        return None

    return WeatherEventCandidate(
        event_slug=event_slug,
        city=city_from_event_slug(event_slug),
        source=snapshot.source,
        station_id=station_id,
        timezone_name=local_tz.key,
        target_date=rule.date,
        volume=snapshot.volume,
        volume_24hr=snapshot.volume_24hr,
        liquidity=snapshot.liquidity,
        market_count=1,
        open_market_count=1 if is_open_price_pair(market) else 0,
    )


def merge_event(old: WeatherEventCandidate, new: WeatherEventCandidate) -> WeatherEventCandidate:
    old.volume += new.volume
    old.volume_24hr += new.volume_24hr
    old.liquidity += new.liquidity
    old.market_count += new.market_count
    old.open_market_count += new.open_market_count
    return old


def is_target_today(event: WeatherEventCandidate, *, lookahead_days: int) -> bool:
    local_today = datetime.now(timezone.utc).astimezone(ZoneInfo(event.timezone_name)).date()
    offset = (event.target_date - local_today).days
    return 0 <= offset <= lookahead_days


async def discover_weather_events(
    client: AsyncPublicClient,
    *,
    city_count: int,
    pages: int,
    page_size: int,
    lookahead_days: int,
    min_liquidity: Decimal,
    max_event_volume: Decimal,
    max_event_liquidity: Decimal | None,
) -> list[WeatherEventCandidate]:
    events: dict[str, WeatherEventCandidate] = {}
    seen_markets: set[str] = set()
    for query in WEATHER_QUERIES:
        paginator = client.search(q=query, events_status="active", sort="volume", page_size=page_size)
        page_count = 0
        async for page in paginator:
            page_count += 1
            for result in page.items:
                for event in result.events:
                    for market in event.markets:
                        market_slug = getattr(market, "slug", "") or ""
                        if market_slug in seen_markets:
                            continue
                        seen_markets.add(market_slug)
                        candidate = event_from_market(market)
                        if candidate is None:
                            continue
                        existing = events.get(candidate.event_slug)
                        if existing is None:
                            events[candidate.event_slug] = candidate
                        else:
                            merge_event(existing, candidate)
            if page_count >= pages:
                break

    filtered = [
        event
        for event in events.values()
        if is_target_today(event, lookahead_days=lookahead_days)
        and event.open_market_count > 0
        and event.liquidity >= min_liquidity
        and event.volume <= max_event_volume
        and (max_event_liquidity is None or event.liquidity <= max_event_liquidity)
    ]
    return sorted(
        filtered,
        key=lambda item: (
            item.volume,
            -item.open_market_count,
            -item.liquidity,
            item.city,
        ),
    )[:city_count]


async def fetch_aviation_metars_for_stations(
    station_ids: list[str],
    *,
    hours: int,
) -> dict[str, list[dict[str, object]]]:
    if not station_ids:
        return {}
    params = {
        "ids": ",".join(sorted(set(station_ids))),
        "format": "json",
        "hours": str(hours),
    }
    try:
        async with httpx.AsyncClient(timeout=30, headers=AVIATION_WEATHER_HEADERS) as client:
            response = await client.get(AVIATION_WEATHER_METAR_URL, params=params)
            response.raise_for_status()
            payload = response.json()
    except Exception:
        return {}
    if not isinstance(payload, list):
        return {}

    grouped: dict[str, list[dict[str, object]]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        station_id = item.get("icaoId") or item.get("station_id") or item.get("id")
        if not isinstance(station_id, str):
            continue
        grouped.setdefault(station_id.upper(), []).append(item)
    return grouped


async def fetch_aviation_metar_cache_for_stations(
    station_ids: list[str],
) -> dict[str, list[dict[str, object]]]:
    wanted = {station_id.upper() for station_id in station_ids if station_id}
    if not wanted:
        return {}

    try:
        async with httpx.AsyncClient(timeout=10, headers=AVIATION_WEATHER_HEADERS) as client:
            response = await client.get(AVIATION_WEATHER_METAR_CACHE_URL)
            response.raise_for_status()
            compressed = response.content
    except Exception:
        return {}

    try:
        text = gzip.decompress(compressed).decode("utf-8", errors="replace")
    except OSError:
        return {}

    data_lines = [
        line
        for line in text.splitlines()
        if line and not line.startswith("#")
    ]
    if not data_lines:
        return {}

    reader = csv.DictReader(io.StringIO("\n".join(data_lines)))
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in reader:
        station_id = (row.get("station_id") or "").upper()
        if station_id not in wanted:
            continue
        temp_c = row.get("temp_c")
        observation_time = row.get("observation_time")
        if not temp_c or not observation_time:
            continue
        grouped.setdefault(station_id, []).append(
            {
                "icaoId": station_id,
                "reportTime": observation_time,
                "temp": temp_c,
                "rawOb": row.get("raw_text"),
            }
        )
    return grouped


def aviation_temperature_points(
    observations: list[dict[str, object]],
    *,
    target_date: date,
    unit: Unit,
    local_tz: ZoneInfo,
) -> tuple[tuple[Decimal, datetime] | None, tuple[Decimal, datetime] | None]:
    high: tuple[Decimal, datetime] | None = None
    latest: tuple[Decimal, datetime] | None = None
    for item in observations:
        observed_at = aviation_observation_time(item)
        if observed_at is None or observed_at.astimezone(local_tz).date() != target_date:
            continue
        value = aviation_observation_temp(item, unit)
        if value is None:
            continue
        if high is None or value > high[0]:
            high = (value, observed_at)
        if latest is None or observed_at > latest[1]:
            latest = (value, observed_at)
    return high, latest


def aviation_temperature_stats(
    observations: list[dict[str, object]],
    *,
    target_date: date,
    local_tz: ZoneInfo,
    source: str = "aviationweather",
) -> TemperatureStats | None:
    high_by_unit: dict[Unit, Decimal] = {}
    latest_by_unit: dict[Unit, Decimal] = {}
    high_time_by_unit: dict[Unit, datetime] = {}
    latest_time_by_unit: dict[Unit, datetime] = {}
    for unit in ("C", "F"):
        high_point, latest_point = aviation_temperature_points(
            observations,
            target_date=target_date,
            unit=unit,
            local_tz=local_tz,
        )
        if high_point is not None:
            high_by_unit[unit] = high_point[0]
            high_time_by_unit[unit] = high_point[1]
        if latest_point is not None:
            latest_by_unit[unit] = latest_point[0]
            latest_time_by_unit[unit] = latest_point[1]
    if not high_by_unit:
        return None
    return TemperatureStats(
        source=source,
        latest_by_unit=latest_by_unit,
        high_by_unit=high_by_unit,
        latest_time_by_unit=latest_time_by_unit,
        high_time_by_unit=high_time_by_unit,
    )


def history_temperature_stats(
    observations: list[dict[str, object]],
) -> TemperatureStats | None:
    high_by_unit: dict[Unit, Decimal] = {}
    latest_by_unit: dict[Unit, Decimal] = {}
    high_time_by_unit: dict[Unit, datetime] = {}
    latest_time_by_unit: dict[Unit, datetime] = {}
    for unit in ("C", "F"):
        high_point: tuple[Decimal, datetime] | None = None
        latest_point: tuple[Decimal, datetime] | None = None
        for item in observations:
            observed_at = item.get("valid_time_gmt")
            if not isinstance(observed_at, int):
                continue
            value = historical_observation_temp(item, unit)
            if value is None:
                continue
            observed_dt = datetime.fromtimestamp(observed_at, tz=timezone.utc)
            if high_point is None or value > high_point[0]:
                high_point = (value, observed_dt)
            if latest_point is None or observed_dt > latest_point[1]:
                latest_point = (value, observed_dt)
        if high_point is not None:
            high_by_unit[unit] = high_point[0]
            high_time_by_unit[unit] = high_point[1]
        if latest_point is not None:
            latest_by_unit[unit] = latest_point[0]
            latest_time_by_unit[unit] = latest_point[1]
    if not high_by_unit:
        return None
    return TemperatureStats(
        source="weather.com",
        latest_by_unit=latest_by_unit,
        high_by_unit=high_by_unit,
        latest_time_by_unit=latest_time_by_unit,
        high_time_by_unit=high_time_by_unit,
    )


def best_temperature_stats(
    *stats_items: TemperatureStats | None,
) -> TemperatureStats | None:
    high_by_unit: dict[Unit, Decimal] = {}
    latest_by_unit: dict[Unit, Decimal] = {}
    high_time_by_unit: dict[Unit, datetime] = {}
    latest_time_by_unit: dict[Unit, datetime] = {}
    sources: set[str] = set()
    for unit in ("C", "F"):
        candidates = [
            item
            for item in stats_items
            if item is not None and unit in item.high_by_unit
        ]
        if not candidates:
            continue
        best = max(candidates, key=lambda item: item.high_by_unit[unit])
        high_by_unit[unit] = best.high_by_unit[unit]
        if unit in best.high_time_by_unit:
            high_time_by_unit[unit] = best.high_time_by_unit[unit]
        latest_candidates = [
            item
            for item in stats_items
            if item is not None
            and unit in item.latest_by_unit
            and unit in item.latest_time_by_unit
        ]
        if latest_candidates:
            latest = max(
                latest_candidates,
                key=lambda item: item.latest_time_by_unit[unit],
            )
            latest_by_unit[unit] = latest.latest_by_unit[unit]
            latest_time_by_unit[unit] = latest.latest_time_by_unit[unit]
            sources.add(latest.source)
        sources.add(best.source)
    if not high_by_unit:
        return None
    return TemperatureStats(
        source="+".join(sorted(sources)),
        latest_by_unit=latest_by_unit,
        high_by_unit=high_by_unit,
        latest_time_by_unit=latest_time_by_unit,
        high_time_by_unit=high_time_by_unit,
    )


async def fetch_history_for_event(event: WeatherEventCandidate) -> list[dict[str, object]]:
    try:
        return await fetch_weather_com_history(event.source, event.target_date)
    except Exception:
        return []


async def benchmark_get(
    url: str,
    *,
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> float | None:
    started_at = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=10, headers=headers) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
    except httpx.HTTPError:
        return None
    return time.perf_counter() - started_at


async def choose_weather_source(
    requested: StrategySource,
    *,
    station_ids: list[str],
    poll_interval: float,
) -> WeatherSourceDecision:
    if requested == "ecmwf":
        raise RuntimeError(
            "ECMWF open data is forecast data, not live observed station highs; "
            "this trigger bot requires observed station history."
        )

    sample_ids = station_ids[: min(len(station_ids), 6)] or ["EGLL"]
    aviation_latency = await benchmark_get(
        AVIATION_WEATHER_METAR_URL,
        params={
            "ids": ",".join(sample_ids),
            "format": "json",
            "hours": "1",
        },
        headers=AVIATION_WEATHER_HEADERS,
    )

    ecmwf_latency = None
    if requested == "auto":
        ecmwf_latency = await benchmark_get(ECMWF_OPEN_DATA_URL)

    reason = (
        "Weather.com/Wunderground history, AviationWeather METAR cache, and "
        "AviationWeather station API are fetched concurrently; the bot uses the "
        "highest rounded observed station high. ECMWF open data is forecast-only "
        "for this use case."
    )
    if aviation_latency is not None:
        reason += f" AviationWeather fallback benchmark latency={aviation_latency:.3f}s."
    if ecmwf_latency is not None:
        reason += f" ECMWF endpoint benchmark latency={ecmwf_latency:.3f}s but is not eligible."

    return WeatherSourceDecision(
        name="weather.com-history+aviationweather-fallback",
        latency_seconds=aviation_latency,
        min_interval_seconds=max(0.1, poll_interval),
        reason=reason,
    )


def event_unit_key(event: WeatherEventCandidate, unit: Unit) -> str:
    return f"{event.url}|{unit}"


def event_label(event: WeatherEventCandidate) -> str:
    return f"{event.city} {event.target_date.isoformat()} {event.station_id}"


def load_state(path: Path) -> TriggerState:
    if not path.exists():
        return TriggerState(fired=set(), observed_maxima={}, checked_maxima={})
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return TriggerState(fired=set(), observed_maxima={}, checked_maxima={})
    if not isinstance(raw, dict):
        return TriggerState(fired=set(), observed_maxima={}, checked_maxima={})
    fired = raw.get("fired")
    observed_maxima = raw.get("observed_maxima")
    checked_maxima = raw.get("checked_maxima")
    return TriggerState(
        fired={str(item) for item in fired} if isinstance(fired, list) else set(),
        observed_maxima=(
            {
                str(key): Decimal(str(value))
                for key, value in observed_maxima.items()
            }
            if isinstance(observed_maxima, dict)
            else {}
        ),
        checked_maxima=(
            {
                str(key): Decimal(str(value))
                for key, value in checked_maxima.items()
            }
            if isinstance(checked_maxima, dict)
            else {}
        ),
    )


def save_state(path: Path, state: TriggerState) -> None:
    payload = {
        "updated_at": utc_now(),
        "fired": sorted(state.fired),
        "observed_maxima": {
            key: str(value)
            for key, value in sorted(state.observed_maxima.items())
        },
        "checked_maxima": {
            key: str(value)
            for key, value in sorted(state.checked_maxima.items())
        },
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def rule_can_buy_no_after_high(rule: TemperatureRule, rounded_high: Decimal) -> bool:
    if rule.op == "eq":
        return rounded_high > rule.low
    if rule.op == "range":
        return rounded_high > rule.high
    if rule.op == "lte":
        return rounded_high > rule.low
    return False


async def actionable_no_markets(
    client: AsyncPublicClient,
    event: WeatherEventCandidate,
    *,
    observed_highs_by_unit: dict[Unit, Decimal],
    rounded_highs_by_unit: dict[Unit, Decimal],
) -> list[ActionableNoMarket]:
    event_payload = await client.get_event(url=event.url)
    markets: list[ActionableNoMarket] = []
    for market in event_payload.markets:
        if not is_tradeable_weather_market(market):
            continue
        snapshot = snapshot_from_market(market)
        fallback_year = snapshot.end_date.year if snapshot.end_date else datetime.now(timezone.utc).year
        try:
            rule = parse_temperature_rule(
                snapshot.question,
                fallback_year=fallback_year,
                slug=snapshot.slug,
            )
        except ValueError:
            continue
        if rule.date != event.target_date:
            continue
        observed_high = observed_highs_by_unit.get(rule.unit)
        rounded_high = rounded_highs_by_unit.get(rule.unit)
        if (
            observed_high is None
            or rounded_high is None
            or not rule_can_buy_no_after_high(rule, rounded_high)
        ):
            continue
        if snapshot.no_token_id is None:
            continue
        markets.append(
            ActionableNoMarket(
                question=snapshot.question,
                url=snapshot.url,
                slug=snapshot.slug,
                token_id=snapshot.no_token_id,
                rule=rule,
                observed_high=observed_high,
                rounded_high=rounded_high,
                event=event,
            )
        )
    return markets


async def no_token_ids_for_events(
    client: AsyncPublicClient,
    events: list[WeatherEventCandidate],
) -> tuple[set[str], dict[str, str]]:
    token_ids: set[str] = set()
    labels: dict[str, str] = {}
    for event in events:
        event_payload = await client.get_event(url=event.url)
        for market in event_payload.markets:
            if not is_tradeable_weather_market(market):
                continue
            snapshot = snapshot_from_market(market)
            if snapshot.no_token_id is not None:
                token_ids.add(snapshot.no_token_id)
                labels[snapshot.no_token_id] = f"{event_label(event)} NO {snapshot.question}"
    return token_ids, labels


def build_no_trade_plans(
    markets: list[ActionableNoMarket],
    *,
    prices: dict[str, Decimal | None],
    max_entry_price: Decimal,
    limit_price: Decimal,
    size: Decimal,
    live: bool,
    fired: set[str],
) -> tuple[list[TradePlan], list[str]]:
    plans: list[TradePlan] = []
    notes: list[str] = []
    for market in markets:
        if market.key in fired:
            notes.append(f"  skip fired: NO {market.question}")
            continue
        price = prices.get(market.token_id)
        if price is None:
            notes.append(f"  skip websocket ask unavailable/stale: NO {market.question}")
            continue
        if price > max_entry_price:
            notes.append(
                f"  skip price {price} > {max_entry_price}: NO {market.question}"
            )
            continue
        plan = build_buy_plan(
            market=snapshot_for_action(market),
            outcome="NO",
            limit_price=limit_price,
            size=size,
            live=live,
        )
        plans.append(plan)
        notes.append(
            f"  plan: BUY {size} NO @ {limit_price} "
            f"(ask={price}, raw_high={market.observed_high}{market.rule.unit}, "
            f"rounded={market.rounded_high}{market.rule.unit}) "
            f"{market.question}"
        )
    return plans, notes


def snapshot_for_action(market: ActionableNoMarket):
    from polybot.types import MarketSnapshot

    return MarketSnapshot(
        question=market.question,
        url=market.url,
        slug=market.slug,
        yes_token_id=None,
        no_token_id=market.token_id,
        yes_price=None,
        no_price=None,
        volume=Decimal("0"),
        volume_24hr=Decimal("0"),
        liquidity=Decimal("0"),
        spread=None,
        end_date=None,
        source=market.event.source,
        description="",
    )


class WeatherTriggerBot:
    def __init__(
        self,
        *,
        live: bool,
        city_count: int,
        poll_interval: float,
        discovery_interval: float,
        pages: int,
        page_size: int,
        lookahead_days: int,
        min_liquidity: Decimal,
        max_event_volume: Decimal,
        max_event_liquidity: Decimal | None,
        max_entry_price: Decimal,
        limit_price: Decimal,
        size: Decimal,
        state_path: Path,
        source: StrategySource,
        weather_hours: int,
        history_ttl_seconds: float,
        price_websocket_max_age: float,
        price_wait_seconds: float,
        stale_retry_seconds: float,
    ) -> None:
        self.live = live
        self.city_count = city_count
        self.poll_interval = poll_interval
        self.discovery_interval = discovery_interval
        self.pages = pages
        self.page_size = page_size
        self.lookahead_days = lookahead_days
        self.min_liquidity = min_liquidity
        self.max_event_volume = max_event_volume
        self.max_event_liquidity = max_event_liquidity
        self.max_entry_price = max_entry_price
        self.limit_price = limit_price
        self.size = size
        self.state_path = state_path
        self.source = source
        self.weather_hours = weather_hours
        self.history_ttl_seconds = history_ttl_seconds
        self.price_websocket_max_age = price_websocket_max_age
        self.price_wait_seconds = price_wait_seconds
        self.stale_retry_seconds = stale_retry_seconds
        self.state = load_state(state_path)
        self.session_fired: set[str] = set()
        self.session_observed_maxima = dict(self.state.observed_maxima)
        self.session_checked_maxima = dict(self.state.checked_maxima)
        self.stale_retry_deadlines: dict[str, float] = {}
        self.price_cache: PriceWebSocketCache | None = None
        self.price_token_ids: set[str] = set()
        self.price_token_labels: dict[str, str] = {}
        self.history_cache: dict[str, tuple[float, list[dict[str, object]]]] = {}

    @property
    def fired(self) -> set[str]:
        return self.state.fired if self.live else self.session_fired

    @property
    def observed_maxima(self) -> dict[str, Decimal]:
        return self.state.observed_maxima if self.live else self.session_observed_maxima

    @property
    def checked_maxima(self) -> dict[str, Decimal]:
        return self.state.checked_maxima if self.live else self.session_checked_maxima

    def maybe_save_state(self) -> None:
        if self.live:
            save_state(self.state_path, self.state)

    async def close_price_cache(self) -> None:
        if self.price_cache is not None:
            await self.price_cache.close()
            self.price_cache = None
            self.price_token_ids = set()

    def log_price_watch(
        self,
        token_id: str,
        previous_ask: Decimal | None,
        best_ask: Decimal | None,
    ) -> None:
        if best_ask is None:
            return
        previous_in_range = previous_ask is not None and previous_ask <= self.max_entry_price
        current_in_range = best_ask <= self.max_entry_price
        if previous_ask is not None and previous_in_range == current_in_range:
            return
        if previous_ask is None and not current_in_range:
            return
        direction = "enter" if current_in_range else "exit"
        label = self.price_token_labels.get(token_id, "unknown NO market")
        print(
            f"[{utc_now()}] websocket price watch {direction} <= {self.max_entry_price}: "
            f"ask {previous_ask} -> {best_ask} token={token_id} {label}"
        )

    async def ensure_price_cache(self, token_ids: set[str]) -> None:
        wanted = {str(token_id) for token_id in token_ids if token_id}
        if not wanted:
            return
        if self.price_cache is not None and wanted.issubset(self.price_token_ids):
            return

        await self.close_price_cache()
        self.price_cache = PriceWebSocketCache(
            wanted,
            max_age_seconds=self.price_websocket_max_age,
            on_best_ask_change=self.log_price_watch,
        )
        self.price_token_ids = wanted
        await self.price_cache.start()
        print(f"[{utc_now()}] price websocket subscribed to {len(wanted)} NO token(s)")

    async def refresh_price_cache_for_events(
        self,
        client: AsyncPublicClient,
        events: list[WeatherEventCandidate],
    ) -> float:
        started_at = time.perf_counter()
        token_ids, labels = await no_token_ids_for_events(client, events)
        self.price_token_labels.update(labels)
        await self.ensure_price_cache(token_ids)
        return time.perf_counter() - started_at

    async def weather_history_for_events(
        self,
        events: list[WeatherEventCandidate],
    ) -> dict[str, list[dict[str, object]]]:
        now = time.monotonic()
        results: dict[str, list[dict[str, object]]] = {}
        stale_events: list[WeatherEventCandidate] = []
        for event in events:
            cached = self.history_cache.get(event.url)
            if (
                cached is not None
                and self.history_ttl_seconds > 0
                and now - cached[0] < self.history_ttl_seconds
            ):
                results[event.url] = cached[1]
            else:
                stale_events.append(event)

        if stale_events:
            histories = await asyncio.gather(
                *(fetch_history_for_event(event) for event in stale_events)
            )
            fetched_at = time.monotonic()
            for event, history in zip(stale_events, histories, strict=True):
                self.history_cache[event.url] = (fetched_at, history)
                results[event.url] = history
        return results

    async def websocket_prices(
        self,
        markets: list[ActionableNoMarket],
    ) -> dict[str, Decimal | None]:
        token_ids = {market.token_id for market in markets}
        await self.ensure_price_cache(token_ids)
        if self.price_cache is None:
            return {token_id: None for token_id in token_ids}

        deadline = time.monotonic() + self.price_wait_seconds
        prices: dict[str, Decimal | None] = {}
        while True:
            prices = {
                token_id: self.price_cache.best_ask(token_id)
                for token_id in token_ids
            }
            if all(price is not None for price in prices.values()):
                return prices
            if time.monotonic() >= deadline:
                return prices
            await asyncio.sleep(0.05)

    async def run_once(self, client: AsyncPublicClient, events: list[WeatherEventCandidate]) -> None:
        cycle_started_at = time.perf_counter()
        station_ids = [event.station_id for event in events]
        weather_started_at = time.perf_counter()
        cache_by_station, metars_by_station, history_by_event = await asyncio.gather(
            fetch_aviation_metar_cache_for_stations(station_ids),
            fetch_aviation_metars_for_stations(
                station_ids,
                hours=self.weather_hours,
            ),
            self.weather_history_for_events(events),
        )
        weather_seconds = time.perf_counter() - weather_started_at
        for event in events:
            await self.process_event(
                client,
                event,
                cache_by_station.get(event.station_id, []),
                metars_by_station.get(event.station_id, []),
                history_by_event.get(event.url, []),
            )
        print(
            f"[{utc_now()}] cycle timing: weather={weather_seconds:.3f}s "
            f"total={time.perf_counter() - cycle_started_at:.3f}s"
        )

    async def process_event(
        self,
        client: AsyncPublicClient,
        event: WeatherEventCandidate,
        cached_metars: list[dict[str, object]],
        metars: list[dict[str, object]],
        history: list[dict[str, object]],
    ) -> None:
        event_started_at = time.perf_counter()
        local_tz = ZoneInfo(event.timezone_name)
        label = event_label(event)
        local_now = datetime.now(timezone.utc).astimezone(local_tz)
        if local_now.date() < event.target_date:
            print(
                f"[{utc_now()}] {label}: skip before local target date "
                f"local_now={local_now.isoformat(timespec='seconds')} "
                f"target_date={event.target_date.isoformat()} "
                f"(event={time.perf_counter() - event_started_at:.3f}s)"
            )
            return
        history_stats = history_temperature_stats(history)
        cache_stats = aviation_temperature_stats(
            cached_metars,
            target_date=event.target_date,
            local_tz=local_tz,
            source="aviationweather-cache",
        )
        aviation_stats = aviation_temperature_stats(
            metars,
            target_date=event.target_date,
            local_tz=local_tz,
        )
        stats = best_temperature_stats(cache_stats, history_stats, aviation_stats)
        observed_highs_by_unit: dict[Unit, Decimal] = {}
        rounded_highs_by_unit: dict[Unit, Decimal] = {}
        trade_check_keys: set[str] = set()
        needs_trade_check = False
        if stats is None:
            print(
                f"[{utc_now()}] {label}: waiting for first local-day weather observation "
                f"(event={time.perf_counter() - event_started_at:.3f}s)"
            )
            return
        for unit in ("C", "F"):
            observed = stats.high_by_unit.get(unit)
            if observed is None:
                continue
            latest = stats.latest_by_unit.get(unit)
            latest_time = stats.latest_time_by_unit.get(unit)
            high_time = stats.high_time_by_unit.get(unit)
            rounded = rounded_resolution_temperature(observed)
            observed_highs_by_unit[unit] = observed
            rounded_highs_by_unit[unit] = rounded
            key = event_unit_key(event, unit)
            previous = self.observed_maxima.get(key)
            checked = self.checked_maxima.get(key)
            if checked is None or rounded > checked:
                needs_trade_check = True
                trade_check_keys.add(key)
            if previous is None:
                self.observed_maxima[key] = observed
                print(
                    f"[{utc_now()}] {label}: baseline source={stats.source} "
                    f"latest={latest}{unit} max={observed}{unit} "
                    f"rounded={rounded}{unit} latest_at="
                    f"{format_observed_time(latest_time, local_tz)} max_at="
                    f"{format_observed_time(high_time, local_tz)} "
                    f"({event.timezone_name})"
                )
            elif observed > previous:
                previous_rounded = rounded_resolution_temperature(previous)
                self.observed_maxima[key] = observed
                print(
                    f"[{utc_now()}] {label}: high increased source={stats.source} "
                    f"latest={latest}{unit} max={observed}{unit} "
                    f"previous_max={previous}{unit} "
                    f"latest_at={format_observed_time(latest_time, local_tz)} "
                    f"max_at={format_observed_time(high_time, local_tz)} "
                    f"(rounded {previous_rounded}{unit} -> {rounded}{unit})"
                )
            else:
                previous_rounded = rounded_resolution_temperature(previous)
                print(
                    f"[{utc_now()}] {label}: no increase source={stats.source} "
                    f"latest={latest}{unit} max={observed}{unit} "
                    f"previous_max={previous}{unit} "
                    f"latest_at={format_observed_time(latest_time, local_tz)} "
                    f"max_at={format_observed_time(high_time, local_tz)} "
                    f"(rounded {rounded}{unit} <= {previous_rounded}{unit})"
                )

        if not rounded_highs_by_unit:
            print(
                f"[{utc_now()}] {label}: waiting for first local-day weather observation "
                f"(event={time.perf_counter() - event_started_at:.3f}s)"
            )
            return
        if not needs_trade_check:
            self.maybe_save_state()
            print(
                f"[{utc_now()}] {label}: timing "
                f"event={time.perf_counter() - event_started_at:.3f}s"
            )
            return

        markets_started_at = time.perf_counter()
        markets = await actionable_no_markets(
            client,
            event,
            observed_highs_by_unit=observed_highs_by_unit,
            rounded_highs_by_unit=rounded_highs_by_unit,
        )
        markets_seconds = time.perf_counter() - markets_started_at
        if not markets:
            for unit, rounded in rounded_highs_by_unit.items():
                key = event_unit_key(event, unit)
                if key in trade_check_keys:
                    self.checked_maxima[key] = rounded
            print(
                f"[{utc_now()}] {label}: no actionable NO markets below current high "
                f"(markets={markets_seconds:.3f}s event={time.perf_counter() - event_started_at:.3f}s)"
            )
            self.maybe_save_state()
            return

        prices_started_at = time.perf_counter()
        prices = await self.websocket_prices(markets)
        prices_seconds = time.perf_counter() - prices_started_at
        plans, notes = build_no_trade_plans(
            markets,
            prices=prices,
            max_entry_price=self.max_entry_price,
            limit_price=self.limit_price,
            size=self.size,
            live=self.live,
            fired=self.fired,
        )
        has_stale_price = any(
            prices.get(market.token_id) is None
            for market in markets
        )
        print(f"[{utc_now()}] {label}: {len(markets)} actionable NO market(s)")
        for note in notes:
            print(note)

        if not plans:
            should_mark_checked = True
            if has_stale_price and self.stale_retry_seconds > 0:
                now = time.monotonic()
                active_deadlines: list[float] = []
                for key in trade_check_keys:
                    deadline = self.stale_retry_deadlines.setdefault(
                        key,
                        now + self.stale_retry_seconds,
                    )
                    active_deadlines.append(deadline)
                if active_deadlines and now < max(active_deadlines):
                    should_mark_checked = False
                    remaining = max(active_deadlines) - now
                    print(
                        f"[{utc_now()}] {label}: stale websocket price; "
                        f"retrying rounded high for {remaining:.1f}s"
                    )
            if should_mark_checked:
                for unit, rounded in rounded_highs_by_unit.items():
                    key = event_unit_key(event, unit)
                    if key in trade_check_keys:
                        self.checked_maxima[key] = rounded
                        self.stale_retry_deadlines.pop(key, None)
            self.maybe_save_state()
            print(
                f"[{utc_now()}] {label}: timing "
                f"markets={markets_seconds:.3f}s websocket_price={prices_seconds:.3f}s "
                f"event={time.perf_counter() - event_started_at:.3f}s"
            )
            return

        from run_event_bot import execute_plans

        execute_started_at = time.perf_counter()
        results = await execute_plans(plans, live=self.live)
        execute_seconds = time.perf_counter() - execute_started_at
        for result in results:
            plan = result.plan
            print(
                f"  {result.status}: {plan.side} {plan.size} {plan.outcome} "
                f"@ {plan.limit_price} -> {result.detail}"
            )
            if result.ok:
                self.fired.add(f"{plan.market.slug}:NO")
        for unit, rounded in rounded_highs_by_unit.items():
            key = event_unit_key(event, unit)
            if key in trade_check_keys:
                self.checked_maxima[key] = rounded
                self.stale_retry_deadlines.pop(key, None)
        self.maybe_save_state()
        print(
            f"[{utc_now()}] {label}: timing "
            f"markets={markets_seconds:.3f}s websocket_price={prices_seconds:.3f}s "
            f"execute={execute_seconds:.3f}s event={time.perf_counter() - event_started_at:.3f}s"
        )

    async def run_forever(self) -> None:
        if self.live and os.getenv("POLYBOT_ENABLE_LIVE") != "1":
            raise RuntimeError("Refusing live trading unless POLYBOT_ENABLE_LIVE=1")

        events: list[WeatherEventCandidate] = []
        next_discovery = 0.0
        source_announced = False
        try:
            async with AsyncPublicClient() as client:
                while True:
                    now = time.monotonic()
                    if now >= next_discovery:
                        discovery_started_at = time.perf_counter()
                        events = await discover_weather_events(
                            client,
                            city_count=self.city_count,
                            pages=self.pages,
                            page_size=self.page_size,
                            lookahead_days=self.lookahead_days,
                            min_liquidity=self.min_liquidity,
                            max_event_volume=self.max_event_volume,
                            max_event_liquidity=self.max_event_liquidity,
                        )
                        discovery_seconds = time.perf_counter() - discovery_started_at
                        if not events:
                            print(
                                f"[{utc_now()}] discovery found no eligible weather events "
                                f"(discovery={discovery_seconds:.3f}s)"
                            )
                        else:
                            print(
                                f"[{utc_now()}] monitoring {len(events)} weather event(s) "
                                f"(discovery={discovery_seconds:.3f}s):"
                            )
                            for event in events:
                                print(
                                    f"  {event.city} {event.target_date} {event.station_id} "
                                    f"vol={event.volume} liq={event.liquidity} url={event.url}"
                                )
                            websocket_seconds = await self.refresh_price_cache_for_events(client, events)
                            print(
                                f"[{utc_now()}] websocket token refresh timing: "
                                f"{websocket_seconds:.3f}s"
                            )
                        if not source_announced:
                            decision = await choose_weather_source(
                                self.source,
                                station_ids=[event.station_id for event in events],
                                poll_interval=self.poll_interval,
                            )
                            print(f"[{utc_now()}] source={decision.name}: {decision.reason}")
                            source_announced = True
                        next_discovery = time.monotonic() + self.discovery_interval

                    if events:
                        try:
                            await self.run_once(client, events)
                        except Exception as error:  # keep the bot alive
                            print(f"[{utc_now()}] ERROR {type(error).__name__}: {error}")
                    await asyncio.sleep(self.poll_interval)
        finally:
            await self.close_price_cache()
