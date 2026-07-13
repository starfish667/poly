from __future__ import annotations

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import httpx
from polymarket import AsyncPublicClient

from polybot.markets import snapshot_from_market
from polybot.prices import PriceWebSocketCache
from polybot.trader import build_buy_plan
from polybot.types import MarketSnapshot, Outcome, TradePlan


BucketKind = Literal["exact", "gt"]
TradePhase = Literal["count-no", "final-yes", "final-no"]
ReviewStatus = Literal["all", "reviewed"]

DEFAULT_EVENT_URL = (
    "https://polymarket.com/event/"
    "how-many-6pt5-or-above-earthquakes-july-6-july-12-20260702194353908"
)
EARTHQUAKE_SEARCH_QUERIES = (
    "6.5 or above earthquakes",
    "6.5 earthquakes",
    "earthquakes July",
    "how many earthquakes",
)
USGS_EVENT_QUERY_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
EASTERN_TZ = ZoneInfo("America/New_York")
UTC_TZ = ZoneInfo("UTC")

MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


@dataclass(frozen=True)
class CountBucket:
    kind: BucketKind
    value: int

    @property
    def label(self) -> str:
        return str(self.value) if self.kind == "exact" else f">{self.value}"

    def is_impossible_after_count(self, count: int) -> bool:
        return self.kind == "exact" and self.value < count

    def matches_final_count(self, count: int) -> bool:
        if self.kind == "exact":
            return self.value == count
        return count > self.value

    def sort_key(self) -> tuple[int, int]:
        return (self.value, 0 if self.kind == "exact" else 1)


@dataclass(frozen=True)
class EarthquakeMarket:
    snapshot: MarketSnapshot
    bucket: CountBucket

    def token_id(self, outcome: Outcome) -> str | None:
        return self.snapshot.yes_token_id if outcome == "YES" else self.snapshot.no_token_id

    def key(self, outcome: Outcome, phase: TradePhase) -> str:
        return f"{self.snapshot.slug}:{self.bucket.label}:{outcome}:{phase}"


@dataclass(frozen=True)
class EarthquakeEvent:
    event_id: str
    usgs_url: str
    title: str
    place: str
    magnitude: Decimal
    observed_at: datetime
    updated_at: datetime | None
    status: str | None


@dataclass(frozen=True)
class EarthquakeSnapshot:
    count: int
    events: list[EarthquakeEvent]
    fetched_at: datetime

    @property
    def latest_update(self) -> datetime | None:
        updates = [event.updated_at for event in self.events if event.updated_at is not None]
        return max(updates) if updates else None


@dataclass(frozen=True)
class EarthquakeRule:
    min_magnitude: Decimal
    start_utc: datetime
    end_utc: datetime
    source_text: str


@dataclass
class EarthquakeTriggerState:
    fired: set[str]
    last_count: int | None
    known_event_ids: set[str]
    final_yes_bought: bool
    event_url: str | None = None


@dataclass(frozen=True)
class EarthquakeEventCandidate:
    url: str
    title: str
    rule: EarthquakeRule
    markets: list[EarthquakeMarket]

    @property
    def sort_key(self) -> tuple[int, datetime]:
        now = datetime.now(timezone.utc)
        if self.rule.start_utc <= now <= self.rule.end_utc:
            return (0, self.rule.start_utc)
        if now < self.rule.start_utc:
            return (1, self.rule.start_utc)
        return (2, self.rule.end_utc)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_decimal(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def parse_datetime_arg(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_et_datetime(
    month_name: str,
    day_text: str,
    year_text: str,
    hour_text: str,
    minute_text: str,
    meridiem: str,
) -> datetime:
    hour = int(hour_text)
    if meridiem.upper() == "AM":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12
    local = datetime(
        int(year_text),
        MONTHS[month_name.lower()],
        int(day_text),
        hour,
        int(minute_text),
        tzinfo=EASTERN_TZ,
    )
    return local.astimezone(timezone.utc)


def parse_rule_from_text(text: str) -> EarthquakeRule | None:
    magnitude_match = re.search(
        r"(?:magnitude\s+of\s+|magnitude\s+)?(\d+(?:\.\d+)?)\s*(?:\+|or\s+(?:higher|above))",
        text,
        flags=re.IGNORECASE,
    )
    if magnitude_match is None:
        magnitude_match = re.search(
            r"(\d+)pt(\d+)[-\s]*(?:or[-\s]*)?(?:above|higher)",
            text,
            flags=re.IGNORECASE,
        )
        min_magnitude = (
            Decimal(f"{magnitude_match.group(1)}.{magnitude_match.group(2)}")
            if magnitude_match is not None
            else None
        )
    else:
        min_magnitude = Decimal(magnitude_match.group(1))

    window_match = re.search(
        r"between\s+"
        r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4}),\s*"
        r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET,\s*and\s+"
        r"([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4}),\s*"
        r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*ET",
        text,
        flags=re.IGNORECASE,
    )
    if min_magnitude is None or window_match is None:
        return None

    start_utc = parse_et_datetime(*window_match.groups()[:6])
    end_utc = parse_et_datetime(*window_match.groups()[6:])
    return EarthquakeRule(
        min_magnitude=min_magnitude,
        start_utc=start_utc,
        end_utc=end_utc,
        source_text="Polymarket rules text",
    )


def parse_bucket_text(value: str) -> CountBucket | None:
    text = value.strip()
    if not text:
        return None

    exact_match = re.fullmatch(r"(\d+)", text)
    if exact_match:
        return CountBucket("exact", int(exact_match.group(1)))

    gt_match = re.fullmatch(r">\s*(\d+)", text)
    if gt_match:
        return CountBucket("gt", int(gt_match.group(1)))

    words_gt = re.search(
        r"(?:more\s+than|greater\s+than|over)\s+(\d+)",
        text,
        flags=re.IGNORECASE,
    )
    if words_gt:
        return CountBucket("gt", int(words_gt.group(1)))

    return None


def parse_bucket_from_market(market: object) -> CountBucket | None:
    candidates = [
        getattr(market, "group_item_title", None),
        getattr(market, "question", None),
        getattr(market, "slug", None),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        bucket = parse_bucket_text(str(candidate))
        if bucket is not None:
            return bucket

    slug = str(getattr(market, "slug", "") or "")
    tail = slug.rsplit("-", 1)[-1]
    if tail.startswith("gt") and tail[2:].isdigit():
        return CountBucket("gt", int(tail[2:]))
    if tail.startswith("over") and tail[4:].isdigit():
        return CountBucket("gt", int(tail[4:]))
    if tail.isdigit():
        return CountBucket("exact", int(tail))
    return None


def is_tradeable_market(market: object) -> bool:
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
    return snapshot.yes_token_id is not None and snapshot.no_token_id is not None


def markets_from_event_payload(event_payload: object) -> list[EarthquakeMarket]:
    markets: list[EarthquakeMarket] = []
    for market in getattr(event_payload, "markets", ()) or ():
        if not is_tradeable_market(market):
            continue
        bucket = parse_bucket_from_market(market)
        if bucket is None:
            continue
        markets.append(EarthquakeMarket(snapshot=snapshot_from_market(market), bucket=bucket))
    return sorted(markets, key=lambda item: item.bucket.sort_key())


def text_from_event_payload(event_payload: object) -> str:
    pieces: list[str] = []
    for attr in ("title", "subtitle", "description", "slug"):
        value = getattr(event_payload, attr, None)
        if value:
            pieces.append(str(value))
    for market in getattr(event_payload, "markets", ()) or ():
        for attr in ("question", "description", "slug", "group_item_title"):
            value = getattr(market, attr, None)
            if value:
                pieces.append(str(value))
    return "\n".join(pieces)


def event_url_from_payload(event_payload: object) -> str | None:
    slug = getattr(event_payload, "slug", None)
    if not slug:
        return None
    return f"https://polymarket.com/event/{slug}"


def event_title_from_payload(event_payload: object) -> str:
    return str(
        getattr(event_payload, "title", None)
        or getattr(event_payload, "slug", None)
        or "earthquake event"
    )


def event_is_open(event_payload: object) -> bool:
    state = getattr(event_payload, "state", None)
    if state is None:
        return True
    if getattr(state, "closed", None) or getattr(state, "archived", None):
        return False
    if getattr(state, "active", None) is False:
        return False
    return True


def candidate_from_event_payload(
    event_payload: object,
    *,
    expected_min_magnitude: Decimal,
    now: datetime,
    lookahead_seconds: float,
    grace_seconds: float,
) -> EarthquakeEventCandidate | None:
    if not event_is_open(event_payload):
        return None
    url = event_url_from_payload(event_payload)
    if url is None:
        return None
    text = text_from_event_payload(event_payload)
    lower_text = text.lower()
    if "earthquake" not in lower_text:
        return None
    rule = parse_rule_from_text(text)
    if rule is None:
        return None
    if rule.min_magnitude != expected_min_magnitude:
        return None
    if rule.end_utc.timestamp() + grace_seconds < now.timestamp():
        return None
    if rule.start_utc.timestamp() > now.timestamp() + lookahead_seconds:
        return None
    markets = markets_from_event_payload(event_payload)
    if len(markets) < 2:
        return None
    return EarthquakeEventCandidate(
        url=url,
        title=event_title_from_payload(event_payload),
        rule=rule,
        markets=markets,
    )


async def discover_earthquake_event(
    client: AsyncPublicClient,
    *,
    expected_min_magnitude: Decimal,
    pages: int,
    page_size: int,
    lookahead_days: float,
    grace_seconds: float,
) -> EarthquakeEventCandidate | None:
    now = datetime.now(timezone.utc)
    lookahead_seconds = lookahead_days * 86400
    candidates: dict[str, EarthquakeEventCandidate] = {}
    for query in EARTHQUAKE_SEARCH_QUERIES:
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
                    candidate = candidate_from_event_payload(
                        event,
                        expected_min_magnitude=expected_min_magnitude,
                        now=now,
                        lookahead_seconds=lookahead_seconds,
                        grace_seconds=grace_seconds,
                    )
                    if candidate is not None:
                        candidates[candidate.url] = candidate
            if page_count >= pages:
                break

    if not candidates:
        return None
    return sorted(candidates.values(), key=lambda item: item.sort_key)[0]


def load_state(path: Path) -> EarthquakeTriggerState:
    if not path.exists():
        return EarthquakeTriggerState(
            fired=set(),
            last_count=None,
            known_event_ids=set(),
            final_yes_bought=False,
            event_url=None,
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return EarthquakeTriggerState(
            fired=set(),
            last_count=None,
            known_event_ids=set(),
            final_yes_bought=False,
            event_url=None,
        )
    if not isinstance(raw, dict):
        return EarthquakeTriggerState(
            fired=set(),
            last_count=None,
            known_event_ids=set(),
            final_yes_bought=False,
            event_url=None,
        )
    fired = raw.get("fired")
    known_event_ids = raw.get("known_event_ids")
    last_count = raw.get("last_count")
    try:
        parsed_last_count = int(last_count) if last_count not in (None, "") else None
    except (TypeError, ValueError):
        parsed_last_count = None
    return EarthquakeTriggerState(
        fired={str(item) for item in fired} if isinstance(fired, list) else set(),
        last_count=parsed_last_count,
        known_event_ids=(
            {str(item) for item in known_event_ids}
            if isinstance(known_event_ids, list)
            else set()
        ),
        final_yes_bought=bool(raw.get("final_yes_bought", False)),
        event_url=str(raw["event_url"]) if raw.get("event_url") else None,
    )


def save_state(path: Path, state: EarthquakeTriggerState) -> None:
    payload = {
        "updated_at": utc_now(),
        "fired": sorted(state.fired),
        "last_count": state.last_count,
        "known_event_ids": sorted(state.known_event_ids),
        "final_yes_bought": state.final_yes_bought,
        "event_url": state.event_url,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def usgs_time(value: object) -> datetime | None:
    if not isinstance(value, int | float):
        return None
    return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)


def iso_for_usgs(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")


async def fetch_usgs_earthquakes(
    *,
    start_utc: datetime,
    end_utc: datetime,
    min_magnitude: Decimal,
    review_status: ReviewStatus,
) -> EarthquakeSnapshot:
    params = {
        "format": "geojson",
        "starttime": iso_for_usgs(start_utc),
        "endtime": iso_for_usgs(end_utc),
        "minmagnitude": str(min_magnitude),
        "eventtype": "earthquake",
        "orderby": "time-asc",
    }
    if review_status == "reviewed":
        params["reviewstatus"] = "reviewed"

    headers = {"User-Agent": "polymarket-earthquake-trigger/1.0"}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        response = await client.get(USGS_EVENT_QUERY_URL, params=params)
        response.raise_for_status()
        payload = response.json()

    events: list[EarthquakeEvent] = []
    for feature in payload.get("features", []) if isinstance(payload, dict) else []:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue
        magnitude = parse_decimal(properties.get("mag"))
        observed_at = usgs_time(properties.get("time"))
        if magnitude is None or observed_at is None:
            continue
        if magnitude < min_magnitude:
            continue
        if observed_at < start_utc or observed_at > end_utc:
            continue
        if str(properties.get("type") or "").lower() != "earthquake":
            continue
        event_id = str(feature.get("id") or properties.get("code") or properties.get("ids") or "")
        if not event_id:
            continue
        events.append(
            EarthquakeEvent(
                event_id=event_id,
                usgs_url=str(properties.get("url") or ""),
                title=str(properties.get("title") or ""),
                place=str(properties.get("place") or ""),
                magnitude=magnitude,
                observed_at=observed_at,
                updated_at=usgs_time(properties.get("updated")),
                status=str(properties.get("status")) if properties.get("status") else None,
            )
        )

    return EarthquakeSnapshot(
        count=len(events),
        events=events,
        fetched_at=datetime.now(timezone.utc),
    )


def snapshot_for_trade(market: EarthquakeMarket) -> MarketSnapshot:
    return market.snapshot


def build_trade_plan(
    market: EarthquakeMarket,
    *,
    outcome: Outcome,
    limit_price: Decimal,
    size: Decimal,
    live: bool,
) -> TradePlan:
    return build_buy_plan(
        market=snapshot_for_trade(market),
        outcome=outcome,
        limit_price=limit_price,
        size=size,
        live=live,
    )


def event_token_ids(markets: list[EarthquakeMarket]) -> set[str]:
    token_ids: set[str] = set()
    for market in markets:
        if market.snapshot.yes_token_id:
            token_ids.add(market.snapshot.yes_token_id)
        if market.snapshot.no_token_id:
            token_ids.add(market.snapshot.no_token_id)
    return token_ids


def latest_event_line(snapshot: EarthquakeSnapshot) -> str:
    if not snapshot.events:
        return "no qualifying earthquakes"
    latest = max(snapshot.events, key=lambda item: item.observed_at)
    return (
        f"latest={latest.magnitude} {latest.place} "
        f"at {latest.observed_at.isoformat(timespec='seconds')}"
    )


class EarthquakeTriggerBot:
    def __init__(
        self,
        *,
        event_url: str,
        live: bool,
        poll_interval: float,
        market_refresh_interval: float,
        state_path: Path,
        min_magnitude: Decimal | None,
        start_utc: datetime | None,
        end_utc: datetime | None,
        review_status: ReviewStatus,
        trade_on_start: bool,
        settlement_delay_seconds: float,
        settlement_update_min_age_seconds: float,
        max_no_entry_price: Decimal,
        no_limit_price: Decimal,
        no_size: Decimal,
        max_yes_entry_price: Decimal,
        yes_limit_price: Decimal,
        yes_size: Decimal,
        price_websocket_max_age: float,
        price_wait_seconds: float,
        auto_discover: bool,
        auto_discover_pages: int,
        auto_discover_page_size: int,
        auto_discover_lookahead_days: float,
        auto_discover_grace_seconds: float,
        once: bool = False,
    ) -> None:
        self.event_url = event_url
        self.live = live
        self.poll_interval = poll_interval
        self.market_refresh_interval = market_refresh_interval
        self.state_path = state_path
        self.min_magnitude = min_magnitude
        self.start_utc = start_utc
        self.end_utc = end_utc
        self.review_status = review_status
        self.trade_on_start = trade_on_start
        self.settlement_delay_seconds = settlement_delay_seconds
        self.settlement_update_min_age_seconds = settlement_update_min_age_seconds
        self.max_no_entry_price = max_no_entry_price
        self.no_limit_price = no_limit_price
        self.no_size = no_size
        self.max_yes_entry_price = max_yes_entry_price
        self.yes_limit_price = yes_limit_price
        self.yes_size = yes_size
        self.price_websocket_max_age = price_websocket_max_age
        self.price_wait_seconds = price_wait_seconds
        self.auto_discover = auto_discover
        self.auto_discover_pages = auto_discover_pages
        self.auto_discover_page_size = auto_discover_page_size
        self.auto_discover_lookahead_days = auto_discover_lookahead_days
        self.auto_discover_grace_seconds = auto_discover_grace_seconds
        self.once = once
        self.state = load_state(state_path)
        self.session_state = EarthquakeTriggerState(
            fired=set(self.state.fired),
            last_count=self.state.last_count,
            known_event_ids=set(self.state.known_event_ids),
            final_yes_bought=self.state.final_yes_bought,
            event_url=self.state.event_url,
        )
        self.price_cache: PriceWebSocketCache | None = None
        self.price_token_ids: set[str] = set()

    @property
    def active_state(self) -> EarthquakeTriggerState:
        return self.state if self.live else self.session_state

    def maybe_save_state(self) -> None:
        if self.live:
            save_state(self.state_path, self.state)

    def activate_event(self, event_url: str) -> None:
        state = self.active_state
        if state.event_url is None:
            state.event_url = event_url
            self.maybe_save_state()
            return
        if state.event_url == event_url:
            return
        print(
            f"[{utc_now()}] switching earthquake event: "
            f"{state.event_url} -> {event_url}"
        )
        state.event_url = event_url
        state.last_count = None
        state.known_event_ids = set()
        state.final_yes_bought = False
        self.maybe_save_state()

    async def close_price_cache(self) -> None:
        if self.price_cache is not None:
            await self.price_cache.close()
            self.price_cache = None
            self.price_token_ids = set()

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
        )
        self.price_token_ids = wanted
        await self.price_cache.start()
        print(f"[{utc_now()}] price websocket subscribed to {len(wanted)} token(s)")

    async def websocket_prices(
        self,
        plans: list[tuple[EarthquakeMarket, Outcome]],
    ) -> dict[tuple[str, Outcome], Decimal | None]:
        token_ids = {
            token_id
            for market, outcome in plans
            if (token_id := market.token_id(outcome)) is not None
        }
        await self.ensure_price_cache(token_ids)
        if self.price_cache is None:
            return {
                (market.snapshot.slug, outcome): None
                for market, outcome in plans
            }

        deadline = time.monotonic() + self.price_wait_seconds
        prices: dict[tuple[str, Outcome], Decimal | None] = {}
        while True:
            prices = {
                (market.snapshot.slug, outcome): self.price_cache.best_ask(market.token_id(outcome))
                for market, outcome in plans
            }
            if all(price is not None for price in prices.values()):
                return prices
            if time.monotonic() >= deadline:
                return prices
            await asyncio.sleep(0.05)

    async def load_event_setup(
        self,
        client: AsyncPublicClient,
        event_url: str,
    ) -> tuple[EarthquakeRule, list[EarthquakeMarket]]:
        self.activate_event(event_url)
        event_payload = await client.get_event(url=event_url)
        text = text_from_event_payload(event_payload)
        parsed_rule = parse_rule_from_text(text)
        min_magnitude = self.min_magnitude
        start_utc = self.start_utc
        end_utc = self.end_utc
        if parsed_rule is not None:
            min_magnitude = min_magnitude or parsed_rule.min_magnitude
            start_utc = start_utc or parsed_rule.start_utc
            end_utc = end_utc or parsed_rule.end_utc

        if min_magnitude is None or start_utc is None or end_utc is None:
            raise RuntimeError(
                "Could not infer earthquake rule from the Polymarket event. "
                "Pass --min-magnitude, --start-utc and --end-utc explicitly."
            )

        markets = markets_from_event_payload(event_payload)
        if not markets:
            raise RuntimeError("No tradeable count-bucket markets found for event")

        rule = EarthquakeRule(
            min_magnitude=min_magnitude,
            start_utc=start_utc,
            end_utc=end_utc,
            source_text=parsed_rule.source_text if parsed_rule else "CLI arguments",
        )
        await self.ensure_price_cache(event_token_ids(markets))
        print(
            f"[{utc_now()}] event setup: M{rule.min_magnitude}+ "
            f"{rule.start_utc.isoformat(timespec='seconds')} -> "
            f"{rule.end_utc.isoformat(timespec='seconds')} UTC "
            f"source={rule.source_text} url={event_url}"
        )
        print(
            f"[{utc_now()}] markets: "
            + ", ".join(f"{market.bucket.label}" for market in markets)
        )
        return rule, markets

    async def discover_event_url(self, client: AsyncPublicClient) -> str | None:
        expected_min_magnitude = self.min_magnitude or Decimal("6.5")
        candidate = await discover_earthquake_event(
            client,
            expected_min_magnitude=expected_min_magnitude,
            pages=self.auto_discover_pages,
            page_size=self.auto_discover_page_size,
            lookahead_days=self.auto_discover_lookahead_days,
            grace_seconds=self.auto_discover_grace_seconds,
        )
        if candidate is None:
            print(
                f"[{utc_now()}] auto-discovery found no eligible M"
                f"{expected_min_magnitude}+ earthquake event"
            )
            return None
        print(
            f"[{utc_now()}] auto-discovery selected {candidate.title}: "
            f"M{candidate.rule.min_magnitude}+ "
            f"{candidate.rule.start_utc.isoformat(timespec='seconds')} -> "
            f"{candidate.rule.end_utc.isoformat(timespec='seconds')} UTC "
            f"url={candidate.url}"
        )
        return candidate.url

    def rule_is_expired_for_auto_discovery(self, rule: EarthquakeRule | None) -> bool:
        if rule is None:
            return True
        now = datetime.now(timezone.utc).timestamp()
        return now > rule.end_utc.timestamp() + self.auto_discover_grace_seconds

    async def execute_candidates(
        self,
        candidates: list[tuple[EarthquakeMarket, Outcome, TradePhase]],
    ) -> int:
        if not candidates:
            return 0

        price_pairs = [(market, outcome) for market, outcome, _phase in candidates]
        prices = await self.websocket_prices(price_pairs)
        plans: list[TradePlan] = []
        state = self.active_state

        for market, outcome, phase in candidates:
            key = market.key(outcome, phase)
            if key in state.fired:
                print(f"  skip fired: BUY {outcome} {market.bucket.label}")
                continue
            price = prices.get((market.snapshot.slug, outcome))
            max_entry = (
                self.max_no_entry_price
                if outcome == "NO"
                else self.max_yes_entry_price
            )
            if price is None:
                print(f"  skip websocket ask unavailable/stale: BUY {outcome} {market.bucket.label}")
                continue
            if price > max_entry:
                print(
                    f"  skip price {price} > {max_entry}: "
                    f"BUY {outcome} {market.bucket.label}"
                )
                continue
            limit_price = self.no_limit_price if outcome == "NO" else self.yes_limit_price
            size = self.no_size if outcome == "NO" else self.yes_size
            plans.append(
                build_trade_plan(
                    market,
                    outcome=outcome,
                    limit_price=limit_price,
                    size=size,
                    live=self.live,
                )
            )
            print(
                f"  plan: BUY {size} {outcome} @ {limit_price} "
                f"(ask={price}) bucket={market.bucket.label}"
            )

        if not plans:
            return 0

        from run_event_bot import execute_plans

        results = await execute_plans(plans, live=self.live)
        accepted = 0
        candidate_by_plan = {
            (market.snapshot.slug, outcome): (market, outcome, phase)
            for market, outcome, phase in candidates
        }
        for result in results:
            plan = result.plan
            print(
                f"  {result.status}: {plan.side} {plan.size} {plan.outcome} "
                f"@ {plan.limit_price} -> {result.detail}"
            )
            if result.ok:
                accepted += 1
                market, outcome, phase = candidate_by_plan[(plan.market.slug, plan.outcome)]
                state.fired.add(market.key(outcome, phase))
                if phase == "final-yes":
                    state.final_yes_bought = True
        self.maybe_save_state()
        return accepted

    async def process_count_update(
        self,
        *,
        snapshot: EarthquakeSnapshot,
        markets: list[EarthquakeMarket],
    ) -> None:
        state = self.active_state
        previous_count = state.last_count
        current_count = snapshot.count
        current_ids = {event.event_id for event in snapshot.events}
        new_ids = current_ids - state.known_event_ids

        if previous_count is None:
            state.last_count = current_count
            state.known_event_ids = current_ids
            self.maybe_save_state()
            print(
                f"[{utc_now()}] earthquake baseline count={current_count}; "
                f"{latest_event_line(snapshot)}"
            )
            if not self.trade_on_start or current_count == 0:
                return
            print(f"[{utc_now()}] trade-on-start enabled for count={current_count}")
        elif current_count > previous_count:
            print(
                f"[{utc_now()}] earthquake count increased "
                f"{previous_count} -> {current_count}; new_ids={sorted(new_ids)}; "
                f"{latest_event_line(snapshot)}"
            )
            state.last_count = current_count
            state.known_event_ids = current_ids
            self.maybe_save_state()
        elif current_count < previous_count:
            print(
                f"[{utc_now()}] earthquake count decreased "
                f"{previous_count} -> {current_count}; likely USGS revision; "
                f"{latest_event_line(snapshot)}"
            )
            state.last_count = current_count
            state.known_event_ids = current_ids
            self.maybe_save_state()
            return
        else:
            if new_ids:
                state.known_event_ids = current_ids
                self.maybe_save_state()
            print(
                f"[{utc_now()}] earthquake count unchanged={current_count}; "
                f"{latest_event_line(snapshot)}"
            )
            return

        candidates = [
            (market, "NO", "count-no")
            for market in markets
            if market.bucket.is_impossible_after_count(current_count)
        ]
        if not candidates:
            print(f"[{utc_now()}] no lower exact-count NO markets for count={current_count}")
            return
        print(f"[{utc_now()}] count update actionable NO candidates={len(candidates)}")
        await self.execute_candidates(candidates)

    async def process_final_yes(
        self,
        *,
        rule: EarthquakeRule,
        snapshot: EarthquakeSnapshot,
        markets: list[EarthquakeMarket],
    ) -> None:
        now = datetime.now(timezone.utc)
        settlement_at = rule.end_utc.timestamp() + self.settlement_delay_seconds
        if now.timestamp() < settlement_at:
            return

        latest_update = snapshot.latest_update
        if (
            latest_update is not None
            and self.settlement_update_min_age_seconds > 0
            and (now - latest_update).total_seconds() < self.settlement_update_min_age_seconds
        ):
            print(
                f"[{utc_now()}] final YES waiting for USGS update age "
                f"{(now - latest_update).total_seconds():.1f}s < "
                f"{self.settlement_update_min_age_seconds:.1f}s"
            )
            return

        matching = [
            market
            for market in markets
            if market.bucket.matches_final_count(snapshot.count)
        ]
        if not matching:
            print(f"[{utc_now()}] no final YES market matches count={snapshot.count}")
            return

        print(
            f"[{utc_now()}] final settlement candidates count={snapshot.count} "
            f"correct_bucket={matching[0].bucket.label}"
        )
        correct_market = matching[0]
        candidates: list[tuple[EarthquakeMarket, Outcome, TradePhase]] = [
            (correct_market, "YES", "final-yes")
        ]
        candidates.extend(
            (market, "NO", "final-no")
            for market in markets
            if market.snapshot.slug != correct_market.snapshot.slug
        )
        await self.execute_candidates(candidates)

    async def run_once(
        self,
        *,
        rule: EarthquakeRule,
        markets: list[EarthquakeMarket],
    ) -> None:
        started_at = time.perf_counter()
        snapshot = await fetch_usgs_earthquakes(
            start_utc=rule.start_utc,
            end_utc=rule.end_utc,
            min_magnitude=rule.min_magnitude,
            review_status=self.review_status,
        )
        fetch_seconds = time.perf_counter() - started_at
        await self.process_count_update(snapshot=snapshot, markets=markets)
        await self.process_final_yes(rule=rule, snapshot=snapshot, markets=markets)
        print(
            f"[{utc_now()}] cycle timing: usgs={fetch_seconds:.3f}s "
            f"total={time.perf_counter() - started_at:.3f}s"
        )

    async def run_forever(self) -> None:
        if self.live and os.getenv("POLYBOT_ENABLE_LIVE") != "1":
            raise RuntimeError("Refusing live trading unless POLYBOT_ENABLE_LIVE=1")

        rule: EarthquakeRule | None = None
        markets: list[EarthquakeMarket] = []
        next_market_refresh = 0.0
        active_event_url: str | None = self.event_url
        try:
            async with AsyncPublicClient() as client:
                while True:
                    now = time.monotonic()
                    if rule is None or now >= next_market_refresh:
                        setup_started_at = time.perf_counter()
                        if self.auto_discover:
                            discovered_url = await self.discover_event_url(client)
                            if discovered_url is not None:
                                active_event_url = discovered_url
                            elif self.rule_is_expired_for_auto_discovery(rule):
                                rule = None
                                markets = []
                                active_event_url = None
                                next_market_refresh = (
                                    time.monotonic() + self.market_refresh_interval
                                )
                                print(
                                    f"[{utc_now()}] no current earthquake event; "
                                    "waiting for next discovery"
                                )
                                if self.once:
                                    return
                                await asyncio.sleep(self.poll_interval)
                                continue
                        if active_event_url is None:
                            raise RuntimeError("No earthquake event URL is configured")
                        rule, markets = await self.load_event_setup(client, active_event_url)
                        next_market_refresh = time.monotonic() + self.market_refresh_interval
                        print(
                            f"[{utc_now()}] market refresh timing: "
                            f"{time.perf_counter() - setup_started_at:.3f}s"
                        )

                    try:
                        await self.run_once(rule=rule, markets=markets)
                    except Exception as error:
                        print(f"[{utc_now()}] ERROR {type(error).__name__}: {error}")
                        if self.once:
                            return
                    if self.once:
                        return
                    await asyncio.sleep(self.poll_interval)
        finally:
            await self.close_price_cache()
