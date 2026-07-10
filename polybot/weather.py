from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Literal
from zoneinfo import ZoneInfo

import httpx

from polybot.markets import snapshot_from_market
from polybot.types import Outcome, Signal


Unit = Literal["C", "F"]
GUANGZHOU_TZ = ZoneInfo("Asia/Shanghai")
UTC_TZ = ZoneInfo("UTC")
AVIATION_WEATHER_METAR_URL = "https://aviationweather.gov/api/data/metar"
AVIATION_WEATHER_HEADERS = {
    "User-Agent": "polymarket-event-bot aviationweather"
}
AVIATION_WEATHER_HOURS = int(os.getenv("AVIATION_WEATHER_HOURS", "72"))
AVIATION_WEATHER_CACHE_SECONDS = float(os.getenv("AVIATION_WEATHER_CACHE_SECONDS", "2"))
_AVIATION_METAR_CACHE: dict[tuple[str, int], tuple[datetime, list[dict[str, object]]]] = {}
STATION_TIME_ZONES = {
    "CYYZ": "America/Toronto",
    "EHAM": "Europe/Amsterdam",
    "EGLC": "Europe/London",
    "EPWA": "Europe/Warsaw",
    "MPMG": "America/Panama",
    "MMMX": "America/Mexico_City",
    "NZWN": "Pacific/Auckland",
    "OEJN": "Asia/Riyadh",
    "RCSS": "Asia/Taipei",
    "RKSI": "Asia/Seoul",
    "WMKK": "Asia/Kuala_Lumpur",
    "WSSS": "Asia/Singapore",
    "ZBAA": "Asia/Shanghai",
    "ZHHH": "Asia/Shanghai",
    "ZGSZ": "Asia/Shanghai",
    "ZSPD": "Asia/Shanghai",
    "ZUUU": "Asia/Shanghai",
    "ZGGG": "Asia/Shanghai",
    "RJTT": "Asia/Tokyo",
    "SBGR": "America/Sao_Paulo",
    "FACT": "Africa/Johannesburg",
    "EDDM": "Europe/Berlin",
    "LEMD": "Europe/Madrid",
    "LFPB": "Europe/Paris",
    "RPLL": "Asia/Manila",
    "SAEZ": "America/Argentina/Buenos_Aires",
    "ZSQD": "Asia/Shanghai",
    "ZUCK": "Asia/Shanghai",
}
COUNTRY_TIME_ZONES = {
    "br": "America/Sao_Paulo",
    "ar": "America/Argentina/Buenos_Aires",
    "ca": "America/Toronto",
    "cn": "Asia/Shanghai",
    "de": "Europe/Berlin",
    "es": "Europe/Madrid",
    "fr": "Europe/Paris",
    "gb": "Europe/London",
    "hk": "Asia/Hong_Kong",
    "in": "Asia/Kolkata",
    "it": "Europe/Rome",
    "jp": "Asia/Tokyo",
    "kr": "Asia/Seoul",
    "mx": "America/Mexico_City",
    "my": "Asia/Kuala_Lumpur",
    "nl": "Europe/Amsterdam",
    "nz": "Pacific/Auckland",
    "pa": "America/Panama",
    "ph": "Asia/Manila",
    "pl": "Europe/Warsaw",
    "sa": "Asia/Riyadh",
    "sg": "Asia/Singapore",
    "th": "Asia/Bangkok",
    "tr": "Europe/Istanbul",
    "tw": "Asia/Taipei",
    "za": "Africa/Johannesburg",
}
WEATHER_HISTORY_API_KEY = os.getenv(
    "WEATHER_HISTORY_API_KEY",
    "e1f10a1e78da46f5b10a1e78da96f525",
)
WEATHER_HISTORY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/100.0.4896.127 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
}


@dataclass(frozen=True)
class TemperatureRule:
    date: date
    unit: Unit
    op: Literal["eq", "range", "lte", "gte"]
    low: Decimal
    high: Decimal

    def matches(self, value: Decimal) -> bool:
        if self.op == "eq":
            return value == self.low
        if self.op == "range":
            return self.low <= value <= self.high
        if self.op == "lte":
            return value <= self.low
        if self.op == "gte":
            return value >= self.low
        raise AssertionError(self.op)


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


def parse_temperature_rule(question: str, *, fallback_year: int, slug: str = "") -> TemperatureRule:
    lower = question.lower()
    slug_lower = slug.lower()
    date_match = re.search(
        r"\bon\s+([a-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?",
        lower,
        flags=re.IGNORECASE,
    )
    if not date_match:
        date_match = re.search(
            r"\bon-([a-z]+)-(\d{1,2})-(\d{4})",
            slug_lower,
            flags=re.IGNORECASE,
        )
    if not date_match:
        raise ValueError(f"Could not parse weather date from question: {question}")
    month_name, day_text, year_text = date_match.groups()
    month = MONTHS[month_name.lower()]
    year = int(year_text) if year_text else fallback_year
    target_date = date(year, month, int(day_text))

    range_match = re.search(
        r"(?:between\s+)?(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)",
        lower,
    )
    if range_match:
        low, high = Decimal(range_match.group(1)), Decimal(range_match.group(2))
        unit = "F" if "f" in lower[range_match.end() : range_match.end() + 8] else "C"
        return TemperatureRule(target_date, unit, "range", low, high)

    unit_match = re.search(
        r"(\d+(?:\.\d+)?)\s*(?:°|º|｡)?\s*([cf])",
        lower,
        flags=re.IGNORECASE,
    )
    slug_temp_match = re.search(r"-(\d+(?:pt\d+)?)c(?:orbelow|orhigher)?$", slug_lower)
    if unit_match:
        value = Decimal(unit_match.group(1))
        unit = unit_match.group(2).upper()
    elif slug_temp_match:
        value = Decimal(slug_temp_match.group(1).replace("pt", "."))
        unit = "C"
    else:
        raise ValueError(f"Could not parse temperature unit from question: {question}")

    if "or below" in lower or "or lower" in lower or "or less" in lower or "orbelow" in slug_lower:
        return TemperatureRule(target_date, unit, "lte", value, value)
    if "or higher" in lower or "or above" in lower or "or more" in lower or "orhigher" in slug_lower:
        return TemperatureRule(target_date, unit, "gte", value, value)
    return TemperatureRule(target_date, unit, "eq", value, value)


def station_url(source: str, target: date) -> str:
    base = source.split("?")[0].rstrip("/")
    base = re.sub(r"/date/\d{4}-\d{1,2}-\d{1,2}$", "", base)
    return f"{base}/date/{target.year}-{target.month}-{target.day}"


def station_id_from_source(source: str) -> str:
    cleaned = source.split("?")[0].rstrip("/")
    station = cleaned.rsplit("/", 1)[-1]
    if not station:
        raise ValueError(f"Could not parse station from source: {source}")
    return station.split(":", 1)[0].upper()


def country_code_from_source(source: str) -> str | None:
    parts = source.split("?")[0].strip("/").split("/")
    try:
        daily_idx = parts.index("daily")
        return parts[daily_idx + 1].lower()
    except (ValueError, IndexError):
        return None


async def fetch_aviation_metars(station_id: str, *, hours: int = 72) -> list[dict[str, object]]:
    station_id = station_id.upper()
    cache_key = (station_id, hours)
    now = datetime.now(timezone.utc)
    cached = _AVIATION_METAR_CACHE.get(cache_key)
    if cached is not None:
        fetched_at, observations = cached
        if (now - fetched_at).total_seconds() <= AVIATION_WEATHER_CACHE_SECONDS:
            return observations

    params = {
        "ids": station_id,
        "format": "json",
        "hours": str(hours),
    }
    async with httpx.AsyncClient(timeout=30, headers=AVIATION_WEATHER_HEADERS) as client:
        response = await client.get(AVIATION_WEATHER_METAR_URL, params=params)
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, list):
        return []
    observations = [item for item in payload if isinstance(item, dict)]
    _AVIATION_METAR_CACHE[cache_key] = (now, observations)
    return observations


def aviation_observation_time(item: dict[str, object]) -> datetime | None:
    raw_epoch = item.get("obsTime")
    if isinstance(raw_epoch, int | float):
        return datetime.fromtimestamp(raw_epoch, tz=timezone.utc)

    raw_report = item.get("reportTime")
    if isinstance(raw_report, str):
        try:
            return datetime.fromisoformat(raw_report.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def aviation_observation_temp(item: dict[str, object], unit: Unit) -> Decimal | None:
    raw = item.get("temp")
    if raw is None:
        return None
    value = Decimal(str(raw))
    if unit == "F":
        value = (value * Decimal("1.8")) + Decimal("32")
    return value.quantize(Decimal("0.1"))


def rounded_resolution_temperature(value: Decimal) -> Decimal:
    return value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def max_temperature_from_aviation_metars(
    observations: list[dict[str, object]],
    *,
    target_date: date,
    unit: Unit,
    local_tz: ZoneInfo = GUANGZHOU_TZ,
) -> Decimal | None:
    values: list[Decimal] = []
    for item in observations:
        observed_at = aviation_observation_time(item)
        if observed_at is None or observed_at.astimezone(local_tz).date() != target_date:
            continue
        value = aviation_observation_temp(item, unit)
        if value is not None:
            values.append(value)
    return max(values) if values else None


def weather_local_tz(source: str) -> ZoneInfo:
    configured = os.getenv("WEATHER_LOCAL_TZ")
    if configured:
        return ZoneInfo(configured)
    station_id = station_id_from_source(source)
    if station_id in STATION_TIME_ZONES:
        return ZoneInfo(STATION_TIME_ZONES[station_id])
    country_code = country_code_from_source(source)
    if country_code in COUNTRY_TIME_ZONES:
        return ZoneInfo(COUNTRY_TIME_ZONES[country_code])
    raise ValueError(f"Unknown local timezone for weather source: {source}")


def temperature_day_complete(rule: TemperatureRule, *, local_tz: ZoneInfo) -> bool:
    return datetime.now(timezone.utc).astimezone(local_tz).date() > rule.date


def resolve_temperature_outcome(
    rule: TemperatureRule,
    observed: Decimal,
    *,
    day_complete: bool,
) -> tuple[Outcome, str] | None:
    rounded = rounded_resolution_temperature(observed)
    final_or_current = "final" if day_complete else "current"
    if rule.op == "eq":
        if rounded > rule.low:
            return "NO", f"{final_or_current} rounded max {rounded}{rule.unit} is above exact strike {rule.low}{rule.unit}"
        if day_complete:
            if rounded == rule.low:
                return "YES", f"final max equals exact strike {rule.low}{rule.unit}"
            return "NO", f"final rounded max {rounded}{rule.unit} is below exact strike {rule.low}{rule.unit}"
        return None

    if rule.op == "range":
        if rounded > rule.high:
            return "NO", f"{final_or_current} rounded max {rounded}{rule.unit} is above range high {rule.high}{rule.unit}"
        if day_complete:
            if rule.low <= rounded <= rule.high:
                return "YES", f"final rounded max {rounded}{rule.unit} is inside {rule.low}-{rule.high}{rule.unit}"
            return "NO", f"final rounded max {rounded}{rule.unit} is below range low {rule.low}{rule.unit}"
        return None

    if rule.op == "lte":
        if rounded > rule.low:
            return "NO", f"{final_or_current} rounded max {rounded}{rule.unit} is above {rule.low}{rule.unit} or below"
        if day_complete:
            return "YES", f"final rounded max {rounded}{rule.unit} is at or below {rule.low}{rule.unit}"
        return None

    if rule.op == "gte":
        if rounded >= rule.low:
            return "YES", f"{final_or_current} rounded max {rounded}{rule.unit} reached {rule.low}{rule.unit} or higher"
        if day_complete:
            return "NO", f"final rounded max {rounded}{rule.unit} never reached {rule.low}{rule.unit}"
        return None

    raise AssertionError(rule.op)


def weather_com_station_id(source: str) -> str:
    cleaned = source.split("?")[0].strip("/")
    parts = cleaned.split("/")
    if not parts:
        raise ValueError(f"Could not parse station from source: {source}")
    station = parts[-1]
    if ":" in station:
        return station

    country = "US"
    try:
        daily_idx = parts.index("daily")
        country = parts[daily_idx + 1].upper()
    except (ValueError, IndexError):
        pass
    return f"{station}:9:{country}"


def weather_com_history_url(source: str, target: date) -> str:
    station_id = weather_com_station_id(source)
    yyyymmdd = target.strftime("%Y%m%d")
    return (
        "https://api.weather.com/v1/location/"
        f"{station_id}/observations/historical.json"
        f"?apiKey={WEATHER_HISTORY_API_KEY}"
        "&units=e"
        f"&startDate={yyyymmdd}"
        f"&endDate={yyyymmdd}"
    )


async def fetch_weather_com_history(source: str, target: date) -> list[dict[str, object]]:
    url = weather_com_history_url(source, target)
    async with httpx.AsyncClient(timeout=30, headers=WEATHER_HISTORY_HEADERS) as client:
        response = await client.get(url)
        response.raise_for_status()
        payload = response.json()
    observations = payload.get("observations")
    if not isinstance(observations, list):
        return []
    return [item for item in observations if isinstance(item, dict)]


def fahrenheit_to_celsius(value: Decimal) -> Decimal:
    return (value - Decimal("32")) / Decimal("1.8")


def historical_observation_temp(item: dict[str, object], unit: Unit) -> Decimal | None:
    raw = item.get("temp")
    if raw is None:
        return None
    value = Decimal(str(raw))
    if unit == "C":
        value = fahrenheit_to_celsius(value)
    return value.quantize(Decimal("0.1"))


def max_temperature_from_history(observations: list[dict[str, object]], unit: Unit) -> Decimal | None:
    values = [
        value
        for item in observations
        if (value := historical_observation_temp(item, unit)) is not None
    ]
    return max(values) if values else None


def _json_array_after(text: str, start: int) -> object | None:
    left = text.find("[", start)
    if left < 0:
        return None
    depth = 0
    in_string = False
    escape = False
    for idx in range(left, len(text)):
        ch = text[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[left : idx + 1])
                except json.JSONDecodeError:
                    return None
    return None


def embedded_observations(html: str) -> list[dict[str, object]]:
    observations: list[dict[str, object]] = []
    for match in re.finditer(r'"observations"\s*:', html):
        parsed = _json_array_after(html, match.end())
        if not isinstance(parsed, list):
            continue
        for item in parsed:
            if isinstance(item, dict):
                observations.append(item)
    return observations


def observation_temp(item: dict[str, object], unit: Unit) -> Decimal | None:
    key = "metric" if unit == "C" else "imperial"
    bucket = item.get(key)
    if not isinstance(bucket, dict):
        return None
    raw = bucket.get("temp")
    if raw is None:
        return None
    return Decimal(str(raw))


def observation_date(item: dict[str, object]) -> date | None:
    raw = item.get("obsTimeLocal") or item.get("valid_time_gmt")
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.split(" ")[0]).date()
        except ValueError:
            return None
    return None


def max_temperature_from_html(html: str, rule: TemperatureRule) -> Decimal | None:
    values: list[Decimal] = []
    for item in embedded_observations(html):
        if observation_date(item) != rule.date:
            continue
        value = observation_temp(item, rule.unit)
        if value is not None:
            values.append(value)
    return max(values) if values else None


async def fetch_wunderground_html(url: str) -> str:
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.text


async def weather_signal(market: object, *, timing: dict[str, float] | None = None) -> Signal | None:
    snapshot = snapshot_from_market(market)
    if "wunderground.com/history/daily" not in snapshot.source.lower():
        return None
    fallback_year = snapshot.end_date.year if snapshot.end_date else datetime.utcnow().year
    rule = parse_temperature_rule(snapshot.question, fallback_year=fallback_year, slug=snapshot.slug)
    local_tz = weather_local_tz(snapshot.source)

    station_id = station_id_from_source(snapshot.source)
    aviation_started_at = time.perf_counter()
    metars = await fetch_aviation_metars(station_id, hours=AVIATION_WEATHER_HOURS)
    if timing is not None:
        timing["aviation_seconds"] = timing.get("aviation_seconds", 0.0) + (
            time.perf_counter() - aviation_started_at
        )
    observed = max_temperature_from_aviation_metars(
        metars,
        target_date=rule.date,
        unit=rule.unit,
        local_tz=local_tz,
    )
    if observed is None:
        return None
    rounded_observed = rounded_resolution_temperature(observed)
    if timing is not None:
        observations = timing.setdefault("weather_observations", [])
        if isinstance(observations, list):
            observations.append((rounded_observed, rule.unit))
    resolved = resolve_temperature_outcome(
        rule,
        observed,
        day_complete=temperature_day_complete(rule, local_tz=local_tz),
    )
    if resolved is None:
        return None
    outcome, detail = resolved
    return Signal(
        market=snapshot,
        outcome=outcome,
        confidence="high",
        reason=(
            f"AviationWeather.gov METAR raw max {observed}{rule.unit}, "
            f"rounded max {rounded_observed}{rule.unit} for {rule.date} "
            f"({local_tz.key}): {detail}"
        ),
        observed_value=f"{rounded_observed}{rule.unit}",
    )
