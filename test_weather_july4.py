from __future__ import annotations

import argparse
import asyncio
from datetime import date
from datetime import datetime, timezone
from decimal import Decimal
from itertools import islice

import httpx
from polymarket import AsyncPublicClient

from polybot.markets import snapshot_from_market
from polybot.weather import (
    Unit,
    embedded_observations,
    fetch_weather_com_history,
    fetch_wunderground_html,
    historical_observation_temp,
    max_temperature_from_history,
    observation_date,
    observation_temp,
    station_url,
    weather_com_history_url,
    weather_com_station_id,
)


DEFAULT_MARKET_URL = (
    "https://polymarket.com/market/"
    "highest-temperature-in-guangzhou-on-july-6-2026-29c"
)


def observation_time(item: dict[str, object]) -> str:
    raw = item.get("obsTimeLocal") or item.get("valid_time_gmt")
    return str(raw) if raw is not None else ""


async def source_from_market(url: str) -> str:
    async with AsyncPublicClient() as client:
        market = await client.get_market(url=url)
    snapshot = snapshot_from_market(market)
    if not snapshot.source:
        raise RuntimeError(f"Market has no resolution source: {snapshot.question}")
    print(f"market: {snapshot.question}")
    print(f"source: {snapshot.source}")
    return snapshot.source


def selected_rows(
    observations: list[dict[str, object]],
    *,
    target_date: date,
    unit: Unit,
) -> list[tuple[str, Decimal]]:
    rows: list[tuple[str, Decimal]] = []
    seen: set[tuple[str, Decimal]] = set()
    for item in observations:
        if observation_date(item) != target_date:
            continue
        value = observation_temp(item, unit)
        if value is None:
            continue
        key = (observation_time(item), value)
        if key in seen:
            continue
        seen.add(key)
        rows.append(key)
    return sorted(rows, key=lambda row: row[0])


def print_observation_sample(observations: list[dict[str, object]]) -> None:
    print("sample_observations:")
    for idx, item in enumerate(islice(observations, 5), start=1):
        keys = ", ".join(sorted(item.keys()))
        print(f"  sample #{idx} keys: {keys}")
        print(f"    obsTimeLocal={item.get('obsTimeLocal')!r}")
        print(f"    valid_time_gmt={item.get('valid_time_gmt')!r}")
        print(f"    metric={item.get('metric')!r}")
        print(f"    imperial={item.get('imperial')!r}")


def historical_observation_time(item: dict[str, object]) -> str:
    raw = item.get("valid_time_gmt")
    if isinstance(raw, int | float):
        return datetime.fromtimestamp(raw, tz=timezone.utc).isoformat()
    return str(raw) if raw is not None else ""


def historical_rows(
    observations: list[dict[str, object]],
    *,
    unit: Unit,
) -> list[tuple[str, Decimal]]:
    rows: list[tuple[str, Decimal]] = []
    seen: set[tuple[str, Decimal]] = set()
    for item in observations:
        value = historical_observation_temp(item, unit)
        if value is None:
            continue
        key = (historical_observation_time(item), value)
        if key in seen:
            continue
        seen.add(key)
        rows.append(key)
    return sorted(rows, key=lambda row: row[0])


def print_rows(rows: list[tuple[str, Decimal]], *, unit: Unit, show_all: bool) -> None:
    print("observations:")
    if show_all:
        display_rows = rows
    else:
        display_rows = rows[:5]
        if len(rows) > 10:
            display_rows.append(("...", Decimal("0")))
        display_rows.extend(rows[-5:] if len(rows) > 5 else [])

    for time_text, value in display_rows:
        if time_text == "...":
            print("  ...")
        else:
            print(f"  {time_text}: {value}{unit}")


async def main_async(args: argparse.Namespace) -> None:
    target_date = date.fromisoformat(args.date)
    unit: Unit = args.unit
    source = args.source or await source_from_market(args.market_url)
    page_url = station_url(source, target_date)
    api_url = weather_com_history_url(source, target_date)

    print(f"target_date: {target_date}")
    print(f"unit: {unit}")
    print(f"weather_com_station_id: {weather_com_station_id(source)}")
    print(f"weather_com_api_url: {api_url}")
    print(f"wunderground_url: {page_url}")

    if args.method in {"api", "auto"}:
        try:
            history = await fetch_weather_com_history(source, target_date)
            history_rows = historical_rows(history, unit=unit)
            print("api_status: OK")
            print(f"api_observations: {len(history)}")
            print(f"api_matched_observations: {len(history_rows)}")
            observed = max_temperature_from_history(history, unit)
            print(f"api_max_temperature: {observed}{unit}" if observed is not None else "api_max_temperature: NONE")
            if history_rows:
                print_rows(history_rows, unit=unit, show_all=args.all)
                if args.method == "auto":
                    return
        except httpx.HTTPStatusError as error:
            print(f"api_status: HTTP {error.response.status_code} {error.response.text[:120]}")
            if args.method == "api":
                return
        except httpx.HTTPError as error:
            print(f"api_status: ERROR {type(error).__name__}: {error}")
            if args.method == "api":
                return

    html = await fetch_wunderground_html(page_url)
    observations = embedded_observations(html)
    rows = selected_rows(observations, target_date=target_date, unit=unit)

    print("page_status: OK")
    print(f"page_no_data_recorded: {'Yes' if 'No Data Recorded' in html else 'No'}")
    print(f"raw_observation_blocks: {len(observations)}")
    print(f"matched_observations: {len(rows)}")
    if not rows:
        print("max_temperature: NONE")
        if args.debug:
            print_observation_sample(observations)
        return

    values = [value for _time, value in rows]
    print(f"max_temperature: {max(values)}{unit}")
    print(f"min_temperature: {min(values)}{unit}")
    print_rows(rows, unit=unit, show_all=args.all)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch a Wunderground daily weather page and print parsed temperatures.")
    parser.add_argument("--market-url", default=DEFAULT_MARKET_URL)
    parser.add_argument("--source", help="Use a Wunderground source URL directly instead of loading the market.")
    parser.add_argument("--date", default="2026-07-04")
    parser.add_argument("--unit", choices=("C", "F"), default="C")
    parser.add_argument("--method", choices=("auto", "api", "page"), default="auto")
    parser.add_argument("--all", action="store_true", help="Print all matched observations.")
    parser.add_argument("--debug", action="store_true", help="Print sample raw observation fields.")
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
