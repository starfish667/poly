from __future__ import annotations

import argparse
import asyncio
import calendar
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from polybot.weather import (
    AVIATION_WEATHER_METAR_URL,
    aviation_observation_temp,
    aviation_observation_time,
    fetch_aviation_metars,
)


def month_bounds(month_text: str) -> tuple[date, date]:
    year_text, month_part = month_text.split("-", 1)
    year = int(year_text)
    month = int(month_part)
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def grouped_daily_highs(
    observations: list[dict[str, object]],
    *,
    start_date: date,
    end_date: date,
    unit: str,
    local_tz: ZoneInfo,
) -> dict[date, tuple[Decimal, list[str], int]]:
    values_by_day: dict[date, list[tuple[Decimal, str]]] = defaultdict(list)
    counts_by_day: dict[date, int] = defaultdict(int)

    for item in observations:
        observed_at = aviation_observation_time(item)
        if observed_at is None:
            continue
        local_at = observed_at.astimezone(local_tz)
        local_day = local_at.date()
        if local_day < start_date or local_day > end_date:
            continue
        value = aviation_observation_temp(item, unit)  # type: ignore[arg-type]
        if value is None:
            continue
        counts_by_day[local_day] += 1
        values_by_day[local_day].append((value, local_at.isoformat(timespec="minutes")))

    highs: dict[date, tuple[Decimal, list[str], int]] = {}
    for local_day, rows in values_by_day.items():
        max_temp = max(value for value, _time in rows)
        max_times = sorted(time_text for value, time_text in rows if value == max_temp)
        highs[local_day] = (max_temp, max_times, counts_by_day[local_day])
    return highs


async def main_async(args: argparse.Namespace) -> None:
    start_date, end_date = month_bounds(args.month)
    local_tz = ZoneInfo(args.tz)
    station_id = args.station.upper()
    observations = await fetch_aviation_metars(station_id, hours=args.hours)
    highs = grouped_daily_highs(
        observations,
        start_date=start_date,
        end_date=end_date,
        unit=args.unit,
        local_tz=local_tz,
    )

    print(f"api: {AVIATION_WEATHER_METAR_URL}?ids={station_id}&format=json&hours={args.hours}")
    print(f"station: {station_id}")
    print(f"month: {args.month}")
    print(f"timezone: {args.tz}")
    print(f"unit: {args.unit}")
    print(f"raw_observations: {len(observations)}")
    print("date,max_temp,observations,max_time_local")

    day = start_date
    while day <= end_date:
        row = highs.get(day)
        if row is None:
            print(f"{day},NONE,0,")
        else:
            max_temp, max_times, count = row
            times = " | ".join(max_times if args.all_times else max_times[:5])
            if not args.all_times and len(max_times) > 5:
                times += f" | ...(+{len(max_times) - 5})"
            print(f"{day},{max_temp}{args.unit},{count},{times}")
        day += timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser(description="List daily METAR high temperatures for a month.")
    parser.add_argument("--station", default="ZGGG")
    parser.add_argument("--month", default="2026-07")
    parser.add_argument("--tz", default="Asia/Shanghai")
    parser.add_argument("--unit", choices=("C", "F"), default="C")
    parser.add_argument("--hours", type=int, default=240)
    parser.add_argument("--all-times", action="store_true")
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
