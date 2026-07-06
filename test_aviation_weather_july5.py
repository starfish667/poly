from __future__ import annotations

import argparse
import asyncio
from datetime import date
from decimal import Decimal
from zoneinfo import ZoneInfo

from polybot.weather import (
    AVIATION_WEATHER_METAR_URL,
    aviation_observation_temp,
    aviation_observation_time,
    fetch_aviation_metars,
    max_temperature_from_aviation_metars,
)


def rows_for_date(
    observations: list[dict[str, object]],
    *,
    target_date: date,
    unit: str,
    local_tz: ZoneInfo,
) -> list[tuple[str, str, Decimal, str]]:
    rows: list[tuple[str, str, Decimal, str]] = []
    for item in observations:
        observed_at = aviation_observation_time(item)
        if observed_at is None:
            continue
        local_at = observed_at.astimezone(local_tz)
        if local_at.date() != target_date:
            continue
        value = aviation_observation_temp(item, unit)  # type: ignore[arg-type]
        if value is None:
            continue
        raw = str(item.get("rawOb") or "")
        rows.append(
            (
                observed_at.isoformat(),
                local_at.isoformat(),
                value,
                raw,
            )
        )
    return sorted(rows, key=lambda row: row[1])


async def main_async(args: argparse.Namespace) -> None:
    target_date = date.fromisoformat(args.date)
    local_tz = ZoneInfo(args.tz)
    station_id = args.station.upper()
    observations = await fetch_aviation_metars(station_id, hours=args.hours)
    rows = rows_for_date(
        observations,
        target_date=target_date,
        unit=args.unit,
        local_tz=local_tz,
    )
    max_temp = max_temperature_from_aviation_metars(
        observations,
        target_date=target_date,
        unit=args.unit,
        local_tz=local_tz,
    )

    print(f"api: {AVIATION_WEATHER_METAR_URL}?ids={station_id}&format=json&hours={args.hours}")
    print(f"station: {station_id}")
    print(f"target_date: {target_date}")
    print(f"timezone: {args.tz}")
    print(f"unit: {args.unit}")
    print(f"raw_observations: {len(observations)}")
    print(f"matched_observations: {len(rows)}")
    print(f"max_temperature: {max_temp}{args.unit}" if max_temp is not None else "max_temperature: NONE")
    if rows:
        print("observations:")
        if args.all or len(rows) <= 10:
            display_rows = rows
        else:
            display_rows = rows[:5] + [("...", "...", Decimal("0"), "")] + rows[-5:]
        for utc_time, local_time, value, raw in display_rows:
            if utc_time == "...":
                print("  ...")
            else:
                print(f"  local={local_time} utc={utc_time} temp={value}{args.unit} raw={raw}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Check highest METAR temperature from AviationWeather.gov.")
    parser.add_argument("--station", default="ZGGG")
    parser.add_argument("--date", default="2026-07-05")
    parser.add_argument("--tz", default="Asia/Shanghai")
    parser.add_argument("--unit", choices=("C", "F"), default="C")
    parser.add_argument("--hours", type=int, default=72)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
