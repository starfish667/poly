from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal

from polymarket import AsyncPublicClient

from polybot.earnings import (
    EarningsRule,
    find_eps_candidate,
    parse_earnings_rule,
    resolution_eps_value,
)
from polybot.markets import snapshot_from_market
from polybot.types import Outcome


EARNINGS_QUERIES = (
    "quarterly earnings",
    "earnings EPS",
    "GAAP EPS",
    "non-GAAP EPS",
    "beat earnings estimate",
)


@dataclass(frozen=True)
class EarningsMarket:
    market: object
    rule: EarningsRule
    question: str
    url: str
    yes_price: Decimal | None
    no_price: Decimal | None
    volume: Decimal
    liquidity: Decimal
    spread: Decimal | None
    end_date: datetime | None
    closed_time: datetime | None


def dec(value: Decimal | None) -> Decimal:
    return value if value is not None else Decimal("0")


def try_market(market: object) -> EarningsMarket | None:
    snapshot = snapshot_from_market(market)
    try:
        rule = parse_earnings_rule(snapshot.question, snapshot.description)
    except ValueError:
        return None
    state = getattr(market, "state", None)
    return EarningsMarket(
        market=market,
        rule=rule,
        question=snapshot.question,
        url=snapshot.url,
        yes_price=snapshot.yes_price,
        no_price=snapshot.no_price,
        volume=snapshot.volume,
        liquidity=snapshot.liquidity,
        spread=snapshot.spread,
        end_date=snapshot.end_date,
        closed_time=getattr(state, "closed_time", None) if state else None,
    )


def sort_date(value: date | None) -> date:
    return value or date.max


def sort_dt_desc(value: datetime | None) -> float:
    if value is None:
        return 0
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


async def collect_markets(
    client: AsyncPublicClient,
    *,
    status: str,
    pages: int,
    page_size: int,
) -> list[EarningsMarket]:
    rows: dict[str, EarningsMarket] = {}
    for query in EARNINGS_QUERIES:
        paginator = client.search(
            q=query,
            events_status=status,
            sort="volume",
            page_size=page_size,
        )
        page_count = 0
        async for page in paginator:
            page_count += 1
            for result in page.items:
                for event in result.events:
                    for market in event.markets:
                        row = try_market(market)
                        if row is None:
                            continue
                        slug = getattr(market, "slug", "") or row.question
                        old = rows.get(slug)
                        if old is None or row.volume > old.volume:
                            rows[slug] = row
            if page_count >= pages:
                break
    return list(rows.values())


def settled_outcome(row: EarningsMarket) -> Outcome | None:
    if row.yes_price is not None and row.yes_price >= Decimal("0.99"):
        return "YES"
    if row.no_price is not None and row.no_price >= Decimal("0.99"):
        return "NO"
    return None


def fdec(value: Decimal | None) -> str:
    if value is None:
        return "-"
    return f"{value.normalize():f}"


def fmt_dt(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.date().isoformat()


def print_active(rows: list[EarningsMarket], top: int) -> None:
    rows = sorted(
        rows,
        key=lambda row: (sort_date(row.rule.release_date), -row.volume),
    )
    print()
    print("ACTIVE EARNINGS")
    print("=" * 120)
    for idx, row in enumerate(rows[:top], start=1):
        print(
            f"{idx:02d}. {row.rule.release_date or '-'} {row.rule.ticker:6s} "
            f"{row.rule.metric:12s} > {row.rule.threshold} "
            f"yes/no={fdec(row.yes_price)}/{fdec(row.no_price)} "
            f"vol={fdec(row.volume)} liq={fdec(row.liquidity)} spread={fdec(row.spread)}"
        )
        print(f"    {row.question}")
        print(f"    {row.url}")


async def print_history(rows: list[EarningsMarket], top: int, delay: float) -> None:
    rows = sorted(
        rows,
        key=lambda row: sort_dt_desc(row.closed_time) or sort_dt_desc(row.end_date),
        reverse=True,
    )
    print()
    print("HISTORICAL SEC CHECK")
    print("=" * 120)
    checked = 0
    for row in rows:
        if checked >= top:
            break
        settled = settled_outcome(row)
        if settled is None:
            continue

        try:
            candidate = await find_eps_candidate(row.rule)
        except Exception as exc:  # noqa: BLE001 - this is a diagnostics script.
            print(
                f"{checked + 1:02d}. {row.rule.release_date or '-'} "
                f"{row.rule.ticker:6s} ERROR {type(exc).__name__}: {exc}"
            )
            checked += 1
            if delay:
                await asyncio.sleep(delay)
            continue

        if candidate is None:
            print(
                f"{checked + 1:02d}. {row.rule.release_date or '-'} "
                f"{row.rule.ticker:6s} {row.rule.metric:12s} > {row.rule.threshold} "
                f"SEC=- predicted=- settled={settled} match=-"
            )
            print(f"    {row.question}")
            print(f"    {row.url}")
            checked += 1
            if delay:
                await asyncio.sleep(delay)
            continue

        eps = resolution_eps_value(candidate.value)
        predicted: Outcome = "YES" if eps > row.rule.threshold else "NO"
        match = "OK" if predicted == settled else "MISMATCH"
        print(
            f"{checked + 1:02d}. {row.rule.release_date or '-'} "
            f"{row.rule.ticker:6s} {row.rule.metric:12s} > {row.rule.threshold} "
            f"SEC={eps} raw={candidate.value} predicted={predicted} "
            f"settled={settled} match={match}"
        )
        print(f"    {row.question}")
        print(f"    {row.url}")
        print(f"    source={candidate.source_url}")
        checked += 1
        if delay:
            await asyncio.sleep(delay)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="List active earnings markets and compare closed earnings markets with SEC data."
    )
    parser.add_argument("--pages", type=int, default=3)
    parser.add_argument("--page-size", type=int, default=25)
    parser.add_argument("--active-top", type=int, default=20)
    parser.add_argument("--history-top", type=int, default=10)
    parser.add_argument("--sec-delay", type=float, default=0.25)
    args = parser.parse_args()

    async with AsyncPublicClient() as client:
        active = await collect_markets(
            client,
            status="active",
            pages=args.pages,
            page_size=args.page_size,
        )
        closed = await collect_markets(
            client,
            status="closed",
            pages=args.pages,
            page_size=args.page_size,
        )

    print_active(active, args.active_top)
    await print_history(closed, args.history_top, args.sec_delay)


if __name__ == "__main__":
    asyncio.run(main())
