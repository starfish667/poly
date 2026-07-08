from __future__ import annotations

import argparse
import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from polymarket import AsyncPublicClient

from polybot.earnings import earnings_signal
from polybot.trader import BatchTrader, build_buy_plan
from polybot.types import Signal, TradePlan
from polybot.weather import weather_signal


PriceLookup = Callable[[Signal], tuple[Decimal, str] | None]


@dataclass
class CollectionTiming:
    polymarket_seconds: float = 0.0
    aviation_seconds: float = 0.0
    signal_seconds: float = 0.0
    total_seconds: float = 0.0
    weather_observations: list[tuple[Decimal, str]] | None = None


def merge_extra_timing(timing: CollectionTiming, extra_timing: dict[str, Any]) -> None:
    timing.aviation_seconds += float(extra_timing.get("aviation_seconds", 0.0))
    observations = extra_timing.get("weather_observations")
    if isinstance(observations, list):
        if timing.weather_observations is None:
            timing.weather_observations = []
        for observation in observations:
            if (
                isinstance(observation, tuple)
                and len(observation) == 2
                and isinstance(observation[0], Decimal)
                and isinstance(observation[1], str)
            ):
                timing.weather_observations.append(observation)


async def load_markets(client: AsyncPublicClient, url: str) -> list[object]:
    if "/event/" in url:
        event = await client.get_event(url=url)
        return list(event.markets)
    return [await client.get_market(url=url)]


async def signal_for(
    strategy: str,
    market: object,
    *,
    timing: CollectionTiming | None = None,
) -> Signal | None:
    extra_timing: dict[str, Any] = {}
    started_at = time.perf_counter()
    if strategy == "weather":
        signal = await weather_signal(market, timing=extra_timing)
        if timing is not None:
            merge_extra_timing(timing, extra_timing)
            timing.signal_seconds += time.perf_counter() - started_at
        return signal
    if strategy == "earnings":
        signal = await earnings_signal(market)
        if timing is not None:
            timing.signal_seconds += time.perf_counter() - started_at
        return signal
    signal = await weather_signal(market, timing=extra_timing)
    if timing is not None:
        merge_extra_timing(timing, extra_timing)
    if signal is not None:
        if timing is not None:
            timing.signal_seconds += time.perf_counter() - started_at
        return signal
    signal = await earnings_signal(market)
    if timing is not None:
        timing.signal_seconds += time.perf_counter() - started_at
    return signal


async def collect_signals(url: str, strategy: str) -> list[Signal]:
    signals, _timing = await collect_signals_timed(url, strategy)
    return signals


async def collect_signals_timed(url: str, strategy: str) -> tuple[list[Signal], CollectionTiming]:
    timing = CollectionTiming()
    started_at = time.perf_counter()
    async with AsyncPublicClient() as client:
        polymarket_started_at = time.perf_counter()
        markets = await load_markets(client, url)
        timing.polymarket_seconds += time.perf_counter() - polymarket_started_at
        signals: list[Signal] = []
        for market in markets:
            signal = await signal_for(strategy, market, timing=timing)
            if signal is not None:
                signals.append(signal)
        timing.total_seconds = time.perf_counter() - started_at
        return signals, timing


def current_outcome_price(signal: Signal) -> Decimal | None:
    if signal.outcome == "YES":
        return signal.market.yes_price
    return signal.market.no_price


def skip_trade_reason(
    signal: Signal,
    *,
    buy_no: bool,
    max_entry_price: Decimal | None,
    price_lookup: PriceLookup | None = None,
) -> str | None:
    if signal.outcome == "NO" and not buy_no:
        return "NO signals are disabled"

    if max_entry_price is None:
        return None

    price_source = f"{signal.outcome} price"
    current_price = None
    if price_lookup is not None:
        quote = price_lookup(signal)
        if quote is not None:
            current_price, price_source = quote
    if current_price is None:
        current_price = current_outcome_price(signal)
    if current_price is None:
        return f"{signal.outcome} price is unavailable"
    if current_price > max_entry_price:
        return (
            f"{price_source} {current_price} is above "
            f"max_entry_price {max_entry_price}"
        )
    return None


def build_plans(
    signals: list[Signal],
    *,
    limit_price: Decimal,
    size: Decimal,
    live: bool,
    buy_no: bool,
    max_entry_price: Decimal | None = None,
    price_lookup: PriceLookup | None = None,
) -> list[TradePlan]:
    plans: list[TradePlan] = []
    for signal in signals:
        if skip_trade_reason(
            signal,
            buy_no=buy_no,
            max_entry_price=max_entry_price,
            price_lookup=price_lookup,
        ):
            continue
        plans.append(
            build_buy_plan(
                market=signal.market,
                outcome=signal.outcome,
                limit_price=limit_price,
                size=size,
                live=live,
            )
        )
    return plans


async def execute_plans(plans: list[TradePlan], *, live: bool):
    if live:
        async with await BatchTrader.create_from_env(live=True) as trader:
            return await trader.execute(plans)

    async with AsyncPublicClient() as public_client:
        trader = BatchTrader(public_client, live=False)  # type: ignore[arg-type]
        return await trader.execute(plans)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Weather/earnings event-trigger Polymarket bot.")
    parser.add_argument("--url", required=True, help="Polymarket market or event URL")
    parser.add_argument("--strategy", choices=("auto", "weather", "earnings"), default="auto")
    parser.add_argument("--limit-price", default="0.95", help="Limit price for BUY orders")
    parser.add_argument("--size", default="5", help="Share size per order")
    parser.add_argument("--max-entry-price", help="Skip if the target outcome is already this price or higher")
    parser.add_argument("--buy-no", action="store_true", help="Also buy NO when signal resolves NO")
    parser.add_argument("--live", action="store_true", help="Actually submit batch order")
    args = parser.parse_args()

    signals = await collect_signals(args.url, args.strategy)
    print(f"signals: {len(signals)}")
    for signal in signals:
        print(f"- {signal.outcome} {signal.market.question}")
        print(f"  {signal.reason}")
        print(f"  {signal.market.url}")
        skip_reason = skip_trade_reason(
            signal,
            buy_no=args.buy_no,
            max_entry_price=Decimal(args.max_entry_price) if args.max_entry_price else None,
        )
        if skip_reason:
            print(f"  skip: {skip_reason}")

    plans = build_plans(
        signals,
        limit_price=Decimal(args.limit_price),
        size=Decimal(args.size),
        live=args.live,
        buy_no=args.buy_no,
        max_entry_price=Decimal(args.max_entry_price) if args.max_entry_price else None,
    )
    print(f"plans: {len(plans)}")
    results = await execute_plans(plans, live=args.live)
    for result in results:
        plan = result.plan
        print(
            f"{result.status}: {plan.side} {plan.size} {plan.outcome} "
            f"@ {plan.limit_price} -> {result.detail}"
        )
        if result.order_id:
            print(f"order_id: {result.order_id}")


if __name__ == "__main__":
    asyncio.run(main())
