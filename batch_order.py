from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal

from polymarket import AsyncPublicClient

from polybot.markets import snapshot_from_market
from polybot.trader import BatchTrader, build_buy_plan
from polybot.types import Outcome, TradePlan


def parse_order(text: str) -> tuple[Outcome, Decimal, Decimal]:
    parts = text.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError("Order must look like YES:0.52:10 or NO:0.18:5")
    raw_outcome, raw_price, raw_size = parts
    outcome = raw_outcome.upper()
    if outcome not in ("YES", "NO"):
        raise argparse.ArgumentTypeError("Outcome must be YES or NO")
    return outcome, Decimal(raw_price), Decimal(raw_size)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Batch-submit Polymarket limit orders.")
    parser.add_argument("--url", required=True, help="Polymarket market or event URL")
    parser.add_argument(
        "--order",
        action="append",
        required=True,
        type=parse_order,
        help="Order as OUTCOME:PRICE:SIZE, e.g. YES:0.52:10. Repeat for a batch.",
    )
    parser.add_argument("--live", action="store_true", help="Actually submit orders")
    args = parser.parse_args()

    async with AsyncPublicClient() as public_client:
        market = await public_client.get_market(url=args.url)
        snapshot = snapshot_from_market(market)

    plans: list[TradePlan] = [
        build_buy_plan(
            market=snapshot,
            outcome=outcome,
            limit_price=price,
            size=size,
            live=args.live,
        )
        for outcome, price, size in args.order
    ]

    if args.live:
        async with await BatchTrader.create_from_env(live=True) as trader:
            results = await trader.execute(plans)
    else:
        # Dry-run does not need wallet credentials.
        async with AsyncPublicClient() as public_client:
            # BatchTrader only uses the client in live mode, so this cast-like reuse is harmless.
            trader = BatchTrader(public_client, live=False)  # type: ignore[arg-type]
            results = await trader.execute(plans)

    print(f"Market: {snapshot.question}")
    print(f"URL: {snapshot.url}")
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
