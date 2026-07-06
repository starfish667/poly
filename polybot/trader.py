from __future__ import annotations

import asyncio
import os
from decimal import Decimal
from typing import Iterable

from polymarket import AsyncSecureClient

from polybot.markets import token_id_for
from polybot.types import MarketSnapshot, Outcome, TradePlan, TradeResult


def build_buy_plan(
    *,
    market: MarketSnapshot,
    outcome: Outcome,
    limit_price: Decimal | str,
    size: Decimal | str,
    live: bool,
) -> TradePlan:
    return TradePlan(
        market=market,
        outcome=outcome,
        token_id=token_id_for(market, outcome),
        side="BUY",
        limit_price=Decimal(str(limit_price)),
        size=Decimal(str(size)),
        live=live,
    )


class BatchTrader:
    def __init__(self, client: AsyncSecureClient, *, live: bool) -> None:
        self.client = client
        self.live = live

    @classmethod
    async def create_from_env(cls, *, live: bool) -> "BatchTrader":
        private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
        wallet = os.getenv("POLYMARKET_WALLET_ADDRESS")
        if not private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY is required for trading client creation")
        client = await AsyncSecureClient.create(private_key=private_key, wallet=wallet)
        return cls(client, live=live)

    async def close(self) -> None:
        await self.client.close()

    async def __aenter__(self) -> "BatchTrader":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def execute(self, plans: Iterable[TradePlan]) -> list[TradeResult]:
        items = list(plans)
        if not items:
            return []

        for plan in items:
            if plan.live != self.live:
                raise RuntimeError("TradePlan live flag does not match BatchTrader live mode")

        if not self.live:
            return [
                TradeResult(
                    plan=plan,
                    ok=True,
                    status="dry-run",
                    detail=(
                        f"Would {plan.side} {plan.size} {plan.outcome} "
                        f"at {plan.limit_price} on {plan.market.question}"
                    ),
                )
                for plan in items
            ]

        if os.getenv("POLYBOT_ENABLE_LIVE") != "1":
            raise RuntimeError("Refusing live trading unless POLYBOT_ENABLE_LIVE=1")

        signed_orders = await asyncio.gather(
            *(
                self.client.create_limit_order(
                    token_id=plan.token_id,
                    side=plan.side,
                    price=str(plan.limit_price),
                    size=str(plan.size),
                )
                for plan in items
            )
        )
        responses = await self.client.post_orders(signed_orders)

        results: list[TradeResult] = []
        for plan, response in zip(items, responses, strict=True):
            if response.ok:
                results.append(
                    TradeResult(
                        plan=plan,
                        ok=True,
                        status=response.status,
                        detail=(
                            f"accepted making={response.making_amount} "
                            f"taking={response.taking_amount}"
                        ),
                        order_id=response.order_id,
                    )
                )
            else:
                results.append(
                    TradeResult(
                        plan=plan,
                        ok=False,
                        status=response.code,
                        detail=response.message,
                    )
                )
        return results
