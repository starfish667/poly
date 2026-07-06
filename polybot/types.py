from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal


Outcome = Literal["YES", "NO"]


@dataclass(frozen=True)
class MarketSnapshot:
    question: str
    url: str
    slug: str
    yes_token_id: str | None
    no_token_id: str | None
    yes_price: Decimal | None
    no_price: Decimal | None
    volume: Decimal
    volume_24hr: Decimal
    liquidity: Decimal
    spread: Decimal | None
    end_date: datetime | None
    source: str
    description: str


@dataclass(frozen=True)
class Signal:
    market: MarketSnapshot
    outcome: Outcome
    confidence: Literal["high", "medium", "low"]
    reason: str
    observed_value: str | None = None


@dataclass(frozen=True)
class TradePlan:
    market: MarketSnapshot
    outcome: Outcome
    token_id: str
    side: Literal["BUY", "SELL"]
    limit_price: Decimal
    size: Decimal
    live: bool


@dataclass(frozen=True)
class TradeResult:
    plan: TradePlan
    ok: bool
    status: str
    detail: str
    order_id: str | None = None
