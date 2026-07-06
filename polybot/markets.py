from __future__ import annotations

from decimal import Decimal

from polybot.types import MarketSnapshot, Outcome


def zero(value: Decimal | None) -> Decimal:
    return value if value is not None else Decimal("0")


def market_url(market: object) -> str:
    events = getattr(market, "events", ()) or ()
    if events and getattr(events[0], "slug", None):
        return f"https://polymarket.com/event/{events[0].slug}"
    return f"https://polymarket.com/market/{getattr(market, 'slug', '')}"


def snapshot_from_market(market: object) -> MarketSnapshot:
    outcomes = getattr(market, "outcomes")
    metrics = getattr(market, "metrics")
    prices = getattr(market, "prices")
    state = getattr(market, "state")
    resolution = getattr(market, "resolution")

    return MarketSnapshot(
        question=getattr(market, "question", "") or "",
        url=market_url(market),
        slug=getattr(market, "slug", "") or "",
        yes_token_id=getattr(outcomes.yes, "token_id", None),
        no_token_id=getattr(outcomes.no, "token_id", None),
        yes_price=getattr(outcomes.yes, "price", None),
        no_price=getattr(outcomes.no, "price", None),
        volume=zero(getattr(metrics, "volume_num", None) or getattr(metrics, "volume", None)),
        volume_24hr=zero(getattr(metrics, "volume_24hr", None)),
        liquidity=zero(getattr(metrics, "liquidity_num", None) or getattr(metrics, "liquidity", None)),
        spread=getattr(prices, "spread", None),
        end_date=getattr(state, "end_date", None),
        source=getattr(resolution, "source", None) or "",
        description=getattr(market, "description", "") or "",
    )


def token_id_for(snapshot: MarketSnapshot, outcome: Outcome) -> str:
    token_id = snapshot.yes_token_id if outcome == "YES" else snapshot.no_token_id
    if token_id is None:
        raise RuntimeError(f"Market does not have a {outcome} token id: {snapshot.question}")
    return token_id
