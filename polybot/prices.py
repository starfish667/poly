from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from polymarket import AsyncPublicClient, PriceRequest
from polymarket.streams import MarketSpec


@dataclass(frozen=True)
class PriceQuote:
    token_id: str
    best_bid: Decimal | None
    best_ask: Decimal | None
    updated_at: float

    def age_seconds(self) -> float:
        return time.monotonic() - self.updated_at


def parse_decimal(value: object) -> Decimal | None:
    if value in (None, "", "0", 0):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def best_bid_from_levels(levels: object) -> Decimal | None:
    prices = [
        price
        for level in levels or []
        if (price := parse_decimal(getattr(level, "price", None))) is not None
    ]
    return max(prices) if prices else None


def best_ask_from_levels(levels: object) -> Decimal | None:
    prices = [
        price
        for level in levels or []
        if (price := parse_decimal(getattr(level, "price", None))) is not None
    ]
    return min(prices) if prices else None


class PriceWebSocketCache:
    def __init__(
        self,
        token_ids: set[str],
        *,
        max_age_seconds: float = 10.0,
    ) -> None:
        self.token_ids = {str(token_id) for token_id in token_ids if token_id}
        self.max_age_seconds = max_age_seconds
        self.quotes: dict[str, PriceQuote] = {}
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._handle: Any | None = None

    async def start(self) -> None:
        if self._task is not None or not self.token_ids:
            return
        self._task = asyncio.create_task(self._run(), name="polymarket-price-websocket")

    async def close(self) -> None:
        self._stop.set()
        handle = self._handle
        if handle is not None:
            await handle.close()
            self._handle = None
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass

    def best_ask(self, token_id: str | None) -> Decimal | None:
        if token_id is None:
            return None
        quote = self.quotes.get(str(token_id))
        if quote is None or quote.best_ask is None:
            return None
        if quote.age_seconds() > self.max_age_seconds:
            return None
        return quote.best_ask

    async def _run(self) -> None:
        subscribed = sorted(self.token_ids)
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with AsyncPublicClient() as client:
                    await self._bootstrap_prices(client, subscribed)
                    await self._bootstrap_order_books(client, subscribed)
                    self._handle = await client.subscribe(
                        MarketSpec(
                            token_ids=subscribed,
                            custom_feature_enabled=True,
                        )
                    )
                    backoff = 1.0
                    async for event in self._handle:
                        if self._stop.is_set():
                            return
                        self._handle_event(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            finally:
                self._handle = None

    async def _bootstrap_order_books(
        self,
        client: AsyncPublicClient,
        token_ids: list[str],
    ) -> None:
        for index in range(0, len(token_ids), 50):
            chunk = token_ids[index : index + 50]
            books = await client.get_order_books(token_ids=chunk)
            for book in books:
                self._upsert_quote(
                    token_id=getattr(book, "token_id", None),
                    best_bid=best_bid_from_levels(getattr(book, "bids", None)),
                    best_ask=best_ask_from_levels(getattr(book, "asks", None)),
                )

    async def _bootstrap_prices(
        self,
        client: AsyncPublicClient,
        token_ids: list[str],
    ) -> None:
        for index in range(0, len(token_ids), 50):
            chunk = token_ids[index : index + 50]
            requests = [
                PriceRequest(token_id=token_id, side="BUY")
                for token_id in chunk
            ]
            prices = await client.get_prices(requests=requests)
            for token_id, by_side in prices.items():
                self._upsert_quote(
                    token_id=token_id,
                    best_bid=None,
                    best_ask=parse_decimal(by_side.get("BUY")),
                )

    def _handle_event(self, event: Any) -> None:
        event_type = getattr(event, "type", None)
        payload = getattr(event, "payload", None)
        if payload is None:
            return

        if event_type == "price_change":
            for change in getattr(payload, "price_changes", ()) or ():
                self._upsert_quote(
                    token_id=getattr(change, "token_id", None),
                    best_bid=parse_decimal(getattr(change, "best_bid", None)),
                    best_ask=parse_decimal(getattr(change, "best_ask", None)),
                )
            return

        token_id = getattr(payload, "token_id", None)
        best_bid = parse_decimal(getattr(payload, "best_bid", None))
        best_ask = parse_decimal(getattr(payload, "best_ask", None))
        if event_type == "book":
            best_bid = best_bid_from_levels(getattr(payload, "bids", None))
            best_ask = best_ask_from_levels(getattr(payload, "asks", None))
        self._upsert_quote(token_id=token_id, best_bid=best_bid, best_ask=best_ask)

    def _upsert_quote(
        self,
        *,
        token_id: object,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
    ) -> None:
        if token_id is None:
            return
        token_text = str(token_id)
        previous = self.quotes.get(token_text)
        self.quotes[token_text] = PriceQuote(
            token_id=token_text,
            best_bid=best_bid if best_bid is not None else (previous.best_bid if previous else None),
            best_ask=best_ask if best_ask is not None else (previous.best_ask if previous else None),
            updated_at=time.monotonic(),
        )
