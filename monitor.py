from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import httpx
from polymarket import AsyncPublicClient
from polymarket.errors import TransportError

from run_event_bot import build_plans, collect_signals_timed, execute_plans, load_markets, skip_trade_reason
from polybot.markets import snapshot_from_market
from polybot.prices import PriceWebSocketCache
from polybot.types import Signal


Strategy = Literal["auto", "weather", "earnings"]


class TeeStream:
    def __init__(self, *streams: object) -> None:
        self.streams = streams

    def write(self, data: str) -> int:
        for stream in self.streams:
            stream.write(data)  # type: ignore[attr-defined]
        return len(data)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()  # type: ignore[attr-defined]

    def isatty(self) -> bool:
        return False


_LOG_FILE_HANDLE: object | None = None


@dataclass(frozen=True)
class WatchItem:
    name: str
    strategy: Strategy
    url: str
    limit_price: Decimal
    size: Decimal
    max_entry_price: Decimal | None
    interval: float
    active_interval: float
    active_from: datetime | None
    active_until: datetime | None

    @property
    def key(self) -> str:
        return f"{self.name}|{self.url}"


@dataclass(frozen=True)
class ProcessOutcome:
    item: WatchItem
    accepted: int
    state_changed: bool
    notes: list[str]


@dataclass
class MonitorState:
    fired: set[str]
    observed_maxima: dict[str, Decimal]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def install_log_file(path: str) -> None:
    global _LOG_FILE_HANDLE
    log_path = Path(path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _LOG_FILE_HANDLE = log_path.open("a", encoding="utf-8", buffering=1)
    sys.stdout = TeeStream(sys.stdout, _LOG_FILE_HANDLE)  # type: ignore[assignment]
    sys.stderr = TeeStream(sys.stderr, _LOG_FILE_HANDLE)  # type: ignore[assignment]
    print(f"[{utc_now()}] logging to {log_path}")


def parse_utc_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def float_setting(entry: dict[str, object], *keys: str, default: float) -> float:
    for key in keys:
        if key in entry:
            return float(entry[key])
    return default


def is_active_window(item: WatchItem, now: datetime) -> bool:
    if item.active_from is None and item.active_until is None:
        return False
    if item.active_from is not None and now < item.active_from:
        return False
    if item.active_until is not None and now > item.active_until:
        return False
    return True


def poll_interval(item: WatchItem, now: datetime) -> float:
    return item.active_interval if is_active_window(item, now) else item.interval


def load_watchlist(
    path: Path,
    *,
    default_interval: float,
    default_active_interval: float,
) -> list[WatchItem]:
    raw = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(raw, list):
        raise ValueError("watchlist must be a JSON array")

    items: list[WatchItem] = []
    for idx, entry in enumerate(raw, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"watchlist item #{idx} must be an object")
        items.append(
            WatchItem(
                name=str(entry.get("name") or f"item-{idx}"),
                strategy=str(entry.get("strategy") or "auto"),  # type: ignore[arg-type]
                url=str(entry["url"]),
                limit_price=Decimal(str(entry.get("limit_price", "0.95"))),
                size=Decimal(str(entry.get("size", "1"))),
                max_entry_price=(
                    Decimal(str(entry["max_entry_price"]))
                    if "max_entry_price" in entry and entry["max_entry_price"] not in (None, "")
                    else None
                ),
                interval=float_setting(
                    entry,
                    "interval",
                    "idle_interval",
                    "interval_seconds",
                    default=default_interval,
                ),
                active_interval=float_setting(
                    entry,
                    "active_interval",
                    "active_interval_seconds",
                    default=default_active_interval,
                ),
                active_from=parse_utc_datetime(entry.get("active_from_utc")),
                active_until=parse_utc_datetime(entry.get("active_until_utc")),
            )
        )
    return items


def load_state(path: Path) -> MonitorState:
    if not path.exists():
        return MonitorState(fired=set(), observed_maxima={})
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError:
        return MonitorState(fired=set(), observed_maxima={})
    if not isinstance(raw, dict):
        return MonitorState(fired=set(), observed_maxima={})
    fired = raw.get("fired")
    observed_maxima = raw.get("observed_maxima")
    return MonitorState(
        fired={str(item) for item in fired} if isinstance(fired, list) else set(),
        observed_maxima=(
            {
                str(key): Decimal(str(value))
                for key, value in observed_maxima.items()
            }
            if isinstance(observed_maxima, dict)
            else {}
        ),
    )


def save_state(path: Path, state: MonitorState) -> None:
    payload: dict[str, Any] = {
        "updated_at": utc_now(),
        "fired": sorted(state.fired),
        "observed_maxima": {
            key: str(value)
            for key, value in sorted(state.observed_maxima.items())
        },
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def signal_key(signal: Signal) -> str:
    return f"{signal.market.slug}:{signal.outcome}"


def signal_token_id(signal: Signal) -> str | None:
    return signal.market.yes_token_id if signal.outcome == "YES" else signal.market.no_token_id


OBSERVED_VALUE_RE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*([CF])\s*$", re.IGNORECASE)


def observed_temperature(signal: Signal, item_url: str) -> tuple[str, Decimal, str] | None:
    if signal.observed_value is None:
        return None
    match = OBSERVED_VALUE_RE.match(signal.observed_value)
    if not match:
        return None
    value = Decimal(match.group(1))
    unit = match.group(2).upper()
    return f"{item_url}|{unit}", value, unit


def filter_temperature_increases(
    signals: list[Signal],
    *,
    item_url: str,
    observed_maxima: dict[str, Decimal],
) -> tuple[list[Signal], list[str], bool]:
    passthrough: list[Signal] = []
    grouped: dict[str, list[Signal]] = {}
    grouped_values: dict[str, tuple[Decimal, str]] = {}

    for signal in signals:
        observed = observed_temperature(signal, item_url)
        if observed is None:
            passthrough.append(signal)
            continue
        key, value, unit = observed
        grouped.setdefault(key, []).append(signal)
        old_value = grouped_values.get(key)
        if old_value is None or value > old_value[0]:
            grouped_values[key] = (value, unit)

    filtered = list(passthrough)
    notes: list[str] = []
    state_changed = False
    for key, grouped_signals in grouped.items():
        value, unit = grouped_values[key]
        previous = observed_maxima.get(key)
        if previous is None:
            observed_maxima[key] = value
            state_changed = True
            notes.append(
                f"  temperature baseline: rounded max {value}{unit}; "
                "waiting for a higher max before triggering"
            )
            continue
        if value <= previous:
            notes.append(
                f"  no temperature increase: rounded max {value}{unit} "
                f"<= previous {previous}{unit}"
            )
            continue
        observed_maxima[key] = value
        state_changed = True
        notes.append(
            f"  temperature increase: rounded max {previous}{unit} -> {value}{unit}"
        )
        filtered.extend(grouped_signals)

    return filtered, notes, state_changed


def websocket_price_lookup(price_cache: PriceWebSocketCache):
    def lookup(signal: Signal) -> tuple[Decimal, str] | None:
        price = price_cache.best_ask(signal_token_id(signal))
        if price is None:
            return None
        return price, f"{signal.outcome} websocket ask"

    return lookup


async def discover_price_token_ids(items: list[WatchItem]) -> set[str]:
    token_ids: set[str] = set()
    async with AsyncPublicClient() as client:
        for item in items:
            try:
                markets = await load_markets(client, item.url)
            except Exception:
                continue
            for market in markets:
                snapshot = snapshot_from_market(market)
                if snapshot.yes_token_id:
                    token_ids.add(snapshot.yes_token_id)
                if snapshot.no_token_id:
                    token_ids.add(snapshot.no_token_id)
    return token_ids


async def process_item(
    item: WatchItem,
    *,
    live: bool,
    include_no: bool,
    fired: set[str],
    observed_maxima: dict[str, Decimal],
    price_cache: PriceWebSocketCache | None = None,
) -> tuple[int, bool, list[str]]:
    started_at = time.perf_counter()
    notes: list[str] = []
    signals, timing = await collect_signals_timed(item.url, item.strategy)
    collected_at = time.perf_counter()
    signals, temperature_notes, state_changed = filter_temperature_increases(
        signals,
        item_url=item.url,
        observed_maxima=observed_maxima,
    )
    fresh = [signal for signal in signals if signal_key(signal) not in fired]
    timing_text = (
        f"polymarket={timing.polymarket_seconds:.3f}s "
        f"aviation={timing.aviation_seconds:.3f}s "
        f"signal={timing.signal_seconds:.3f}s "
        f"collect={collected_at - started_at:.3f}s"
    )
    if not fresh:
        notes.append(
            f"[{utc_now()}] {item.name}: no fresh signal "
            f"({timing_text} total={time.perf_counter() - started_at:.3f}s)"
        )
        notes.extend(temperature_notes)
        return 0, state_changed, notes

    notes.append(
        f"[{utc_now()}] {item.name}: {len(fresh)} fresh signal(s) "
        f"({timing_text})"
    )
    notes.extend(temperature_notes)
    for signal in fresh:
        notes.append(f"  {signal.outcome} {signal.market.question}")
        notes.append(f"  {signal.reason}")
        skip_reason = skip_trade_reason(
            signal,
            buy_no=include_no,
            max_entry_price=item.max_entry_price,
            price_lookup=websocket_price_lookup(price_cache) if price_cache is not None else None,
        )
        if skip_reason:
            notes.append(f"  skip: {skip_reason}")

    plans = build_plans(
        fresh,
        limit_price=item.limit_price,
        size=item.size,
        live=live,
        buy_no=include_no,
        max_entry_price=item.max_entry_price,
        price_lookup=websocket_price_lookup(price_cache) if price_cache is not None else None,
    )
    if not plans:
        notes.append(f"  no trade plans after filters (total={time.perf_counter() - started_at:.3f}s)")
        return 0, state_changed, notes

    execute_started_at = time.perf_counter()
    results = await execute_plans(plans, live=live)
    executed_at = time.perf_counter()
    signal_by_market_outcome = {
        (signal.market.slug, signal.outcome): signal
        for signal in fresh
    }
    accepted = 0
    for result in results:
        plan = result.plan
        notes.append(
            f"  {result.status}: {plan.side} {plan.size} {plan.outcome} "
            f"@ {plan.limit_price} -> {result.detail}"
        )
        if result.ok:
            accepted += 1
            signal = signal_by_market_outcome[(plan.market.slug, plan.outcome)]
            fired.add(signal_key(signal))
    notes.append(
        f"  timing: polymarket={timing.polymarket_seconds:.3f}s "
        f"aviation={timing.aviation_seconds:.3f}s "
        f"signal={timing.signal_seconds:.3f}s "
        f"collect={collected_at - started_at:.3f}s "
        f"execute={executed_at - execute_started_at:.3f}s "
        f"total={executed_at - started_at:.3f}s"
    )
    return accepted, state_changed, notes


async def safe_process_item(
    item: WatchItem,
    *,
    live: bool,
    include_no: bool,
    fired: set[str],
    observed_maxima: dict[str, Decimal],
    semaphore: asyncio.Semaphore,
    price_cache: PriceWebSocketCache | None = None,
) -> ProcessOutcome:
    async with semaphore:
        try:
            accepted, state_changed, notes = await process_item(
                item,
                live=live,
                include_no=include_no,
                fired=fired,
                observed_maxima=observed_maxima,
                price_cache=price_cache,
            )
            return ProcessOutcome(
                item=item,
                accepted=accepted,
                state_changed=state_changed,
                notes=notes,
            )
        except (httpx.HTTPStatusError, httpx.TransportError, TransportError) as error:
            return ProcessOutcome(
                item=item,
                accepted=0,
                state_changed=False,
                notes=[f"[{utc_now()}] {item.name}: transient network issue: {error}"],
            )
        except Exception as error:  # keep the monitor alive
            return ProcessOutcome(
                item=item,
                accepted=0,
                state_changed=False,
                notes=[f"[{utc_now()}] {item.name}: ERROR {type(error).__name__}: {error}"],
            )


async def run_monitor(args: argparse.Namespace) -> None:
    watchlist_path = Path(args.watchlist)
    state_path = Path(args.state)
    state = load_state(state_path)
    session_seen: set[str] = set()
    next_due: dict[str, datetime] = {}
    semaphore = asyncio.Semaphore(args.max_concurrency)
    price_cache: PriceWebSocketCache | None = None

    if args.price_websocket:
        items = load_watchlist(
            watchlist_path,
            default_interval=args.interval,
            default_active_interval=args.active_interval,
        )
        token_ids = await discover_price_token_ids(items)
        if token_ids:
            price_cache = PriceWebSocketCache(
                token_ids,
                max_age_seconds=args.price_websocket_max_age,
            )
            await price_cache.start()
            print(f"[{utc_now()}] price websocket subscribed to {len(token_ids)} token(s)")
        else:
            print(f"[{utc_now()}] price websocket enabled, but no token ids were found")

    try:
        while True:
            items = load_watchlist(
                watchlist_path,
                default_interval=args.interval,
                default_active_interval=args.active_interval,
            )
            now = datetime.now(timezone.utc)
            item_keys = {item.key for item in items}
            for key in list(next_due):
                if key not in item_keys:
                    del next_due[key]
            for item in items:
                next_due.setdefault(item.key, now)

            due_items = items if args.once else [
                item for item in items if next_due.get(item.key, now) <= now
            ]
            if not due_items:
                if not next_due:
                    sleep_for = args.interval
                else:
                    sleep_for = max(
                        0.1,
                        min((due_at - now).total_seconds() for due_at in next_due.values()),
                    )
                await asyncio.sleep(min(sleep_for, 1.0))
                continue

            active_count = sum(1 for item in due_items if is_active_window(item, now))
            cycle_started_at = time.perf_counter()
            print(
                f"[{utc_now()}] cycle start: {len(due_items)}/{len(items)} due, "
                f"active={active_count}, live={args.live}"
            )
            cycle_fired = set(state.fired)
            if not args.live:
                cycle_fired |= session_seen
            cycle_observed_maxima = dict(state.observed_maxima)

            outcomes = await asyncio.gather(
                *(
                    safe_process_item(
                        item,
                        live=args.live,
                        include_no=not args.yes_only,
                        fired=cycle_fired,
                        observed_maxima=cycle_observed_maxima,
                        semaphore=semaphore,
                        price_cache=price_cache,
                    )
                    for item in due_items
                )
            )
            accepted_total = 0
            state_changed = False
            completed_at = datetime.now(timezone.utc)
            for outcome in outcomes:
                item = outcome.item
                for note in outcome.notes:
                    print(note)
                accepted_total += outcome.accepted
                state_changed = state_changed or outcome.state_changed
                interval = poll_interval(item, completed_at)
                next_due[item.key] = completed_at + timedelta(seconds=interval)

            if accepted_total or state_changed:
                if args.live:
                    state.fired = cycle_fired
                    state.observed_maxima = cycle_observed_maxima
                    save_state(state_path, state)
                else:
                    session_seen = cycle_fired
                    state.observed_maxima = cycle_observed_maxima

            if args.once:
                return
            next_interval = min(
                max(0.1, (due_at - datetime.now(timezone.utc)).total_seconds())
                for due_at in next_due.values()
            )
            print(
                f"[{utc_now()}] cycle done in {time.perf_counter() - cycle_started_at:.3f}s; "
                f"next check in {next_interval:.1f}s"
            )
    finally:
        if price_cache is not None:
            await price_cache.close()


def main() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except AttributeError:
        pass

    parser = argparse.ArgumentParser(description="Continuous weather/earnings Polymarket monitor.")
    parser.add_argument("--watchlist", default="watchlist.json")
    parser.add_argument("--state", default="monitor_state.json")
    parser.add_argument("--interval", type=float, default=15, help="Normal polling interval in seconds")
    parser.add_argument("--active-interval", type=float, default=3, help="Polling interval inside active windows")
    parser.add_argument("--max-concurrency", type=int, default=4, help="Maximum watch items checked at once")
    parser.add_argument("--log-file", help="Append console output to this file")
    parser.add_argument("--price-websocket", action="store_true", help="Use Polymarket websocket best ask for price filters")
    parser.add_argument("--price-websocket-max-age", type=float, default=10, help="Maximum websocket quote age in seconds")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--yes-only", action="store_true", help="Do not buy NO signals")
    parser.add_argument("--live", action="store_true", help="Actually batch-submit orders")
    args = parser.parse_args()
    if args.log_file:
        install_log_file(args.log_file)

    asyncio.run(run_monitor(args))


if __name__ == "__main__":
    main()
