from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import httpx
from polymarket.errors import TransportError

from run_event_bot import build_plans, collect_signals_timed, execute_plans, skip_trade_reason
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
    notes: list[str]


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


def load_state(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError:
        return set()
    if not isinstance(raw, dict):
        return set()
    fired = raw.get("fired")
    if not isinstance(fired, list):
        return set()
    return {str(item) for item in fired}


def save_state(path: Path, fired: set[str]) -> None:
    payload: dict[str, Any] = {
        "updated_at": utc_now(),
        "fired": sorted(fired),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def signal_key(signal: Signal) -> str:
    return f"{signal.market.slug}:{signal.outcome}"


async def process_item(
    item: WatchItem,
    *,
    live: bool,
    include_no: bool,
    fired: set[str],
) -> tuple[int, list[str]]:
    started_at = time.perf_counter()
    notes: list[str] = []
    signals, timing = await collect_signals_timed(item.url, item.strategy)
    collected_at = time.perf_counter()
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
        return 0, notes

    notes.append(
        f"[{utc_now()}] {item.name}: {len(fresh)} fresh signal(s) "
        f"({timing_text})"
    )
    for signal in fresh:
        notes.append(f"  {signal.outcome} {signal.market.question}")
        notes.append(f"  {signal.reason}")
        skip_reason = skip_trade_reason(
            signal,
            buy_no=include_no,
            max_entry_price=item.max_entry_price,
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
    )
    if not plans:
        notes.append(f"  no trade plans after filters (total={time.perf_counter() - started_at:.3f}s)")
        return 0, notes

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
    return accepted, notes


async def safe_process_item(
    item: WatchItem,
    *,
    live: bool,
    include_no: bool,
    fired: set[str],
    semaphore: asyncio.Semaphore,
) -> ProcessOutcome:
    async with semaphore:
        try:
            accepted, notes = await process_item(
                item,
                live=live,
                include_no=include_no,
                fired=fired,
            )
            return ProcessOutcome(item=item, accepted=accepted, notes=notes)
        except (httpx.HTTPStatusError, httpx.TransportError, TransportError) as error:
            return ProcessOutcome(
                item=item,
                accepted=0,
                notes=[f"[{utc_now()}] {item.name}: transient network issue: {error}"],
            )
        except Exception as error:  # keep the monitor alive
            return ProcessOutcome(
                item=item,
                accepted=0,
                notes=[f"[{utc_now()}] {item.name}: ERROR {type(error).__name__}: {error}"],
            )


async def run_monitor(args: argparse.Namespace) -> None:
    watchlist_path = Path(args.watchlist)
    state_path = Path(args.state)
    fired = load_state(state_path)
    session_seen: set[str] = set()
    next_due: dict[str, datetime] = {}
    semaphore = asyncio.Semaphore(args.max_concurrency)

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
        cycle_fired = set(fired)
        if not args.live:
            cycle_fired |= session_seen

        outcomes = await asyncio.gather(
            *(
                safe_process_item(
                    item,
                    live=args.live,
                    include_no=not args.yes_only,
                    fired=cycle_fired,
                    semaphore=semaphore,
                )
                for item in due_items
            )
        )
        accepted_total = 0
        completed_at = datetime.now(timezone.utc)
        for outcome in outcomes:
            item = outcome.item
            for note in outcome.notes:
                print(note)
            accepted_total += outcome.accepted
            interval = poll_interval(item, completed_at)
            next_due[item.key] = completed_at + timedelta(seconds=interval)

        if accepted_total:
            if args.live:
                fired = cycle_fired
                save_state(state_path, fired)
            else:
                session_seen = cycle_fired

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
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--yes-only", action="store_true", help="Do not buy NO signals")
    parser.add_argument("--live", action="store_true", help="Actually batch-submit orders")
    args = parser.parse_args()
    if args.log_file:
        install_log_file(args.log_file)

    asyncio.run(run_monitor(args))


if __name__ == "__main__":
    main()
