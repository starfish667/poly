from __future__ import annotations

import html
import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import httpx

from polybot.markets import snapshot_from_market
from polybot.types import Signal


SEC_HEADERS = {
    "User-Agent": os.getenv("SEC_USER_AGENT", "polymarket-event-bot research"),
    "Accept-Encoding": "gzip, deflate",
}
SEC_LOOKAHEAD_DAYS = int(os.getenv("EARNINGS_SEC_LOOKAHEAD_DAYS", "2"))
SEC_CACHE_DIR = Path(os.getenv("POLYBOT_CACHE_DIR", ".polybot_cache"))
COMPANY_TICKERS_CACHE = SEC_CACHE_DIR / "company_tickers.json"
COMPANY_TICKERS_MAX_AGE = timedelta(days=7)
EPS_RESOLUTION_QUANTUM = Decimal("0.01")


@dataclass(frozen=True)
class EarningsRule:
    ticker: str
    metric: str
    threshold: Decimal
    release_date: date | None


@dataclass(frozen=True)
class EpsCandidate:
    value: Decimal
    source_url: str
    context: str
    score: int


def parse_earnings_rule(question: str, description: str) -> EarningsRule:
    ticker_match = re.search(r"\(([A-Z][A-Z0-9.\-]{0,8})\)", question)
    if not ticker_match:
        raise ValueError(f"Could not parse ticker from question: {question}")
    ticker = ticker_match.group(1)

    text = " ".join(description.split())
    metric_match = re.search(
        r"reports\s+(GAAP EPS|non-GAAP EPS)\s+greater than\s+\$?(-?\d+(?:\.\d+)?)",
        text,
        flags=re.IGNORECASE,
    )
    if not metric_match:
        metric_match = re.search(
            r"(GAAP EPS|non-GAAP EPS).*?\bis\s+\$?(-?\d+(?:\.\d+)?)",
            text,
            flags=re.IGNORECASE,
        )
    if not metric_match:
        raise ValueError(f"Could not parse EPS threshold from market description: {question}")

    release_date = None
    for pattern, fmt in (
        (
            r"(?:estimated|scheduled) to release earnings on ([A-Za-z]+ \d{1,2}, \d{4})",
            "%B %d, %Y",
        ),
        (
            r"(?:estimated|scheduled) to release earnings on (\d{4}-\d{2}-\d{2})",
            "%Y-%m-%d",
        ),
    ):
        date_match = re.search(pattern, text, flags=re.IGNORECASE)
        if date_match:
            release_date = datetime.strptime(date_match.group(1), fmt).date()
            break

    return EarningsRule(
        ticker=ticker,
        metric=metric_match.group(1).lower(),
        threshold=Decimal(metric_match.group(2)),
        release_date=release_date,
    )


def strip_html(raw: str) -> str:
    raw = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", raw)
    raw = re.sub(r"(?s)<[^>]+>", " ", raw)
    raw = html.unescape(raw)
    return re.sub(r"\s+", " ", raw)


def resolution_eps_value(value: Decimal) -> Decimal:
    return value.quantize(EPS_RESOLUTION_QUANTUM, rounding=ROUND_HALF_UP)


def within_sec_lookup_window(rule: EarningsRule) -> bool:
    if rule.release_date is None:
        return True
    today = datetime.now(timezone.utc).date()
    return today >= rule.release_date - timedelta(days=SEC_LOOKAHEAD_DAYS)


def read_cached_company_tickers(*, allow_stale: bool = False) -> dict[str, object] | None:
    if not COMPANY_TICKERS_CACHE.exists():
        return None
    try:
        payload = json.loads(COMPANY_TICKERS_CACHE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    fetched_at_raw = payload.get("fetched_at") if isinstance(payload, dict) else None
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(fetched_at_raw, str) or not isinstance(data, dict):
        return None
    if allow_stale:
        return data
    try:
        fetched_at = datetime.fromisoformat(fetched_at_raw)
    except ValueError:
        return None
    if datetime.now(timezone.utc) - fetched_at > COMPANY_TICKERS_MAX_AGE:
        return None
    return data


def write_cached_company_tickers(data: dict[str, object]) -> None:
    SEC_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
    COMPANY_TICKERS_CACHE.write_text(json.dumps(payload), encoding="utf-8")


async def sec_get(client: httpx.AsyncClient, url: str, *, retries: int = 3) -> httpx.Response:
    for attempt in range(retries):
        response = await client.get(url, headers=SEC_HEADERS)
        if response.status_code != 429 and response.status_code < 500:
            response.raise_for_status()
            return response
        retry_after = response.headers.get("retry-after")
        if retry_after and retry_after.isdigit():
            delay = min(float(retry_after), 30.0)
        else:
            delay = min(2.0 ** attempt, 15.0)
        await asyncio.sleep(delay)
    response.raise_for_status()
    return response


async def ticker_cik(client: httpx.AsyncClient, ticker: str) -> str:
    payload = read_cached_company_tickers()
    if payload is None:
        try:
            response = await sec_get(client, "https://www.sec.gov/files/company_tickers.json")
            payload = response.json()
            write_cached_company_tickers(payload)
        except httpx.HTTPError:
            stale = read_cached_company_tickers(allow_stale=True)
            if stale is None:
                raise
            payload = stale
    wanted = ticker.upper().replace(".", "-")
    for item in payload.values():
        if isinstance(item, dict) and item.get("ticker", "").upper() == wanted:
            return str(item["cik_str"]).zfill(10)
    raise ValueError(f"Could not find CIK for ticker {ticker}")


async def recent_filing_docs(
    client: httpx.AsyncClient,
    cik: str,
    *,
    earliest: date | None,
    latest: date | None,
) -> list[tuple[str, str, str]]:
    response = await sec_get(client, f"https://data.sec.gov/submissions/CIK{cik}.json")
    recent = response.json()["filings"]["recent"]
    docs: list[tuple[str, str, str]] = []
    for form, accession, filing_date, primary in zip(
        recent["form"],
        recent["accessionNumber"],
        recent["filingDate"],
        recent["primaryDocument"],
        strict=False,
    ):
        if form not in {"8-K", "10-Q", "10-K"}:
            continue
        filing_day = date.fromisoformat(filing_date)
        if earliest is not None and filing_day < earliest:
            continue
        if latest is not None and filing_day > latest:
            continue
        accession_dir = accession.replace("-", "")
        base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_dir}"
        docs.append((filing_date, form, f"{base}/{primary}"))
        try:
            index = await sec_get(client, f"{base}/index.json", retries=2)
        except httpx.HTTPError:
            continue
        try:
            files = index.json()["directory"]["item"]
        except (KeyError, TypeError, json.JSONDecodeError):
            continue
        for file_info in files:
            name = file_info.get("name", "")
            lower = name.lower()
            if lower.endswith((".htm", ".html")) and ("ex" in lower or "earn" in lower):
                docs.append((filing_date, form, f"{base}/{name}"))
    return docs


def metric_keywords(metric: str) -> tuple[str, ...]:
    if metric == "non-gaap eps":
        return (
            "non-gaap diluted earnings per share",
            "non-gaap eps",
            "adjusted diluted earnings per share",
            "adjusted eps",
            "adjusted earnings per share",
        )
    return (
        "gaap diluted earnings per share",
        "gaap eps",
        "diluted earnings per share",
        "earnings per share",
    )


def eps_candidates(text: str, metric: str, source_url: str) -> list[EpsCandidate]:
    lower = text.lower()
    candidates: list[EpsCandidate] = []
    for keyword in metric_keywords(metric):
        start = 0
        while True:
            idx = lower.find(keyword, start)
            if idx < 0:
                break
            start = idx + len(keyword)
            window = text[max(0, idx - 350) : idx + 700]
            for number_match in re.finditer(r"\$?\(?(-?\d+\.\d{1,3})\)?", window):
                value = Decimal(number_match.group(1))
                score = 20
                context_low = window.lower()
                if "diluted" in context_low:
                    score += 5
                if "non-gaap" in context_low and metric == "non-gaap eps":
                    score += 8
                if "gaap" in context_low and metric == "gaap eps":
                    score += 4
                candidates.append(
                    EpsCandidate(
                        value=value,
                        source_url=source_url,
                        context=window[:500],
                        score=score,
                    )
                )
    return candidates


async def find_eps_candidate(rule: EarningsRule) -> EpsCandidate | None:
    if not within_sec_lookup_window(rule):
        return None

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        cik = await ticker_cik(client, rule.ticker)
        earliest = rule.release_date - timedelta(days=7) if rule.release_date else None
        latest = rule.release_date + timedelta(days=45) if rule.release_date else None
        docs = await recent_filing_docs(client, cik, earliest=earliest, latest=latest)
        all_candidates: list[EpsCandidate] = []
        for _filing_date, _form, url in docs[:20]:
            try:
                response = await sec_get(client, url, retries=2)
            except httpx.HTTPError:
                continue
            text = strip_html(response.text)
            all_candidates.extend(eps_candidates(text, rule.metric, url))
        if not all_candidates:
            return None
        return sorted(all_candidates, key=lambda item: item.score, reverse=True)[0]


async def earnings_signal(market: object) -> Signal | None:
    snapshot = snapshot_from_market(market)
    try:
        rule = parse_earnings_rule(snapshot.question, snapshot.description)
    except ValueError:
        return None

    candidate = await find_eps_candidate(rule)
    if candidate is None:
        return None

    resolved_value = resolution_eps_value(candidate.value)
    outcome = "YES" if resolved_value > rule.threshold else "NO"
    return Signal(
        market=snapshot,
        outcome=outcome,
        confidence="medium",
        reason=(
            f"{rule.ticker} {rule.metric} candidate {candidate.value} "
            f"(rounded {resolved_value}) "
            f"vs strike {rule.threshold} from {candidate.source_url}"
        ),
        observed_value=str(resolved_value),
    )
