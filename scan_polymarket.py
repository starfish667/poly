from __future__ import annotations

from polymarket import PublicClient


def show_object(name: str, value: object) -> None:
    print(name, type(value), getattr(value, "__dict__", value))


def main() -> None:
    client = PublicClient()
    paginator = client.list_events(
        closed=False,
        order="volume24hr",
        ascending=True,
        page_size=5,
    )

    print("paginator:", type(paginator))
    print("paginator methods:", [name for name in dir(paginator) if not name.startswith("_")])

    page = paginator.first_page()
    events = page.items
    print("events:", len(events))
    print("has_more:", page.has_more)
    print("next_cursor:", page.next_cursor)

    for event in events[:5]:
        markets = getattr(event, "markets", None) or []
        print()
        print("EVENT", getattr(event, "id", None), getattr(event, "title", None))
        print("slug:", getattr(event, "slug", None))
        print("volume24hr:", getattr(event, "volume24hr", None) or getattr(event, "volume_24hr", None))
        print("volume:", getattr(event, "volume", None))
        print("liquidity:", getattr(event, "liquidity", None))
        print("end_date:", getattr(event, "end_date", None) or getattr(event, "endDate", None))
        print("markets:", len(markets))
        print("event fields:", sorted(event.__dict__.keys()))
        show_object("event.metrics", getattr(event, "metrics", None))
        show_object("event.trading", getattr(event, "trading", None))
        show_object("event.resolution", getattr(event, "resolution", None))
        show_object("event.schedule", getattr(event, "schedule", None))
        show_object("event.category", getattr(event, "category", None))
        show_object("event.tags", getattr(event, "tags", None))
        if markets:
            market = markets[0]
            print("first market:", getattr(market, "question", None))
            print("market fields:", sorted(market.__dict__.keys()))
            show_object("market.metrics", getattr(market, "metrics", None))
            show_object("market.trading", getattr(market, "trading", None))
            show_object("market.resolution", getattr(market, "resolution", None))
            show_object("market.outcomes", getattr(market, "outcomes", None))


if __name__ == "__main__":
    main()
