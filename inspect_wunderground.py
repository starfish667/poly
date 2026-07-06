from __future__ import annotations

import re
import sys
from urllib.parse import urljoin

import httpx


def main() -> None:
    url = sys.argv[1]
    text = httpx.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}).text
    print("len", len(text))
    for needle in ("observations", "historical", "temperature", "api.weather.com"):
        print(needle, text.find(needle))
    urls = sorted(set(re.findall(r"https://api\.weather\.com[^\"'\\<>\s]+", text)))
    print("api urls", len(urls))
    for item in urls[:80]:
        print(item[:500])
    scripts = re.findall(r'<script[^>]+src=["\']([^"\']+)', text)
    print("scripts", len(scripts))
    for item in scripts[-80:]:
        print(item[:500])
    if "--deep" not in sys.argv:
        return

    client = httpx.Client(timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    queue = [urljoin(url, item) for item in scripts]
    seen = set(queue)
    hits: list[tuple[str, str, int]] = []
    while queue:
        script_url = queue.pop(0)
        script_text = client.get(script_url).text
        for needle in ("pws/history", "observations/historical", "historical", "daily-history", "airport-history"):
            idx = script_text.find(needle)
            if idx >= 0:
                hits.append((script_url, needle, idx))
        for chunk in re.findall(r"chunk-[A-Z0-9]+\.js", script_text):
            chunk_url = urljoin(script_url, chunk)
            if chunk_url not in seen:
                seen.add(chunk_url)
                queue.append(chunk_url)
    print("deep_hits", len(hits))
    for script_url, needle, idx in hits:
        print(needle, idx, script_url)


if __name__ == "__main__":
    main()
