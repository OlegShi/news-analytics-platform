# news_client.py
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")
NEWS_SOURCES = os.getenv(
    "NEWS_SOURCES",
    "bbc-news,cnn,reuters,the-verge"  # you can tweak this list
)
NEWS_API_URL = "https://newsapi.org/v2/top-headlines"


async def fetch_top_headlines() -> List[Dict[str, Any]]:
    """Fetch latest top headlines from NewsAPI."""
    if not NEWS_API_KEY:
        return []

    params = {
        "language": "en",
        "pageSize": 20,
        "sources": NEWS_SOURCES,
    }
    headers = {"X-Api-Key": NEWS_API_KEY}

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(NEWS_API_URL, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        return data.get("articles", [])


def map_newsapi_article(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map NewsAPI article to our 'raw event' schema for Kafka."""
    source = (raw.get("source") or {}).get("name", "") or "unknown"
    url = raw.get("url") or ""
    published_at = raw.get("publishedAt")

    return {
        "id": url,  # we treat URL as unique id
        "source": source,
        "title": raw.get("title") or "",
        "description": raw.get("description") or "",
        "url": url,
        "published_at": published_at,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        # optional extra fields the pipeline can use later
        "author": raw.get("author"),
        "content": raw.get("content"),
    }
