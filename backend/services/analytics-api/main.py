import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId

app = FastAPI(title="News Analytics API Gateway")

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "news_analytics")
MONGO_ENRICHED_COLLECTION = os.getenv("MONGO_ENRICHED_COLLECTION", "articles_enriched")

mongo_client: Optional[AsyncIOMotorClient] = None
enriched_collection = None

# --- CORS ---
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def serialize_article(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert MongoDB document to JSON-safe dict.
    """
    doc = dict(doc)  # shallow copy
    _id = doc.get("_id")
    if isinstance(_id, ObjectId):
        doc["id"] = str(_id)
    doc.pop("_id", None)
    return doc


@app.on_event("startup")
async def startup_event():
    global mongo_client, enriched_collection

    print("[api-gateway] Connecting to MongoDB...")
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    db = mongo_client[MONGO_DB_NAME]
    enriched_collection = db[MONGO_ENRICHED_COLLECTION]
    print(
        f"[api-gateway] Connected to MongoDB at {MONGO_URL}, "
        f"db={MONGO_DB_NAME}, collection={MONGO_ENRICHED_COLLECTION}"
    )


@app.on_event("shutdown")
async def shutdown_event():
    global mongo_client
    if mongo_client is not None:
        mongo_client.close()
        print("[api-gateway] MongoDB connection closed.")


@app.get("/api/health")
async def health_check():
    return {"status": "ok", "service": "api-gateway"}


# ============== ARTICLES LIST ==============

@app.get("/api/articles/enriched")
async def get_enriched_articles(
    limit: int = Query(20, ge=1, le=100),
    sentiment: Optional[str] = Query(None, description="Filter by sentiment label: positive|neutral|negative"),
    source: Optional[str] = Query(None, description="Filter by source (lowercase)"),
):
    """
    Return latest enriched articles from Mongo.
    Supports optional filtering by sentiment and source.
    """
    global enriched_collection
    if enriched_collection is None:
        return {"status": "error", "message": "Enriched collection not initialized"}

    query: Dict[str, Any] = {}

    if sentiment:
        query["sentiment.label"] = sentiment.lower()

    if source:
        query["source"] = source.lower()

    cursor = (
        enriched_collection
        .find(query)
        .sort("enriched_at", -1)
        .limit(limit)
    )

    articles: List[Dict[str, Any]] = []
    async for doc in cursor:
        articles.append(serialize_article(doc))

    return {
        "status": "ok",
        "count": len(articles),
        "items": articles,
    }


# ============== ANALYTICS: SENTIMENT OVER TIME ==============

@app.get("/api/analytics/sentiment-over-time")
async def sentiment_over_time(
    hours: int = Query(24, ge=1, le=168, description="How many hours back to look"),
    bucket: str = Query("hour", pattern="^(hour|day)$", description="Group by hour or day"),
):
    """
    Aggregate sentiment counts over time buckets (hour/day) for the last N hours.
    Returns data in the form:
    [
      { "time": "...", "positive": 3, "neutral": 5, "negative": 1 },
      ...
    ]
    """
    global enriched_collection
    if enriched_collection is None:
        return {"status": "error", "message": "Enriched collection not initialized"}

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)

    unit = "hour" if bucket == "hour" else "day"

    pipeline = [
        {
            "$match": {
                "enriched_at": {
                    "$gte": start.isoformat(),
                    "$lte": now.isoformat(),
                }
            }
        },
        {
            "$project": {
                "sentiment": "$sentiment.label",
                "time_bucket": {
                    "$dateTrunc": {
                        "date": {"$toDate": "$enriched_at"},
                        "unit": unit,
                        "timezone": "UTC",
                    }
                },
            }
        },
        {
            "$group": {
                "_id": {
                    "time_bucket": "$time_bucket",
                    "sentiment": "$sentiment",
                },
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.time_bucket": 1}},
    ]

    cursor = enriched_collection.aggregate(pipeline)

    # Build a dict keyed by time string
    buckets: Dict[str, Dict[str, int]] = {}

    async for row in cursor:
        tb = row["_id"]["time_bucket"]
        sentiment = (row["_id"]["sentiment"] or "neutral").lower()
        count = row["count"]

        time_str = tb.isoformat()
        if time_str not in buckets:
            buckets[time_str] = {"positive": 0, "neutral": 0, "negative": 0}

        if sentiment in buckets[time_str]:
            buckets[time_str][sentiment] += count

    # Convert to sorted list
    items: List[Dict[str, Any]] = []
    for time_str in sorted(buckets.keys()):
        counts = buckets[time_str]
        items.append(
            {
                "time": time_str,
                "positive": counts["positive"],
                "neutral": counts["neutral"],
                "negative": counts["negative"],
            }
        )

    return {"status": "ok", "items": items}


# ============== ANALYTICS: TOP KEYWORDS ==============

@app.get("/api/analytics/top-keywords")
async def top_keywords(
    limit: int = Query(10, ge=1, le=50),
    hours: int = Query(24, ge=1, le=168),
):
    """
    Return top keywords for the last N hours:
    [
      { "keyword": "market", "count": 5 },
      ...
    ]
    """
    global enriched_collection
    if enriched_collection is None:
        return {"status": "error", "message": "Enriched collection not initialized"}

    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)

    pipeline = [
        {
            "$match": {
                "enriched_at": {
                    "$gte": start.isoformat(),
                    "$lte": now.isoformat(),
                }
            }
        },
        {"$unwind": "$keywords"},
        {
            "$group": {
                "_id": "$keywords",
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": limit},
    ]

    cursor = enriched_collection.aggregate(pipeline)

    items: List[Dict[str, Any]] = []
    async for row in cursor:
        items.append(
            {
                "keyword": row["_id"],
                "count": row["count"],
            }
        )

    return {"status": "ok", "items": items}
