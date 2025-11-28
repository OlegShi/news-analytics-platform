import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId

app = FastAPI(title="News Analytics API Gateway")

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "news_analytics")
MONGO_ENRICHED_COLLECTION = os.getenv("MONGO_ENRICHED_COLLECTION", "articles_enriched")

mongo_client: Optional[AsyncIOMotorClient] = None
enriched_collection = None


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
    print(f"[api-gateway] Connected to MongoDB at {MONGO_URL}, db={MONGO_DB_NAME}, collection={MONGO_ENRICHED_COLLECTION}")


@app.on_event("shutdown")
async def shutdown_event():
    global mongo_client
    if mongo_client is not None:
        mongo_client.close()
        print("[api-gateway] MongoDB connection closed.")


@app.get("/api/health")
async def health_check():
    return {"status": "ok", "service": "api-gateway"}


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