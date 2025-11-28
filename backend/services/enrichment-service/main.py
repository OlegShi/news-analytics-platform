import asyncio
import json
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI(title="News Enrichment Service")

# === Config from environment ===
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
NORMALIZED_TOPIC = os.getenv("NEWS_NORMALIZED_TOPIC", "news.normalized")
ENRICHED_TOPIC = os.getenv("NEWS_ENRICHED_TOPIC", "news.enriched")

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "news_analytics")
MONGO_COLLECTION = os.getenv("MONGO_ENRICHED_COLLECTION", "articles_enriched")

# === Globals ===
consumer: Optional[AIOKafkaConsumer] = None
producer: Optional[AIOKafkaProducer] = None
mongo_client: Optional[AsyncIOMotorClient] = None
enriched_collection = None
consumer_task: Optional[asyncio.Task] = None
stop_event = asyncio.Event()

# Naive word lists for “sentiment”
POSITIVE_WORDS = {
    "good", "great", "positive", "up", "gain", "improve", "strong",
    "rise", "success", "excellent", "win", "winning"
}
NEGATIVE_WORDS = {
    "bad", "negative", "down", "loss", "drop", "fall", "weak",
    "decline", "fail", "failure", "crisis", "risk", "danger"
}


def compute_sentiment(text: str) -> Dict[str, Any]:
    """
    Very naive sentiment: count positive/negative words.
    """
    text_lower = text.lower()
    tokens = re.findall(r"[a-z]+", text_lower)

    pos_count = sum(1 for t in tokens if t in POSITIVE_WORDS)
    neg_count = sum(1 for t in tokens if t in NEGATIVE_WORDS)

    if pos_count > neg_count:
        label = "positive"
    elif neg_count > pos_count:
        label = "negative"
    else:
        label = "neutral"

    score = pos_count - neg_count

    return {
        "label": label,
        "score": score,
        "positive_count": pos_count,
        "negative_count": neg_count,
    }


def extract_keywords(text: str, max_keywords: int = 5) -> List[str]:
    """
    Simple keyword extraction:
    - lowercase
    - remove very short words & obvious stopwords
    - take most frequent remaining words
    """
    text_lower = text.lower()
    tokens = re.findall(r"[a-z]{3,}", text_lower)

    stopwords = {
        "the", "and", "for", "with", "from", "this", "that", "have",
        "will", "about", "into", "after", "before", "over", "under",
        "on", "off", "you", "your", "their", "they", "are", "was",
        "were", "has", "had", "been", "more", "less", "very"
    }

    filtered = [t for t in tokens if t not in stopwords]

    freq: Dict[str, int] = {}
    for t in filtered:
        freq[t] = freq.get(t, 0) + 1

    sorted_terms = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)
    keywords = [term for term, _ in sorted_terms[:max_keywords]]
    return keywords


def enrich_article(normalized: Dict[str, Any]) -> Dict[str, Any]:
    """
    Take a normalized article and add enrichment fields.
    """
    text_for_sentiment = f"{normalized.get('title', '')}. {normalized.get('summary', '')}"
    sentiment = compute_sentiment(text_for_sentiment)
    keywords = extract_keywords(text_for_sentiment)

    enriched = dict(normalized)  # shallow copy
    enriched["enriched_at"] = datetime.now(timezone.utc).isoformat()
    enriched["sentiment"] = sentiment
    enriched["keywords"] = keywords

    return enriched


async def consume_loop():
    global consumer, producer, enriched_collection

    assert consumer is not None
    assert producer is not None
    assert enriched_collection is not None

    print("[enrichment-service] Starting consume loop...")

    try:
        async for msg in consumer:
            if stop_event.is_set():
                break

            try:
                normalized_event = msg.value  # JSON dict
                enriched = enrich_article(normalized_event)

                # Insert into Mongo
                result = await enriched_collection.insert_one(enriched)

                # Prepare Kafka-safe event
                event_for_kafka = enriched.copy()
                event_for_kafka.pop("_id", None)
                event_for_kafka["mongo_id"] = str(result.inserted_id)

                await producer.send_and_wait(ENRICHED_TOPIC, event_for_kafka)

                print(f"[enrichment-service] Enriched article_id={enriched.get('article_id')}")
            except Exception as ex:
                print(f"[enrichment-service] Error processing message: {ex}")
    except Exception as ex:
        print(f"[enrichment-service] Fatal consume loop error: {ex}")


@app.on_event("startup")
async def startup_event():
    """
    Initialize Mongo, Kafka producer & consumer, and start consume loop.
    """
    global consumer, producer, mongo_client, enriched_collection, consumer_task

    print("[enrichment-service] Connecting to MongoDB...")
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    db = mongo_client[MONGO_DB_NAME]
    enriched_collection = db[MONGO_COLLECTION]
    print(f"[enrichment-service] Connected to MongoDB at {MONGO_URL}, db={MONGO_DB_NAME}, collection={MONGO_COLLECTION}")

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    consumer = AIOKafkaConsumer(
        NORMALIZED_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="enrichment-service-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )

    max_attempts = 10
    delay_seconds = 3

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[enrichment-service] Kafka connect attempt {attempt}/{max_attempts} to {KAFKA_BROKER}...")
            await producer.start()
            await consumer.start()
            print("[enrichment-service] Kafka producer and consumer started successfully.")
            break
        except KafkaConnectionError as ex:
            print(f"[enrichment-service] Kafka connection failed: {ex}")
            if attempt == max_attempts:
                print("[enrichment-service] Max attempts reached. Giving up.")
                raise
            print(f"[enrichment-service] Retrying in {delay_seconds} seconds...")
            await asyncio.sleep(delay_seconds)

    stop_event.clear()
    consumer_task = asyncio.create_task(consume_loop())


@app.on_event("shutdown")
async def shutdown_event():
    global consumer, producer, mongo_client, consumer_task

    print("[enrichment-service] Shutting down...")
    stop_event.set()

    if consumer_task is not None:
        await asyncio.sleep(0.1)
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    if consumer is not None:
        await consumer.stop()
        print("[enrichment-service] Kafka consumer stopped.")

    if producer is not None:
        await producer.stop()
        print("[enrichment-service] Kafka producer stopped.")

    if mongo_client is not None:
        mongo_client.close()
        print("[enrichment-service] MongoDB connection closed.")


@app.get("/api/enrichment/health")
async def health_check():
    return {"status": "ok", "service": "enrichment-service"}
