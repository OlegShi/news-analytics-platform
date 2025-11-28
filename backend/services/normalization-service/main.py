import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI(title="News Normalization Service")

# === Config from environment ===
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC = os.getenv("NEWS_RAW_TOPIC", "news.raw")
NORMALIZED_TOPIC = os.getenv("NEWS_NORMALIZED_TOPIC", "news.normalized")

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "news_analytics")
MONGO_COLLECTION = os.getenv("MONGO_ARTICLES_COLLECTION", "articles_normalized")

# === Globals for connections ===
consumer: Optional[AIOKafkaConsumer] = None
producer: Optional[AIOKafkaProducer] = None
mongo_client: Optional[AsyncIOMotorClient] = None
articles_collection = None
consumer_task: Optional[asyncio.Task] = None
stop_event = asyncio.Event()


def normalize_article(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Take a raw news event (dict) and return a normalized article dict.
    """

    def parse_dt(value: Any) -> Optional[str]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return None

    article_id = raw.get("id") or raw.get("article_id") or str(uuid.uuid4())
    source = (raw.get("source") or "unknown").strip().lower()
    title = (raw.get("title") or "").strip()
    description = (raw.get("description") or raw.get("summary") or "").strip()
    url = (raw.get("url") or "").strip()

    published_at = parse_dt(raw.get("published_at"))
    fetched_at = parse_dt(raw.get("fetched_at"))

    normalized = {
        "article_id": article_id,
        "source": source,
        "title": title,
        "summary": description,
        "url": url,
        "published_at": published_at,
        "fetched_at": fetched_at,
        "normalized_at": datetime.now(timezone.utc).isoformat(),
        "raw_payload": raw,
    }

    return normalized


async def consume_loop():
    """
    Main loop that consumes messages from Kafka, normalizes them,
    saves to MongoDB, and republishes to normalized topic.
    """
    global consumer, producer, articles_collection

    assert consumer is not None
    assert producer is not None
    assert articles_collection is not None

    print("[normalization-service] Starting consume loop...")

    try:
        async for msg in consumer:
            if stop_event.is_set():
                break

            try:
                raw_event = msg.value  # JSON dict
                normalized = normalize_article(raw_event)

                if not normalized["url"] or not normalized["title"]:
                    print("[normalization-service] Skipping article without url/title")
                    continue

                # Store in Mongo
                result = await articles_collection.insert_one(normalized)

                # Build Kafka-safe event (no ObjectId)
                event_for_kafka = normalized.copy()
                event_for_kafka.pop("_id", None)
                event_for_kafka["mongo_id"] = str(result.inserted_id)

                # Publish to normalized topic
                await producer.send_and_wait(NORMALIZED_TOPIC, event_for_kafka)

                print(f"[normalization-service] Processed article_id={normalized['article_id']}")
            except Exception as ex:
                print(f"[normalization-service] Error processing message: {ex}")
    except Exception as ex:
        print(f"[normalization-service] Fatal consume loop error: {ex}")


@app.on_event("startup")
async def startup_event():
    """
    Initialize Kafka connections and MongoDB, with retries for Kafka.
    """
    global consumer, producer, mongo_client, articles_collection, consumer_task

    print("[normalization-service] Connecting to MongoDB...")
    mongo_client = AsyncIOMotorClient(MONGO_URL)
    db = mongo_client[MONGO_DB_NAME]
    articles_collection = db[MONGO_COLLECTION]
    print(f"[normalization-service] Connected to MongoDB at {MONGO_URL}, db={MONGO_DB_NAME}, collection={MONGO_COLLECTION}")

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    consumer = AIOKafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="normalization-service-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )

    max_attempts = 10
    delay_seconds = 3

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[normalization-service] Kafka connect attempt {attempt}/{max_attempts} to {KAFKA_BROKER}...")
            await producer.start()
            await consumer.start()
            print("[normalization-service] Kafka producer and consumer started successfully.")
            break
        except KafkaConnectionError as ex:
            print(f"[normalization-service] Kafka connection failed: {ex}")
            if attempt == max_attempts:
                print("[normalization-service] Max attempts reached. Giving up.")
                raise
            print(f"[normalization-service] Retrying in {delay_seconds} seconds...")
            await asyncio.sleep(delay_seconds)

    stop_event.clear()
    consumer_task = asyncio.create_task(consume_loop())


@app.on_event("shutdown")
async def shutdown_event():
    """
    Cleanly stop consumer, producer, and Mongo connection.
    """
    global consumer, producer, mongo_client, consumer_task

    print("[normalization-service] Shutting down...")
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
        print("[normalization-service] Kafka consumer stopped.")

    if producer is not None:
        await producer.stop()
        print("[normalization-service] Kafka producer stopped.")

    if mongo_client is not None:
        mongo_client.close()
        print("[normalization-service] MongoDB connection closed.")


@app.get("/api/normalization/health")
async def health_check():
    return {"status": "ok", "service": "normalization-service"}
