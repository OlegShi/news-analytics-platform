import asyncio
import json
import os
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI

app = FastAPI(title="News Ingestion Service")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
NEWS_RAW_TOPIC = os.getenv("NEWS_RAW_TOPIC", "news.raw")

producer: AIOKafkaProducer | None = None


@app.on_event("startup")
async def startup_event():
    """
    Runs when the service starts.
    We create a Kafka producer and try to connect with retries,
    because Kafka may still be starting up when this container starts.
    """
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    max_attempts = 10
    delay_seconds = 3

    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[ingestion-service] Attempt {attempt}/{max_attempts} to connect to Kafka at {KAFKA_BROKER}...")
            await producer.start()
            print("[ingestion-service] Kafka producer started successfully.")
            break
        except KafkaConnectionError as ex:
            print(f"[ingestion-service] Kafka connection failed: {ex}")
            if attempt == max_attempts:
                print("[ingestion-service] Max attempts reached. Giving up.")
                # Let FastAPI crash startup here: better to fail fast than run half-broken
                raise
            print(f"[ingestion-service] Retrying in {delay_seconds} seconds...")
            await asyncio.sleep(delay_seconds)


@app.on_event("shutdown")
async def shutdown_event():
    """
    Runs when the service shuts down.
    We close the Kafka producer cleanly.
    """
    global producer
    if producer is not None:
        await producer.stop()
        print("[ingestion-service] Kafka producer stopped.")


@app.get("/api/ingestion/health")
async def health_check():
    return {"status": "ok", "service": "ingestion-service"}


@app.post("/api/ingestion/produce-test")
async def produce_test_message():
    """
    Manually trigger production of a dummy news item
    to the 'news.raw' topic, to test Kafka connection.
    """
    global producer
    if producer is None:
        return {"status": "error", "message": "Producer not initialized"}

    now = datetime.now(timezone.utc)
    dummy_news = {
        "id": str(uuid.uuid4()),
        "source": "dummy-source",
        "title": "Test News Article",
        "description": "This is a test news item produced by ingestion-service.",
        "url": "https://example.com/test-article",
        "published_at": now.isoformat(),
        "fetched_at": now.isoformat(),
    }

    await producer.send_and_wait(NEWS_RAW_TOPIC, dummy_news)
    return {
        "status": "ok",
        "topic": NEWS_RAW_TOPIC,
        "produced_id": dummy_news["id"],
    }
