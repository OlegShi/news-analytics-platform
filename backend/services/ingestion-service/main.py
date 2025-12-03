import asyncio
import json
import os
import uuid
import random
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI, Query

from news_client import fetch_top_headlines, map_newsapi_article

app = FastAPI(title="News Ingestion Service")

# === Config from environment ===
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC = os.getenv("NEWS_RAW_TOPIC", "news.raw")

NEWS_API_ENABLED = os.getenv("NEWS_API_ENABLED", "false").lower() == "true"
NEWS_POLL_INTERVAL = int(os.getenv("NEWS_POLL_INTERVAL", "300"))

# === Globals ===
producer: Optional[AIOKafkaProducer] = None
realtime_task: Optional[asyncio.Task] = None
stop_event = asyncio.Event()
seen_ids: set[str] = set()


# ---------------------------------------------------------------------------
# Realtime News Loop
# ---------------------------------------------------------------------------

async def realtime_news_loop() -> None:
    """
    Background task: periodically pull news from NewsAPI and push to Kafka.
    """
    global seen_ids
    print(
        f"[ingestion-service] realtime_news_loop started. "
        f"Poll interval = {NEWS_POLL_INTERVAL}s"
    )

    while not stop_event.is_set():
        try:
            print("[ingestion-service] Fetching top headlines from NewsAPI ...")
            articles = await fetch_top_headlines()

            if not articles:
                print("[ingestion-service] No articles received from NewsAPI.")
                await asyncio.sleep(NEWS_POLL_INTERVAL)
                continue

            new_count = 0

            for raw_article in articles:
                mapped = map_newsapi_article(raw_article)
                article_id = mapped.get("id")

                if not article_id:
                    # ensure every article has an id
                    article_id = str(uuid.uuid4())
                    mapped["id"] = article_id

                # simple de-dup: skip if we've already sent this id
                if article_id in seen_ids:
                    continue

                seen_ids.add(article_id)

                # Send JSON bytes to Kafka
                await producer.send_and_wait(
                    RAW_TOPIC,
                    json.dumps(mapped).encode("utf-8"),
                )
                new_count += 1

            # Optional: cap the size of the seen_ids set
            if len(seen_ids) > 10_000:
                # crude trimming – good enough for demo
                seen_ids = set(list(seen_ids)[-5_000:])

            print(
                f"[ingestion-service] Sent {new_count} new articles "
                f"to Kafka topic '{RAW_TOPIC}'."
            )

        except Exception as exc:  # noqa: BLE001
            print(f"[ingestion-service] Error in realtime loop: {exc!r}")

        # Wait until the next poll cycle (unless shutdown is signaled)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=NEWS_POLL_INTERVAL)
        except asyncio.TimeoutError:
            # normal: timeout expired, loop continues
            pass


# ---------------------------------------------------------------------------
# Demo article generator (used by /produce-test)
# ---------------------------------------------------------------------------

def generate_demo_article() -> Dict[str, Any]:
    """
    Generate a random demo news article with different sources, topics,
    and sentiment hints (positive/negative words for the enrichment service).
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    templates = [
        {
            "source": "cnn",
            "title": "Tech stocks rise after strong earnings report",
            "description": (
                "Markets show strong positive gains as major tech companies "
                "report excellent results and improved outlook."
            ),
            "url": "https://example.com/tech-stocks-rise",
        },
        {
            "source": "bbc",
            "title": "Global markets fall amid economic crisis fears",
            "description": (
                "Investors react to bad news as weak data and growing risk "
                "fuel fear of a major economic crisis and losses."
            ),
            "url": "https://example.com/markets-fall-crisis",
        },
        {
            "source": "guardian",
            "title": "New climate initiative shows positive impact",
            "description": (
                "A new climate program delivers good results and strong improvement "
                "in air quality across several major cities."
            ),
            "url": "https://example.com/climate-initiative",
        },
        {
            "source": "reuters",
            "title": "Company faces crisis after product failure",
            "description": (
                "Product failures and negative reviews lead to a sharp decline "
                "and dangerous loss of market share."
            ),
            "url": "https://example.com/product-crisis",
        },
        {
            "source": "coindesk",
            "title": "Crypto markets show strong recovery",
            "description": (
                "Digital assets show strong rise and excellent performance "
                "after weeks of decline."
            ),
            "url": "https://example.com/crypto-recovery",
        },
        {
            "source": "espn",
            "title": "Local team secures dramatic win in final minutes",
            "description": (
                "Fans celebrate a great win as the team shows strong performance "
                "and positive spirit."
            ),
            "url": "https://example.com/team-dramatic-win",
        },
        {
            "source": "nytimes",
            "title": "Government faces criticism over new policy",
            "description": (
                "Opposition warns of negative impact and risk as critics say "
                "the plan may fail and cause further problems."
            ),
            "url": "https://example.com/government-policy-criticism",
        },
    ]

    base = random.choice(templates)

    return {
        "id": str(uuid.uuid4()),
        "source": base["source"],
        "title": base["title"],
        "description": base["description"],
        "url": base["url"],
        "published_at": now_iso,
        "fetched_at": now_iso,
    }


# ---------------------------------------------------------------------------
# Lifecycle hooks
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    """
    Initialize Kafka producer with retry logic + start realtime fetching loop if enabled.
    """
    global producer, realtime_task, stop_event

    stop_event = asyncio.Event()  # reset in case of reload

    # We send JSON bytes manually, so we don't use value_serializer here.
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
    )

    # Retry Kafka connection until it succeeds
    attempt = 0
    while True:
        attempt += 1
        try:
            print(
                f"[ingestion-service] Kafka connect attempt {attempt} "
                f"to {KAFKA_BROKER} ..."
            )
            await producer.start()
            print("[ingestion-service] Kafka producer started successfully.")
            break
        except KafkaConnectionError as ex:
            print(f"[ingestion-service] Kafka connection failed: {ex}")
            await asyncio.sleep(3)

    # Start realtime loop if enabled
    if NEWS_API_ENABLED:
        print("[ingestion-service] Realtime news fetching ENABLED.")
        realtime_task = asyncio.create_task(realtime_news_loop())
    else:
        print("[ingestion-service] Realtime news fetching DISABLED.")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Cleanly stop realtime task + Kafka producer.
    """
    global producer, realtime_task

    print("[ingestion-service] Shutting down...")

    # Signal realtime loop to stop
    stop_event.set()

    # Cancel and await the background task
    if realtime_task:
        realtime_task.cancel()
        with suppress(asyncio.CancelledError):
            await realtime_task
        print("[ingestion-service] Realtime task stopped.")

    # Stop Kafka producer
    if producer is not None:
        await producer.stop()
        print("[ingestion-service] Kafka producer stopped.")


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

@app.post("/api/ingestion/produce-test")
async def produce_test(
    count: int = Query(1, ge=1, le=200, description="How many demo articles to produce"),
):
    """
    Produce `count` random demo articles into the raw Kafka topic.
    Use ?count=20 to generate 20 at once.
    """
    global producer

    if producer is None:
        return {"status": "error", "message": "Kafka producer is not initialized"}

    produced_ids = []

    for _ in range(count):
        event = generate_demo_article()
        # Send JSON bytes to Kafka
        await producer.send_and_wait(RAW_TOPIC, json.dumps(event).encode("utf-8"))
        produced_ids.append(event["id"])

    return {
        "status": "ok",
        "produced": len(produced_ids),
        "ids": produced_ids,
    }


@app.get("/api/ingestion/health")
async def health_check():
    return {"status": "ok", "service": "ingestion-service"}
