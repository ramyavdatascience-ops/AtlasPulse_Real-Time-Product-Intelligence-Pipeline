import json
import time
import random
import uuid
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

fake = Faker()

# ── connect to Kafka ──────────────────────────────────────────
def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=30000,
                api_version=(3, 5, 0)
            )
            print("✅ Connected to Kafka successfully\n")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka... {e}")
            time.sleep(3)

TOPIC = os.getenv("KAFKA_TOPIC_CONFLUENCE", "confluence-events")

# ── realistic Confluence-style data ──────────────────────────
TEAMS = ["Platform Engineering", "Jira Cloud Core", "Confluence Content",
         "Bitbucket DevOps", "Data & Analytics", "Mobile Apps"]

EVENT_TYPES = ["page_created", "page_updated", "page_viewed",
               "comment_added", "ai_assist", "space_created"]

SPACE_KEYS = ["PLAT", "JIRA", "CONF", "BDEV", "DATA", "MOBL"]

# ── generate one realistic Confluence event ───────────────────
def generate_confluence_event():
    team = random.choice(TEAMS)
    event_type = random.choice(EVENT_TYPES)
    return {
        "event_id":         str(uuid.uuid4()),
        "event_type":       event_type,
        "event_ts":         datetime.now(timezone.utc).isoformat(),
        "team_id":          f"team_{TEAMS.index(team) + 1}",
        "team_name":        team,
        "user_id":          f"user_{random.randint(1, 200)}",
        "user_email":       fake.email(),
        "space_key":        random.choice(SPACE_KEYS),
        "page_id":          f"PAGE-{random.randint(10000, 99999)}",
        "page_title":       fake.sentence(nb_words=5),
        "response_time_ms": random.randint(80, 800),
        "ai_assist_used":   event_type == "ai_assist",
        "source_app":       "confluence"
    }

# ── push events to Kafka continuously ─────────────────────────
print("🚀 AtlasPulse — Confluence producer started")
print(f"   Pushing to topic: {TOPIC}")
print("   Press Ctrl+C to stop\n")

producer = create_producer()

count = 0
while True:
    try:
        event = generate_confluence_event()
        producer.send(TOPIC, value=event)
        producer.flush()
        count += 1
        print(f"  ✅ [{count}] {event['event_type']} | {event['team_name']} | {event['event_ts']}")
        time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n⛔ Producer stopped.")
        producer.close()
        break
    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(2)