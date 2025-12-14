import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = "clickstream"
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

pages = ["/", "/pricing", "/docs", "/signup", "/login", "/blog", "/careers"]
countries = ["US", "CA", "GB", "DE", "IN", "BR", "AU"]
devices = ["mobile", "desktop", "tablet"]
referrers = ["google", "bing", "newsletter", "twitter", "direct", "reddit"]

def event(i: int) -> dict:
    return {
        "event_id": f"evt_{i}_{random.randint(1000,9999)}",
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "user_id": random.randint(1, 5000),
        "session_id": f"s_{random.randint(1, 200000)}",
        "page": random.choice(pages),
        "country": random.choice(countries),
        "device": random.choice(devices),
        "referrer": random.choice(referrers),
        "purchase_usd": round(max(0, random.gauss(3, 12)), 2),
    }

if __name__ == "__main__":
    print("Producing events to Kafka topic 'clickstream'... CTRL+C to stop.")
    i = 1
    while True:
        e = event(i)
        producer.send(TOPIC, e)
        if i % 50 == 0:
            producer.flush()
            print(f"sent {i} events; latest={e}")
        i += 1
        time.sleep(0.05)
