import random
import time
import json
from datetime import datetime
import requests
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})

ACTIONS = ["view", "click", "add_to_cart", "purchase"]
PAGES = ["/home", "/products/42", "/products/77", "/cart", "/checkout"]

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_event(user_id: int) -> dict:
    return {
        "user_id": user_id,
        "timestamp": datetime.now().isoformat(),
        "page": random.choice(PAGES),
        "action": random.choices(
            ACTIONS,
            weights=[0.6, 0.25, 0.1, 0.05],  # more views, fewer purchases
        )[0],
    }

def run_event_generator(num_users=100, events_per_sec=10):
    user_ids = list(range(1, num_users + 1))
    interval = 1.0 / events_per_sec
    count = 0
    while count < 20:
        user = random.choice(user_ids)
        event = generate_event(user)
        p.produce("events", json.dumps(event).encode("utf-8"), callback=delivery_report)
        count += 1
        time.sleep(interval)
        print(f"Produced event: {event}")

if __name__ == "__main__":
    run_event_generator()


