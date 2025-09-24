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
        "item_id": random.randint(1, 100),
        "event_type": random.choice(ACTIONS),
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
    try:
        users_request = requests.get("http://localhost:8000/features/sample_users")
        events_request = requests.get("http://localhost:8000/events/recent/10")
        print("Users response:", users_request.json())
        print("Events response:", events_request.json())
    except Exception as e:
        print("Error fetching users:", e)
