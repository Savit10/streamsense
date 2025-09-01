from fastapi import FastAPI
import sqlite3
from confluent_kafka import Consumer
import json, time, threading
from contextlib import asynccontextmanager

DB_FILE = "features.db"

# --- DB Setup ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS user_features (
            user_id INTEGER PRIMARY KEY,
            last_event_ts REAL,
            event_count INTEGER,
            add_to_cart_count INTEGER,
            purchase_count INTEGER
        )
    """)
    conn.commit()
    conn.close()

# --- Kafka Consumer ---
def consume_events():
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'feature-service',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe(['events'])

    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()

    while True:
        msg = c.poll(1.0)
        if msg is None: 
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))
        user = event["user_id"]
        action = event["action"]
        ts = time.time()

        # Update features
        cur.execute("SELECT * FROM user_features WHERE user_id=?", (user,))
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO user_features VALUES (?, ?, ?, ?, ?)",
                (user, ts, 1, int(action=='add_to_cart'), int(action=='purchase'))
            )
        else:
            _, last_ts, count, addc, purch = row
            cur.execute("""
                UPDATE user_features
                SET last_event_ts=?, event_count=?, add_to_cart_count=?, purchase_count=?
                WHERE user_id=?
            """, (ts, count+1, addc + int(action=='add_to_cart'), purch + int(action=='purchase'), user))
        conn.commit()

    c.close()


# --- FastAPI startup event for DB and consumer thread ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    t = threading.Thread(target=consume_events, daemon=True)
    t.start()
    yield

app = FastAPI(lifespan=lifespan)

# --- API ---
@app.get("/features/{user_id:int}")
def get_features(user_id: int):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT * FROM user_features WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row:
        return {
            "user_id": row[0],
            "last_event_ts": row[1],
            "event_count": row[2],
            "add_to_cart_count": row[3],
            "purchase_count": row[4],
        }
    return {"user_id": user_id, "status": "not_found"}

# --- API ---

# Endpoint to get 10 user details
@app.get("/features/sample_users")
def get_sample_users():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT * FROM user_features LIMIT 10")
    rows = cur.fetchall()
    conn.close()
    users = []
    for row in rows:
        users.append({
            "user_id": row[0],
            "last_event_ts": row[1],
            "event_count": row[2],
            "add_to_cart_count": row[3],
            "purchase_count": row[4],
        })
    return users

