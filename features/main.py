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
            last_event_ts DATETIME DEFAULT CURRENT_TIMESTAMP,
            event_count INTEGER,
            add_to_cart_count INTEGER,
            purchase_count INTEGER
        );
    """)
    c.execute("""
            CREATE TABLE IF NOT EXISTS events (
              event_id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER,
              item_id INTEGER,
              event_type TEXT NOT NULL CHECK (event_type IN ('view', 'click', 'add_to_cart', 'purchase')),
              event_value REAL,
              event_ts DATETIME DEFAULT CURRENT_TIMESTAMP
        );
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
        user_id, timestamp, item_id, event_type = event["user_id"], event["timestamp"], event["item_id"], event["event_type"]
        ts = time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f"))
        event_value = [1, 0.8, 0.5, 0.2][["view", "click", "add_to_cart", "purchase"].index(event_type)]


        # Update features
        cur.execute("SELECT * FROM user_features WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO user_features VALUES (?, ?, ?, ?, ?)",
                (user_id, ts, 1, int(event_type=='add_to_cart'), int(event_type=='purchase'))
            )
        else:
            _, last_ts, count, addc, purch = row
            cur.execute("""
                UPDATE user_features
                SET last_event_ts=?, event_count=?, add_to_cart_count=?, purchase_count=?
                WHERE user_id=?
            """, (ts, count+1, addc + int(event_type=='add_to_cart'), purch + int(event_type=='purchase'), user_id))
        
        # Log event
        cur.execute("""
            INSERT INTO events (user_id, item_id, event_type, event_value, event_ts
            ) VALUES (?, ?, ?, ?, ?)
        """, (user_id, item_id, event_type, event_value, timestamp))
        conn.commit()


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

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/events/recent/{n:int}")
def get_recent_events(n: int):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT * FROM events ORDER BY event_id DESC LIMIT ?", (n,))
    rows = cur.fetchall()
    conn.close()
    events = []
    for row in rows:
        events.append({
            "event_id": row[0],
            "user_id": row[1],
            "item_id": row[2],
            "event_type": row[3],
            "event_value": row[4],
            "event_ts": row[5],
        })
    return events

