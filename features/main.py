from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'feature-service',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['events'])

while True:
    msg = c.poll(1.0)  # 1s timeout
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    print(f"Received event: {msg.value().decode('utf-8')}")
