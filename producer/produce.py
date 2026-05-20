import json, os, random, time
from datetime import datetime, timezone
from confluent_kafka import Producer

bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "events")

producer = Producer({"bootstrap.servers": bootstrap})

regions = ["EU", "US", "APAC"]
types = ["click", "view", "purchase"]

print(f"Sending events to {bootstrap} topic '{topic}' …")
i = 0
while True:
    i += 1
    event = {
        "id": i,
        "ts": datetime.now(timezone.utc).isoformat(),
        "type": random.choice(types),
        "amount": round(random.uniform(1, 500), 2),
        "region": random.choice(regions),
    }
    producer.produce(topic, value=json.dumps(event).encode("utf-8"))
    producer.poll(0)
    if i % 20 == 0:
        producer.flush()
        print(f"Sent {i} messages")
    time.sleep(0.2)  # ~5 msgs/sec