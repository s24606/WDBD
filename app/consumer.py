"""
Kafka consumer that reads JSON messages from all three hospital CDC topics
and prints a summary of each event. Run locally to validate messages flow.

    pip install kafka-python
    python app/consumer.py
"""
import json
import os
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")

TOPICS = [
    "hospital.public.patients",
    "hospital.public.appointments",
    "hospital.public.lab_results",
]

OP_LABELS = {
    "r": "READ (snapshot)",
    "c": "CREATE",
    "u": "UPDATE",
    "d": "DELETE",
}

# ── per-table summary fields to print ────────────────────────────────────────

SUMMARY_FIELDS = {
    "hospital.public.patients":     ["patient_id", "first_name", "last_name", "is_active"],
    "hospital.public.appointments": ["appointment_id", "patient_id", "department", "status"],
    "hospital.public.lab_results":  ["result_id", "patient_id", "test_name", "is_abnormal"],
}


def summarise(topic: str, payload: dict) -> str:
    op = payload.get("op", "?")
    row = payload.get("after") or payload.get("before") or {}
    fields = {k: row.get(k) for k in SUMMARY_FIELDS.get(topic, [])}
    return f"op={OP_LABELS.get(op, op):<20} | {fields}"


def main() -> None:
    print(f"Connecting to Kafka at {BOOTSTRAP} ...")
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[BOOTSTRAP],
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=10_000,
    )
    print(f"Subscribed to: {', '.join(TOPICS)}")
    print("Reading messages (Ctrl+C to stop) ...\n")

    counts = {t: 0 for t in TOPICS}
    try:
        for msg in consumer:
            counts[msg.topic] += 1
            print(f"[{msg.topic.split('.')[-1]:15}] {summarise(msg.topic, msg.value)}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("\n--- Summary ---")
        for topic, n in counts.items():
            print(f"  {topic}: {n} messages")


if __name__ == "__main__":
    main()