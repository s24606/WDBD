"""
End-to-end pipeline consistency validation for the Hospital Data Platform.

Usage:
  python app/validate_consistency.py            # all checks
  python app/validate_consistency.py --database # DB only
  python app/validate_consistency.py --kafka    # Kafka only
  python app/validate_consistency.py --minio    # MinIO only

Requires: pip install -r scripts/requirements.txt
"""
import argparse
import io
import json
import sys

TABLES = ("patients", "appointments", "lab_results")
EXPECTED_COUNTS = {"patients": 100, "appointments": 500, "lab_results": 200}
PK_COLUMNS = {"patients": "patient_id", "appointments": "appointment_id", "lab_results": "result_id"}
KAFKA_TOPICS = [f"hospital.public.{t}" for t in TABLES]


# ── output helpers ────────────────────────────────────────────────────────────

def ok(msg):
    print(f"  [PASS] {msg}")
    return True

def fail(msg):
    print(f"  [FAIL] {msg}")
    return False


# ── database ──────────────────────────────────────────────────────────────────

def check_database():
    print("--- Database ---")
    try:
        import psycopg2
    except ImportError:
        return [fail("psycopg2 not installed — pip install psycopg2-binary")]

    results = []
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="hospital",
        user="postgres", password="postgres",
    )
    cur = conn.cursor()

    for table in TABLES:
        # table exists
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
            (table,),
        )
        if not cur.fetchone()[0]:
            results.append(fail(f"{table}: table does not exist"))
            continue

        # row count
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        expected = EXPECTED_COUNTS[table]
        if count == expected:
            results.append(ok(f"{table}: {count} rows"))
        else:
            results.append(fail(f"{table}: {count} rows (expected {expected})"))

        # NULL PKs
        pk = PK_COLUMNS[table]
        cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {pk} IS NULL")
        nulls = cur.fetchone()[0]
        if nulls == 0:
            results.append(ok(f"{table}.{pk}: no NULLs"))
        else:
            results.append(fail(f"{table}.{pk}: {nulls} NULL values"))

    conn.close()
    return results


# ── kafka ─────────────────────────────────────────────────────────────────────

def check_kafka():
    print("--- Kafka ---")
    try:
        from kafka import KafkaConsumer, KafkaAdminClient
    except ImportError:
        return [fail("kafka-python-ng not installed — pip install kafka-python-ng")]

    results = []
    admin = KafkaAdminClient(bootstrap_servers="localhost:9094")
    existing_topics = set(admin.list_topics())
    admin.close()

    for topic in KAFKA_TOPICS:
        if topic not in existing_topics:
            results.append(fail(f"{topic}: topic missing"))
            continue

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers="localhost:9094",
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda b: b.decode("utf-8"),
        )
        messages = list(consumer)
        consumer.close()

        if not messages:
            results.append(fail(f"{topic}: 0 messages"))
            continue

        # validate JSON + op field on first message
        try:
            payload = json.loads(messages[0].value)
            has_op = "op" in payload
        except (json.JSONDecodeError, Exception):
            has_op = False

        if has_op:
            results.append(ok(f"{topic}: {len(messages)} messages, valid JSON with 'op' field"))
        else:
            results.append(fail(f"{topic}: {len(messages)} messages but first message has no 'op' field"))

    return results


# ── minio ─────────────────────────────────────────────────────────────────────

def check_minio():
    print("--- MinIO ---")
    try:
        import boto3
        from botocore.client import Config
        import pyarrow.parquet as pq
    except ImportError as e:
        return [fail(f"{e.name} not installed — pip install boto3 pyarrow")]

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"),
    )

    results = []
    for table in TABLES:
        prefix = f"silver/{table}/"
        resp = s3.list_objects_v2(Bucket="hospital", Prefix=prefix)
        contents = resp.get("Contents", [])

        parquet_keys = [o["Key"] for o in contents if o["Key"].endswith(".parquet")]
        has_delta    = any("_delta_log" in o["Key"] for o in contents)

        if not parquet_keys:
            results.append(fail(f"silver/{table}: no parquet files"))
            continue
        if not has_delta:
            results.append(fail(f"silver/{table}: _delta_log missing"))
            continue

        rows = 0
        for key in parquet_keys:
            body = s3.get_object(Bucket="hospital", Key=key)["Body"].read()
            rows += pq.read_metadata(io.BytesIO(body)).num_rows

        results.append(ok(f"silver/{table}: {rows} rows, {len(parquet_keys)} parquet file(s), _delta_log present"))

    return results


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Hospital pipeline consistency check")
    parser.add_argument("--database", action="store_true")
    parser.add_argument("--kafka",    action="store_true")
    parser.add_argument("--minio",    action="store_true")
    args = parser.parse_args()

    run_all = not any([args.database, args.kafka, args.minio])

    print("=== Pipeline Consistency Validation ===\n")
    results = []

    if run_all or args.database:
        results += check_database()
        print()
    if run_all or args.kafka:
        results += check_kafka()
        print()
    if run_all or args.minio:
        results += check_minio()
        print()

    passed = sum(results)
    total  = len(results)
    print(f"{passed}/{total} checks passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()