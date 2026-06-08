"""
Full stack health validation for the Hospital Data Platform.
Exit code 0 = all checks passed (CI/CD compatible).

Requires (pip install):  psycopg2-binary  kafka-python-ng  requests  boto3
"""
import json
import socket
import subprocess
import sys


# ── helpers ───────────────────────────────────────────────────────────────────

def port_reachable(host, port):
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


def result(passed, msg):
    return passed, msg


# ── checks ────────────────────────────────────────────────────────────────────

def check_containers():
    proc = subprocess.run(
        ["docker", "compose", "ps", "--all", "--format", "json"],
        capture_output=True, text=True,
    )
    states = {}
    for line in proc.stdout.strip().splitlines():
        if line.strip():
            c = json.loads(line)
            states[c.get("Service", "")] = c.get("State", "")

    long_running = [
        "postgres", "kafka", "debezium", "schema-registry",
        "kafka-ui", "minio", "spark-master", "spark-worker", "spark-app",
    ]
    not_running = [s for s in long_running if states.get(s) != "running"]
    if not_running:
        return result(False, f"not running: {', '.join(not_running)}")
    return result(True, "all long-running services up")


def check_port(label, host, port):
    if port_reachable(host, port):
        return result(True, "reachable")
    return result(False, f"{host}:{port} unreachable")


def check_postgres():
    try:
        import psycopg2
    except ImportError:
        return result(False, "psycopg2 not installed — pip install psycopg2-binary")
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="hospital",
        user="postgres", password="postgres",
    )
    cur = conn.cursor()
    counts = {}
    for table in ("patients", "appointments", "lab_results"):
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        counts[table] = cur.fetchone()[0]
    conn.close()

    expected = {"patients": 100, "appointments": 500, "lab_results": 200}
    wrong = [f"{t}={counts[t]} (expected {e})" for t, e in expected.items() if counts[t] != e]
    if wrong:
        return result(False, "; ".join(wrong))
    return result(True, f"patients={counts['patients']}, appointments={counts['appointments']}, lab_results={counts['lab_results']}")


def check_kafka():
    try:
        from kafka import KafkaAdminClient
    except ImportError:
        return result(False, "kafka-python-ng not installed — pip install kafka-python-ng")
    admin = KafkaAdminClient(bootstrap_servers="localhost:9094")
    topics = set(admin.list_topics())
    admin.close()

    expected = {"hospital.public.patients", "hospital.public.appointments", "hospital.public.lab_results"}
    missing = expected - topics
    if missing:
        return result(False, f"missing: {', '.join(missing)}")
    return result(True, "all 3 hospital topics present")


def check_debezium():
    try:
        import requests
    except ImportError:
        return result(False, "requests not installed — pip install requests")
    r = requests.get("http://localhost:8083/connectors/hospital-connector/status", timeout=5)
    r.raise_for_status()
    state = r.json()["connector"]["state"]
    if state != "RUNNING":
        return result(False, f"connector state: {state}")
    return result(True, "connector RUNNING")


def check_minio():
    try:
        import io
        import boto3
        from botocore.client import Config
        import pyarrow.parquet as pq
    except ImportError as e:
        return result(False, f"{e.name} not installed — pip install boto3 pyarrow")
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version="s3v4"),
    )
    missing = []
    counts = {}
    for table in ("patients", "appointments", "lab_results"):
        resp = s3.list_objects_v2(Bucket="hospital", Prefix=f"silver/{table}/")
        contents = resp.get("Contents", [])
        parquet_keys = [o["Key"] for o in contents if o["Key"].endswith(".parquet")]
        has_delta    = any("_delta_log" in o["Key"] for o in contents)
        if not parquet_keys or not has_delta:
            missing.append(table)
            counts[table] = "?"
            continue
        rows = 0
        for key in parquet_keys:
            body = s3.get_object(Bucket="hospital", Key=key)["Body"].read()
            rows += pq.read_metadata(io.BytesIO(body)).num_rows
        counts[table] = rows

    if missing:
        return result(False, f"silver layer incomplete for: {', '.join(missing)}")
    summary = ", ".join(f"{t}={counts[t]}" for t in ("patients", "appointments", "lab_results"))
    return result(True, f"rows — {summary}")


# ── runner ────────────────────────────────────────────────────────────────────

def run_check(label, fn, *args):
    try:
        passed, msg = fn(*args)
    except Exception as e:
        passed, msg = False, str(e)
    tag = "PASS" if passed else "FAIL"
    print(f"  [{tag}] {label}: {msg}")
    return passed


def main():
    print("=== Hospital Platform Health Check ===\n")

    checks = [
        ("Containers",          check_containers,                       ),
        ("postgres:5432",       check_port, "postgres", "localhost", 5432),
        ("kafka:9092",          check_port, "kafka",    "localhost", 9092),
        ("minio:9000",          check_port, "minio",    "localhost", 9000),
        ("debezium:8083",       check_port, "debezium", "localhost", 8083),
        ("spark-master:8081",   check_port, "spark-master", "localhost", 8081),
        ("PostgreSQL row counts", check_postgres,                       ),
        ("Kafka topics",        check_kafka,                            ),
        ("Debezium connector",  check_debezium,                         ),
        ("MinIO silver layer",  check_minio,                            ),
    ]

    results = []
    for row in checks:
        label, fn, *args = row
        results.append(run_check(label, fn, *args))

    passed = sum(results)
    total  = len(results)
    print(f"\n{passed}/{total} checks passed")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()