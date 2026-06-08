"""One-command deployment for the Hospital Data Platform."""
import subprocess
import socket
import sys
import time

SERVICES_TO_POLL = [
    ("postgres",     "localhost", 5432,  30),
    ("kafka",        "localhost", 9092,  90),
    ("minio",        "localhost", 9000,  30),
    ("debezium",     "localhost", 8083, 120),
    ("spark-master", "localhost", 8081,  60),
]

SERVICE_URLS = [
    ("Kafka UI",        "http://localhost:8080"),
    ("Spark Master UI", "http://localhost:8081"),
    ("MinIO Console",   "http://localhost:9001   (minioadmin / minioadmin)"),
    ("Debezium",        "http://localhost:8083"),
    ("Schema Registry", "http://localhost:8085"),
    ("Spark Worker UI", "http://localhost:8082"),
]


def check_tool(cmd):
    try:
        subprocess.run(cmd, capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def preflight():
    print("=== Pre-flight checks ===")
    ok = True
    for label, cmd in [
        ("Docker daemon",   ["docker", "info"]),
        ("Docker Compose",  ["docker", "compose", "version"]),
    ]:
        if check_tool(cmd):
            print(f"  [OK]   {label}")
        else:
            print(f"  [FAIL] {label} not available")
            ok = False
    return ok


def wait_for_port(host, port, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(2)
    return False


def start_stack():
    print("\n=== Starting stack ===")
    result = subprocess.run(["docker", "compose", "up", "--build", "-d"])
    return result.returncode == 0


def poll_services():
    print("\n=== Waiting for services to be reachable ===")
    all_up = True
    for name, host, port, timeout in SERVICES_TO_POLL:
        print(f"  {name:<16} ", end="", flush=True)
        if wait_for_port(host, port, timeout):
            print("UP")
        else:
            print(f"TIMEOUT (>{timeout}s) — check: docker compose logs {name}")
            all_up = False
    return all_up


def print_summary():
    print("\n=== Stack is ready ===")
    print(f"  {'Service':<20} URL")
    print("  " + "-" * 60)
    for label, url in SERVICE_URLS:
        print(f"  {label:<20} {url}")
    print("\nRun python health_check.py to validate the full pipeline.")
    print()


def main():
    if not preflight():
        sys.exit(1)
    if not start_stack():
        print("docker compose up failed — check the output above.")
        sys.exit(1)
    if not poll_services():
        print("\nSome services failed to become reachable.")
        print("Run:  docker compose ps --all")
        sys.exit(1)
    print_summary()


if __name__ == "__main__":
    main()