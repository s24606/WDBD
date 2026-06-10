# Quickstart

## Prerequisites

Docker Desktop running, Python 3.10+, dependencies installed:

```bash
pip install -r scripts/requirements.txt
```

## Start

```bash
python scripts/deploy.py
```

Brings up all services, waits for each to be healthy, then prints a summary. Takes ~2 minutes on first run (image pulls + jar downloads).

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Spark Master UI | http://localhost:8081 | — |
| Spark Worker UI | http://localhost:8082 | — |
| Debezium REST API | http://localhost:8083 | — |
| Schema Registry | http://localhost:8085 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | postgres / postgres / hospital |

## Verify data landed in MinIO

```bash
docker run --rm --network wdbd_demo-net --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc ls -r local/hospital/"
```

Expect `.snappy.parquet` and `_delta_log/` entries under both `bronze/` and `silver/` for all three tables.

## Check service health

```bash
# all services status and exit codes
docker compose ps -a

# 14-point validation (containers, ports, row counts, Kafka topics, MinIO Delta files)
python scripts/health_check.py
```

## Stop and reset

```bash
python scripts/cleanup.py
```