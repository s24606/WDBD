# Deployment Guide

## Prerequisites

| Requirement | Version |
|---|---|
| Docker Desktop | 4.x or later |
| Docker Compose | v2 (bundled with Docker Desktop) |
| Python | 3.10+ |

Install host-side dependencies:

```bash
pip install -r scripts/requirements.txt
```

---

## Deployment

### One-command deployment

```bash
python scripts/deploy.py
```

This script:
1. Checks that Docker and Docker Compose are available
2. Runs `docker compose up --build -d`
3. Polls each service until healthy (postgres, kafka, minio, debezium, spark-master)
4. Prints a summary table of service URLs on success

### Manual deployment

```bash
docker compose up --build -d
docker compose ps -a
```

One-shot services (`ingestion`, `debezium-init`, `minio-init`) should show `Exited (0)`. Long-running services should show `(healthy)`.

---

## Service URLs and Credentials

| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Spark Master UI | http://localhost:8081 | — |
| Spark Worker UI | http://localhost:8082 | — |
| Debezium REST API | http://localhost:8083 | — |
| Schema Registry | http://localhost:8085 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO API | http://localhost:9000 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | user: postgres / pass: postgres / db: hospital |
| Kafka (external) | localhost:9094 | — |

---

## Verifying the Pipeline

### 1. Container status

```bash
docker compose ps -a
```

All long-running services up and healthy. One-shot services exited with code 0.

### 2. Monitor spark-app startup

`spark-app` downloads ~350 MB of JARs on first run and then processes an initial Kafka snapshot. This takes ~3 minutes before any data appears in MinIO. Follow progress with:

```bash
docker logs -f spark-app
```

The pipeline is ready when you see `ProcessingTimeExecutor: Current batch is falling behind` — this means the first batch ran. After that, run the health check.

### 3. Full stack validation

```bash
python scripts/health_check.py
```

Runs 14 checks: container status, port reachability, PostgreSQL row counts, Kafka topic existence, Schema Registry, Debezium connector status, MinIO liveness, and Delta file presence in MinIO. Exits 0 on pass, 1 on any failure.

### 3. PostgreSQL tables

```bash
docker exec -it postgres psql -U postgres -d hospital -c "\dt"
docker exec -it postgres psql -U postgres -d hospital -c "SELECT 'patients' AS tbl, count(*) FROM patients UNION ALL SELECT 'appointments', count(*) FROM appointments UNION ALL SELECT 'lab_results', count(*) FROM lab_results;"
```

Expected: 100 / 500 / 200 rows.

To inject an additional row and immediately trigger a CDC event:

```bash
python app/postgresql_insert.py --patient      # new patient
python app/postgresql_insert.py --appointment  # new appointment (random existing patient)
python app/postgresql_insert.py --lab_result   # new lab result (random existing appointment)
python app/postgresql_insert.py               # random table
```

Each call prints the inserted values and produces one `op: "c"` message on the corresponding Kafka topic.

### 4. Kafka topics

Open Kafka UI at http://localhost:8080 — three topics should be present:
- `hospital.public.patients`
- `hospital.public.appointments`
- `hospital.public.lab_results`

Or read them from the host:

```bash
python app/consumer.py
```

### 5. Debezium connector status

```bash
curl http://localhost:8083/connectors/hospital-connector/status
```

`"state": "RUNNING"` confirms CDC is active.

### 6. MinIO Delta files

```bash
docker run --rm --network wdbd_demo-net --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc ls -r local/hospital/"
```

Expected: `.snappy.parquet` and `_delta_log/` under both `bronze/` and `silver/` for all three tables. Silver files are smaller than bronze (filtered data).

### 7. End-to-end consistency

```bash
python app/validate_consistency.py            # all checks
python app/validate_consistency.py --database # PostgreSQL only
python app/validate_consistency.py --kafka    # Kafka only
python app/validate_consistency.py --minio    # MinIO only
```

---

## Data Layers

| Layer | Path | Content |
|---|---|---|
| Bronze | `s3a://hospital/bronze/{table}` | Raw CDC envelope from Debezium — all operations, no filtering |
| Silver | `s3a://hospital/silver/{table}` | Filtered and transformed: adults only (patients), completed appointments, abnormal lab results |

Checkpoints are stored at `s3a://hospital/checkpoints/{bronze\|silver}/{table}` and allow Spark to resume from the last committed offset after a restart.

---

## Querying Data from MinIO

### MinIO Console (browser)

1. Open http://localhost:9001
2. Log in with `minioadmin / minioadmin`
3. Browse the `hospital` bucket — navigate into `bronze/` or `silver/` to inspect Parquet files

### mc CLI

```bash
# list all files
docker run --rm --network wdbd_demo-net --entrypoint /bin/sh minio/mc -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin && mc ls -r local/hospital/"

# download a Parquet file for inspection
docker run --rm --network wdbd_demo-net --entrypoint /bin/sh minio/mc -c \
  "mc alias set local http://minio:9000 minioadmin minioadmin && mc cp local/hospital/silver/patients/part-00000*.parquet /tmp/patients.parquet"
```

---

## Teardown

```bash
python scripts/cleanup.py
```

Prompts for confirmation before running `docker compose down -v` (removes containers and volumes). Optionally removes built images.

---

## Troubleshooting

**spark-app keeps restarting**  
Spark downloads ~350 MB of JARs on first run. If it crashes before finishing, restart it:
```bash
docker compose restart spark-app
```

**Debezium connector not registered**  
`debezium-init` may have run before Debezium was fully ready. Re-register manually:
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/hospital-connector.json
```

**No data in MinIO after a few minutes**  
Check spark-app logs for errors:
```bash
docker logs spark-app 2>&1 | Select-String "ERROR|Exception"
```

**One-shot service exited with non-zero code**  
```bash
docker compose ps -a
docker logs <service-name>
```