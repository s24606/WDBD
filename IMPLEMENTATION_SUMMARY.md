# Implementation Summary

## Overview

A fully containerised hospital data platform built across 6 tasks. The pipeline ingests synthetic hospital data into PostgreSQL, captures row-level changes with Debezium, streams them through Kafka, processes them in Spark Structured Streaming, and writes the results to MinIO in Delta Lake format.

## Data Flow

```
CSV files
    └─► PostgreSQL (hospital DB)
            └─► Debezium (CDC via WAL / pgoutput)
                    └─► Kafka topics (JSON, 3 topics)
                                └─► Spark Structured Streaming
                                        ├─► Bronze layer (MinIO, Delta)
                                        └─► Silver layer (MinIO, Delta, filtered)
```

## Domain Model

Three tables loaded from CSV:

| Table | Rows | Key columns |
|---|---|---|
| `patients` | 100 | patient_id (PK), date_of_birth, is_active |
| `appointments` | 500 | appointment_id (PK), patient_id (FK), status |
| `lab_results` | 200 | result_id (PK), patient_id (FK), appointment_id (FK), is_abnormal |

---

## Task 1 — Python Ingestion

**File:** `app/load_data.py`

- Connects to PostgreSQL with retry/backoff (30 attempts, 2 s delay)
- Reads three CSVs with `pandas.read_csv`
- Creates tables dynamically by mapping pandas dtypes to PostgreSQL types (`int64` → `INTEGER`, `float64` → `FLOAT`, `object` → `TEXT`) with PK and FK constraints
- Bulk-inserts all rows using `psycopg2.extras.execute_values`; idempotent via `TRUNCATE … RESTART IDENTITY CASCADE`
- After a 5 s delay, simulates CDC events so Debezium captures real UPDATE operations:
  - 20 appointments: `scheduled` → `completed`
  - 5 appointments: `scheduled` → `cancelled`
  - 10 lab_results: `is_abnormal` toggled
  - 5 patients: `is_active` set to `false`

**File:** `app/generate_csv.py`

- Generates deterministic synthetic data (`random.seed(42)`) using faker
- patients: 85 adults (18–80) + 15 minors (15–17)
- appointments: ~5 per patient; status split 60 % completed / 25 % scheduled / 10 % cancelled / 5 % no_show
- lab_results: from completed appointments only; 7 test types with reference ranges; ~50 % abnormal

**docker-compose.yml:**
- `postgres` service: image `postgres:15`, WAL configured for Debezium (`wal_level=logical`, `max_replication_slots=5`, `max_wal_senders=5`), healthcheck via `pg_isready`
- `ingestion` service: built from `app/Dockerfile`, depends on postgres healthy, mounts `./data:/data`

---

## Task 2 — Debezium CDC

**File:** `debezium/hospital-connector.json`

| Parameter | Value |
|---|---|
| `connector.class` | `io.debezium.connector.postgresql.PostgresConnector` |
| `plugin.name` | `pgoutput` (built into PostgreSQL 10+) |
| `snapshot.mode` | `initial` — full snapshot on first connect, then streaming |
| `table.include.list` | `public.patients, public.appointments, public.lab_results` |
| `key/value.converter` | `JsonConverter` with `schemas.enable=false` |

**docker-compose.yml:**
- `debezium` service: image `debezium/connect:2.6`, depends on kafka (healthy) + postgres (healthy), healthcheck via `curl /connectors`, `restart: on-failure`
- `debezium-init` service: one-shot `curlimages/curl` container that POSTs `hospital-connector.json` to the Debezium REST API; depends on debezium (healthy) + ingestion (completed)

Kafka topics created:

| Topic | Initial message count |
|---|---|
| `hospital.public.patients` | 100 |
| `hospital.public.appointments` | 500 |
| `hospital.public.lab_results` | 200 |

Each message envelope: `{ "op": "r|c|u|d", "before": {...}, "after": {...}, "source": {...} }`

---

## Task 3 — Kafka & Schema Registry

**docker-compose.yml:**
- `kafka` service: `confluentinc/cp-kafka:7.7.0` in KRaft mode (no ZooKeeper), dual listeners:
  - `PLAINTEXT://kafka:9092` — internal (Debezium, Spark)
  - `EXTERNAL://localhost:9094` — host-side tooling
- `schema-registry` service: `confluentinc/cp-schema-registry:7.7.0`, port `8085`
- `kafka-ui` service: `provectuslabs/kafka-ui`, port `8080`

**File:** `app/consumer.py`

- Reads all 3 hospital topics from earliest offset using `kafka-python-ng`
- Bootstraps to `localhost:9094` (external listener) or `$KAFKA_BOOTSTRAP` env var
- Deserialises JSON, extracts `op` field, prints key columns per table
- 10 s consumer timeout; prints summary statistics on exit

---

## Task 4 — Spark Structured Streaming

**File:** `spark/app/stream_job.py`

Reads Debezium JSON envelope from Kafka, extracts `$.after`, parses against predefined schemas (`PATIENT_SCHEMA`, `APPOINTMENT_SCHEMA`, `LAB_RESULT_SCHEMA`), applies business rules:

| Silver stream | Filter | Transformation |
|---|---|---|
| `patients_adults` | `age >= 18` | `age` derived from `date_of_birth` (days-since-epoch INT32) |
| `appointments_completed` | `status = 'completed'` | — |
| `lab_results_abnormal` | `is_abnormal = true` | — |

Date conversion for age:
```python
dob = date_add(lit("1970-01-01").cast("date"), col("date_of_birth"))
age = floor(datediff(current_date(), dob) / 365)
```

**docker-compose.yml:**
- `spark-master`: `spark:3.5.1-python3`, port 7077 (cluster) + 8081 (UI), healthcheck via `curl localhost:8080`
- `spark-worker`: depends on spark-master (healthy), 2 cores / 2 GB
- `spark-app`: `spark-submit` with packages for Kafka, Delta Lake, Hadoop AWS, AWS SDK; depends on spark-master (healthy), spark-worker (started), debezium-init (completed), minio-init (completed)

---

## Task 5 — MinIO + Delta Lake

**docker-compose.yml:**
- `minio`: `minio/minio:latest`, ports 9000 (API) + 9001 (console), credentials `minioadmin/minioadmin`, healthcheck via `/minio/health/live`
- `minio-init`: one-shot `minio/mc` container that creates the `hospital` bucket

**spark/app/stream_job.py — SparkSession configuration:**
- Delta Lake: `DeltaSparkSessionExtension`, `DeltaCatalog`
- S3A/MinIO: endpoint `http://minio:9000`, `SimpleAWSCredentialsProvider`, path-style access enabled

**6 concurrent streaming queries:**

| Query name | Kafka topic | Output path | Filter |
|---|---|---|---|
| `bronze_patients` | `hospital.public.patients` | `s3a://hospital/bronze/patients` | none (raw) |
| `bronze_appointments` | `hospital.public.appointments` | `s3a://hospital/bronze/appointments` | none (raw) |
| `bronze_lab_results` | `hospital.public.lab_results` | `s3a://hospital/bronze/lab_results` | none (raw) |
| `patients_adults` | `hospital.public.patients` | `s3a://hospital/silver/patients` | age >= 18 |
| `appointments_completed` | `hospital.public.appointments` | `s3a://hospital/silver/appointments` | status = completed |
| `lab_results_abnormal` | `hospital.public.lab_results` | `s3a://hospital/silver/lab_results` | is_abnormal = true |

Checkpoints stored at `s3a://hospital/checkpoints/{bronze|silver}/{table}`.

**Verified output (MinIO CLI):**

```
bronze/patients/part-00000-*.snappy.parquet     8.4 KB
bronze/appointments/part-00000-*.snappy.parquet 18 KB
bronze/lab_results/part-00000-*.snappy.parquet  7.9 KB
silver/patients/part-00000-*.snappy.parquet     3.7 KB
silver/appointments/part-00000-*.snappy.parquet 5.7 KB
silver/lab_results/part-00000-*.snappy.parquet  4.1 KB
```

Silver files are smaller than bronze as expected — data has been filtered.

---

## Task 6 — Automation & Reliability

### Docker Compose reliability

- Healthchecks added to: `postgres`, `kafka`, `debezium`, `minio`, `spark-master`
- All `service_started` race conditions replaced with `service_healthy` or `service_completed_successfully`
- `restart: on-failure` on all long-running services
- One-shot services (`ingestion`, `debezium-init`, `minio-init`) have no restart policy

### Startup dependency order

```
postgres ──healthy──► ingestion ──completed──►┐
                                              debezium-init ──completed──►┐
kafka ──healthy──► debezium ──healthy──────────────────────────────────────┤
                                                                           spark-app
minio ──healthy──► minio-init ──completed──────────────────────────────────┤
                                                                           │
kafka ──healthy──► spark-master ──healthy──► spark-worker ─────────────────┘
```

### Operations scripts (`scripts/`)

| Script | Purpose |
|---|---|
| `deploy.py` | Pre-flight checks, `docker compose up --build -d`, per-service readiness polling, summary table |
| `health_check.py` | 14-point validation: containers, ports, PG row counts, Kafka topics, Schema Registry, Debezium connector, MinIO Delta files; CI/CD compatible exit codes |
| `cleanup.py` | Confirmation prompt, `docker compose down -v`, optional image removal |

### Validation script (`app/validate_consistency.py`)

End-to-end pipeline check with `--database`, `--kafka`, `--minio` flags:
- Database: table existence, row counts (100/500/200), NULL PK check
- Kafka: topic existence, message count > 0, valid JSON with `op` field
- MinIO: Silver paths exist, `.parquet` files present, `_delta_log/` confirms Delta format

### Documentation

| File | Purpose |
|---|---|
| `README.md` | Project overview, pipeline diagram, tech stack, repo structure |
| `QUICKSTART.md` | Start command, service URL table, MinIO verification, health check, stop |
| `DEPLOYMENT_GUIDE.md` | Full deployment reference, verification steps, data layer querying, troubleshooting |

---

## Complete File Inventory

| File | Task | Description |
|---|---|---|
| `app/generate_csv.py` | 1 | Synthetic hospital CSV generator |
| `app/load_data.py` | 1 | CSV → PostgreSQL ingestion + CDC simulation |
| `app/Dockerfile` | 1 | Container image for ingestion service |
| `app/requirements.txt` | 1 | psycopg2-binary, pandas, kafka-python-ng |
| `app/consumer.py` | 3 | Kafka consumer for all 3 CDC topics |
| `app/validate_consistency.py` | 6 | End-to-end pipeline validation |
| `data/patients.csv` | 1 | 100 rows |
| `data/appointments.csv` | 1 | 500 rows |
| `data/lab_results.csv` | 1 | 200 rows |
| `debezium/hospital-connector.json` | 2 | Debezium PostgreSQL CDC connector config |
| `spark/app/stream_job.py` | 4/5 | Spark Structured Streaming (6 queries, bronze + silver) |
| `scripts/deploy.py` | 6 | One-command deployment |
| `scripts/health_check.py` | 6 | 14-point stack validation |
| `scripts/cleanup.py` | 6 | Safe teardown |
| `scripts/requirements.txt` | 6 | Host-side ops dependencies |
| `docker-compose.yml` | 1–6 | 12 services, healthchecks, dependency ordering |