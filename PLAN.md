# Mini Data Platform — Project Plan

## Objective

Build a mini data platform in Docker that simulates a hospital business process, ingests data into PostgreSQL, captures changes with Debezium, streams through Kafka, processes in Spark, and stores in MinIO in Delta format.

## Tech Stack

| Layer | Tool |
|---|---|
| Simulation | Python (pandas, psycopg2, faker) |
| Database | PostgreSQL 15 (logical replication enabled) |
| CDC | Debezium |
| Streaming | Apache Kafka (KRaft, no ZooKeeper) |
| Processing | Apache Spark 3.5.1 |
| Storage | MinIO (S3-compatible, Delta Lake) |
| Orchestration | Docker Compose |

---

## Task Breakdown

### Task 1 — Simulating a Business Process with Python ✓
### Task 2 — Connecting Debezium to Capture Changes in PostgreSQL ✓
### Task 3 — Kafka Setup & Streaming Events in JSON Format ✓
### Task 4 — Integrating Spark with Kafka for Data Processing ✓
### Task 5 — Storing Processed Data in MinIO using Delta Lake ✓
### Task 6 — Automating Deployment & Ensuring Reliability ← current (not started)

---

## Task 1 — Hospital Scenario: Detailed Plan

### Domain Schema

**patients** (100 rows)

| Column | Type | Notes |
|---|---|---|
| patient_id | INT (PK) | |
| first_name | VARCHAR | |
| last_name | VARCHAR | |
| date_of_birth | DATE | used to derive age |
| gender | VARCHAR | M/F |
| email | VARCHAR | |
| phone | VARCHAR | format: XXX XXX XXX |
| registered_at | TIMESTAMP | |
| is_active | BOOLEAN | can be deactivated |

**appointments** (500 rows)

| Column | Type | Notes |
|---|---|---|
| appointment_id | INT (PK) | |
| patient_id | INT (FK) | |
| doctor_name | VARCHAR | |
| department | VARCHAR | cardiology, oncology, neurology, orthopedics, general_practice, pediatrics, dermatology |
| scheduled_at | TIMESTAMP | |
| status | VARCHAR | scheduled / completed / cancelled / no_show |
| notes | TEXT | |

**lab_results** (200 rows)

| Column | Type | Notes |
|---|---|---|
| result_id | INT (PK) | |
| patient_id | INT (FK) | |
| appointment_id | INT (FK) | only completed appointments |
| test_name | VARCHAR | glucose, CBC, cholesterol, etc. |
| result_value | FLOAT | numeric result |
| unit | VARCHAR | mg/dL, g/dL, etc. |
| reference_min | FLOAT | normal range lower bound |
| reference_max | FLOAT | normal range upper bound |
| is_abnormal | BOOLEAN | derived: value outside reference range |
| recorded_at | TIMESTAMP | |

### Relationships

```
patients (100)
    ├── appointments (500)   ~5 per patient
    └── lab_results  (200)   ~2 per patient, linked to an appointment
```

### Spark Business Rules (used in Task 4)

| Stream | Filter Rule | Rationale |
|---|---|---|
| patients | age >= 18 | adults only |
| appointments | status = 'completed' | only completed visits |
| lab_results | is_abnormal = true | flag out-of-range results |

---

### File Structure

```
WDBD/
├── app/
│   ├── generate_csv.py          # one-off generator script (outputs to data/)
│   ├── load_data.py             # Task 1 — loads CSVs into PostgreSQL
│   ├── consumer.py              # Task 3 — Kafka consumer
│   ├── Dockerfile
│   └── requirements.txt         # psycopg2-binary, pandas, kafka-python-ng
├── data/
│   ├── patients.csv             # 100 rows (committed)
│   ├── appointments.csv         # 500 rows (committed)
│   └── lab_results.csv          # 200 rows (committed)
├── spark/
│   └── app/
│       └── stream_job.py        # Task 4 — Spark structured streaming job
├── debezium/
│   └── hospital-connector.json  # Debezium connector config (Task 2)
├── docs/
│   └── ProjectDescription.pdf
├── docker-compose.yml
└── PLAN.md
```

---

### load_data.py — Execution Flow

1. **Wait for Postgres** — retry loop until connection succeeds
2. **Read CSVs** — `pandas.read_csv` for all three files
3. **Dynamic table creation** — infer column types from pandas dtypes → `CREATE TABLE IF NOT EXISTS` with PK/FK constraints
4. **Bulk INSERT** — `execute_values` in batches; idempotent (`TRUNCATE … RESTART IDENTITY CASCADE` first)
5. **Simulate CDC events** (after `time.sleep(5)` so Debezium captures them as real UPDATEs):
   - 20 appointments: `scheduled` → `completed`
   - 5 appointments: `scheduled` → `cancelled`
   - 10 lab_results: `is_abnormal` corrected (data correction simulation)
   - 5 patients: `is_active` set to `false` (discharged)

---

### docker-compose.yml Additions

**postgres service:**
```yaml
postgres:
  image: postgres:15
  command: postgres -c wal_level=logical   # required for Task 2 Debezium
  environment:
    POSTGRES_DB: hospital
    POSTGRES_USER: postgres
    POSTGRES_PASSWORD: postgres
  ports: ["5432:5432"]
  volumes:
    - postgres_data:/var/lib/postgresql/data
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U postgres"]
    interval: 5s
    retries: 10
```

**ingestion service:**
```yaml
ingestion:
  build: ./app
  depends_on:
    postgres: { condition: service_healthy }
  volumes:
    - ./data:/data
  environment:
    - PGHOST=postgres
    - PGDB=hospital
    - PGUSER=postgres
    - PGPASSWORD=postgres
    - CSV_DIR=/data
```

---

### Running Task 1

```bash
# 1. (optional) regenerate CSVs
python app/generate_csv.py

# 2. start postgres and wait for it to be healthy, then load data
docker compose up --build postgres ingestion

# 3. verify data in postgres
docker exec -it postgres psql -U postgres -d hospital
```

```sql
\dt
SELECT status, COUNT(*) FROM appointments GROUP BY status;
SELECT is_active, COUNT(*) FROM patients GROUP BY is_active;
SELECT is_abnormal, COUNT(*) FROM lab_results GROUP BY is_abnormal;
```

```bash
# check ingestion logs
docker logs ingestion

# clean reset (wipes postgres volume)
docker compose down -v
```

---

### Task 1 Deliverables Checklist ✓

- [x] `app/generate_csv.py` — generates 3 CSV files
- [x] `data/patients.csv` — 100 rows committed
- [x] `data/appointments.csv` — 500 rows committed
- [x] `data/lab_results.csv` — 200 rows committed
- [x] `app/load_data.py` — reads CSVs, creates tables dynamically, inserts + CDC updates
- [x] `app/Dockerfile`
- [x] `app/requirements.txt`
- [x] `docker-compose.yml` updated with `postgres` + `ingestion` services

---

## Task 2 — Debezium CDC

### How it works

Debezium runs as a Kafka Connect worker. On first start it takes a **snapshot** of the configured tables (sending all existing rows as `op: "r"` events), then streams all subsequent changes from PostgreSQL's WAL as `op: "u"` (update) or `op: "d"` (delete) events.

### PostgreSQL prerequisites (set in Task 1)

| Parameter | Value | Purpose |
|---|---|---|
| `wal_level` | `logical` | full row-level change data in WAL |
| `max_replication_slots` | `5` | Debezium occupies one slot |
| `max_wal_senders` | `5` | concurrent WAL streaming connections |

### Connector config — `debezium/hospital-connector.json`

| Key | Value |
|---|---|
| `plugin.name` | `pgoutput` (built into PostgreSQL 10+) |
| `snapshot.mode` | `initial` — snapshot existing rows on first connect |
| `table.include.list` | `public.patients, public.appointments, public.lab_results` |
| `key/value.converter` | `JsonConverter` with schemas disabled |

### Kafka topics created

| Topic | Messages |
|---|---|
| `hospital.public.patients` | 100 |
| `hospital.public.appointments` | 500 |
| `hospital.public.lab_results` | 200 |

### docker-compose.yml additions

**debezium service** — Kafka Connect worker, waits for postgres (healthy) + kafka (started):
```yaml
debezium:
  image: debezium/connect:2.6
  ports: ["8083:8083"]
  environment:
    BOOTSTRAP_SERVERS: kafka:9092
    KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
    VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
    KEY_CONVERTER_SCHEMAS_ENABLE: "false"
    VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
```

**debezium-init service** — registers connector once debezium is healthy and ingestion has completed:
```yaml
debezium-init:
  image: curlimages/curl:8.7.1
  depends_on:
    debezium: { condition: service_healthy }
    ingestion: { condition: service_completed_successfully }
  command: curl -X POST http://debezium:8083/connectors -d @/debezium/hospital-connector.json
```

### Running Task 2

```bash
docker compose up --build postgres ingestion kafka kafka-ui debezium debezium-init
```

Verify in Kafka UI at `localhost:8080` — three hospital topics populated with JSON messages.

To generate UPDATE events after setup:
```sql
UPDATE patients SET is_active = false WHERE patient_id IN (1, 2, 3);
UPDATE appointments SET status = 'completed' WHERE appointment_id IN (10, 20, 30);
UPDATE lab_results SET is_abnormal = NOT is_abnormal WHERE result_id IN (5, 15, 25);
```

```bash
# check connector status
curl http://localhost:8083/connectors/hospital-connector/status

# clean reset
docker compose down -v
```

### Task 2 Deliverables Checklist ✓

---

## Task 3 — Kafka & Schema Registry

### What was added

- **Schema Registry** (`confluentinc/cp-schema-registry:7.7.0`) — running on port `8085`, passive for now (plain JSON doesn't register schemas); will be used in Task 4+
- **Kafka external listener** — added `EXTERNAL://localhost:9094` so host-side scripts can reach Kafka. Internal services still use `kafka:9092`
- **`app/consumer.py`** — reads all 3 hospital topics from earliest offset, deserializes JSON, prints `op` type + key fields

### Kafka listener split

| Listener | Address | Used by |
|---|---|---|
| `PLAINTEXT` | `kafka:9092` | Debezium, Spark (inside Docker) |
| `EXTERNAL` | `localhost:9094` | consumer.py, local tooling (host) |

### Consumer output format

```
[patients       ] op=READ (snapshot)      | {'patient_id': 1, 'first_name': 'Danielle', ...}
[appointments   ] op=UPDATE               | {'appointment_id': 10, 'status': 'completed', ...}
```

### Running Task 3

```bash
# start full stack
docker compose up --build postgres ingestion kafka schema-registry kafka-ui debezium debezium-init

# run consumer (separate terminal)
python app/consumer.py

# trigger manual UPDATE events to test CDC
docker exec -it postgres psql -U postgres -d hospital
```

```sql
UPDATE patients SET is_active = false WHERE patient_id IN (1, 2, 3);
UPDATE appointments SET status = 'completed' WHERE appointment_id IN (10, 20, 30);
UPDATE lab_results SET is_abnormal = NOT is_abnormal WHERE result_id IN (5, 15, 25);
```

```bash
# verify Schema Registry is up
curl http://localhost:8085/subjects

# clean reset
docker compose down -v
```

### Task 3 Deliverables Checklist ✓

- [x] `docker-compose.yml` — Kafka cluster running (KRaft, dual listeners)
- [x] `docker-compose.yml` — Schema Registry running on port `8085`
- [x] JSON serialization configured via Debezium `JsonConverter`
- [x] `app/consumer.py` — reads and decodes JSON messages from all 3 topics

---

## Task 4 — Spark Structured Streaming

### What was added

**`spark/app/stream_job.py`** — three independent structured streams replacing the original stub:

| Stream | Topic | Transformation | Filter |
|---|---|---|---|
| `patients_adults` | `hospital.public.patients` | derive `age` from `date_of_birth` (days-since-epoch) | `age >= 18` |
| `appointments_completed` | `hospital.public.appointments` | — | `status = 'completed'` |
| `lab_results_abnormal` | `hospital.public.lab_results` | — | `is_abnormal = true` |

### Debezium JSON envelope

Each Kafka message has the structure:
```json
{ "op": "r", "before": null, "after": { ...row... }, "source": {...} }
```
Spark extracts `$.after` with `get_json_object`, parses it with `from_json` against a defined schema, then applies the filter.

### Date handling

`date_of_birth` arrives as **days since epoch** (INT32) from Debezium's PostgreSQL connector. Conversion to age:
```python
dob = date_add(lit("1970-01-01").cast("date"), col("date_of_birth"))
age = floor(datediff(current_date(), dob) / 365)
```

### docker-compose change

`spark-app` now depends on `debezium-init` (topics populated before Spark starts) and sleeps 20s for Spark master to stabilise.

### Running Task 4

```bash
docker compose up --build postgres ingestion kafka schema-registry kafka-ui debezium debezium-init spark-master spark-worker spark-app

# follow Spark output
docker logs -f spark-app
```

Expected output every 15 seconds — three console batches:
- `patients_adults` — ~85 rows with computed `age` column
- `appointments_completed` — completed appointments only
- `lab_results_abnormal` — out-of-range results only

```bash
# clean reset
docker compose down -v
```

### Task 4 Deliverables Checklist ✓

- [x] `docker-compose.yml` — Spark master, worker, app services present
- [x] `spark/app/stream_job.py` — reads JSON messages from all 3 Kafka topics
- [x] Transformations: age derivation (patients), status filter (appointments), abnormal filter (lab_results)

---

## Task 5 — Storing Processed Data in MinIO using Delta Lake ✓

### What was added

**MinIO** replaces the console sink — all three Spark streams now write Delta Lake files to an S3-compatible object store running inside Docker.

#### docker-compose.yml additions

| Service | Image | Purpose |
|---|---|---|
| `minio` | `minio/minio:latest` | S3-compatible object store, ports 9000 (API) + 9001 (console) |
| `minio-init` | `minio/mc:latest` | Creates `hospital` bucket on first boot |

`spark-app` extended:
- depends on `minio-init` (bucket exists before streaming starts)
- `--packages` extended with `io.delta:delta-spark_2.12:3.2.0`, `org.apache.hadoop:hadoop-aws:3.3.4`, `com.amazonaws:aws-java-sdk-bundle:1.12.262`

#### spark/app/stream_job.py changes

`build_spark()` now configures:
- Delta Lake extensions (`DeltaSparkSessionExtension`, `DeltaCatalog`)
- S3A / MinIO connector (endpoint, credentials, path-style access, `SimpleAWSCredentialsProvider`)

All three `writeStream` sinks changed from `format("console")` → `format("delta")`:

| Query | Output path | Checkpoint |
|---|---|---|
| `bronze_patients` | `s3a://hospital/bronze/patients` | `s3a://hospital/checkpoints/bronze/patients` |
| `bronze_appointments` | `s3a://hospital/bronze/appointments` | `s3a://hospital/checkpoints/bronze/appointments` |
| `bronze_lab_results` | `s3a://hospital/bronze/lab_results` | `s3a://hospital/checkpoints/bronze/lab_results` |
| `patients_adults` | `s3a://hospital/silver/patients` | `s3a://hospital/checkpoints/silver/patients` |
| `appointments_completed` | `s3a://hospital/silver/appointments` | `s3a://hospital/checkpoints/silver/appointments` |
| `lab_results_abnormal` | `s3a://hospital/silver/lab_results` | `s3a://hospital/checkpoints/silver/lab_results` |

### Running Task 5

```bash
docker compose up --build postgres ingestion kafka schema-registry kafka-ui debezium debezium-init minio minio-init spark-master spark-worker spark-app
```

Wait ~60 s for the pipeline to complete one or two micro-batches, then verify:

**Option A — MinIO console (browser)**
Open `http://localhost:9001` → log in with `minioadmin / minioadmin` → browse `hospital/` bucket.
Expected folders: `bronze/` and `silver/`, each containing `patients/`, `appointments/`, `lab_results/`.
Each table folder contains `.snappy.parquet` files + a `_delta_log/` directory with JSON transaction logs.
Silver files will be smaller than bronze (filtered data).

**Option B — Spark logs**
```bash
docker logs -f spark-app
```
Signs of a healthy run:
- 6x `ResolveWriteToStream` warnings at startup — all 6 streaming queries initialised
- `ProcessingTimeExecutor: Current batch is falling behind` — first batch ran (normal on initial snapshot load)
- No `StreamingQueryException` — no write failures to MinIO

Note: streaming progress metrics (`numOutputRows`) are not printed at the default log level. Use Option C to confirm data landed.

**Option C — MinIO CLI**
```bash
docker run --rm --network wdbd_demo-net --entrypoint /bin/sh minio/mc -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc ls -r local/hospital/"
```
Expected output: `.snappy.parquet` and `_delta_log/` entries under both `bronze/` and `silver/` for all three tables.

```bash
# clean reset
docker compose down -v
```

### Task 5 Deliverables Checklist ✓

- [x] `docker-compose.yml` — `minio` service running on ports 9000/9001
- [x] `docker-compose.yml` — `minio-init` creates `hospital` bucket automatically
- [x] `spark/app/stream_job.py` — SparkSession configured for Delta Lake + S3A/MinIO
- [x] All three streams write Delta format to `s3a://hospital/{table}`
- [x] Checkpoints stored at `s3a://hospital/checkpoints/{table}` for fault tolerance

---

## Task 6 — Automating Deployment & Ensuring Reliability

### What to add

Task 6 covers four areas identified from the project requirements and reference implementation comparison.

---

### A — Medallion storage layers in MinIO

Current flat paths (`s3a://hospital/patients`) hold filtered/transformed data that is effectively Silver. Restructure to make the layers explicit:

| Layer | Path | Content |
|---|---|---|
| Bronze | `s3a://hospital/bronze/{table}` | Raw Debezium CDC envelope as-is (op, before, after, source) — no filtering |
| Silver | `s3a://hospital/silver/{table}` | Filtered/transformed output (current logic: adults only, completed, abnormal) |

`stream_job.py` changes:
- Add a second `writeStream` per topic that writes the raw `$.after` JSON to Bronze before applying the filter.
- Move current filtered sinks to Silver paths.
- Update checkpoint paths accordingly (`s3a://hospital/checkpoints/bronze/{table}` and `s3a://hospital/checkpoints/silver/{table}`).
- Update `minio-init` bucket setup if needed.

---

### B — Docker Compose reliability

Healthchecks and proper dependency ordering so services never race-start.

| Service | Healthcheck command |
|---|---|
| `kafka` | `kafka-broker-api-versions --bootstrap-server localhost:9092` |
| `spark-master` | `curl -f http://localhost:8080` |

Dependency upgrades (`service_started` → `service_healthy`):

| Service | Dependency |
|---|---|
| `debezium` | `kafka: service_healthy` |
| `schema-registry` | `kafka: service_healthy` |
| `kafka-ui` | `kafka: service_healthy` |
| `spark-worker` | `spark-master: service_healthy` |
| `spark-app` | `spark-master: service_healthy` |

`restart: on-failure` added to all long-running services: `postgres`, `kafka`, `debezium`, `schema-registry`, `kafka-ui`, `minio`, `spark-master`, `spark-worker`, `spark-app`.

One-shot init services (`ingestion`, `debezium-init`, `minio-init`) get no restart policy — they run once and exit.

Remove `sleep 20` from spark-app startup command (replaced by healthcheck dependency).

---

### C — Standalone operations scripts


Three Python scripts in `scripts/`, runnable with `python scripts/<script>.py`:
Dependencies: `pip install -r scripts/requirements.txt`

#### `scripts/deploy.py`
One-command deployment with:
- Docker / Docker Compose pre-flight checks (`subprocess` + `docker info`)
- `docker compose up --build -d` via `subprocess`
- Per-service readiness polling using `socket.connect` (postgres 5432, kafka 9092, minio 9000, debezium 8083, spark-master 8081)
- Summary table of service URLs on success

#### `scripts/health_check.py`
Validates the running stack (14 checks):
- Container status via `docker compose ps` output parsed with `subprocess`
- Port reachability: postgres 5432, kafka 9092, minio 9000/9001, debezium 8083, schema-registry 8085, spark-master 8081
- Postgres: `psycopg2` connection + row counts on all three tables
- Kafka: topic list via `kafka-python-ng` confirms all three hospital topics exist
- Schema Registry: `/subjects` endpoint reachable
- Debezium: connector status via `requests` GET to `/connectors/hospital-connector/status`
- MinIO: liveness endpoint + Silver layer row counts per table via `boto3` + `pyarrow`
- PASS / FAIL output per check; non-zero `sys.exit` on any failure (CI/CD compatible)

#### `scripts/cleanup.py`
Safe teardown:
- `input()` confirmation prompt before destructive steps
- `docker compose down -v` via `subprocess`
- Optional: remove built images (`docker compose down --rmi local`)

---

### D — Data pipeline validation script

`app/validate_consistency.py` — runnable from host, validates end-to-end:

```
python app/validate_consistency.py            # full check
python app/validate_consistency.py --database # DB only
python app/validate_consistency.py --kafka    # Kafka only
python app/validate_consistency.py --minio    # MinIO only
```

Checks:
- **Database**: tables exist, row counts match expected (100 / 500 / 200), no unexpected NULLs in PKs
- **Kafka**: topics `hospital.public.patients/appointments/lab_results` exist, each has > 0 messages, messages are valid JSON with `op` field
- **MinIO**: Silver paths exist for all three tables, at least one `.parquet` file present in each, `_delta_log/` directory exists confirming Delta format

---

### E — Documentation

#### `DEPLOYMENT_GUIDE.md`
- Architecture overview (pipeline diagram)
- Prerequisites
- Step-by-step deployment with `deploy.py`
- Service URLs and credentials table
- Medallion layer explanation and how to query each layer
- Troubleshooting (common failure modes and fixes)

#### `QUICKSTART.md`
- 30-second setup (`python deploy.py`)
- Service URL cheat-sheet
- How to verify data in MinIO
- How to run the validation script

---

### Startup order (target)

```
postgres ──healthy──► ingestion ──completed──►┐
                                              debezium-init ──completed──►┐
kafka ──healthy──► debezium ──healthy──────────────────────────────────────┤
                                                                           spark-app
minio ──healthy──► minio-init ──completed──────────────────────────────────┤
                                                                           │
kafka ──healthy──► spark-master ──healthy──► spark-worker ─────────────────┘
```

---

### Task 6 Deliverables Checklist

- [x] `docker-compose.yml` — healthchecks on `kafka` and `spark-master`
- [x] `docker-compose.yml` — all `service_started` races eliminated
- [x] `docker-compose.yml` — `restart: on-failure` on all long-running services
- [x] `docker-compose.yml` — `sleep 20` hack removed from spark-app
- [x] `spark/app/stream_job.py` — Bronze sink added (raw CDC envelope, all three topics)
- [x] `spark/app/stream_job.py` — Silver paths updated to `s3a://hospital/silver/{table}`
- [x] `scripts/deploy.py` — one-command deployment with health polling and summary
- [x] `scripts/health_check.py` — full stack validation with CI-compatible exit codes
- [x] `scripts/cleanup.py` — safe teardown with confirmation
- [x] `scripts/requirements.txt` — host-side dependencies for ops scripts
- [x] `app/validate_consistency.py` — end-to-end pipeline validation (DB / Kafka / MinIO)
- [ ] `DEPLOYMENT_GUIDE.md` — operations guide with troubleshooting
- [ ] `QUICKSTART.md` — quick reference