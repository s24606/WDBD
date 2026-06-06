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
### Task 2 — Connecting Debezium to Capture Changes in PostgreSQL ← current
### Task 3 — Kafka Setup & Streaming Events in JSON Format
### Task 4 — Integrating Spark with Kafka for Data Processing
### Task 5 — Storing Processed Data in MinIO using Delta Lake
### Task 6 — Automating Deployment & Ensuring Reliability

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
│   ├── load_data.py             # main deliverable
│   ├── Dockerfile
│   └── requirements.txt         # psycopg2-binary, pandas
├── data/
│   ├── patients.csv             # 100 rows (committed)
│   ├── appointments.csv         # 500 rows (committed)
│   └── lab_results.csv          # 200 rows (committed)
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

### Task 1 Deliverables Checklist

- [x] `app/generate_csv.py` — generates 3 CSV files
- [x] `data/patients.csv` — 100 rows committed
- [x] `data/appointments.csv` — 500 rows committed
- [x] `data/lab_results.csv` — 200 rows committed
- [x] `app/load_data.py` — reads CSVs, creates tables dynamically, inserts + CDC updates
- [x] `app/Dockerfile`
- [x] `app/requirements.txt`
- [x] `docker-compose.yml` updated with `postgres` + `ingestion` services