# Hospital Mini Data Platform

A containerised end-to-end data pipeline that simulates a hospital business process and moves data from a transactional database through a streaming layer into a Delta Lake object store.

```
PostgreSQL → Debezium (CDC) → Kafka → Spark Structured Streaming → MinIO (Delta Lake)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Source database | PostgreSQL 15 |
| Change data capture | Debezium 2.6 |
| Message broker | Apache Kafka 7.7 (KRaft, no ZooKeeper) |
| Schema registry | Confluent Schema Registry |
| Stream processing | Apache Spark 3.5.1 |
| Object storage | MinIO (S3-compatible) |
| Storage format | Delta Lake 3.2 |
| Orchestration | Docker Compose |

## Repository Structure

```
WDBD/
├── app/
│   ├── load_data.py              # loads CSVs into PostgreSQL, simulates CDC events
│   ├── generate_csv.py           # generates synthetic hospital CSVs
│   ├── postgresql_insert.py      # inserts a single row into a chosen table (manual CDC trigger)
│   ├── consumer.py               # Kafka consumer for reading CDC topics
│   ├── validate_consistency.py   # end-to-end pipeline validation
│   ├── Dockerfile
│   └── requirements.txt
├── data/
│   ├── patients.csv              # 100 rows
│   ├── appointments.csv          # 500 rows
│   └── lab_results.csv           # 200 rows
├── spark/app/
│   └── stream_job.py             # Spark Structured Streaming job (bronze + silver)
├── debezium/
│   └── hospital-connector.json   # Debezium PostgreSQL connector config
├── scripts/
│   ├── deploy.py                 # one-command deployment
│   ├── health_check.py           # 14-point stack validation
│   ├── cleanup.py                # safe teardown
│   └── requirements.txt
├── docs/
│   ├── ProjectDescription.pdf
│   ├── 0_Architecture.pptx
│   ├── 1_KafkaTutorial.pdf
│   ├── 2_kafka_spark_tutorial.pdf
│   ├── 3_DebeziumTutorial.pdf
│   └── 4_spark_starschema_eng.pdf
├── docker-compose.yml
├── PLAN.md
├── README.md
├── QUICKSTART.md
└── DEPLOYMENT_GUIDE.md
```

## Prerequisites

- Docker Desktop 4.x or later
- Python 3.10+
- `pip install -r scripts/requirements.txt`

## Get Started

```bash
python scripts/deploy.py
```

See [QUICKSTART.md](QUICKSTART.md) for service URLs and quick verification commands.  
See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for full deployment and operations reference.