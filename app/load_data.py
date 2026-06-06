"""
Reads patients.csv, appointments.csv, lab_results.csv,
creates hospital tables in PostgreSQL dynamically,
bulk-inserts all rows, then simulates CDC events (UPDATEs)
so Debezium captures a realistic mix of INSERTs and UPDATEs.

Environment variables (all have defaults for local dev):
    PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD, CSV_DIR
"""
import os
import time

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ── connection config ─────────────────────────────────────────────────────────

PG = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", 5432)),
    dbname=os.getenv("PGDB", "hospital"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "postgres"),
)
CSV_DIR = os.getenv("CSV_DIR", "data")

# ── table schema metadata ─────────────────────────────────────────────────────
# Insertion order matters — parents before children (FK constraints).

TABLES = [
    (
        "patients",
        {
            "pk": "patient_id",
            "fks": [],
            "type_overrides": {
                "date_of_birth": "DATE",
                "registered_at": "TIMESTAMP",
            },
            "bool_cols": ["is_active"],
        },
    ),
    (
        "appointments",
        {
            "pk": "appointment_id",
            "fks": [("patient_id", "patients", "patient_id")],
            "type_overrides": {"scheduled_at": "TIMESTAMP"},
            "bool_cols": [],
        },
    ),
    (
        "lab_results",
        {
            "pk": "result_id",
            "fks": [
                ("patient_id",     "patients",     "patient_id"),
                ("appointment_id", "appointments", "appointment_id"),
            ],
            "type_overrides": {"recorded_at": "TIMESTAMP"},
            "bool_cols": ["is_abnormal"],
        },
    ),
]

# pandas dtype → PostgreSQL type
DTYPE_MAP = {
    "int64":   "INTEGER",
    "int32":   "INTEGER",
    "float64": "FLOAT",
    "float32": "FLOAT",
    "bool":    "BOOLEAN",
    "object":  "TEXT",
}

# ── helpers ───────────────────────────────────────────────────────────────────

def wait_for_postgres(max_retries: int = 30, delay: int = 2):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**PG)
            print(f"  connected to PostgreSQL (attempt {attempt})")
            return conn
        except psycopg2.OperationalError as exc:
            print(f"  [{attempt}/{max_retries}] postgres not ready: {exc}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL after retries.")


def _pg_type(col: str, dtype, meta: dict) -> str:
    if col in meta.get("type_overrides", {}):
        return meta["type_overrides"][col]
    return DTYPE_MAP.get(str(dtype), "TEXT")


def create_table(cur, table: str, df: pd.DataFrame, meta: dict) -> None:
    col_defs = []
    for col, dtype in df.dtypes.items():
        pg_t = _pg_type(col, dtype, meta)
        pk_suffix = " PRIMARY KEY" if col == meta["pk"] else ""
        col_defs.append(f"  {col} {pg_t}{pk_suffix}")

    for fk_col, ref_table, ref_col in meta.get("fks", []):
        col_defs.append(
            f"  CONSTRAINT fk_{table}_{fk_col}"
            f" FOREIGN KEY ({fk_col}) REFERENCES {ref_table}({ref_col})"
        )

    ddl = f"CREATE TABLE IF NOT EXISTS {table} (\n" + ",\n".join(col_defs) + "\n);"
    cur.execute(ddl)
    print(f"  created table '{table}'")


def bulk_insert(cur, table: str, df: pd.DataFrame) -> None:
    cols = list(df.columns)
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    execute_values(
        cur,
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s",
        rows,
    )
    print(f"  inserted {len(rows):>4} rows → '{table}'")


# ── CDC simulation ────────────────────────────────────────────────────────────

def simulate_cdc(conn) -> None:
    print("\n--- CDC simulation (UPDATEs) ---")
    with conn.cursor() as cur:

        cur.execute("""
            UPDATE appointments SET status = 'completed'
            WHERE appointment_id IN (
                SELECT appointment_id FROM appointments
                WHERE status = 'scheduled'
                ORDER BY random() LIMIT 20
            )
        """)
        print(f"  appointments → completed : {cur.rowcount} rows")

        cur.execute("""
            UPDATE appointments SET status = 'cancelled'
            WHERE appointment_id IN (
                SELECT appointment_id FROM appointments
                WHERE status = 'scheduled'
                ORDER BY random() LIMIT 5
            )
        """)
        print(f"  appointments → cancelled : {cur.rowcount} rows")

        cur.execute("""
            UPDATE lab_results SET is_abnormal = NOT is_abnormal
            WHERE result_id IN (
                SELECT result_id FROM lab_results ORDER BY random() LIMIT 10
            )
        """)
        print(f"  lab_results corrected    : {cur.rowcount} rows")

        cur.execute("""
            UPDATE patients SET is_active = false
            WHERE patient_id IN (
                SELECT patient_id FROM patients
                WHERE is_active = true
                ORDER BY random() LIMIT 5
            )
        """)
        print(f"  patients deactivated     : {cur.rowcount} rows")

    conn.commit()


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=== Hospital data loader ===\n")

    conn = wait_for_postgres()

    # 1 — read CSVs and fix boolean columns (CSV stores 'True'/'False' strings)
    print("\n--- Reading CSVs ---")
    dataframes: dict[str, pd.DataFrame] = {}
    for table, meta in TABLES:
        df = pd.read_csv(f"{CSV_DIR}/{table}.csv")
        for col in meta.get("bool_cols", []):
            df[col] = df[col].map({"True": True, "False": False, True: True, False: False})
        dataframes[table] = df
        print(f"  {table}.csv : {len(df)} rows")

    # 2 — create tables dynamically from CSV column names + dtype mapping
    print("\n--- Creating tables ---")
    with conn.cursor() as cur:
        for table, meta in TABLES:
            create_table(cur, table, dataframes[table], meta)
    conn.commit()

    # 3 — truncate for idempotency, then bulk insert (parents first)
    print("\n--- Inserting data ---")
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE patients, appointments, lab_results RESTART IDENTITY CASCADE"
        )
        for table, meta in TABLES:
            bulk_insert(cur, table, dataframes[table])
    conn.commit()

    # 4 — pause, then simulate realistic CDC updates for Debezium
    print("\nWaiting 5 s before CDC updates ...")
    time.sleep(5)
    simulate_cdc(conn)

    conn.close()
    print("\n=== Done ===")


if __name__ == "__main__":
    main()