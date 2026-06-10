"""
Inserts one new row into a hospital table.

Usage:
    python postgresql_insert.py               # random table
    python postgresql_insert.py --patient
    python postgresql_insert.py --appointment
    python postgresql_insert.py --lab_result

Connection env vars (defaults match docker-compose.yml):
    PGHOST, PGPORT, PGDB, PGUSER, PGPASSWORD
"""
import argparse
import os
import random
from datetime import datetime, timedelta, date

import psycopg2

PG = dict(
    host=os.getenv("PGHOST", "localhost"),
    port=int(os.getenv("PGPORT", 5432)),
    dbname=os.getenv("PGDB", "hospital"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "postgres"),
)

# ── sample data pools ─────────────────────────────────────────────────────────

FIRST_NAMES = ["Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace", "Hank"]
LAST_NAMES  = ["Smith", "Brown", "Wilson", "Taylor", "Anderson", "Thomas", "Jackson"]
GENDERS     = ["M", "F"]
DOMAINS     = ["example.com", "mail.org", "test.net"]

DOCTORS      = ["Dr. Adams", "Dr. Lee", "Dr. Patel", "Dr. Torres", "Dr. White"]
DEPARTMENTS  = ["cardiology", "neurology", "orthopedics", "pediatrics", "oncology",
                "dermatology", "radiology"]
STATUSES     = ["scheduled", "completed", "cancelled"]
NOTES        = [
    "Follow-up required.",
    "Patient responded well.",
    "Referred to specialist.",
    "Routine check-up.",
    "",
]

LAB_TESTS = {
    # test_name: (unit, ref_min, ref_max)
    "glucose":     ("mg/dL",  70.0,  100.0),
    "ALT":         ("U/L",     7.0,   56.0),
    "creatinine":  ("mg/dL",   0.7,    1.3),
    "cholesterol": ("mg/dL",   0.0,  200.0),
    "TSH":         ("mIU/L",   0.4,    4.0),
    "hemoglobin":  ("g/dL",   12.0,   17.5),
}

# ── helpers ───────────────────────────────────────────────────────────────────

def _rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _rand_ts(start: datetime, end: datetime) -> datetime:
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta))


def _next_id(cur, table: str, pk: str) -> int:
    cur.execute(f"SELECT COALESCE(MAX({pk}), 0) + 1 FROM {table}")
    return cur.fetchone()[0]

# ── insert functions ──────────────────────────────────────────────────────────

def insert_patient(cur) -> dict:
    new_id     = _next_id(cur, "patients", "patient_id")
    first_name = random.choice(FIRST_NAMES)
    last_name  = random.choice(LAST_NAMES)
    dob        = _rand_date(date(1950, 1, 1), date(2005, 12, 31))
    gender     = random.choice(GENDERS)
    email      = f"{first_name.lower()}.{last_name.lower()}{new_id}@{random.choice(DOMAINS)}"
    phone      = f"{random.randint(100,999)} {random.randint(100,999)} {random.randint(100,999)}"
    reg_at     = _rand_ts(datetime(2023, 1, 1), datetime(2026, 6, 10))
    is_active  = True

    cur.execute(
        """
        INSERT INTO patients
            (patient_id, first_name, last_name, date_of_birth,
             gender, email, phone, registered_at, is_active)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (new_id, first_name, last_name, dob,
         gender, email, phone, reg_at, is_active),
    )
    return {
        "table": "patients",
        "patient_id": new_id,
        "name": f"{first_name} {last_name}",
        "dob": dob.isoformat(),
        "gender": gender,
        "email": email,
        "registered_at": reg_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


def insert_appointment(cur) -> dict:
    new_id = _next_id(cur, "appointments", "appointment_id")

    cur.execute("SELECT patient_id FROM patients ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("No patients in DB — run load_data.py first.")
    patient_id = row[0]

    doctor     = random.choice(DOCTORS)
    department = random.choice(DEPARTMENTS)
    sched_at   = _rand_ts(datetime(2025, 1, 1), datetime(2027, 1, 1))
    status     = random.choice(STATUSES)
    notes      = random.choice(NOTES)

    cur.execute(
        """
        INSERT INTO appointments
            (appointment_id, patient_id, doctor_name, department,
             scheduled_at, status, notes)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (new_id, patient_id, doctor, department, sched_at, status, notes),
    )
    return {
        "table": "appointments",
        "appointment_id": new_id,
        "patient_id": patient_id,
        "doctor": doctor,
        "department": department,
        "scheduled_at": sched_at.strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
    }


def insert_lab_result(cur) -> dict:
    new_id = _next_id(cur, "lab_results", "result_id")

    cur.execute(
        """
        SELECT a.patient_id, a.appointment_id
        FROM appointments a
        ORDER BY random() LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("No appointments in DB — run load_data.py first.")
    patient_id, appointment_id = row

    test_name, (unit, ref_min, ref_max) = random.choice(list(LAB_TESTS.items()))
    spread       = (ref_max - ref_min) * 0.5
    result_value = round(random.uniform(ref_min - spread, ref_max + spread), 2)
    is_abnormal  = not (ref_min <= result_value <= ref_max)
    recorded_at  = _rand_ts(datetime(2025, 1, 1), datetime(2026, 6, 10))

    cur.execute(
        """
        INSERT INTO lab_results
            (result_id, patient_id, appointment_id, test_name, result_value,
             unit, reference_min, reference_max, is_abnormal, recorded_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (new_id, patient_id, appointment_id, test_name, result_value,
         unit, ref_min, ref_max, is_abnormal, recorded_at),
    )
    return {
        "table": "lab_results",
        "result_id": new_id,
        "patient_id": patient_id,
        "appointment_id": appointment_id,
        "test": test_name,
        "value": f"{result_value} {unit}  (ref {ref_min}–{ref_max})",
        "is_abnormal": is_abnormal,
        "recorded_at": recorded_at.strftime("%Y-%m-%d %H:%M:%S"),
    }

# ── main ──────────────────────────────────────────────────────────────────────

INSERTERS = {
    "patient":     insert_patient,
    "appointment": insert_appointment,
    "lab_result":  insert_lab_result,
}


def parse_args() -> str | None:
    parser = argparse.ArgumentParser(description="Insert a row into a hospital table.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--patient",     action="store_true", help="Insert into patients")
    group.add_argument("--appointment", action="store_true", help="Insert into appointments")
    group.add_argument("--lab_result",  action="store_true", help="Insert into lab_results")
    args = parser.parse_args()

    if args.patient:
        return "patient"
    if args.appointment:
        return "appointment"
    if args.lab_result:
        return "lab_result"
    return None


def main() -> None:
    table_key = parse_args()
    inserter  = INSERTERS[table_key] if table_key else random.choice(list(INSERTERS.values()))

    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            result = inserter(cur)
        conn.commit()
        print(f"Inserted into '{result['table']}':")
        for k, v in result.items():
            if k != "table":
                print(f"  {k}: {v}")
    except Exception as exc:
        conn.rollback()
        print(f"ERROR: {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
