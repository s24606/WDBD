"""
Generate synthetic hospital CSVs for the WDBD semester project.
Run once locally; commit the produced CSV files to the repo.

    pip install faker
    python app/generate_csv.py
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

try:
    from faker import Faker
except ImportError:
    raise SystemExit("Install dependency first:  pip install faker")

random.seed(42)
fake = Faker()
Faker.seed(42)

OUT_DIR = Path(__file__).parent.parent / "data"
NOW = datetime(2026, 6, 1)

# ── helpers ───────────────────────────────────────────────────────────────────

def rand_ts(days_back_min: int, days_back_max: int) -> str:
    delta = timedelta(
        days=random.randint(days_back_min, days_back_max),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return (NOW - delta).strftime("%Y-%m-%d %H:%M:%S")


def rand_ts_future(days_min: int, days_max: int) -> str:
    delta = timedelta(
        days=random.randint(days_min, days_max),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return (NOW + delta).strftime("%Y-%m-%d %H:%M:%S")


def rand_dob(age_min: int, age_max: int) -> str:
    age = random.randint(age_min, age_max)
    dob = NOW - timedelta(days=age * 365 + random.randint(0, 364))
    return dob.strftime("%Y-%m-%d")


# ── patients (100 rows) ───────────────────────────────────────────────────────

GENDERS = ["M"] * 50 + ["F"] * 50


def generate_patients(n: int = 100) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        # first 85 are adults (age 18-80), last 15 are minors (age 15-17)
        dob = rand_dob(18, 80) if i <= 85 else rand_dob(15, 17)
        rows.append({
            "patient_id":    i,
            "first_name":    fake.first_name(),
            "last_name":     fake.last_name(),
            "date_of_birth": dob,
            "gender":        random.choice(GENDERS),
            "email":         fake.email(),
            "phone":         f"{random.randint(100,999)} {random.randint(100,999)} {random.randint(100,999)}",
            "registered_at": rand_ts(365, 730),
            "is_active":     True,
        })
    return rows


# ── appointments (500 rows) ───────────────────────────────────────────────────

DEPARTMENTS = [
    "cardiology", "oncology", "neurology", "orthopedics",
    "general_practice", "pediatrics", "dermatology",
]

STATUS_WEIGHTS = (
    ["completed"] * 60 + ["scheduled"] * 25 + ["cancelled"] * 10 + ["no_show"] * 5
)


def generate_appointments(patients: list[dict], total: int = 500) -> list[dict]:
    patient_ids = [p["patient_id"] for p in patients]
    # base: 5 per patient; shuffle and trim to exact total
    pool = (patient_ids * 5)[:total]
    random.shuffle(pool)

    rows = []
    for i, pid in enumerate(pool, start=1):
        status = random.choice(STATUS_WEIGHTS)
        # scheduled appointments are in the future; others in the past
        scheduled_at = rand_ts_future(1, 60) if status == "scheduled" else rand_ts(1, 365)
        rows.append({
            "appointment_id": i,
            "patient_id":     pid,
            "doctor_name":    f"Dr. {fake.last_name()}",
            "department":     random.choice(DEPARTMENTS),
            "scheduled_at":   scheduled_at,
            "status":         status,
            "notes":          fake.sentence() if random.random() < 0.4 else "",
        })
    return rows


# ── lab_results (200 rows) ────────────────────────────────────────────────────

TESTS = [
    {"test_name": "glucose",     "unit": "mg/dL", "ref_min": 70.0,  "ref_max": 100.0, "lo": 40.0,  "hi": 220.0},
    {"test_name": "cholesterol", "unit": "mg/dL", "ref_min": 0.0,   "ref_max": 200.0, "lo": 90.0,  "hi": 360.0},
    {"test_name": "HbA1c",       "unit": "%",     "ref_min": 4.0,   "ref_max": 5.6,   "lo": 3.0,   "hi": 13.0},
    {"test_name": "creatinine",  "unit": "mg/dL", "ref_min": 0.7,   "ref_max": 1.3,   "lo": 0.3,   "hi": 6.0},
    {"test_name": "hemoglobin",  "unit": "g/dL",  "ref_min": 12.0,  "ref_max": 17.0,  "lo": 5.0,   "hi": 21.0},
    {"test_name": "TSH",         "unit": "mIU/L", "ref_min": 0.4,   "ref_max": 4.0,   "lo": 0.05,  "hi": 12.0},
    {"test_name": "ALT",         "unit": "U/L",   "ref_min": 7.0,   "ref_max": 56.0,  "lo": 3.0,   "hi": 250.0},
]


def generate_lab_results(appointments: list[dict], total: int = 200) -> list[dict]:
    completed = [a for a in appointments if a["status"] == "completed"]
    sampled = random.sample(completed, min(total, len(completed)))

    rows = []
    for i, appt in enumerate(sampled, start=1):
        t = random.choice(TESTS)
        value = round(random.uniform(t["lo"], t["hi"]), 2)
        is_abnormal = not (t["ref_min"] <= value <= t["ref_max"])

        appt_dt = datetime.strptime(appt["scheduled_at"], "%Y-%m-%d %H:%M:%S")
        recorded_at = (appt_dt + timedelta(hours=random.randint(1, 48))).strftime("%Y-%m-%d %H:%M:%S")

        rows.append({
            "result_id":      i,
            "patient_id":     appt["patient_id"],
            "appointment_id": appt["appointment_id"],
            "test_name":      t["test_name"],
            "result_value":   value,
            "unit":           t["unit"],
            "reference_min":  t["ref_min"],
            "reference_max":  t["ref_max"],
            "is_abnormal":    is_abnormal,
            "recorded_at":    recorded_at,
        })
    return rows


# ── write & summarise ─────────────────────────────────────────────────────────

def write_csv(rows: list[dict], filename: str) -> None:
    path = OUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  wrote {filename:25s} ({len(rows)} rows)")


if __name__ == "__main__":
    print("Generating hospital CSVs ...\n")

    patients     = generate_patients(100)
    appointments = generate_appointments(patients, 500)
    lab_results  = generate_lab_results(appointments, 200)

    write_csv(patients,     "patients.csv")
    write_csv(appointments, "appointments.csv")
    write_csv(lab_results,  "lab_results.csv")

    # ── sanity stats ──────────────────────────────────────────────────────────
    cutoff = (NOW - timedelta(days=18 * 365)).strftime("%Y-%m-%d")
    adults   = sum(1 for p in patients     if p["date_of_birth"] <= cutoff)
    statuses = {}
    for a in appointments:
        statuses[a["status"]] = statuses.get(a["status"], 0) + 1
    abnormal = sum(1 for r in lab_results if r["is_abnormal"])

    print(f"\nSummary:")
    print(f"  patients     : {len(patients)} total  |  {adults} adults  |  {len(patients)-adults} minors")
    print(f"  appointments : {len(appointments)} total  |  " +
          "  ".join(f"{k}: {v}" for k, v in sorted(statuses.items())))
    print(f"  lab_results  : {len(lab_results)} total  |  {abnormal} abnormal  "
          f"({abnormal / len(lab_results) * 100:.0f}%)")
    print("\nDone. Commit patients.csv, appointments.csv, lab_results.csv.")