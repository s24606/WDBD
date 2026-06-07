"""
Spark Structured Streaming job — reads CDC events from Kafka,
deserializes Debezium JSON payloads, applies hospital business rules,
and writes filtered results to console.

Business rules:
  patients     → filter age >= 18  (derive age from date_of_birth)
  appointments → filter status = 'completed'
  lab_results  → filter is_abnormal = true
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, get_json_object,
    floor, datediff, current_date, date_add, lit,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, FloatType, BooleanType,
)

KAFKA_BOOTSTRAP = "kafka:9092"

# ── Debezium `after` field schemas ────────────────────────────────────────────
# Debezium + PostgreSQL + JsonConverter serializes:
#   DATE      → INT32  (days since 1970-01-01, can be negative)
#   TIMESTAMP → INT64  (microseconds since epoch)

PATIENT_SCHEMA = StructType([
    StructField("patient_id",    IntegerType()),
    StructField("first_name",    StringType()),
    StructField("last_name",     StringType()),
    StructField("date_of_birth", IntegerType()),
    StructField("gender",        StringType()),
    StructField("email",         StringType()),
    StructField("phone",         StringType()),
    StructField("registered_at", LongType()),
    StructField("is_active",     BooleanType()),
])

APPOINTMENT_SCHEMA = StructType([
    StructField("appointment_id", IntegerType()),
    StructField("patient_id",     IntegerType()),
    StructField("doctor_name",    StringType()),
    StructField("department",     StringType()),
    StructField("scheduled_at",   LongType()),
    StructField("status",         StringType()),
    StructField("notes",          StringType()),
])

LAB_RESULT_SCHEMA = StructType([
    StructField("result_id",      IntegerType()),
    StructField("patient_id",     IntegerType()),
    StructField("appointment_id", IntegerType()),
    StructField("test_name",      StringType()),
    StructField("result_value",   FloatType()),
    StructField("unit",           StringType()),
    StructField("reference_min",  FloatType()),
    StructField("reference_max",  FloatType()),
    StructField("is_abnormal",    BooleanType()),
    StructField("recorded_at",    LongType()),
])

# ── helpers ───────────────────────────────────────────────────────────────────

def read_topic(spark, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw")
    )


def parse_debezium(df, schema):
    """Extract Debezium `after` payload; drop DELETE events (after = null)."""
    return (
        df
        .withColumn("op",    get_json_object(col("raw"), "$.op"))
        .withColumn("after", get_json_object(col("raw"), "$.after"))
        .filter(col("after").isNotNull())
        .withColumn("data",  from_json(col("after"), schema))
        .select("op", "data.*")
    )

# ── per-table streams ─────────────────────────────────────────────────────────

def patients_stream(spark):
    df = parse_debezium(read_topic(spark, "hospital.public.patients"), PATIENT_SCHEMA)
    epoch = lit("1970-01-01").cast("date")
    dob   = date_add(epoch, col("date_of_birth"))
    return (
        df
        .withColumn("age", floor(datediff(current_date(), dob) / 365))
        .filter(col("age") >= 18)
        .select("patient_id", "first_name", "last_name", "age", "gender", "is_active")
    )


def appointments_stream(spark):
    df = parse_debezium(read_topic(spark, "hospital.public.appointments"), APPOINTMENT_SCHEMA)
    return (
        df
        .filter(col("status") == "completed")
        .select("appointment_id", "patient_id", "department", "doctor_name", "status")
    )


def lab_results_stream(spark):
    df = parse_debezium(read_topic(spark, "hospital.public.lab_results"), LAB_RESULT_SCHEMA)
    return (
        df
        .filter(col("is_abnormal") == True)
        .select("result_id", "patient_id", "test_name", "result_value", "unit",
                "reference_min", "reference_max")
    )

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    spark = (
        SparkSession.builder
        .appName("HospitalCDCStream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    queries = [
        patients_stream(spark)
        .writeStream
        .queryName("patients_adults")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", "/opt/spark/output/checkpoints/patients")
        .trigger(processingTime="15 seconds")
        .start(),

        appointments_stream(spark)
        .writeStream
        .queryName("appointments_completed")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", "/opt/spark/output/checkpoints/appointments")
        .trigger(processingTime="15 seconds")
        .start(),

        lab_results_stream(spark)
        .writeStream
        .queryName("lab_results_abnormal")
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", "/opt/spark/output/checkpoints/lab_results")
        .trigger(processingTime="15 seconds")
        .start(),
    ]

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()