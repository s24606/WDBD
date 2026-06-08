"""
Spark Structured Streaming job — reads CDC events from Kafka,
deserializes Debezium JSON payloads, and writes to MinIO in Delta format
using a two-layer medallion architecture:

  Bronze  s3a://hospital/bronze/{table}   all CDC rows, parsed, no filtering
  Silver  s3a://hospital/silver/{table}   filtered by hospital business rules:
    patients     → age >= 18  (derived from date_of_birth)
    appointments → status = 'completed'
    lab_results  → is_abnormal = true
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
MINIO_ENDPOINT  = "http://minio:9000"
MINIO_USER      = "minioadmin"
MINIO_PASSWORD  = "minioadmin"
BUCKET          = "s3a://hospital"
BRONZE          = f"{BUCKET}/bronze"
SILVER          = f"{BUCKET}/silver"

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

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HospitalCDCStream")
        # Delta Lake
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


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

# ── Bronze streams (all CDC rows, no business-rule filter) ────────────────────

def bronze_patients_stream(spark):
    return parse_debezium(read_topic(spark, "hospital.public.patients"), PATIENT_SCHEMA)


def bronze_appointments_stream(spark):
    return parse_debezium(read_topic(spark, "hospital.public.appointments"), APPOINTMENT_SCHEMA)


def bronze_lab_results_stream(spark):
    return parse_debezium(read_topic(spark, "hospital.public.lab_results"), LAB_RESULT_SCHEMA)

# ── Silver streams (filtered by hospital business rules) ──────────────────────

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
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    queries = [
        # ── Bronze: all CDC rows, no filtering ───────────────────────────────
        bronze_patients_stream(spark)
        .writeStream
        .queryName("bronze_patients")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{BUCKET}/checkpoints/bronze/patients")
        .trigger(processingTime="15 seconds")
        .start(f"{BRONZE}/patients"),

        bronze_appointments_stream(spark)
        .writeStream
        .queryName("bronze_appointments")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{BUCKET}/checkpoints/bronze/appointments")
        .trigger(processingTime="15 seconds")
        .start(f"{BRONZE}/appointments"),

        bronze_lab_results_stream(spark)
        .writeStream
        .queryName("bronze_lab_results")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{BUCKET}/checkpoints/bronze/lab_results")
        .trigger(processingTime="15 seconds")
        .start(f"{BRONZE}/lab_results"),

        # ── Silver: business-rule filtered ───────────────────────────────────
        patients_stream(spark)
        .writeStream
        .queryName("patients_adults")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{BUCKET}/checkpoints/silver/patients")
        .trigger(processingTime="15 seconds")
        .start(f"{SILVER}/patients"),

        appointments_stream(spark)
        .writeStream
        .queryName("appointments_completed")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{BUCKET}/checkpoints/silver/appointments")
        .trigger(processingTime="15 seconds")
        .start(f"{SILVER}/appointments"),

        lab_results_stream(spark)
        .writeStream
        .queryName("lab_results_abnormal")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{BUCKET}/checkpoints/silver/lab_results")
        .trigger(processingTime="15 seconds")
        .start(f"{SILVER}/lab_results"),
    ]

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()