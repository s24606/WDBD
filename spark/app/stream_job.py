from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               StringType, DoubleType, TimestampType)

# 1) Create Spark session
spark = (
    SparkSession.builder
        .appName("KafkaToSparkStreaming")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2) Define schema for the JSON inside Kafka "value"
event_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("ts", StringType(), True),       # parse later or keep as string
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("region", StringType(), True),
])

# 3) Read stream from Kafka
df_raw = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "events")
        .option("startingOffsets", "earliest")
        .load()
)

# 4) Parse the Kafka "value" (bytes) as JSON to columns
df_parsed = (
    df_raw
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_timestamp")
        .select(
            from_json(col("json_str"), event_schema).alias("e"),
            col("kafka_timestamp")
        )
    .select(
        col("e.id").alias("id"),
        col("e.ts").alias("ts"),            # ISO string
        col("e.type").alias("type"),
        col("e.amount").alias("amount"),
        col("e.region").alias("region"),
        col("kafka_timestamp")
    )
)

# 5) Write to console (debug) and to parquet (data lake-ish)
# Console sink (nice for learning)
query_console = (
    df_parsed.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .start()
)

# Parquet sink with checkpointing
query_parquet = (
    df_parsed.writeStream
        .format("parquet")
        .option("path", "/opt/spark/output/parquet/events")
        .option("checkpointLocation", "/opt/spark/output/checkpoints/events_to_parquet")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
)

# 6) Wait for termination (until container is stopped)
query_console.awaitTermination()
query_parquet.awaitTermination()