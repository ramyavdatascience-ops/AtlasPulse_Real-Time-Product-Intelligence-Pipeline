import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    BooleanType, IntegerType, TimestampType
)
from dotenv import load_dotenv

load_dotenv()

# ── S3 paths (your Delta Lake on AWS) ──────────────────────────
S3_BUCKET       = os.getenv("S3_BUCKET")
BASE_PATH       = f"s3a://{S3_BUCKET}"
BRONZE_JIRA     = f"{BASE_PATH}/bronze/jira_events"
BRONZE_CONF     = f"{BASE_PATH}/bronze/confluence_events"
CHECKPOINT_JIRA = f"{BASE_PATH}/checkpoints/bronze_jira"
CHECKPOINT_CONF = f"{BASE_PATH}/checkpoints/bronze_confluence"

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
AWS_ACCESS_KEY  = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY  = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION      = os.getenv("AWS_REGION", "eu-north-1")

# ── create Spark session with Delta Lake + S3 support ─────────
spark = SparkSession.builder \
    .appName("AtlasPulse-Bronze") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session started")
print(f"✅ Writing to S3 bucket: {S3_BUCKET}\n")

# ── schema for Jira events ────────────────────────────────────
jira_schema = StructType([
    StructField("event_id",         StringType(),  True),
    StructField("event_type",       StringType(),  True),
    StructField("event_ts",         StringType(),  True),
    StructField("team_id",          StringType(),  True),
    StructField("team_name",        StringType(),  True),
    StructField("user_id",          StringType(),  True),
    StructField("user_email",       StringType(),  True),
    StructField("project_key",      StringType(),  True),
    StructField("issue_id",         StringType(),  True),
    StructField("priority",         StringType(),  True),
    StructField("response_time_ms", IntegerType(), True),
    StructField("ai_assist_used",   BooleanType(), True),
    StructField("source_app",       StringType(),  True),
])

# ── schema for Confluence events ──────────────────────────────
confluence_schema = StructType([
    StructField("event_id",         StringType(),  True),
    StructField("event_type",       StringType(),  True),
    StructField("event_ts",         StringType(),  True),
    StructField("team_id",          StringType(),  True),
    StructField("team_name",        StringType(),  True),
    StructField("user_id",          StringType(),  True),
    StructField("user_email",       StringType(),  True),
    StructField("space_key",        StringType(),  True),
    StructField("page_id",          StringType(),  True),
    StructField("page_title",       StringType(),  True),
    StructField("response_time_ms", IntegerType(), True),
    StructField("ai_assist_used",   BooleanType(), True),
    StructField("source_app",       StringType(),  True),
])

# ── read Jira events from Kafka ───────────────────────────────
jira_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", "jira-events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# ── parse JSON payload ────────────────────────────────────────
jira_parsed = jira_raw \
    .select(from_json(
        col("value").cast("string"), jira_schema
    ).alias("data")) \
    .select("data.*") \
    .withColumn("ingested_at", current_timestamp())

# ── read Confluence events from Kafka ─────────────────────────
confluence_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", "confluence-events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

confluence_parsed = confluence_raw \
    .select(from_json(
        col("value").cast("string"), confluence_schema
    ).alias("data")) \
    .select("data.*") \
    .withColumn("ingested_at", current_timestamp())

# ── write Jira Bronze to Delta Lake on S3 ──────────────────────
print("🚀 Starting Bronze writer — Jira events...")
jira_query = jira_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_JIRA) \
    .start(BRONZE_JIRA)

# ── write Confluence Bronze to Delta Lake on S3 ────────────────
print("🚀 Starting Bronze writer — Confluence events...")
confluence_query = confluence_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_CONF) \
    .start(BRONZE_CONF)

print("\n✅ Bronze layer streaming — writing to Delta Lake on S3")
print(f"   Jira       → {BRONZE_JIRA}")
print(f"   Confluence → {BRONZE_CONF}")
print("\n   Press Ctrl+C to stop\n")

# ── keep running until stopped ────────────────────────────────
try:
    jira_query.awaitTermination()
except KeyboardInterrupt:
    print("\n⛔ Stopping Bronze writers...")
    jira_query.stop()
    confluence_query.stop()
    spark.stop()
    print("✅ Done.")

