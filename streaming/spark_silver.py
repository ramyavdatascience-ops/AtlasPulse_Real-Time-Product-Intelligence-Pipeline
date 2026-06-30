import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, length,
    current_timestamp, row_number,
    session_window, min as spark_min, max as spark_max,
    count, unix_timestamp
)
from pyspark.sql.window import Window
from dotenv import load_dotenv

load_dotenv()

# ── S3 paths ───────────────────────────────────────────────────
S3_BUCKET        = os.getenv("S3_BUCKET")
BASE_PATH        = f"s3a://{S3_BUCKET}"

BRONZE_JIRA      = f"{BASE_PATH}/bronze/jira_events"
BRONZE_CONF      = f"{BASE_PATH}/bronze/confluence_events"

SILVER_JIRA      = f"{BASE_PATH}/silver/jira_events_clean"
SILVER_CONF      = f"{BASE_PATH}/silver/confluence_events_clean"

QUARANTINE_JIRA  = f"{BASE_PATH}/quarantine/jira_events"
QUARANTINE_CONF  = f"{BASE_PATH}/quarantine/confluence_events"

CHECKPOINT_JIRA  = f"{BASE_PATH}/checkpoints/silver_jira"
CHECKPOINT_CONF  = f"{BASE_PATH}/checkpoints/silver_confluence"

AWS_ACCESS_KEY   = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY   = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION       = os.getenv("AWS_REGION", "eu-north-1")

# ── Spark session ──────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("AtlasPulse-Silver") \
    .master("local[*]") \
    .config("spark.jars.packages",
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
    .config("spark.hadoop.fs.s3a.endpoint",
            f"s3.{AWS_REGION}.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session started")
print(f"✅ Reading Bronze from S3: {S3_BUCKET}\n")

# ══════════════════════════════════════════════════════════════
# JIRA SILVER TRANSFORM
# ══════════════════════════════════════════════════════════════

jira_bronze = spark.read \
    .format("delta") \
    .load(BRONZE_JIRA)

print(f"📥 Bronze Jira rows read: {jira_bronze.count()}")

# ── step 1: cast event_ts string → proper timestamp ────────────
jira_typed = jira_bronze \
    .withColumn("event_ts",
        to_timestamp(col("event_ts"))
    )

# ── step 2: define what makes a row valid ───────────────────────
validity_check = (
    col("event_id").isNotNull() &
    col("event_ts").isNotNull() &
    col("team_id").isNotNull() &
    col("user_id").isNotNull() &
    col("user_email").isNotNull() &
    (length(col("user_email")) > 3)
)

# ── step 3a: the good rows — these continue down the Silver path ─
jira_filtered = jira_typed.filter(validity_check)

# ── step 3b: the bad rows — these go to quarantine instead of being dropped ─
jira_quarantine = jira_typed \
    .filter(~validity_check) \
    .withColumn("quarantine_reason",
        when(col("event_id").isNull(), "missing event_id")
        .when(col("event_ts").isNull(), "invalid timestamp")
        .when(col("team_id").isNull(), "missing team_id")
        .when(col("user_id").isNull(), "missing user_id")
        .when(col("user_email").isNull(), "missing email")
        .otherwise("email too short")
    ) \
    .withColumn("quarantined_at", current_timestamp())

print(f"⚠️  Jira rows quarantined: {jira_quarantine.count()}")

# ── step 4: deduplicate the GOOD rows by event_id ────────────────
rows_before_dedup = jira_filtered.count()

window = Window \
    .partitionBy("event_id") \
    .orderBy(col("ingested_at").desc())

jira_deduped = jira_filtered \
    .withColumn("row_num", row_number().over(window)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

rows_after_dedup = jira_deduped.count()
duplicates_removed = rows_before_dedup - rows_after_dedup

print(f"🔁 Jira duplicates removed: {duplicates_removed} (of {rows_before_dedup} valid rows)")

# ── step 5: add Silver metadata columns ──────────────────────────
jira_silver = jira_deduped \
    .withColumn("silver_processed_at", current_timestamp()) \
    .withColumn("is_ai_event",
        when(col("event_type") == "ai_assist", True)
        .otherwise(False)
    ) \
    .withColumn("cost_estimate_usd",
        when(col("event_type") == "ai_assist", 0.000126)
        .otherwise(0.0)
    )

print(f"✅ Silver Jira rows after cleaning: {jira_silver.count()}")

# ── step 6: write the clean rows to Silver Delta table on S3 ─────
print("🚀 Writing Jira Silver to S3...")
jira_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_JIRA)

print(f"✅ Jira Silver written → {SILVER_JIRA}")

# ── step 7: write the quarantined rows to their own Delta table ──
print("🚀 Writing Jira Quarantine to S3...")
jira_quarantine.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(QUARANTINE_JIRA)

print(f"✅ Jira Quarantine written → {QUARANTINE_JIRA}")
# ══════════════════════════════════════════════════════════════
# JIRA SESSION WINDOWING
# ══════════════════════════════════════════════════════════════
# Groups each user's scattered events into "sessions" — a session ends
# when there's a 30-minute gap of inactivity. This changes the grain
# of the data from one-row-per-event to one-row-per-session, which is
# what makes a metric like "average session length per team" possible.

SESSION_JIRA = f"{BASE_PATH}/silver/jira_user_sessions"
SESSION_GAP = "30 minutes"

jira_sessions = jira_silver \
    .groupBy(
        col("user_id"),
        col("team_id"),
        col("team_name"),
        session_window(col("event_ts"), SESSION_GAP)
    ) \
    .agg(
        spark_min(col("event_ts")).alias("session_start"),
        spark_max(col("event_ts")).alias("session_end"),
        count("*").alias("events_in_session")
    ) \
    .withColumn("session_duration_minutes",
        (unix_timestamp(col("session_end")) -
         unix_timestamp(col("session_start"))) / 60
    ) \
    .drop("session_window")

print(f"📊 Jira sessions created: {jira_sessions.count()}")

print("🚀 Writing Jira Sessions to S3...")
jira_sessions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SESSION_JIRA)

print(f"✅ Jira Sessions written → {SESSION_JIRA}")

# ══════════════════════════════════════════════════════════════
# CONFLUENCE SILVER TRANSFORM
# ══════════════════════════════════════════════════════════════

conf_bronze = spark.read \
    .format("delta") \
    .load(BRONZE_CONF)

print(f"\n📥 Bronze Confluence rows read: {conf_bronze.count()}")

# ── step 1: cast event_ts string → proper timestamp ─────────────
conf_typed = conf_bronze \
    .withColumn("event_ts",
        to_timestamp(col("event_ts"))
    )

# ── step 2: define validity — same pattern as Jira, different fields ─
conf_validity_check = (
    col("event_id").isNotNull() &
    col("event_ts").isNotNull() &
    col("team_id").isNotNull() &
    col("user_id").isNotNull() &
    col("space_key").isNotNull() &
    col("page_id").isNotNull()
)

# ── step 3a: good rows continue down the Silver path ─────────────
conf_filtered = conf_typed.filter(conf_validity_check)

# ── step 3b: bad rows go to their own quarantine table ────────────
conf_quarantine = conf_typed \
    .filter(~conf_validity_check) \
    .withColumn("quarantine_reason",
        when(col("event_id").isNull(), "missing event_id")
        .when(col("event_ts").isNull(), "invalid timestamp")
        .when(col("team_id").isNull(), "missing team_id")
        .when(col("user_id").isNull(), "missing user_id")
        .when(col("space_key").isNull(), "missing space_key")
        .otherwise("missing page_id")
    ) \
    .withColumn("quarantined_at", current_timestamp())

print(f"⚠️  Confluence rows quarantined: {conf_quarantine.count()}")

# ── step 4: deduplicate the GOOD rows by event_id ─────────────────
rows_before_dedup_conf = conf_filtered.count()

window_conf = Window \
    .partitionBy("event_id") \
    .orderBy(col("ingested_at").desc())

conf_deduped = conf_filtered \
    .withColumn("row_num", row_number().over(window_conf)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

rows_after_dedup_conf = conf_deduped.count()
duplicates_removed_conf = rows_before_dedup_conf - rows_after_dedup_conf

print(f"🔁 Confluence duplicates removed: {duplicates_removed_conf} (of {rows_before_dedup_conf} valid rows)")

# ── step 5: add Silver metadata columns ───────────────────────────
conf_silver = conf_deduped \
    .withColumn("silver_processed_at", current_timestamp()) \
    .withColumn("is_ai_event",
        when(col("event_type") == "ai_assist", True)
        .otherwise(False)
    ) \
    .withColumn("cost_estimate_usd",
        when(col("event_type") == "ai_assist", 0.000126)
        .otherwise(0.0)
    )

print(f"✅ Silver Confluence rows after cleaning: {conf_silver.count()}")

# ── step 6: write the clean rows to Silver Delta table on S3 ──────
print("🚀 Writing Confluence Silver to S3...")
conf_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SILVER_CONF)

print(f"✅ Confluence Silver written → {SILVER_CONF}")

# ── step 7: write the quarantined rows to their own Delta table ───
print("🚀 Writing Confluence Quarantine to S3...")
conf_quarantine.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(QUARANTINE_CONF)

print(f"✅ Confluence Quarantine written → {QUARANTINE_CONF}")
# ══════════════════════════════════════════════════════════════
# CONFLUENCE SESSION WINDOWING
# ══════════════════════════════════════════════════════════════
# Same concept as Jira sessions — group a user's scattered Confluence
# events into sessions separated by 30 minutes of inactivity. We also
# group by page_id here, since "how long did this user spend on this
# specific page" is a more useful Confluence metric than just "session
# length" in general — pages are the core unit of work in Confluence.

SESSION_CONF = f"{BASE_PATH}/silver/confluence_user_sessions"

conf_sessions = conf_silver \
    .groupBy(
        col("user_id"),
        col("team_id"),
        col("team_name"),
        col("space_key"),
        col("page_id"),
        session_window(col("event_ts"), SESSION_GAP)
    ) \
    .agg(
        spark_min(col("event_ts")).alias("session_start"),
        spark_max(col("event_ts")).alias("session_end"),
        count("*").alias("events_in_session")
    ) \
    .withColumn("session_duration_minutes",
        (unix_timestamp(col("session_end")) -
         unix_timestamp(col("session_start"))) / 60
    ) \
    .drop("session_window")

print(f"📊 Confluence sessions created: {conf_sessions.count()}")

print("🚀 Writing Confluence Sessions to S3...")
conf_sessions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(SESSION_CONF)

print(f"✅ Confluence Sessions written → {SESSION_CONF}")
# ── final summary ─────────────────────────────────────────────
print("\n" + "="*55)
print("✅ SILVER LAYER COMPLETE")
print(f"   Jira Silver           → {SILVER_JIRA}")
print(f"   Jira Quarantine       → {QUARANTINE_JIRA}")
print(f"   Confluence Silver     → {SILVER_CONF}")
print(f"   Confluence Quarantine → {QUARANTINE_CONF}")
print("="*55)

spark.stop()