import os
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════
# WHAT THIS SCRIPT DOES
# ══════════════════════════════════════════════════════════════
# This script runs automated data quality checks on your Silver
# Delta Lake tables using Great Expectations (GX).
#
# Think of it like unit tests — but for your DATA, not your code.
#
# Without this: bad data flows silently into Gold and corrupts
# your TEAM_COST_SUMMARY. Finance sees wrong numbers. Nobody
# knows why until someone manually investigates.
#
# With this: if Silver has unexpected nulls, wrong value ranges,
# duplicate event IDs, or schema drift — this script catches it
# immediately, logs exactly which check failed and why, and
# the pipeline stops before Gold is written.
#
# In production at Atlassian, this would run as a task in the
# Airflow DAG between Silver and Gold:
# glue_etl → silver → [this script] → gold
# If this fails → gold never runs → dashboard stays clean
#
# What we check:
# 1. event_id is never null (every event must have an ID)
# 2. event_type only contains known valid values
# 3. response_time_ms is within a realistic range
# 4. cost_estimate_usd is never negative
# 5. is_ai_event is always boolean (never null)
# 6. No duplicate event_ids in Silver (dedup worked correctly)
# 7. Row count is above minimum (pipeline produced real data)
# ══════════════════════════════════════════════════════════════

# ── S3 paths ───────────────────────────────────────────────────
S3_BUCKET   = os.getenv("S3_BUCKET")
BASE_PATH   = f"s3a://{S3_BUCKET}"
SILVER_JIRA = f"{BASE_PATH}/silver/jira_events_clean"

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "eu-north-1")

# ── Spark session ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("AtlasPulse-DataQuality") \
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
print("✅ Running data quality checks on Silver Jira events\n")

# ── read Silver Jira events ───────────────────────────────────
df = spark.read.format("delta").load(SILVER_JIRA)
total_rows = df.count()
print(f"📥 Silver Jira rows loaded: {total_rows}\n")

# ══════════════════════════════════════════════════════════════
# EXPECTATION ENGINE
# ══════════════════════════════════════════════════════════════
# We build our own lightweight expectation runner here using
# pure PySpark instead of the full GX framework.
# Why? GX 0.18 has heavy setup requirements (data context,
# checkpoints, stores) that add complexity without adding value
# for a portfolio project. The pattern is identical — define
# expectations, run them, report results.
# In production you'd use the full GX suite with data docs,
# Slack alerts, and a results store in S3.

results = []  # collect all check results here
passed   = 0
failed   = 0

def run_check(name, description, passed_bool, details=""):
    """
    Run one data quality check and record the result.

    name        → short identifier e.g. "event_id_not_null"
    description → plain English what we're checking
    passed_bool → True if data is clean, False if something's wrong
    details     → extra info shown when a check fails
    """
    global passed, failed
    status = "✅ PASSED" if passed_bool else "❌ FAILED"
    if passed_bool:
        passed += 1
    else:
        failed += 1

    result = {
        "check": name,
        "description": description,
        "status": "PASSED" if passed_bool else "FAILED",
        "details": details,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    results.append(result)
    print(f"  {status} | {name}")
    if not passed_bool:
        print(f"           ↳ {details}")

# ══════════════════════════════════════════════════════════════
# THE ACTUAL CHECKS
# ══════════════════════════════════════════════════════════════

print("=" * 55)
print("Running expectation suite: silver_jira_events")
print("=" * 55)

# ── Check 1: event_id is never null ──────────────────────────
# Every Kafka message must have a unique event_id (UUID).
# If this is null it means the producer had a bug or the
# Bronze→Silver parsing failed silently. A null event_id
# also breaks deduplication since we can't dedupe by null.
null_event_ids = df.filter(col("event_id").isNull()).count()
run_check(
    name="event_id_not_null",
    description="event_id must never be null",
    passed_bool=null_event_ids == 0,
    details=f"{null_event_ids} rows have null event_id"
)

# ── Check 2: event_type contains only known values ────────────
# Our producer only generates 6 known event types.
# If a new, unexpected value appears it means either:
# a) a new producer is sending events we haven't accounted for
# b) data corruption happened somewhere in the pipeline
# Either way we want to know immediately.
valid_event_types = {
    "issue_created", "issue_updated", "issue_closed",
    "comment_added", "ai_assist", "sprint_started"
}
invalid_types = df.filter(
    ~col("event_type").isin(list(valid_event_types))
).count()
run_check(
    name="event_type_valid_values",
    description=f"event_type must be one of {valid_event_types}",
    passed_bool=invalid_types == 0,
    details=f"{invalid_types} rows have unexpected event_type values"
)

# ── Check 3: response_time_ms is within realistic range ───────
# Our producer generates response times between 80-800ms.
# Values outside 0-5000ms would indicate data corruption
# or a producer bug generating impossible values.
# We use 5000 as upper bound (generous) to avoid false alarms
# if a real system has occasional slow responses.
out_of_range = df.filter(
    (col("response_time_ms") < 0) |
    (col("response_time_ms") > 5000)
).count()
run_check(
    name="response_time_in_range",
    description="response_time_ms must be between 0 and 5000",
    passed_bool=out_of_range == 0,
    details=f"{out_of_range} rows have response_time_ms outside 0-5000ms"
)

# ── Check 4: cost_estimate_usd is never negative ──────────────
# Cost can be 0 (non-AI events) but never negative.
# A negative cost would silently reduce team totals in Gold,
# making a team look cheaper than they actually are.
negative_costs = df.filter(col("cost_estimate_usd") < 0).count()
run_check(
    name="cost_not_negative",
    description="cost_estimate_usd must be >= 0",
    passed_bool=negative_costs == 0,
    details=f"{negative_costs} rows have negative cost_estimate_usd"
)

# ── Check 5: is_ai_event is never null ────────────────────────
# This boolean column was added by Silver transform.
# If it's null it means the withColumn() in spark_silver.py
# silently failed — a serious pipeline bug worth catching.
null_ai_flag = df.filter(col("is_ai_event").isNull()).count()
run_check(
    name="is_ai_event_not_null",
    description="is_ai_event must never be null",
    passed_bool=null_ai_flag == 0,
    details=f"{null_ai_flag} rows have null is_ai_event"
)

# ── Check 6: no duplicate event_ids in Silver ─────────────────
# Silver's deduplication step should have removed all duplicates.
# If duplicates still exist it means the Window dedup logic
# in spark_silver.py has a bug, and Gold will double-count costs.
total_distinct = df.select("event_id").distinct().count()
has_duplicates = total_distinct < total_rows
run_check(
    name="no_duplicate_event_ids",
    description="Silver must contain no duplicate event_ids",
    passed_bool=not has_duplicates,
    details=f"{total_rows - total_distinct} duplicate event_ids found"
)

# ── Check 7: minimum row count ────────────────────────────────
# Silver must have at least 100 rows to be considered meaningful.
# If it has fewer, something went wrong upstream — Bronze might
# be empty, or the Silver job crashed before writing all data.
# This is a pipeline health check, not a data correctness check.
MIN_ROWS = 100
run_check(
    name="minimum_row_count",
    description=f"Silver must have at least {MIN_ROWS} rows",
    passed_bool=total_rows >= MIN_ROWS,
    details=f"Only {total_rows} rows found, expected >= {MIN_ROWS}"
)

# ── Check 8: AI event cost consistency ────────────────────────
# Every row where is_ai_event=True must have cost > 0.
# Every row where is_ai_event=False must have cost = 0.
# A mismatch means the cost logic in Silver is broken.
ai_with_zero_cost = df.filter(
    (col("is_ai_event") == True) &
    (col("cost_estimate_usd") == 0)
).count()
non_ai_with_cost = df.filter(
    (col("is_ai_event") == False) &
    (col("cost_estimate_usd") > 0)
).count()
run_check(
    name="ai_cost_consistency",
    description="AI events must have cost > 0, non-AI events must have cost = 0",
    passed_bool=(ai_with_zero_cost == 0 and non_ai_with_cost == 0),
    details=f"{ai_with_zero_cost} AI events with zero cost, {non_ai_with_cost} non-AI events with cost"
)

# ══════════════════════════════════════════════════════════════
# RESULTS SUMMARY
# ══════════════════════════════════════════════════════════════

print("\n" + "=" * 55)
print("DATA QUALITY REPORT — Silver Jira Events")
print("=" * 55)
print(f"  Total checks run : {len(results)}")
print(f"  Passed           : {passed} ✅")
print(f"  Failed           : {failed} ❌")
print(f"  Data quality score: {round(passed/len(results)*100, 1)}%")
print("=" * 55)

# ── write results to a JSON file for audit trail ──────────────
# This creates a timestamped record of every quality check run.
# In production, you'd write this to S3 and build a Grafana
# panel showing data quality score over time.
report = {
    "pipeline": "atlaspulse",
    "table": "silver_jira_events_clean",
    "run_timestamp": datetime.now(timezone.utc).isoformat(),
    "total_rows_checked": total_rows,
    "checks_passed": passed,
    "checks_failed": failed,
    "quality_score_pct": round(passed/len(results)*100, 1),
    "results": results
}

os.makedirs("quality/reports", exist_ok=True)
report_path = f"quality/reports/dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(report_path, "w") as f:
    json.dump(report, f, indent=2)

print(f"\n📄 Full report saved → {report_path}")

# ── fail the pipeline if any checks failed ────────────────────
# This is the critical line — if any check failed, we raise
# an exception. In Airflow, this would cause the DQ task to
# fail, which would prevent the Gold layer from running.
# This is exactly how production data quality gates work.
if failed > 0:
    spark.stop()
    raise Exception(
        f"❌ Data quality gate FAILED — {failed} check(s) failed. "
        f"Gold layer will not run until Silver data is clean."
    )

print("\n✅ All data quality checks passed — pipeline may proceed to Gold")
spark.stop()