import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    round as spark_round, current_timestamp,
    max as spark_max, min as spark_min,
    when, lag, lit
)
from pyspark.sql.window import Window
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════
# WHAT THIS SCRIPT DOES — read this first
# ══════════════════════════════════════════════════════════════
# This is the Gold layer — the final step of the entire AtlasPulse
# pipeline. Everything built so far leads to this one script.
#
# It answers one business question:
# "Which Atlassian team spent the most on AI features this month,
#  are they over budget, and how does that compare to last month?"
#
# To answer this, it joins TWO data sources:
#
# Source A — Silver jira_events_clean (S3)
#   → what happened: every AI assist event with its cost
#   → produced by spark_silver.py
#
# Source B — Staging teams (S3)
#   → who they are: team name, monthly budget, plan tier, region
#   → produced by aurora_to_s3.py (extracted from PostgreSQL)
#
# Output — Gold TEAM_COST_SUMMARY (S3 + printed to screen)
#   → one row per team per month
#   → total events, AI calls, cost, budget used %, cost tier
#   → this is what a finance dashboard or BI tool would query
# ══════════════════════════════════════════════════════════════

# ── S3 paths ───────────────────────────────────────────────────
S3_BUCKET    = os.getenv("S3_BUCKET")
BASE_PATH    = f"s3a://{S3_BUCKET}"

# inputs — both already written to S3 by earlier pipeline steps
SILVER_JIRA  = f"{BASE_PATH}/silver/jira_events_clean"
STAGING_TEAMS = f"{BASE_PATH}/staging/teams"

# output — the final Gold table
GOLD_SUMMARY = f"{BASE_PATH}/gold/team_cost_summary"

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "eu-north-1")

# ── Spark session ─────────────────────────────────────────────
# Same config as Silver — Delta + S3 support.
# No Kafka JAR needed here — Gold only reads from S3, not Kafka.
spark = SparkSession.builder \
    .appName("AtlasPulse-Gold-TeamCostSummary") \
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
print("✅ Building Gold layer — TEAM_COST_SUMMARY\n")

# ══════════════════════════════════════════════════════════════
# STEP 1 — READ SILVER JIRA EVENTS
# ══════════════════════════════════════════════════════════════
# This is the cleaned, deduped, enriched Jira event data.
# Each row is one event — issue_created, ai_assist, comment_added etc.
# The is_ai_event and cost_estimate_usd columns were added by Silver.
# We read it as a plain batch read — Gold runs once daily, not streaming.

print("📥 Reading Silver Jira events...")
jira_silver = spark.read \
    .format("delta") \
    .load(SILVER_JIRA)

print(f"   ✅ {jira_silver.count()} Silver Jira rows loaded\n")

# ══════════════════════════════════════════════════════════════
# STEP 2 — READ STAGING TEAMS (from PostgreSQL via Glue ETL)
# ══════════════════════════════════════════════════════════════
# This is the relational metadata extracted from PostgreSQL by
# aurora_to_s3.py. It tells us each team's budget, plan tier,
# department, and region — things the event stream doesn't carry.
# Without this join, we can count AI calls but can't say whether
# a team is over their budget or what plan tier they're on.

print("📥 Reading Staging teams metadata...")
teams = spark.read \
    .format("delta") \
    .load(STAGING_TEAMS)

print(f"   ✅ {teams.count()} teams loaded\n")

# ══════════════════════════════════════════════════════════════
# STEP 3 — AGGREGATE EVENTS BY TEAM AND MONTH
# ══════════════════════════════════════════════════════════════
# Before joining with teams, we first summarise the raw events.
# This collapses 1143 individual event rows into one row per team,
# giving us totals that finance and engineering actually care about.
#
# Why aggregate BEFORE the join and not after?
# Because it's more efficient — we reduce 1143 rows down to 6 rows
# (one per team) FIRST, then join 6 rows against 6 teams.
# If we joined first, we'd be carrying all 1143 rows through the
# join operation unnecessarily. This is called "push down aggregation"
# and is a standard query optimisation pattern.

print("🔄 Aggregating events by team...")

team_metrics = jira_silver \
    .groupBy("team_id", "team_name") \
    .agg(
        # total number of events of any type (issues, comments, AI etc.)
        count("*").alias("total_events"),

        # only count events where is_ai_event = True
        # this is the number that drives the cost calculation
        spark_sum(
            when(col("is_ai_event") == True, 1).otherwise(0)
        ).alias("ai_api_calls"),

        # sum of all cost_estimate_usd values
        # each ai_assist event contributes $0.000126
        # non-AI events contribute $0.0
        spark_round(
            spark_sum(col("cost_estimate_usd")), 4
        ).alias("total_cost_usd"),

        # average response time across all events for this team
        # useful for spotting performance degradation per team
        spark_round(
            avg(col("response_time_ms")), 1
        ).alias("avg_response_time_ms"),

        # earliest and latest event timestamps
        # tells us the actual date range this summary covers
        spark_min(col("event_ts")).alias("period_start"),
        spark_max(col("event_ts")).alias("period_end")
    )

print(f"   ✅ Aggregated into {team_metrics.count()} team summaries\n")

# ══════════════════════════════════════════════════════════════
# STEP 4 — JOIN WITH TEAMS METADATA
# ══════════════════════════════════════════════════════════════
# This is the core of the Gold layer — combining streaming event
# aggregates with relational metadata on the shared team_id key.
#
# Left join because: we want ALL teams in the output, even if a
# team had zero events this period (new team, or a quiet month).
# An inner join would silently drop teams with no events — that
# would make it look like we have fewer teams than we do, which
# could mislead a finance report.
#
# After this join, each row has BOTH:
#   - what the team did   (from Silver Jira events)
#   - who the team is     (from PostgreSQL via Glue ETL)

print("🔗 Joining event aggregates with team metadata...")

gold_joined = team_metrics.join(
    teams.select(
        "team_id",
        "department",
        "plan_tier",
        col("monthly_budget").cast("double"),
        "region"
    ),
    on="team_id",
    how="left"
)

# ══════════════════════════════════════════════════════════════
# STEP 5 — DERIVE BUSINESS METRICS
# ══════════════════════════════════════════════════════════════
# Now that we have event data AND team metadata together,
# we can compute the metrics that actually answer business questions.
#
# budget_used_pct → tells finance how much of the monthly budget
#   has been consumed. Over 100% = overspent.
#
# cost_tier → categorises teams into HIGH/MEDIUM/LOW so a dashboard
#   can immediately highlight which teams need attention without
#   someone having to read every number.
#
# budget_status → plain English flag for alerting:
#   OVER_BUDGET, WARNING (>80%), ON_TRACK (<80%)

print("📊 Deriving business metrics...")

gold_enriched = gold_joined \
    .withColumn("budget_used_pct",
        # what percentage of the monthly budget has been spent?
        # guard against division by zero with NULLIF pattern —
        # if monthly_budget is 0 or null, return null instead of crashing
        spark_round(
            (col("total_cost_usd") /
             when(col("monthly_budget") == 0, None)
             .otherwise(col("monthly_budget"))) * 100,
            1
        )
    ) \
    .withColumn("cost_tier",
        # categorise by absolute cost, not percentage —
        # a small team might be 90% of a tiny budget but still LOW cost
        when(col("total_cost_usd") > 1.0,  "HIGH")
        .when(col("total_cost_usd") > 0.5,  "MEDIUM")
        .otherwise("LOW")
    ) \
    .withColumn("budget_status",
        # plain English alert flag for the dashboard
        when(col("budget_used_pct") > 100, "OVER_BUDGET")
        .when(col("budget_used_pct") > 80,  "WARNING")
        .otherwise("ON_TRACK")
    ) \
    .withColumn("gold_created_at", current_timestamp())

# ══════════════════════════════════════════════════════════════
# STEP 6 — SELECT FINAL COLUMNS IN LOGICAL ORDER
# ══════════════════════════════════════════════════════════════
# Explicitly select and order columns so the Gold table schema is
# clean and predictable — no accidental duplicate columns from the
# join, no internal Spark metadata columns leaking through.
# This is the schema that a BI tool like Tableau or Metabase would
# connect to and expose to business users.

gold_final = gold_enriched.select(
    # identity
    col("team_id"),
    col("team_name"),
    col("department"),
    col("plan_tier"),
    col("region"),

    # volume metrics
    col("total_events"),
    col("ai_api_calls"),

    # cost metrics
    col("total_cost_usd"),
    col("monthly_budget"),
    col("budget_used_pct"),

    # performance metrics
    col("avg_response_time_ms"),

    # status flags
    col("cost_tier"),
    col("budget_status"),

    # time range this summary covers
    col("period_start"),
    col("period_end"),

    # audit — when was this Gold table last computed?
    col("gold_created_at")
)

# ══════════════════════════════════════════════════════════════
# STEP 7 — SHOW RESULTS IN TERMINAL BEFORE WRITING
# ══════════════════════════════════════════════════════════════
# Print the Gold table to screen so you can see the actual
# business answer before it gets written to S3.
# This is what a finance analyst or engineering manager would see
# on their dashboard — which team spent what, and are they on track.

print("\n" + "="*55)
print("📊 TEAM COST SUMMARY — GOLD LAYER OUTPUT")
print("="*55)

gold_final.select(
    "team_name",
    "plan_tier",
    "total_events",
    "ai_api_calls",
    "total_cost_usd",
    "monthly_budget",
    "budget_used_pct",
    "cost_tier",
    "budget_status"
).orderBy(col("total_cost_usd").desc()) \
 .show(truncate=False)

# ══════════════════════════════════════════════════════════════
# STEP 8 — WRITE GOLD TABLE TO S3 AS DELTA LAKE
# ══════════════════════════════════════════════════════════════
# Write the final Gold table to S3 as Delta Lake.
# mode("overwrite") is correct here — Gold is a summary table
# that gets fully recomputed on every pipeline run.
# It always reflects the latest state of Silver + Staging data.
# Redshift Spectrum or Athena would point to this path to
# serve BI queries without moving the data again.

print("\n🚀 Writing Gold table to S3...")

gold_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(GOLD_SUMMARY)

print(f"✅ Gold table written → {GOLD_SUMMARY}")

# ── final summary ─────────────────────────────────────────────
print("\n" + "="*55)
print("✅ GOLD LAYER COMPLETE")
print(f"   Output → {GOLD_SUMMARY}")
print("   Schema → team_id, team_name, department, plan_tier,")
print("            region, total_events, ai_api_calls,")
print("            total_cost_usd, monthly_budget,")
print("            budget_used_pct, cost_tier, budget_status")
print("   Ready for Redshift Spectrum / Athena / BI tools")
print("="*55)

# ══════════════════════════════════════════════════════════════
# STEP 9 — WRITE GOLD TABLE TO POSTGRESQL FOR GRAFANA
# ══════════════════════════════════════════════════════════════
# Grafana can't read S3 Delta files directly — it needs a SQL
# database. So we write the same Gold data into PostgreSQL too.
# This is a standard pattern: S3 for the data lake (long-term
# storage, downstream Spark jobs), PostgreSQL for BI tools
# and dashboards that need SQL access.
#
# We collect() the Gold DataFrame back to the driver as a pandas
# DataFrame, then write it to PostgreSQL using SQLAlchemy.
# This is safe here because Gold is always tiny — 6 rows, one
# per team. We'd never collect() a Silver or Bronze table this way.

print("\n🔄 Writing Gold table to PostgreSQL for Grafana...")

from sqlalchemy import create_engine as pg_engine

PG_HOST = os.getenv("AURORA_HOST")
PG_DB   = os.getenv("AURORA_DB")
PG_USER = os.getenv("AURORA_USER")
PG_PASS = os.getenv("AURORA_PASSWORD")
PG_PORT = os.getenv("AURORA_PORT", 5432)

# convert Spark DataFrame → pandas → PostgreSQL
# toPandas() is the reverse of createDataFrame() —
# it pulls all rows from the distributed Spark DataFrame
# back to the driver machine as a pandas DataFrame.
# Only safe on small Gold summary tables.
gold_pandas = gold_final.toPandas()

engine = pg_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

gold_pandas.to_sql(
    name="team_cost_summary",
    con=engine,
    if_exists="replace",   # drop and recreate table on every run
    index=False            # don't write the pandas row index as a column
)

engine.dispose()
print("✅ Gold table written to PostgreSQL → team_cost_summary")
print(f"   {len(gold_pandas)} rows written")

spark.stop()