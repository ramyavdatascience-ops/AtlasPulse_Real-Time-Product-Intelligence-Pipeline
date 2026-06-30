import os
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from dotenv import load_dotenv

load_dotenv()

# ── S3 destination paths ──────────────────────────────────────
# These staging paths hold relational data extracted from PostgreSQL.
# They sit between the operational DB and the Gold layer —
# Spark's Gold job reads from here, not directly from PostgreSQL.
S3_BUCKET        = os.getenv("S3_BUCKET")
BASE_PATH        = f"s3a://{S3_BUCKET}"
STAGING_TEAMS    = f"{BASE_PATH}/staging/teams"
STAGING_USERS    = f"{BASE_PATH}/staging/users"
STAGING_PROJECTS = f"{BASE_PATH}/staging/projects"

# ── PostgreSQL connection details ─────────────────────────────
PG_HOST = os.getenv("AURORA_HOST")
PG_DB   = os.getenv("AURORA_DB")
PG_USER = os.getenv("AURORA_USER")
PG_PASS = os.getenv("AURORA_PASSWORD")
PG_PORT = os.getenv("AURORA_PORT", 5432)

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "eu-north-1")

# ── Spark session ─────────────────────────────────────────────
# Arrow configs are critical here:
# arrow.pyspark.enabled → uses Apache Arrow to serialise pandas→Spark,
#   which is far more stable than the default pickle-based serialiser
#   on Windows when dealing with PostgreSQL data types.
# arrow.pyspark.fallback.enabled → if Arrow still fails for any column
#   type, fall back gracefully instead of crashing the whole job.
spark = SparkSession.builder \
    .appName("AtlasPulse-GlueETL-AuroraToS3") \
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
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
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
print(f"✅ Destination: s3://{S3_BUCKET}/staging/\n")

# ── SQLAlchemy engine ─────────────────────────────────────────
# SQLAlchemy wraps psycopg2 in a way that pandas read_sql understands.
# We dispose the engine after all reads are done to cleanly
# close the connection pool — good practice to avoid connection leaks.
engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

# ══════════════════════════════════════════════════════════════
# HELPER — read PostgreSQL table and sanitise all column types
# ══════════════════════════════════════════════════════════════
# The Python worker crash was caused by Spark failing to serialise
# PostgreSQL-specific types (Decimal, datetime with timezone, bool)
# across the JVM-Python boundary.
# Fix: convert everything to plain Python types BEFORE passing to Spark.
# After this function, every column is either str, float, or bool —
# types that Arrow and Spark both handle without issues.
def read_postgres_table(table_name: str) -> pd.DataFrame:
    print(f"📥 Extracting {table_name} from PostgreSQL...")
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

    # Sanitise column types one by one
    for column in df.columns:
        dtype = str(df[column].dtype)
        if dtype.startswith("datetime"):
            # Convert timezone-aware datetimes to plain ISO strings —
            # Arrow can't always handle timezone info from PostgreSQL
            df[column] = df[column].astype(str)
        elif dtype == "object":
            # object = mixed types or strings — cast to str to be safe
            df[column] = df[column].astype(str)
        elif dtype == "bool":
            # Keep booleans as bool — Arrow handles this fine
            df[column] = df[column].astype(bool)
        else:
            # int, float, Decimal → float (covers PostgreSQL DECIMAL too)
            df[column] = pd.to_numeric(df[column], errors="coerce")

    print(f"   ✅ {len(df)} rows extracted from {table_name}")
    return df

# ══════════════════════════════════════════════════════════════
# EXTRACT — read all three tables from PostgreSQL
# ══════════════════════════════════════════════════════════════
teams_pd    = read_postgres_table("teams")
users_pd    = read_postgres_table("users")
projects_pd = read_postgres_table("projects")

# Close all PostgreSQL connections — we're done reading
engine.dispose()
print("\n✅ All tables extracted from PostgreSQL\n")

# ══════════════════════════════════════════════════════════════
# CONVERT pandas → Spark DataFrame
# ══════════════════════════════════════════════════════════════
# Now that all types are plain Python primitives, Arrow serialises
# them cleanly across the JVM boundary without crashing.
print("🔄 Converting pandas DataFrames to Spark...")
teams_df    = spark.createDataFrame(teams_pd)
users_df    = spark.createDataFrame(users_pd)
projects_df = spark.createDataFrame(projects_pd)
print("✅ Conversion complete\n")

# ══════════════════════════════════════════════════════════════
# TRANSFORM — add source tag and extraction timestamp
# ══════════════════════════════════════════════════════════════
# source column → tells downstream Gold jobs where this data came from
# extracted_at  → audit trail: when was this snapshot taken?
#                 If a join looks wrong in Gold, check this timestamp
#                 to see if you're joining against stale metadata.
teams_df = teams_df \
    .withColumn("source", lit("postgresql_teams")) \
    .withColumn("extracted_at", current_timestamp())

users_df = users_df \
    .withColumn("source", lit("postgresql_users")) \
    .withColumn("extracted_at", current_timestamp())

projects_df = projects_df \
    .withColumn("source", lit("postgresql_projects")) \
    .withColumn("extracted_at", current_timestamp())

# ══════════════════════════════════════════════════════════════
# LOAD — write each table to S3 as Delta Lake
# ══════════════════════════════════════════════════════════════
# mode("overwrite") → full snapshot extraction every run.
# This is correct for small dimension tables like teams/users
# where we always want the freshest copy.
# For large tables in production, you'd use Glue job bookmarks
# or CDC (Change Data Capture) for incremental loads instead.
print("🚀 Loading tables to S3 staging...")

teams_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(STAGING_TEAMS)
print(f"✅ teams    → {STAGING_TEAMS}")

users_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(STAGING_USERS)
print(f"✅ users    → {STAGING_USERS}")

projects_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(STAGING_PROJECTS)
print(f"✅ projects → {STAGING_PROJECTS}")

# ── final summary ─────────────────────────────────────────────
print("\n" + "="*55)
print("✅ GLUE ETL COMPLETE")
print(f"   teams    → {STAGING_TEAMS}")
print(f"   users    → {STAGING_USERS}")
print(f"   projects → {STAGING_PROJECTS}")
print("   All tables landed in S3 as Delta Lake")
print("   Ready for Gold layer join")
print("="*55)

spark.stop()