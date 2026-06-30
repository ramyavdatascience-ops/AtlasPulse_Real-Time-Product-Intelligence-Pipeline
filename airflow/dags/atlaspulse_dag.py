from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys

# ── project root on HOST machine ──────────────────────────────
# This path is where your scripts live on Windows.
# The DAG file runs inside Docker but subprocess.run() executes
# on the host machine via the mounted volume.
PROJECT_ROOT = "E://data-eng-projects//de_pro3//atlaspulse"

# ── Python executable on host ─────────────────────────────────
PYTHON = "python"

default_args = {
    "owner": "atlaspulse",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ── task functions ────────────────────────────────────────────
def run_glue_etl():
    result = subprocess.run(
        [PYTHON, "glue_jobs/aurora_to_s3.py"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Glue ETL failed:\n{result.stderr}")
    print("✅ Glue ETL completed successfully")

def run_silver():
    result = subprocess.run(
        [PYTHON, "streaming/spark_silver.py"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Silver transform failed:\n{result.stderr}")
    print("✅ Silver transform completed successfully")

def run_gold():
    result = subprocess.run(
        [PYTHON, "gold/team_cost_summary.py"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Gold layer failed:\n{result.stderr}")
    print("✅ Gold layer completed successfully")

# ── define the DAG ────────────────────────────────────────────
with DAG(
    dag_id="atlaspulse_pipeline",
    default_args=default_args,
    description="AtlasPulse — full pipeline: Glue ETL → Silver → Gold",
    schedule="@daily",
    catchup=False,
    tags=["atlaspulse", "data-engineering", "delta-lake", "s3"],
) as dag:

    glue_etl = PythonOperator(
        task_id="glue_etl_aurora_to_s3",
        python_callable=run_glue_etl,
    )

    silver = PythonOperator(
        task_id="spark_silver_transform",
        python_callable=run_silver,
    )

    gold = PythonOperator(
        task_id="spark_gold_team_cost_summary",
        python_callable=run_gold,
    )

    glue_etl >> silver >> gold