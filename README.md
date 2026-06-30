# AtlasPulse — Real-Time Product Intelligence Pipeline

A production-grade, end-to-end real-time data engineering pipeline that simulates Jira and Confluence product event tracking at scale. Built using Apache Kafka, PySpark Structured Streaming, Delta Lake, PostgreSQL, Apache Airflow, Grafana, and AWS S3, this project demonstrates modern streaming, ETL, data quality, and analytics patterns used in enterprise environments.

> **Designed to showcase production-ready Data Engineering skills for roles at companies like Atlassian, Amazon, Microsoft, and other cloud-native organizations.**

---

# 🏗️ Architecture

```text
Python Producers (Jira/Confluence Simulated Events)
                     │
                     ▼
             Apache Kafka (Docker)
                     │
                     ▼
     PySpark Structured Streaming
                     │
                     ▼
        Bronze Delta Lake (AWS S3)
                     │
                     ▼
        Silver Delta Lake (AWS S3)
     • Data Validation
     • Quarantine Tables
     • Deduplication
     • Session Windowing
                     │
         PostgreSQL (Metadata)
                     │
                     ▼
        Glue-Style ETL (PySpark)
                     │
                     ▼
         Staging Delta Lake (S3)
                     │
                     ▼
        Gold Delta Lake (AWS S3)
         TEAM_COST_SUMMARY
                     │
      ┌──────────────┼──────────────┐
      ▼              ▼              ▼
 PostgreSQL   Data Quality    Apache Airflow
 (Grafana)     Validation     Orchestration
      │
      ▼
 Grafana Dashboard
```

---

# 📌 Project Highlights

- Real-time event ingestion using Apache Kafka
- Streaming ETL with PySpark Structured Streaming
- Bronze → Silver → Gold Medallion Architecture
- ACID-compliant Delta Lake storage on AWS S3
- Exactly-once processing using Spark checkpointing
- Data quality validation with automated expectation checks
- Quarantine tables for invalid records
- Deduplication with audit metrics
- Session window analytics using Spark
- Glue-style ETL from PostgreSQL to S3
- Business-ready Gold layer aggregations
- Airflow DAG orchestration
- Grafana dashboards for live monitoring
- Dockerized local infrastructure

---

# 🛠 Tech Stack

| Category | Technologies |
|-----------|--------------|
| Language | Python, SQL, PySpark |
| Streaming | Apache Kafka, Spark Structured Streaming |
| Storage | Delta Lake, AWS S3 |
| Database | PostgreSQL |
| ETL | PySpark (Glue-style) |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations / Custom Validation Suite |
| Visualization | Grafana |
| Infrastructure | Docker, Docker Compose |
| Cloud | AWS (S3, IAM) |

---

# 📂 Medallion Architecture

## 🥉 Bronze Layer

- Raw events ingested directly from Kafka
- Immutable source of truth
- Checkpointed streaming
- Exactly-once processing

---

## 🥈 Silver Layer

Business-ready cleaned dataset featuring:

- Data validation
- Invalid row quarantine
- Duplicate detection
- Session windowing
- Standardized schema

---

## 🥇 Gold Layer

Aggregated business metrics combining streaming events with relational metadata.

Main output:

```
TEAM_COST_SUMMARY
```

Includes:

- Team AI Spend
- Budget Utilization
- Event Counts
- AI Usage Statistics
- Cost Tier Classification

---

# ✅ Data Quality

The pipeline validates data before Gold processing.

Implemented checks include:

- Event ID is not null
- Valid event types
- Response time within acceptable limits
- Non-negative AI cost
- AI flag validation
- Duplicate detection
- Minimum row threshold
- AI cost consistency validation

If validation fails:

- Gold pipeline stops
- Invalid rows remain quarantined
- Validation report is generated

Current Status:

```
8 / 8 Checks Passed
100% Data Quality Score
```

---

# 📊 Dashboard

Grafana visualizes live business metrics including:

- AI Spend by Team
- Budget Utilization Gauge
- Team Cost Summary Table
- Pipeline Totals
- Total AI Cost
- Total Events Processed

---

# 🔄 Pipeline Orchestration

Apache Airflow orchestrates the batch workflow.

```
PostgreSQL
      │
      ▼
Glue ETL
      │
      ▼
Silver Transform
      │
      ▼
Gold Aggregation
      │
      ▼
Data Quality
```

Streaming services (Kafka + Spark) run continuously while Airflow schedules daily batch transformations.

---

# 🚀 Local Setup

Start infrastructure:

```bash
cd infra
docker-compose up -d
```

Install dependencies:

```bash
pip install -r infra/requirements.txt
```

Seed PostgreSQL:

```bash
python ingestion/setup_postgres.py
```

Run Kafka producers:

```bash
python ingestion/producer_jira.py
python ingestion/producer_confluence.py
```

Start Bronze Streaming:

```bash
python streaming/spark_bronze.py
```

Run Batch Pipeline:

```bash
python glue_jobs/aurora_to_s3.py

python streaming/spark_silver.py

python gold/team_cost_summary.py
```

Run Data Quality:

```bash
python quality/expectations_silver.py
```

---

# 📁 Project Structure

```text
atlaspulse/
│
├── airflow/
│   └── dags/
│
├── glue_jobs/
│
├── gold/
│
├── ingestion/
│
├── infra/
│
├── quality/
│
├── streaming/
│
├── README.md
│
└── .gitignore
```

---

# 🎯 Learning Outcomes

This project demonstrates practical experience with:

- Real-time Streaming Pipelines
- Event-Driven Architecture
- Delta Lake Medallion Design
- Production ETL Patterns
- Spark Structured Streaming
- Cloud Data Lakes
- Data Quality Engineering
- Workflow Orchestration
- Business Intelligence Pipelines
- Docker-based Infrastructure

---

# 📖 Why AtlasPulse?

Modern SaaS companies generate millions of product events every day. This project simulates how organizations like Atlassian transform those raw application events into reliable business insights using streaming data pipelines, data lakes, automated quality checks, and interactive dashboards.

AtlasPulse showcases the end-to-end lifecycle of production-grade data engineering—from ingestion to analytics—following industry best practices used in large-scale cloud environments.
