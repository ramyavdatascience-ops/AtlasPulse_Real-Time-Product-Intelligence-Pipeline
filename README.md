markdown# 
AtlasPulse — Real-Time Product Intelligence Pipeline

A real-time data engineering pipeline that simulates Jira/Confluence-style product event tracking at scale — built end-to-end with Kafka, Spark Structured Streaming, Delta Lake, PostgreSQL, Airflow, and Grafana.

> Built as a portfolio project to demonstrate production-grade streaming + batch data engineering patterns used at companies like Atlassian.

---

## Architecture
Python Producers (Jira/Confluence simulated events)
│
▼
Apache Kafka (Docker)
│
▼
PySpark Structured Streaming  ──────►  Bronze Delta Lake (AWS S3)
│
▼
Silver Delta Lake (AWS S3)
• Quarantine validation
• Deduplication audit trail
• Session windowing
│
PostgreSQL (team/user metadata)              │
│                                    │
▼                                    │
Glue-style ETL ──────► S3 Staging ───────────┤
▼
Gold Delta Lake (AWS S3)
TEAM_COST_SUMMARY
│
┌───────────────┼───────────────┐
▼               ▼               ▼
PostgreSQL      Great Expectations  Apache Airflow
(for Grafana)   (8-check DQ suite)  (orchestration)
│
▼
Grafana Dashboard
(live, 4 panels)

---

## What this project demonstrates

| Layer | Technology | What it does |
|---|---|---|
| Ingestion | Apache Kafka (Docker) | Simulates real-time Jira/Confluence event streams |
| Stream Processing | PySpark Structured Streaming | Exactly-once, checkpointed ingestion into Delta Lake |
| Storage | Delta Lake on AWS S3 | ACID-compliant Bronze/Silver/Gold medallion architecture |
| Data Quality | Quarantine tables + Great Expectations | Bad rows routed, not dropped; 8-point automated DQ suite |
| Feature Engineering | PySpark session windowing | Converts raw events into business-meaningful user sessions |
| Relational Source | PostgreSQL | Operational metadata (teams, users, projects) — Aurora-equivalent |
| ETL | Glue-style PySpark job | Extracts PostgreSQL → lands in S3 as Delta Lake staging |
| Aggregation | Gold layer join | Streaming events + relational metadata → TEAM_COST_SUMMARY |
| Orchestration | Apache Airflow (Docker) | DAG: Glue ETL → Silver → Gold, scheduled daily |
| Visualization | Grafana | Live dashboard — spend by team, budget gauges, full summary table |

---

## Pipeline Layers Explained

**Bronze** — Raw events exactly as received from Kafka, written via streaming with checkpointing for exactly-once semantics. No transformations — this is the immutable source of truth.

**Silver** — Cleaned and validated data. Invalid rows are routed to a separate quarantine Delta table (not dropped) with a logged reason for failure. Deduplication is measured and logged. Events are grouped into user sessions using Spark's `session_window` function.

**Gold** — Business-ready aggregates. Joins streaming Silver events with relational team metadata (extracted from PostgreSQL via a Glue-style ETL job) to produce `TEAM_COST_SUMMARY` — showing AI spend, budget utilization, and cost tier per team.

---

## Data Quality

An 8-check automated suite validates the Silver layer before Gold can run:

- `event_id` never null
- `event_type` contains only known values
- `response_time_ms` within realistic bounds
- `cost_estimate_usd` never negative
- `is_ai_event` never null
- No duplicate `event_id`s post-deduplication
- Minimum row count threshold
- AI/non-AI cost consistency check

Results are logged to a timestamped JSON report for audit trail. If any check fails, the pipeline halts before Gold runs — preventing bad data from reaching the dashboard.

**Current result: 8/8 checks passed — 100% data quality score**

---

## Dashboard

Live Grafana dashboard connected to PostgreSQL, with 4 panels:

- **AI Spend by Team** — bar chart ranking teams by cost
- **Budget Used %** — gauge showing budget consumption per team
- **Team Cost Summary** — full table view (events, cost, budget, status)
- **Pipeline Totals** — stat panel for total events/calls/spend

---

## Orchestration

An Airflow DAG orchestrates the batch portion of the pipeline:
glue_etl_aurora_to_s3  →  spark_silver_transform  →  spark_gold_team_cost_summary

Scheduled to run `@daily`, with retry logic and task dependencies enforcing correct execution order. Streaming components (Kafka producers, Spark Bronze) run continuously as long-lived processes, separate from the daily batch DAG — matching how real production systems separate streaming and batch orchestration.

---

## Tech Stack
Languages       Python, SQL, PySpark
Streaming       Apache Kafka, Spark Structured Streaming
Storage         Delta Lake, AWS S3
Database        PostgreSQL
Orchestration   Apache Airflow
Data Quality    Custom PySpark expectation suite
Visualization   Grafana
Infrastructure  Docker, Docker Compose
Cloud           AWS (S3, IAM)

---

## Local Setup

All infrastructure runs via Docker — Kafka, PostgreSQL, Grafana, and Airflow.

```bash
# 1. Start infrastructure
cd infra
docker-compose up -d

# 2. Install Python dependencies
pip install -r infra/requirements.txt

# 3. Seed PostgreSQL with team/user metadata
python ingestion/setup_postgres.py

# 4. Start event producers (separate terminals)
python ingestion/producer_jira.py
python ingestion/producer_confluence.py

# 5. Start Bronze streaming layer
python streaming/spark_bronze.py

# 6. Run the batch pipeline
python glue_jobs/aurora_to_s3.py
python streaming/spark_silver.py
python gold/team_cost_summary.py

# 7. Run data quality checks
python quality/expectations_silver.py
```

Dashboards available at:
- Kafka UI → `localhost:8080`
- Grafana → `localhost:3000`
- Airflow → `localhost:8081`

---

## Project Structure
atlaspulse/
├── ingestion/          # Kafka producers + PostgreSQL seed script
├── streaming/           # Spark Bronze (streaming) + Silver (batch) jobs
├── glue_jobs/           # Glue-style ETL: PostgreSQL → S3
├── gold/                 # Gold layer: TEAM_COST_SUMMARY
├── quality/             # Data quality expectation suite
├── airflow/dags/        # Pipeline orchestration DAG
└── infra/                # Docker Compose for all services

---

## Why this project

This pipeline was built to demonstrate the architectural patterns used by companies running large-scale SaaS products — real-time event ingestion, medallion-architecture data lakes, relational-to-lake ETL, automated data quality gates, and orchestrated batch processing. It mirrors how a team like Atlassian's would track product usage (Jira/Confluence events) and translate that into business metrics like team-level AI cost attribution.
