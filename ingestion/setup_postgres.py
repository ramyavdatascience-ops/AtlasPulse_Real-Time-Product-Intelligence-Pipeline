import psycopg2
from dotenv import load_dotenv
import os
import random
from datetime import datetime, timezone

load_dotenv()

# ── connect to PostgreSQL ──────────────────────────────────────
conn = psycopg2.connect(
    host=os.getenv("AURORA_HOST"),
    dbname=os.getenv("AURORA_DB"),
    user=os.getenv("AURORA_USER"),
    password=os.getenv("AURORA_PASSWORD"),
    port=os.getenv("AURORA_PORT", 5432)
)
cursor = conn.cursor()
print("✅ Connected to PostgreSQL\n")

# ══════════════════════════════════════════════════════════════
# CREATE TABLES
# ══════════════════════════════════════════════════════════════

# ── teams table ───────────────────────────────────────────────
# Stores team-level metadata — plan tier, monthly budget, region.
# This is the kind of data that lives in an operational DB like Aurora,
# not in a data lake — it changes slowly and is managed by the app.
cursor.execute("""
    DROP TABLE IF EXISTS teams CASCADE;
    CREATE TABLE teams (
        team_id          VARCHAR(20)    PRIMARY KEY,
        team_name        VARCHAR(100)   NOT NULL,
        department       VARCHAR(100),
        plan_tier        VARCHAR(20),
        monthly_budget   DECIMAL(10,2),
        region           VARCHAR(50),
        created_at       TIMESTAMP      DEFAULT NOW(),
        is_active        BOOLEAN        DEFAULT TRUE
    );
""")
print("✅ teams table created")

# ── users table ───────────────────────────────────────────────
# Stores user-level metadata — role, seniority, which team they belong to.
cursor.execute("""
    DROP TABLE IF EXISTS users CASCADE;
    CREATE TABLE users (
        user_id          VARCHAR(20)    PRIMARY KEY,
        team_id          VARCHAR(20)    REFERENCES teams(team_id),
        full_name        VARCHAR(100),
        role             VARCHAR(50),
        seniority        VARCHAR(20),
        is_active        BOOLEAN        DEFAULT TRUE,
        joined_at        TIMESTAMP      DEFAULT NOW()
    );
""")
print("✅ users table created")

# ── projects table ────────────────────────────────────────────
cursor.execute("""
    DROP TABLE IF EXISTS projects CASCADE;
    CREATE TABLE projects (
        project_key      VARCHAR(20)    PRIMARY KEY,
        team_id          VARCHAR(20)    REFERENCES teams(team_id),
        project_name     VARCHAR(100),
        status           VARCHAR(20),
        created_at       TIMESTAMP      DEFAULT NOW()
    );
""")
print("✅ projects table created\n")

# ══════════════════════════════════════════════════════════════
# SEED DATA — realistic Atlassian-style metadata
# ══════════════════════════════════════════════════════════════

# ── insert teams ──────────────────────────────────────────────
teams = [
    ("team_1", "Platform Engineering", "Engineering",    "Enterprise", 8000.00, "APAC"),
    ("team_2", "Jira Cloud Core",      "Product",        "Enterprise", 9500.00, "EMEA"),
    ("team_3", "Confluence Content",   "Product",        "Business",   5000.00, "APAC"),
    ("team_4", "Bitbucket DevOps",     "Engineering",    "Business",   6000.00, "AMER"),
    ("team_5", "Data & Analytics",     "Data",           "Enterprise", 7500.00, "APAC"),
    ("team_6", "Mobile Apps",          "Engineering",    "Business",   4500.00, "EMEA"),
]

cursor.executemany("""
    INSERT INTO teams
        (team_id, team_name, department, plan_tier, monthly_budget, region)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (team_id) DO NOTHING;
""", teams)
print(f"✅ {len(teams)} teams inserted")

# ── insert users ──────────────────────────────────────────────
roles      = ["Engineer", "Senior Engineer", "Staff Engineer",
              "Product Manager", "Data Analyst", "Tech Lead"]
seniority  = ["Junior", "Mid", "Senior", "Staff", "Principal"]
team_ids   = [t[0] for t in teams]

users = []
for i in range(1, 201):
    users.append((
        f"user_{i}",
        random.choice(team_ids),
        f"User {i}",
        random.choice(roles),
        random.choice(seniority),
    ))

cursor.executemany("""
    INSERT INTO users
        (user_id, team_id, full_name, role, seniority)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO NOTHING;
""", users)
print(f"✅ {len(users)} users inserted")

# ── insert projects ───────────────────────────────────────────
projects = [
    ("PLAT-001", "team_1", "Infrastructure Modernisation", "active"),
    ("PLAT-002", "team_1", "Cost Optimisation Q3",         "active"),
    ("JIRA-001", "team_2", "Jira Cloud Migration",         "active"),
    ("JIRA-002", "team_2", "AI Features Rollout",          "active"),
    ("CONF-001", "team_3", "Docs Platform Rewrite",        "active"),
    ("BDEV-001", "team_4", "CI/CD Pipeline Upgrade",       "active"),
    ("DATA-001", "team_5", "AtlasPulse Pipeline",          "active"),
    ("DATA-002", "team_5", "ML Feature Store",             "active"),
    ("MOBL-001", "team_6", "iOS App Redesign",             "active"),
    ("MOBL-002", "team_6", "Android Performance",          "active"),
]

cursor.executemany("""
    INSERT INTO projects
        (project_key, team_id, project_name, status)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (project_key) DO NOTHING;
""", projects)
print(f"✅ {len(projects)} projects inserted")

conn.commit()
cursor.close()
conn.close()

print("\n" + "="*50)
print("✅ PostgreSQL seed complete")
print("   Tables: teams, users, projects")
print("   Rows:   6 teams, 200 users, 10 projects")
print("="*50)