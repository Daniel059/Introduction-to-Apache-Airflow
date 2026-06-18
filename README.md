# 🌬️ Apache Airflow — Introduction & DAG Fundamentals

> **ALX Data Engineering Programme** | Module: Workflow Orchestration  
> **Author:** Daniel Nzioki Musyoka | [GitHub: Daniel059](https://github.com/Daniel059)  
> **Date:** June 2026

---

## 📋 Table of Contents

- [Overview](#overview)
- [What is Apache Airflow?](#what-is-apache-airflow)
- [Directed Acyclic Graphs (DAGs)](#directed-acyclic-graphs-dags)
- [Airflow Architecture](#airflow-architecture)
- [Key Concepts](#key-concepts)
- [The Airflow UI](#the-airflow-ui)
- [Writing Your First DAG](#writing-your-first-dag)
- [Scheduling Reference](#scheduling-reference)
- [Operators Reference](#operators-reference)
- [Task States Reference](#task-states-reference)
- [Real-World Application](#real-world-application)
- [Review Questions](#review-questions)
- [Resources](#resources)

---

## Overview

This repository documents my learning from **Module 1: Introduction to Apache Airflow** as part of the ALX Data Engineering Programme. It covers the core concepts needed to build, schedule, and monitor data pipelines using Airflow and Directed Acyclic Graphs (DAGs).

**What you will find here:**
- Conceptual notes on DAGs and Airflow architecture
- Annotated first DAG code example
- Scheduling, operator, and task state reference tables
- Real-world connections to ETL work at Kenya Revenue Authority (KRA)

---

## What is Apache Airflow?

Apache Airflow is an open-source platform for **authoring, scheduling, and monitoring** data workflows (pipelines). It was created at Airbnb in 2014 and is now maintained by the Apache Software Foundation.

### The problem it solves

Before Airflow, data teams managed pipelines with **cron jobs and bash scripts**. The issues:

| Problem | Impact |
|---|---|
| No retry logic | A failed script at 2 AM goes unnoticed until someone checks manually |
| No visibility | Impossible to see which step failed or read logs centrally |
| No dependency management | Cron cannot say "only run step 2 after step 1 succeeds" |
| No backfilling | Running pipelines for past dates requires custom scripting |

Airflow solves all of these with a single, code-based, visually-monitored system.

### Core principle

> Write your pipeline as Python code. Airflow handles running it, retrying failures, and showing you exactly what happened.

---

## Directed Acyclic Graphs (DAGs)

A **DAG** is the central concept in Airflow. Every pipeline you write is a DAG.

```
DAG = Directed + Acyclic + Graph
```

| Term | Meaning | Why it matters |
|---|---|---|
| **Directed** | Each task has a clear direction — A must run before B | Tasks always execute in a defined, predictable order |
| **Acyclic** | No loops allowed — the pipeline cannot circle back | Guarantees the pipeline always has a defined end point |
| **Graph** | A network of tasks (nodes) connected by arrows (edges) | Enables parallel execution of independent tasks |

### Visual example — a simple ETL pipeline

```
[Extract] ──► [Transform] ──► [Load] ──► [Notify]
    Pull           Clean &        Write       Send
  from API         reshape     to warehouse   alert
```

Each arrow represents a **dependency**. Airflow will not start `Transform` until `Extract` succeeds.

### DAG properties that matter

```python
with DAG(
    dag_id="kra_vat_pipeline",
    schedule_interval="0 6 * * *",   # 6 AM daily
    start_date=datetime(2026, 1, 1),
    catchup=False,                   # don't backfill missed runs
    retries=2,                       # retry failed tasks twice
) as dag:
    ...
```

| Property | What it does |
|---|---|
| `catchup=False` | Prevents Airflow from running for every missed interval since `start_date` — always set this in development |
| `retries` | Auto-retries a failed task before marking it as failed |
| Parallel execution | Independent tasks with no dependency between them run simultaneously |
| Backfilling | Re-run a DAG for historical dates — useful when adding new reports that need past data |

---

## Airflow Architecture

Airflow is a system of **five cooperating components**. Understanding each one makes debugging much easier.

```
┌─────────────────────────────────────────────────────────┐
│                    Web Server (UI)                       │
│              localhost:8080 — monitoring only            │
└────────────────────────┬────────────────────────────────┘
                         │ reads state
┌────────────────────────▼────────────────────────────────┐
│                   Metadata Database                      │
│        PostgreSQL — stores all state & run history       │
└────────────┬───────────────────────┬────────────────────┘
             │ reads/writes          │ reads/writes
┌────────────▼────────┐   ┌─────────▼──────────────────── ┐
│     Scheduler       │   │         Executor               │
│  Decides what runs  │──►│  Decides how tasks are run     │
│  and when           │   │  (Local / Celery / Kubernetes) │
└─────────────────────┘   └──────────────┬─────────────────┘
                                         │ dispatches to
                              ┌──────────▼──────────┐
                              │       Workers        │
                              │  Execute task code   │
                              │  Write logs, report  │
                              └─────────────────────-┘
```

### Component breakdown

| Component | Analogy | Role |
|---|---|---|
| **Scheduler** | Factory floor manager | Monitors all DAGs; when a task's dependencies are met and schedule time arrives, it queues the task |
| **Executor** | Staffing strategy | Determines *how* tasks are distributed — LocalExecutor (one machine) vs CeleryExecutor (many machines) |
| **Workers** | Factory workers | The actual processes that run your Python/Bash/SQL code, write logs, and report results |
| **Web Server** | Monitoring dashboard | The Airflow UI — reads state from the metadata DB and displays it; does *not* run tasks |
| **Metadata DB** | Company records | A PostgreSQL database storing DAG run history, task states, and configuration |

> **Docker context:** When running Airflow via Docker Compose, each of these five components runs as a separate container. A scheduler container consuming high CPU is the Scheduler component above doing its continuous monitoring work.

---

## Key Concepts

### Tasks and Operators

A **task** is one unit of work in a DAG. Each task is built using an **operator** — a reusable template defining the type of work.

```python
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# PythonOperator — run any Python function
extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=my_extract_function,
)

# BashOperator — run a shell command
check_file = BashOperator(
    task_id="check_file_exists",
    bash_command="ls /data/input/ | grep .csv",
)

# PostgresOperator — run SQL
load_task = PostgresOperator(
    task_id="load_to_warehouse",
    postgres_conn_id="postgres_default",
    sql="INSERT INTO fact_sales SELECT * FROM staging_sales;",
)
```

### Setting Task Dependencies

```python
# Method 1: Bitshift operator (most common)
t1 >> t2 >> t3          # t1 → t2 → t3 (sequential)

# Method 2: Multiple dependencies
[t1, t2] >> t3          # t1 AND t2 must finish before t3

# Method 3: Fan-out
t1 >> [t2, t3]          # t1 finishes, then t2 and t3 run in parallel

# Method 4: set_downstream / set_upstream
t1.set_downstream(t2)   # equivalent to t1 >> t2
```

---

## The Airflow UI

The web interface at `http://localhost:8080` is your main tool for monitoring pipelines. No command line needed for day-to-day operations.

### The three views you will use daily

| View | How to access | What it shows |
|---|---|---|
| **DAGs list** | Home page | All DAGs, schedule, last run status, pause/unpause toggle |
| **Grid view** | Click a DAG name | Every run as a column, every task as a row — colour-coded by state |
| **Task log** | Click a task → "Log" | Full stdout output — where you find errors and debug |

### Daily monitoring workflow

```
DAGs list                Grid view               Task log
─────────────            ──────────────          ──────────────────────
Are all pipelines   →    Which specific     →    Why did that
healthy? Any red?        run/task failed?         specific task fail?
```

### Other useful UI features

| Feature | How to use |
|---|---|
| **Graph view** | Visual DAG structure — boxes, arrows, current state colours |
| **Trigger DAG** | Play button — runs immediately outside schedule, useful for testing |
| **Clear task** | Right-click a task → Clear — resets state so scheduler re-runs it without rerunning the whole DAG |
| **Pause toggle** | Left side of DAGs list — pauses/unpauses a DAG without deleting it |

---

## Writing Your First DAG

Every Airflow DAG is a `.py` file saved in the `dags/` folder. The scheduler scans this folder automatically — no manual registration needed.

### Full annotated example

```python
# ── PART 1: Imports ────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# ── PART 2: DAG object ─────────────────────────────────────────
with DAG(
    dag_id="my_first_dag",          # unique name — appears in UI
    schedule_interval="@daily",     # cron expression or shortcut
    start_date=datetime(2026, 1, 1),
    catchup=False,                  # don't run for missed past dates
    tags=["learning", "example"],   # optional — for filtering in UI
) as dag:

    # ── PART 3: Task definitions ───────────────────────────────
    def extract():
        print("Pulling data from source system...")
        # Your extraction logic here

    def transform():
        print("Cleaning and reshaping data...")
        # Your transformation logic here

    def load():
        print("Writing to data warehouse...")
        # Your loading logic here

    t1 = PythonOperator(task_id="extract",   python_callable=extract)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load",      python_callable=load)

    # ── PART 4: Dependencies ───────────────────────────────────
    t1 >> t2 >> t3    # extract → transform → load
```

### DAG parameter reference

| Parameter | Type | Description |
|---|---|---|
| `dag_id` | `str` | Unique identifier. Shown in the UI. Use underscores, no spaces. |
| `schedule_interval` | `str` | Cron expression or shortcut (`@daily`, `@hourly`). Use `None` for manual-only. |
| `start_date` | `datetime` | When scheduling begins. Set to a past date. |
| `catchup` | `bool` | `False` = only run going forward. `True` = backfill all missed intervals. |
| `retries` | `int` | How many times to retry a failed task before giving up. |
| `retry_delay` | `timedelta` | How long to wait between retries. |
| `tags` | `list[str]` | Optional labels for filtering DAGs in the UI. |

### ⚠️ Common beginner mistakes

```python
# ❌ WRONG — never name your file airflow.py
# Python imports itself instead of the Airflow library → confusing ImportError

# ❌ WRONG — catchup defaults to True in older Airflow versions
with DAG(dag_id="my_dag", start_date=datetime(2024, 1, 1)) as dag:
    # This will try to run for every day since Jan 2024!
    ...

# ✅ CORRECT — always set catchup explicitly
with DAG(dag_id="my_dag", start_date=datetime(2024, 1, 1), catchup=False) as dag:
    ...

# ❌ WRONG — circular dependency (Airflow will reject this DAG)
t1 >> t2 >> t1  # t1 depends on t2 which depends on t1 = loop!

# ❌ WRONG — task_id must be unique within a DAG
t1 = PythonOperator(task_id="process", ...)
t2 = PythonOperator(task_id="process", ...)  # duplicate ID!
```

---

## Scheduling Reference

### Cron expression format

```
┌─────── minute (0–59)
│ ┌───── hour (0–23)
│ │ ┌─── day of month (1–31)
│ │ │ ┌─ month (1–12)
│ │ │ │ ┌ day of week (0–6, Sunday=0)
│ │ │ │ │
* * * * *
```

### Common schedules

| Expression | Meaning |
|---|---|
| `@hourly` / `"0 * * * *"` | Every hour at :00 |
| `@daily` / `"0 0 * * *"` | Every day at midnight |
| `@weekly` / `"0 0 * * 0"` | Every Sunday at midnight |
| `@monthly` / `"0 0 1 * *"` | 1st of every month at midnight |
| `"0 6 * * *"` | Every day at 6:00 AM |
| `"0 6 * * 1-5"` | Every weekday at 6:00 AM |
| `"0 */6 * * *"` | Every 6 hours |
| `"30 7 * * 1"` | Every Monday at 7:30 AM |
| `None` | No schedule — manual trigger only |

---

## Operators Reference

| Operator | Import | Use case |
|---|---|---|
| `PythonOperator` | `airflow.operators.python` | Run any Python function — most flexible |
| `BashOperator` | `airflow.operators.bash` | Run shell commands or scripts |
| `PostgresOperator` | `airflow.providers.postgres.operators.postgres` | Execute SQL on PostgreSQL |
| `OracleOperator` | `airflow.providers.oracle.operators.oracle` | Execute SQL on Oracle (e.g. KRA iTax) |
| `EmailOperator` | `airflow.operators.email` | Send email notifications |
| `DummyOperator` | `airflow.operators.empty` | Placeholder / logical grouping |
| `BranchPythonOperator` | `airflow.operators.python` | Conditional branching — run different paths based on logic |
| `HttpOperator` | `airflow.providers.http.operators.http` | Make HTTP API calls |

---

## Task States Reference

These are the colours you see in the Airflow UI Grid and Graph views.

| State | Colour | Meaning | Action to take |
|---|---|---|---|
| `success` | 🟢 Green | Task completed without errors | Nothing — all good |
| `failed` | 🔴 Red | Task threw an exception | Check the task log |
| `running` | 🔵 Light blue | Currently executing on a worker | Wait or monitor |
| `queued` | 🟤 Brown | Waiting for an available worker | Check executor capacity |
| `upstream_failed` | 🟠 Orange | A dependency task failed, so this was skipped | Fix the upstream task first |
| `skipped` | 🩷 Pink | Bypassed due to branching logic | Expected if using BranchOperator |
| `no status` | ⬜ White | Never been run | Trigger the DAG |

---

## Real-World Application

### Mapping today's concepts to KRA work

The ETL workflows at Kenya Revenue Authority follow the exact DAG pattern covered in this module:

```python
# Conceptual representation of a KRA VAT pipeline as an Airflow DAG

with DAG(
    dag_id="kra_vat_monthly_pipeline",
    schedule_interval="0 5 1 * *",   # 5 AM on the 1st of each month
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Extract VAT return data from iTax Oracle DB (KRA_INT schema)
    extract = OracleOperator(
        task_id="extract_vat_returns",
        oracle_conn_id="itax_oracle",
        sql="""
            SELECT taxpayer_pin, return_period, vat_payable
            FROM KRA_INT.V_ESK_I_REGISTRATION_I
            WHERE return_period = '{{ ds_nodash[:6] }}'
        """,
    )

    # Transform and aggregate
    transform = PythonOperator(
        task_id="transform_vat_data",
        python_callable=aggregate_vat_by_sector,
    )

    # Load results to Power BI dataset
    load = PythonOperator(
        task_id="refresh_powerbi_dashboard",
        python_callable=push_to_powerbi,
    )

    # Notify stakeholders
    notify = EmailOperator(
        task_id="notify_team",
        to=["analytics@kra.go.ke"],
        subject="Monthly VAT Pipeline — Complete",
        html_content="The VAT analytics dashboard has been refreshed.",
    )

    extract >> transform >> load >> notify
```

### Why this matters for data engineering

- **Observability:** Every run is logged. If the VAT extraction fails at 5 AM, you see it immediately in the Airflow UI instead of discovering it when a stakeholder asks why the dashboard is stale.
- **Reliability:** Automatic retries mean transient Oracle connection errors don't require manual intervention.
- **Auditability:** Full run history — critical in a tax authority context where data lineage matters.

---

## Review Questions

Test yourself after going through the module:

1. What does the "A" in DAG stand for, and why does it matter for pipelines?
2. Name the five components of Airflow and the role each one plays.
3. What is the difference between an **Executor** and a **Worker**?
4. What does `catchup=False` do, and why should you always set it in development?
5. Write the cron expression for a DAG that runs every weekday at 7:30 AM.
6. A task shows `upstream_failed` in the UI. What does this mean? What should you check first?
7. You add a new `.py` file to the `dags/` folder. What else do you need to do to register it?
8. What is the `>>` operator used for in a DAG file?
9. What is the difference between **Grid view** and **Graph view** in the Airflow UI?
10. Why should you never name your DAG file `airflow.py`?


## Repository Structure

```
apache-airflow-intro/
│
├── README.md                    # This file — full module notes
├── dags/
│   ├── my_first_dag.py          # Annotated beginner DAG
│   └── kra_vat_pipeline.py      # Real-world example DAG
├── notes/
│   └── Apache_Airflow_Beginner_Notes.docx
└── .gitignore
```

---

*Part of my Data Engineering learning journey — follow along on [LinkedIn](https://www.linkedin.com/in/daniel-nzioki-musyoka/) 