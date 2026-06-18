"""
my_first_dag.py
───────────────
A fully annotated beginner DAG for learning Apache Airflow.
Part of: ALX Data Engineering Programme — Module: Workflow Orchestration

Author: Daniel Nzioki Musyoka
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ── Default arguments ──────────────────────────────────────────────────────────
# These apply to every task in the DAG unless overridden on individual tasks.
default_args = {
    "owner": "daniel",
    "retries": 2,                          # retry a failed task twice
    "retry_delay": timedelta(minutes=5),   # wait 5 minutes between retries
    "email_on_failure": False,             # set True in production
}

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="my_first_dag",
    description="Beginner ETL pipeline — Extract, Transform, Load",
    schedule_interval="@daily",            # runs every day at midnight
    start_date=datetime(2026, 1, 1),
    catchup=False,                         # IMPORTANT: prevents backfilling
    default_args=default_args,
    tags=["learning", "etl", "beginner"],  # used for filtering in the UI
) as dag:

    # ── Task 1: Extract ────────────────────────────────────────────────────────
    def extract():
        """
        Pulls raw data from a source system.
        In a real pipeline this might query an API or a database.
        """
        print("Extracting data from source system...")
        print("Rows pulled: 1,500")
        # In production: return data or write to a staging location

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    # ── Task 2: Transform ──────────────────────────────────────────────────────
    def transform():
        """
        Cleans and reshapes the raw data.
        In a real pipeline this might filter nulls, rename columns, aggregate.
        """
        print("Transforming data...")
        print("Rows after cleaning: 1,487")

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    # ── Task 3: Load ───────────────────────────────────────────────────────────
    def load():
        """
        Writes the transformed data to a destination.
        In a real pipeline this might insert into PostgreSQL or a data warehouse.
        """
        print("Loading data to warehouse...")
        print("Load complete.")

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    # ── Task 4: Verify (BashOperator example) ──────────────────────────────────
    t_verify = BashOperator(
        task_id="verify_load",
        bash_command='echo "Pipeline completed at $(date). All tasks succeeded."',
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    # Read: extract → transform → load → verify
    t_extract >> t_transform >> t_load >> t_verify
