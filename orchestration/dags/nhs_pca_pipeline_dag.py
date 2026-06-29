from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "rejisha",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nhs_pca_pipeline",
    description="End-to-end NHS PCA Medallion pipeline: Bronze -> Silver -> Gold",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nhs-pca"],
) as dag:

    run_ingestion = BashOperator(
        task_id="run_ingestion",
        bash_command=f"cd {PROJECT_DIR} && python run_ingestion.py",
    )

    run_silver = BashOperator(
    task_id="run_silver",
    bash_command=f"cd {PROJECT_DIR} && python run_silver.py",
    )

    load_silver_to_sql = BashOperator(
        task_id="load_silver_to_sql",
        bash_command=f"cd {PROJECT_DIR} && python load_silver_to_sql.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT_DIR}/gold && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_DIR}/gold && dbt test",
    )

    run_ingestion >> run_silver >> load_silver_to_sql >> dbt_run >> dbt_test