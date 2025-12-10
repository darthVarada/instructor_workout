from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="dbt_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "duckdb", "s3"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test",
    )

    dbt_run >> dbt_test
