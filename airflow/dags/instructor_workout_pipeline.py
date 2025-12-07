from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "kingston",
    "depends_on_past": False,
    "retries": 0, 
}

with DAG(
    dag_id="instructor_workout_pipeline",
    default_args=default_args,
    description="Pipeline completo Bronze → Silver → Gold",
    schedule_interval="*/30 * * * *",  # 🔥 RODA AUTOMATICAMENTE A CADA 30 MINUTOS
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    create_lake = BashOperator(
        task_id="create_lake_structure",
        bash_command="python /opt/airflow/scripts/create_data_lake_structure_s3.py"
    )

    ingest_hevy = BashOperator(
        task_id="ingest_hevy",
        bash_command="python /opt/airflow/scripts/hevy_ingest_incremental_s3.py"
    )

    ingest_kaggle = BashOperator(
        task_id="ingest_kaggle",
        bash_command="python /opt/airflow/scripts/kaggle_ingest_s3.py"
    )

    ingest_users_form = BashOperator(
        task_id="ingest_users_form",
        bash_command="python /opt/airflow/scripts/users_form_ingest_s3.py"
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="python /opt/airflow/scripts/silver_transform.py"
    )

    gold_metrics = BashOperator(
        task_id="gold_metrics",
        bash_command="python /opt/airflow/scripts/gold_metrics.py"
    )

    create_lake >> [ingest_hevy, ingest_kaggle, ingest_users_form] >> silver_transform >> gold_metrics
