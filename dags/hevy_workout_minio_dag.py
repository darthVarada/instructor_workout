# dags/hevy_workout_minio_dag.py
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajuste o caminho dos imports conforme a estrutura exata do projeto no Airflow
from instructor_workout.etl.ingestion.hevy_ingest_incremental_minio import (
    main as hevy_ingest_main,
)
from instructor_workout.etl.upload_silver_to_minio import (
    main as upload_silver_main,
)


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hevy_workout_minio_etl",
    description="Ingestão incremental da Hevy + upload da camada Silver para MinIO",
    default_args=default_args,
    schedule_interval="0 5 * * *",  # todos os dias às 05:00 (UTC)
    start_date=datetime(2025, 11, 24),
    catchup=False,
    max_active_runs=1,
    tags=["hevy", "workout", "minio", "bronze-silver"],
) as dag:

    ingest_bronze = PythonOperator(
        task_id="hevy_ingest_incremental_minio",
        python_callable=hevy_ingest_main,
    )

    upload_silver = PythonOperator(
        task_id="upload_silver_to_minio",
        python_callable=upload_silver_main,
    )

    # Ordem de execução:
    ingest_bronze >> upload_silver
