from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from datetime import datetime

default_args = {
    "owner": "kingston",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="instructor_workout_pipeline",
    default_args=default_args,
    description="Pipeline completo Bronze → Silver → Gold",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # =========================
    # BRONZE
    # =========================

    create_lake = BashOperator(
        task_id="create_lake_structure",
        bash_command="python /opt/airflow/scripts/create_data_lake_structure_s3.py"
    )

    ingest_hevy = BashOperator(
        task_id="ingest_hevy",
        bash_command="python /opt/airflow/scripts/hevy_ingest_incremental_s3.py"
    )

    ingest_hevy_base = BashOperator(
        task_id="ingest_hevy_base",
        bash_command="python /opt/airflow/scripts/ingest_synthetic_base_to_bronze.py"
    )

    ingest_kaggle = BashOperator(
        task_id="ingest_kaggle",
        bash_command="python /opt/airflow/scripts/kaggle_ingest_s3.py"
    )

    ingest_users_form = BashOperator(
        task_id="ingest_users_form",
        bash_command="python /opt/airflow/scripts/users_form_ingest_s3.py"
    )

    depara_ingest = BashOperator(
        task_id="ingest_depara_heavy_kaggle",
        bash_command="python /opt/airflow/scripts/depara_heavy_kaggle_ingest_s3.py"
    )

    # =========================
    # SILVER
    # =========================

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="python /opt/airflow/scripts/silver_synthetic_transform.py"
    )

    silver_kaggle = BashOperator(
        task_id="silver_kaggle",
        bash_command="python /opt/airflow/scripts/silver_kaggle_transform.py"
    )

    silver_users = BashOperator(
        task_id="silver_users",
        bash_command="python /opt/airflow/scripts/silver_users_transform.py"
    )

    silver_depara = BashOperator(
        task_id="silver_depara_heavy_kaggle",
        bash_command="python /opt/airflow/scripts/silver_depara_heavy_kaggle_transform.py"
    )

    # =========================
    # GOLD
    # =========================

    gold_metrics = BashOperator(
        task_id="gold_metrics",
        bash_command="python /opt/airflow/scripts/gold_metrics.py"
    )

    gold_dim_exercise = BashOperator(
        task_id="gold_dim_exercise",
        bash_command="python /opt/airflow/scripts/gold_dim_exercises.py"
    )

    # =========================
    # DEPENDÊNCIAS (FORMA CORRETA)
    # =========================

    # 1️⃣ Create lake primeiro
    chain(
        create_lake,
        [ingest_hevy, ingest_kaggle, ingest_users_form, ingest_hevy_base, depara_ingest],
    )

    # 2️⃣ TODAS as ingests disparam TODAS as silvers
    for ingest in [ingest_hevy, ingest_kaggle, ingest_users_form, ingest_hevy_base, depara_ingest]:
        ingest >> [silver_transform, silver_kaggle, silver_users, silver_depara]

    # 3️⃣ TODAS as silvers disparam TODOS os golds
    for silver in [silver_transform, silver_kaggle, silver_users, silver_depara]:
        silver >> [gold_metrics, gold_dim_exercise]
