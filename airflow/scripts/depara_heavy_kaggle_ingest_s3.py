# scripts/depara_heavy_kaggle_ingest_s3.py
import os
from instructor_workout.etl.utils.aws_client import get_s3

BUCKET = "instructor-workout-datas"

# Caminho dentro do container (já que você monta ../data -> /opt/airflow/../data)
LOCAL_PATH = "/opt/airflow/../data/bronze/hevy/depara_manual_heavy_kaggle.csv"

# Onde isso vai ficar no S3 (bronze)
BRONZE_KEY = "bronze/raw/hevy/depara_manual_heavy_kaggle.csv"


def ingest_depara_heavy_kaggle():
    print("=== 📥 Ingestão de-para Heavy ↔ Kaggle → S3 Bronze ===")

    if not os.path.exists(LOCAL_PATH):
        raise FileNotFoundError(
            f"❌ Arquivo não encontrado: {LOCAL_PATH}. "
            "Confere se o volume ../data está montado e o caminho está certo."
        )

    s3 = get_s3()

    print(f"➡️ Enviando {LOCAL_PATH} para s3://{BUCKET}/{BRONZE_KEY}")
    s3.upload_file(LOCAL_PATH, BUCKET, BRONZE_KEY)

    print("✅ de-para enviado para o Bronze com sucesso!")


if __name__ == "__main__":
    ingest_depara_heavy_kaggle()
