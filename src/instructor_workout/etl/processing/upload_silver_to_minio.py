# src/instructor_workout/etl/processing/upload_silver_to_minio.py

import os
from pathlib import Path
import boto3

from instructor_workout.etl.utils.minio_setup import ensure_buckets_exist


def get_s3_client():
    """Retorna cliente S3 configurado para MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
    )


def main():
    print("\n=== 🚀 Upload Silver → MinIO ===\n")

    # 1) Garante que os buckets existem
    ensure_buckets_exist()

    # 2) Lê variáveis de ambiente
    SILVER_BUCKET = os.getenv("MINIO_SILVER_BUCKET", "silver")

    # 3) Caminho local
    local_path = Path("data/silver/synthetic_realistic_workout.csv")

    if not local_path.exists():
        print(f"❌ ERRO: Arquivo não encontrado: {local_path}")
        return

    # 4) Caminho no S3/MinIO
    key = "hevy/workouts/sets/synthetic_realistic_workout.csv"

    print(f"Enviando {local_path} -> s3://{SILVER_BUCKET}/{key}")

    # 5) Cliente MinIO
    s3 = get_s3_client()

    try:
        s3.upload_file(str(local_path), SILVER_BUCKET, key)
        print(f"✓ Upload concluído com sucesso!")
    except Exception as e:
        print("\n❌ ERRO durante upload:")
        print(e)
        return


if __name__ == "__main__":
    main()
