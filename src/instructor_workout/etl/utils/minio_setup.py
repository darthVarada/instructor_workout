import os
import boto3
from botocore.exceptions import ClientError


def get_s3_client():
    """Retorna cliente S3 configurado para MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1"
    )


def ensure_bucket(s3, bucket_name: str):
    """Cria bucket no MinIO se não existir."""
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"• Bucket '{bucket_name}' já existe.")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket", "NotFound"):
            print(f"→ Criando bucket '{bucket_name}'...")
            s3.create_bucket(Bucket=bucket_name)
            print(f"✓ Bucket criado: {bucket_name}")
        else:
            raise


def ensure_buckets_exist():
    """Cria todos os buckets necessários para bronze, silver e gold."""
    s3 = get_s3_client()

    buckets = [
        os.getenv("MINIO_BRONZE_BUCKET", "bronze"),
        os.getenv("MINIO_SILVER_BUCKET", "silver"),
        os.getenv("MINIO_GOLD_BUCKET", "gold"),
    ]

    print("🔍 Verificando buckets no MinIO...\n")
    for b in buckets:
        ensure_bucket(s3, b)
    print("\n✔ Todos os buckets estão prontos!\n")
