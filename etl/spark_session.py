import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "instructor_workout_etl") -> SparkSession:
    """
    Cria uma SparkSession configurada para acessar o MinIO via s3a://
    usando variáveis de ambiente:

    - S3_ENDPOINT_URL (ex: http://localhost:9000)
    - MINIO_ACCESS_KEY
    - MINIO_SECRET_KEY
    """
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    return spark
