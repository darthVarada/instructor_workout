import os
import time
from pyspark.sql import SparkSession


def get_spark():
    return (
        SparkSession.builder.appName("spark_minio_test")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def test_spark_minio():
    print("\n🚀 Iniciando teste PySpark + MinIO...\n")
    t0_total = time.perf_counter()

    # 1) Criar SparkSession
    print("⏳ Criando SparkSession...")
    t0_spark = time.perf_counter()
    spark = get_spark()
    t1_spark = time.perf_counter()
    print(f"✔ SparkSession criada com sucesso! (tempo: {t1_spark - t0_spark:.2f}s)\n")

    # 2) Criar DataFrame simples
    print("⏳ Criando DataFrame de teste...")
    t0_df = time.perf_counter()
    df = spark.createDataFrame(
        [("Victor", 28), ("Lucas", 32)],
        ["nome", "idade"],
    )
    t1_df = time.perf_counter()
    print(f"✔ DataFrame criado (tempo: {t1_df - t0_df:.2f}s)")
    df.show()

    bucket = os.getenv("MINIO_BUCKET", "bronze")
    path = f"s3a://{bucket}/spark_test/df.parquet"

    # 3) Escrever no MinIO
    print("\n⏳ Escrevendo DataFrame no MinIO...")
    t0_write = time.perf_counter()
    df.write.mode("overwrite").parquet(path)
    t1_write = time.perf_counter()
    print(f"✔ DataFrame escrito no MinIO em: {path} (tempo: {t1_write - t0_write:.2f}s)")

    # 4) Ler novamente do MinIO
    print("\n⏳ Lendo DataFrame de volta do MinIO...")
    t0_read = time.perf_counter()
    df_read = spark.read.parquet(path)
    t1_read = time.perf_counter()
    print(f"✔ DataFrame lido novamente do MinIO (tempo: {t1_read - t0_read:.2f}s)")
    df_read.show()

    t1_total = time.perf_counter()
    print(f"\n🎉 TESTE COMPLETO: Spark + MinIO funcionando!")
    print(f"⏱ Tempo total do teste: {t1_total - t0_total:.2f}s\n")


if __name__ == "__main__":
    test_spark_minio()
