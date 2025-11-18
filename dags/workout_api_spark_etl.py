import os
from datetime import datetime
from instructor_workout.etl.spark_session import get_spark
from instructor_workout.etl.schema import WORKOUT_SCHEMA

from prefect import flow, task

from instructor_workout.etl.spark_session import get_spark
from instructor_workout.etl.schema import WORKOUT_SCHEMA


BUCKET = os.getenv("MINIO_BUCKET", "instructor-workout")


@task
def extract_workouts_from_csv(csv_path: str) -> str:
    """
    Simula a extração de uma API usando o CSV como fonte.
    Retorna o caminho temporário onde os dados foram salvos em Parquet local,
    apenas para demonstração.
    """
    spark = get_spark("extract_workouts_from_csv")

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(WORKOUT_SCHEMA)
        .load(csv_path)
    )

    # Apenas para debug: mostrar schema e algumas linhas
    df.printSchema()
    df.show(5, truncate=False)

    # Neste exemplo, vamos retornar o caminho de uma view temporária ou apenas um nome lógico,
    # mas como o Spark funciona com DataFrame, na prática passamos o caminho do raw em MinIO
    # na próxima etapa.
    # Para já encaixar com o próximo passo, vamos só devolver o DataFrame como "tabela temporária".
    df.createOrReplaceTempView("workouts_raw")

    # Aqui poderíamos escrever diretamente no MinIO, mas vamos deixar para a próxima task.
    return "workouts_raw"


@task
def write_raw_to_minio(temp_view_name: str) -> str:
    """
    Lê a view temporária criada na etapa anterior e escreve em raw no MinIO.
    Retorna o path s3a:// que foi escrito.
    """
    spark = get_spark("write_raw_to_minio")

    df = spark.table(temp_view_name)

    today = datetime.utcnow()
    raw_path = (
        f"s3a://{BUCKET}/raw/workouts/"
        f"year={today.year}/month={today.month:02d}/day={today.day:02d}"
    )

    (
        df.write.mode("overwrite")
        .format("parquet")
        .save(raw_path)
    )

    print(f"[RAW] Dados escritos em: {raw_path}")
    return raw_path


@task
def silver_transform(raw_path: str) -> str:
    """
    Lê o raw do MinIO, aplica uma transformação simples e grava em silver.
    """
    spark = get_spark("silver_transform")

    df_raw = spark.read.parquet(raw_path)

    # Exemplo de transformação simples:
    # - Normalizar colunas numéricas (remover nulos)
    # - Limpar espaços
    df_silver = (
        df_raw
        .withColumnRenamed("title", "workout_title")
        .withColumnRenamed("exercise_title", "exercise_name")
        # Aqui entrariam as regras de negócio de limpeza/normalização
    )

    today = datetime.utcnow()
    silver_path = (
        f"s3a://{BUCKET}/silver/workouts/"
        f"year={today.year}/month={today.month:02d}/day={today.day:02d}"
    )

    (
        df_silver.write.mode("overwrite")
        .format("parquet")
        .save(silver_path)
    )

    print(f"[SILVER] Dados escritos em: {silver_path}")
    return silver_path


@flow(name="workout_api_spark_etl")
def workout_api_spark_etl_flow(
    csv_path: str = "data/workout_data.csv",
):
    """
    Flow Prefect que:
    1) Lê o CSV (por enquanto simulando a API)
    2) Escreve em raw no MinIO
    3) Aplica transform simples e escreve silver
    """
    temp_view = extract_workouts_from_csv(csv_path)
    raw_path = write_raw_to_minio(temp_view)
    silver_path = silver_transform(raw_path)

    return {
        "temp_view": temp_view,
        "raw_path": raw_path,
        "silver_path": silver_path,
    }


if __name__ == "__main__":
    # Execução direta para teste local
    result = workout_api_spark_etl_flow()
    print(result)
