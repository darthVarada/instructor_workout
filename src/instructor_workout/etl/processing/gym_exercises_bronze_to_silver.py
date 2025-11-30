# src/instructor_workout/etl/processing/gym_exercises_bronze_to_silver.py
import io
import os

from datetime import datetime

import pandas as pd

from instructor_workout.etl.ingestion.minio_client import get_s3_client


def bronze_to_silver_gym_exercises() -> None:
    print("=== 🚀 Transformação Bronze → Silver (Gym Exercises) ===")

    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET")
    silver_bucket = os.getenv("MINIO_SILVER_BUCKET")

    if not bronze_bucket or not silver_bucket:
        raise RuntimeError("❌ MINIO_BRONZE_BUCKET ou MINIO_SILVER_BUCKET não configurados.")

    silver_key = "gym_exercises/data.parquet"

    client = get_s3_client()

    print("➡️ Buscando arquivos no Bronze...")

    bronze_objects = client.list_objects_v2(
        Bucket=bronze_bucket,
        Prefix="gym_exercises/",
    )

    contents = bronze_objects.get("Contents", [])

    if len(contents) == 0:
        raise RuntimeError("❌ Nenhum arquivo de gym_exercises encontrado no Bronze.")

    latest_obj = sorted(contents, key=lambda x: x["LastModified"], reverse=True)[0]
    bronze_key = latest_obj["Key"]

    print(f"➡️ Último arquivo encontrado no Bronze: {bronze_key}")

    print("➡️ Baixando parquet do Bronze...")

    obj = client.get_object(Bucket=bronze_bucket, Key=bronze_key)
    parquet_bytes = obj["Body"].read()

    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    print(f"➡️ Linhas carregadas: {len(df)}")

    # ------------------------------------------------------------------------
    # 🧼 LIMPEZA / PADRONIZAÇÃO PARA SILVER
    # ------------------------------------------------------------------------

    print("➡️ Normalizando colunas...")

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )

    # Normalizar listas em colunas
    def normalize_list(col: str) -> None:
        if col not in df:
            return

        df[col] = (
            df[col]
            .astype(str)
            .str.replace("[", "", regex=False)
            .str.replace("]", "", regex=False)
            .str.replace("'", "", regex=False)
            .str.split(",")
        )

        df[col] = df[col].apply(
            lambda x: [v.strip() for v in x if v.strip()] if isinstance(x, list) else []
        )

    for col in ["primary_muscles", "secondary_muscles", "instructions"]:
        normalize_list(col)

    for col in ["body_part", "equipment", "type", "difficulty", "mechanics"]:
        if col in df:
            df[col] = df[col].astype(str).str.lower().str.strip()

    # Remover duplicados
    if "title" in df:
        df = df.drop_duplicates(subset=["title"])
    elif "exercise_name" in df:
        df = df.drop_duplicates(subset=["exercise_name"])

    # Criar ID único
    if "title" in df:
        df["exercise_id"] = (
            df["title"]
            .str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace("-", "_", regex=False)
        )
    else:
        df["exercise_id"] = df.index.astype(str)

    df["ingestion_date"] = pd.Timestamp.utcnow()

    print("✔️ Dados estruturados para Silver!")

    # Gerar Parquet em memória
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    print(f"➡️ Salvando Silver consolidado em: {silver_bucket}/{silver_key}")

    client.upload_fileobj(
        Fileobj=buffer,
        Bucket=silver_bucket,
        Key=silver_key,
    )

    print("✔️ Silver de Gym Exercises atualizado com sucesso!")


if __name__ == "__main__":
    bronze_to_silver_gym_exercises()
