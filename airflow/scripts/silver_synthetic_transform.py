import os
import pandas as pd
import tempfile
from instructor_workout.etl.utils.aws_client import get_s3

BUCKET = "instructor-workout-datas"

BASE_KEY = "bronze/raw/synthetic_realistic_workout_base/base_full.parquet"
INCREMENTAL_PREFIX = "bronze/raw/synthetic_realistic_workout/"
SILVER_KEY = "silver/synthetic_realistic_workout/merged.parquet"

def ingest_and_merge_silver():
    print("\n=== 🚀 Criando Silver Synthetic Realistic Workout ===")

    s3 = get_s3()
    tmp = tempfile.mkdtemp()

    # ============================
    # 1) BASE
    # ============================
    base_path = os.path.join(tmp, "base.parquet")
    print("⬇️ Baixando base...")
    s3.download_file(BUCKET, BASE_KEY, base_path)
    df_base = pd.read_parquet(base_path)
    print(f"✅ Base carregada: {len(df_base)} linhas")

    # ============================
    # 2) INCREMENTAIS
    # ============================
    print("⬇️ Buscando incrementais da API...")
    paginator = s3.get_paginator("list_objects_v2")

    dfs_inc = []

    for page in paginator.paginate(Bucket=BUCKET, Prefix=INCREMENTAL_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                local = os.path.join(tmp, os.path.basename(key))
                s3.download_file(BUCKET, key, local)
                df = pd.read_parquet(local)
                dfs_inc.append(df)

    if dfs_inc:
        df_inc = pd.concat(dfs_inc, ignore_index=True)
        print(f"✅ Incrementais carregados: {len(df_inc)} linhas")
        df_all = pd.concat([df_base, df_inc], ignore_index=True)
    else:
        print("⚠️ Nenhum incremental encontrado — usando apenas a base.")
        df_all = df_base

    # ============================
    # 3) CONVERSÕES DE DATA
    # ============================
    print("🕒 Convertendo datas...")
    df_all["start_time"] = pd.to_datetime(df_all["start_time"], errors="coerce")
    df_all["end_time"] = pd.to_datetime(df_all["end_time"], errors="coerce")

    # ============================
    # 4) DURAÇÃO FINAL
    # ============================
    print("⏱️ Calculando duração...")
    df_all["duracao"] = (
        (df_all["end_time"] - df_all["start_time"]).dt.total_seconds() / 60
    )

    # fallback se vier nulo
    df_all["duracao"] = df_all["duracao"].fillna(df_all["duration_seconds"] / 60)

    # ============================
    # 5) SALVANDO SILVER
    # ============================
    silver_path = os.path.join(tmp, "silver.parquet")
    df_all.to_parquet(silver_path, index=False)

    print(f"⬆️ Enviando para S3 → s3://{BUCKET}/{SILVER_KEY}")
    s3.upload_file(silver_path, BUCKET, SILVER_KEY)

    print("✅ Silver criada com sucesso!\n")

if __name__ == "__main__":
    ingest_and_merge_silver()
