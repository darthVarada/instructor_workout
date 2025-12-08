import json
import os
from datetime import datetime
import pandas as pd
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from botocore.exceptions import ClientError

# ====================================
# CONFIG
# ====================================
BUCKET = "instructor-workout-datas"
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    region_name=AWS_REGION
)

# ====================================
# FUNÇÃO CORRETA PARA LER PARQUET DO S3
# ====================================
def read_parquet_from_s3(prefix: str):
    """Lê o parquet mais recente dentro do prefixo usando pyarrow BufferReader (SEM SEEK)."""
    try:
        objs = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
        if not objs:
            print(f"⚠ Nenhum arquivo encontrado em: {prefix}")
            return None

        latest = max(objs, key=lambda x: x["LastModified"])
        key = latest["Key"]

        print(f"📥 Lendo: s3://{BUCKET}/{key}")

        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = obj["Body"].read()

        # Lê corretamente usando BufferReader → evita erro de SEEK
        table = pq.read_table(pa.BufferReader(data))

        return table.to_pandas()

    except Exception as e:
        print(f"❌ Erro ao ler parquet de {prefix}: {e}")
        return None


# ====================================
# FUNÇÃO PRINCIPAL — GOLD
# ====================================
def generate_gold():

    print("\n=== 🚀 Gerando Camada GOLD ===\n")

    # -----------------------------
    # 1) DIM USERS
    # -----------------------------
    users_df = read_parquet_from_s3("silver/users/")
    if users_df is None:
        print("❌ Falha ao carregar users silver!")
        return

    print(f"Users carregado: {users_df.shape}")

    key_users = f"gold/dim_users/dim_users_{datetime.today().strftime('%Y%m%d')}.parquet"
    s3.put_object(
        Bucket=BUCKET,
        Key=key_users,
        Body=users_df.to_parquet(index=False),
        ContentType="application/octet-stream"
    )
    print(f"✔ GOLD salvo: s3://{BUCKET}/{key_users}")

    # -----------------------------
    # 2) DIM EXERCISES — KAGGLE
    # -----------------------------
    kaggle_df = read_parquet_from_s3("silver/kaggle/")
    if kaggle_df is None:
        print("⚠ Kaggle não encontrado, ignorando dim_exercises")
    else:
        print(f"Kaggle carregado: {kaggle_df.shape}")
        if "title" in kaggle_df.columns:
            key_ex = f"gold/dim_exercises/dim_exercises_{datetime.today().strftime('%Y%m%d')}.parquet"
            s3.put_object(
                Bucket=BUCKET,
                Key=key_ex,
                Body=kaggle_df.to_parquet(index=False),
                ContentType="application/octet-stream"
            )
        else:
            print("⚠ Kaggle sem coluna 'title' → não criar dimensão exercises")

    # -----------------------------
    # 3) FACT WORKOUTS (HEVY)
    # -----------------------------
    hevy_df = read_parquet_from_s3("silver/synthetic_realistic_workout/")
    if hevy_df is None:
        print("❌ Falha ao carregar Hevy Silver!")
        return

    print(f"Hevy carregado: {hevy_df.shape}")

    key_fact = f"gold/fact_workouts/fact_workouts_{datetime.today().strftime('%Y%m%d')}.parquet"
    s3.put_object(
        Bucket=BUCKET,
        Key=key_fact,
        Body=hevy_df.to_parquet(index=False),
        ContentType="application/octet-stream"
    )
    print(f"✔ GOLD salvo: s3://{BUCKET}/{key_fact}")

    # -----------------------------
    # 4) MÉTRICAS GOLD
    # -----------------------------
    print("\n=== 📊 Gerando métricas ===\n")

    metrics = {}

    # Garantir coluna date
    if "start_time" not in hevy_df.columns:
        print("❌ Hevy não tem a coluna start_time!")
        return

    hevy_df["date"] = pd.to_datetime(hevy_df["start_time"]).dt.date

    # --- 4.1 Volume total por dia
    if {"weight", "reps"}.issubset(hevy_df.columns):
        hevy_df["volume"] = hevy_df["weight"] * hevy_df["reps"]
        volume_por_dia = hevy_df.groupby("date")["volume"].sum()
        metrics["volume_por_dia"] = {str(k): float(v) for k, v in volume_por_dia.items()}
    else:
        metrics["volume_por_dia"] = {}

    # --- 4.2 Quantidade de treinos por dia
    workouts_por_dia = hevy_df.groupby("date").size()
    metrics["workouts_por_dia"] = {str(k): int(v) for k, v in workouts_por_dia.items()}

    # --- 4.3 Exercícios mais frequentes
    if "exercise_name" in hevy_df.columns:
        freq = hevy_df["exercise_name"].value_counts().head(50)
        metrics["exercicios_mais_frequentes"] = {
            str(k): int(v) for k, v in freq.items()
        }
    else:
        metrics["exercicios_mais_frequentes"] = {}

    # -----------------------------
    # 5) SALVA METRICS JSON
    # -----------------------------
    key_metrics = f"gold/metrics/metrics_{datetime.today().strftime('%Y%m%d')}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key_metrics,
        Body=json.dumps(metrics, indent=4).encode("utf-8"),
        ContentType="application/json"
    )

    print(f"✔ METRICS salvo: s3://{BUCKET}/{key_metrics}")
    print("\n🎉 GOLD FINALIZADO COM SUCESSO!\n")


# ====================================
# RUN
# ====================================
if __name__ == "__main__":
    generate_gold()
