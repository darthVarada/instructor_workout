# scripts/gold_dim_exercises_kaggle.py

import pandas as pd
from io import BytesIO
from instructor_workout.etl.utils.aws_client import get_s3

BUCKET = "instructor-workout-datas"

SILVER_KAGGLE_PREFIX = "silver/kaggle/"
GOLD_DIM_KAGGLE_KEY = "gold/kaggle_dim/dim_exercises_kaggle.parquet"


def _read_parquet_from_s3(s3, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def _get_latest_silver_kaggle_key(s3) -> str:
    """
    Busca o parquet de Kaggle mais recente na silver.
    Exemplo: silver/kaggle/gym_exercises_silver_20251210.parquet
    """
    resp = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=SILVER_KAGGLE_PREFIX,
    )
    contents = resp.get("Contents", [])
    if not contents:
        raise RuntimeError("❌ Nenhum arquivo encontrado em silver/kaggle/")

    latest = sorted(contents, key=lambda x: x["LastModified"])[-1]
    return latest["Key"]


def build_dim_exercises_kaggle():
    print("\n=== 🥇 GOLD - Dimensão de Exercícios (Kaggle) ===\n")

    s3 = get_s3()

    # 1) Pega a silver mais recente
    kaggle_key = _get_latest_silver_kaggle_key(s3)
    print(f"➡️ Lendo Silver Kaggle: s3://{BUCKET}/{kaggle_key}")

    df = _read_parquet_from_s3(s3, kaggle_key)
    print(f"   Linhas Kaggle (silver): {len(df)}")
    print(f"   Colunas Kaggle (silver): {list(df.columns)}")

    # 2) Normaliza nomes de colunas principais
    if "Exercise Name" not in df.columns:
        raise RuntimeError("❌ Coluna 'Exercise Name' não encontrada na silver do Kaggle.")

    df = df.rename(
        columns={
            "Exercise Name": "exercise_name_kaggle",
            "Main_muscle": "main_muscle",
            "Target_Muscles": "target_muscles",
            "Synergist_Muscles": "synergist_muscles",
            "Equipment": "equipment",
        }
    )

    # 3) Seleciona só o que interessa pra dimensão
    cols_keep = [
        "exercise_name_kaggle",
        "main_muscle",
        "target_muscles",
        "synergist_muscles",
        "equipment",
        "Preparation",
        "Execution",
    ]
    cols_keep = [c for c in cols_keep if c in df.columns]

    dim = df[cols_keep].drop_duplicates().reset_index(drop=True)

    # 4) Cria surrogate key
    dim.insert(0, "sk_exercise_kaggle", dim.index + 1)
    dim["sk_exercise_kaggle"] = dim["sk_exercise_kaggle"].astype("Int64")

    print(f"✅ Linhas na dim_exercises_kaggle: {len(dim)}")

    # 5) Salva no /tmp e sobe pro S3
    tmp_path = "/tmp/dim_exercises_kaggle.parquet"
    dim.to_parquet(tmp_path, index=False)

    print(f"⬆️ Enviando para S3 → s3://{BUCKET}/{GOLD_DIM_KAGGLE_KEY}")
    s3.upload_file(tmp_path, BUCKET, GOLD_DIM_KAGGLE_KEY)

    print(f"🎉 Dimensão Kaggle salva em: s3://{BUCKET}/{GOLD_DIM_KAGGLE_KEY}\n")


if __name__ == "__main__":
    build_dim_exercises_kaggle()
