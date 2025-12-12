# scripts/gold_fact_workout_sets.py

import pandas as pd
from io import BytesIO
from instructor_workout.etl.utils.aws_client import get_s3

BUCKET = "instructor-workout-datas"

# Silver heavy (tabelão de sets)
SILVER_HEAVY_KEY = "silver/synthetic_realistic_workout/merged.parquet"

# De/para heavy ↔ kaggle
SILVER_DEPARA_KEY = "silver/depara/depara_heavy_kaggle.parquet"

# Silver Kaggle (vamos pegar sempre o mais recente)
SILVER_KAGGLE_PREFIX = "silver/kaggle/"

# Fato final
GOLD_FACT_WORKOUT_KEY = "gold/fact_train/fact_workout_sets.parquet"


def _read_parquet_from_s3(s3, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def _get_latest_silver_kaggle_key(s3) -> str:
    """
    Busca o parquet de Kaggle mais recente na silver.
    Ex.: silver/kaggle/gym_exercises_silver_20251210.parquet
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


def build_fact_workout_sets():
    print("\n=== 🥇 GOLD - Fato de Sets de Treino (fact_workout_sets) ===\n")

    s3 = get_s3()

    # ============================
    # 1) Silver Heavy
    # ============================
    print(f"➡️ Lendo Silver Heavy: s3://{BUCKET}/{SILVER_HEAVY_KEY}")
    df_heavy = _read_parquet_from_s3(s3, SILVER_HEAVY_KEY)
    print(f"   Linhas heavy (sets): {len(df_heavy)}")

    # ============================
    # 2) De/Para Heavy ↔ Kaggle
    # ============================
    print(f"➡️ Lendo De/Para Heavy ↔ Kaggle: s3://{BUCKET}/{SILVER_DEPARA_KEY}")
    df_depara = _read_parquet_from_s3(s3, SILVER_DEPARA_KEY)
    print(f"   Linhas de-para: {len(df_depara)}")
    print(f"   Colunas de-para: {list(df_depara.columns)}")

    if "exercise_title_heavy" not in df_depara.columns:
        raise RuntimeError("❌ 'exercise_title_heavy' não encontrada no de-para.")
    if "exercise_name_kaggle" not in df_depara.columns:
        raise RuntimeError("❌ 'exercise_name_kaggle' não encontrada no de-para.")
    if "sk_exercise" not in df_depara.columns:
        raise RuntimeError("❌ 'sk_exercise' não encontrada no de-para.")

    # ============================
    # 3) Silver Kaggle (músculos) – LENDO DA SILVER
    # ============================
    kaggle_key = _get_latest_silver_kaggle_key(s3)
    print(f"➡️ Lendo Silver Kaggle (último disponível): s3://{BUCKET}/{kaggle_key}")
    df_kaggle = _read_parquet_from_s3(s3, kaggle_key)
    print(f"   Linhas Kaggle (silver): {len(df_kaggle)}")
    print(f"   Colunas Kaggle (silver): {list(df_kaggle.columns)}")

    if "Exercise Name" not in df_kaggle.columns:
        raise RuntimeError("❌ Coluna 'Exercise Name' não encontrada na silver do Kaggle.")

    # Renomeia para nomes padronizados
    df_kaggle = df_kaggle.rename(
        columns={
            "Exercise Name": "exercise_name_kaggle",
            "Main_muscle": "main_muscle",
            "Target_Muscles": "target_muscles",
            "Synergist_Muscles": "synergist_muscles",
            "Equipment": "equipment",
        }
    )

    # Mantém só o que interessa e evita multiplicar linhas:
    kaggle_cols_keep = [
        "exercise_name_kaggle",
        "main_muscle",
        "target_muscles",
        "synergist_muscles",
        "equipment",
    ]
    kaggle_cols_keep = [c for c in kaggle_cols_keep if c in df_kaggle.columns]

    df_kaggle_dim = (
        df_kaggle[kaggle_cols_keep]
        .drop_duplicates(subset=["exercise_name_kaggle"])
        .reset_index(drop=True)
    )

    print(f"   Linhas Kaggle após drop_duplicates pelo exercício: {len(df_kaggle_dim)}")

    # ============================
    # 4) Join Heavy + De/Para
    # ============================
    print("🔗 Fazendo join heavy + de-para (por exercise_title)...")
    df_fact = df_heavy.merge(
        df_depara[["exercise_title_heavy", "exercise_name_kaggle", "sk_exercise"]],
        left_on="exercise_title",
        right_on="exercise_title_heavy",
        how="left",
    )

    # ============================
    # 5) Join com Kaggle (músculos)
    # ============================
    print("💪 Enriquecendo com músculos do Kaggle (silver)...")
    df_fact = df_fact.merge(
        df_kaggle_dim,
        on="exercise_name_kaggle",
        how="left",
    )

    # Remove coluna auxiliar
    if "exercise_title_heavy" in df_fact.columns:
        df_fact = df_fact.drop(columns=["exercise_title_heavy"])

    # ============================
    # 6) Tipagens / organização
    # ============================
    if "user_id" in df_fact.columns:
        df_fact["user_id"] = df_fact["user_id"].astype("string")

    if "id_exercice" in df_fact.columns:
        df_fact["id_exercice"] = df_fact["id_exercice"].astype("string")

    if "sk_exercise" in df_fact.columns:
        df_fact["sk_exercise"] = df_fact["sk_exercise"].astype("Int64")

    col_order = [
        # chaves
        "user_id",
        "sk_exercise",
        "id_exercice",
        "id",
        "routine_id",
        # contexto do treino
        "title",
        "description",
        "start_time",
        "end_time",
        "duracao",
        # contexto do exercício / set
        "exercise_title",
        "superset_id",
        "exercise_notes",
        "set_index",
        "set_type",
        # métricas
        "weight_kg",
        "reps",
        "distance_km",
        "duration_seconds",
        "rpe",
        # músculos Kaggle
        "main_muscle",
        "target_muscles",
        "synergist_muscles",
        "equipment",
        # metadados
        "updated_at",
        "created_at",
        "exercise_name_kaggle",
        "exercises",
    ]

    col_order = [c for c in col_order if c in df_fact.columns]
    other_cols = [c for c in df_fact.columns if c not in col_order]
    df_fact = df_fact[col_order + other_cols]

    print(f"✅ fact_workout_sets montada com {len(df_fact)} linhas")

    # ============================
    # 7) Salvar no S3
    # ============================
    tmp_path = "/tmp/fact_workout_sets.parquet"
    df_fact.to_parquet(tmp_path, index=False)

    print(f"⬆️ Enviando para S3 → s3://{BUCKET}/{GOLD_FACT_WORKOUT_KEY}")
    s3.upload_file(tmp_path, BUCKET, GOLD_FACT_WORKOUT_KEY)

    print(f"🎉 Fato salva com sucesso em: s3://{BUCKET}/{GOLD_FACT_WORKOUT_KEY}\n")


if __name__ == "__main__":
    build_fact_workout_sets()
