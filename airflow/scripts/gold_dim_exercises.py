# scripts/gold_dim_exercises.py

import pandas as pd
from io import BytesIO
from instructor_workout.etl.utils.aws_client import get_s3

BUCKET = "instructor-workout-datas"

# Caminhos fixos
SILVER_HEAVY_KEY = "silver/synthetic_realistic_workout/merged.parquet"
SILVER_DEPARA_KEY = "silver/depara/depara_heavy_kaggle.parquet"
SILVER_KAGGLE_PREFIX = "silver/kaggle/"

GOLD_DIM_EXERCISE_KEY = "gold/dim_exercise_heavy_kaggle/dim_exercise.parquet"


def _read_parquet_from_s3(s3, key: str) -> pd.DataFrame:
    """Lê um parquet do S3 e devolve um DataFrame."""
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def _get_latest_kaggle_key(s3) -> str:
    """Busca o parquet de Kaggle mais recente dentro de silver/kaggle/."""
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=SILVER_KAGGLE_PREFIX)
    contents = resp.get("Contents")

    if not contents:
        raise RuntimeError("❌ Nenhum arquivo encontrado em silver/kaggle/")

    latest = max(contents, key=lambda x: x["LastModified"])
    return latest["Key"]


def build_dim_exercise():
    print("\n=== 🥇 GOLD - Dimensão de Exercícios ===\n")

    s3 = get_s3()

    # (Opcional) heavy – hoje a gente nem usa diretamente, mas já deixa carregado se quiser enriquecer depois
    print("➡️ Lendo Silver Heavy…")
    df_heavy = _read_parquet_from_s3(s3, SILVER_HEAVY_KEY)
    print(f"   Heavy shape: {df_heavy.shape}")

    # Kaggle – sempre o parquet mais recente
    print("➡️ Lendo Silver Kaggle (último disponível)…")
    kaggle_key = _get_latest_kaggle_key(s3)
    print(f"   ✅ Usando parquet Kaggle: s3://{BUCKET}/{kaggle_key}")
    df_kaggle = _read_parquet_from_s3(s3, kaggle_key)
    print(f"   Kaggle shape: {df_kaggle.shape}")

    # De-para manual Heavy x Kaggle
    print("➡️ Lendo Silver De/Para…")
    df_depara = _read_parquet_from_s3(s3, SILVER_DEPARA_KEY)
    print(df_depara.head(2))
    print(df_depara.dtypes)

    print("   Colunas de df_depara:", df_depara.columns.tolist())
    # esperado: ['sk_exercise', 'exercise_title_heavy', 'exercise_name_kaggle']

    # --- Prepara kaggle para o join ---

    df_kaggle_ex = (
        df_kaggle[["Exercise Name", "Main_muscle", "Equipment"]]
        .drop_duplicates()
        .rename(columns={"Exercise Name": "exercise_name_kaggle"})
    )

    # --- Monta dimensão de exercícios ---
    # join usando o mesmo nome que está no de-para: exercise_name_kaggle
    dim = df_depara.merge(
        df_kaggle_ex,
        on="exercise_name_kaggle",
        how="left",
    )

    # NÃO recria sk_exercise – já veio do de-para
    if "sk_exercise" not in dim.columns:
        # fallback de segurança (só se, por algum motivo, o de-para vier sem SK)
        dim = dim.reset_index(drop=True)
        dim.insert(0, "sk_exercise", dim.index + 1)

    print(f"✅ Linhas na dim_exercise: {len(dim)}")
    print("   Colunas finais:", dim.columns.tolist())

    # Salva no /tmp e envia para o S3
    tmp_path = "/tmp/dim_exercise.parquet"
    dim.to_parquet(tmp_path, index=False)

    s3.upload_file(tmp_path, BUCKET, GOLD_DIM_EXERCISE_KEY)
    print(f"🥇 Dimensão salva em: s3://{BUCKET}/{GOLD_DIM_EXERCISE_KEY}")


if __name__ == "__main__":
    build_dim_exercise()
