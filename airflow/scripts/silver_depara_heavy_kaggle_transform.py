# scripts/silver_depara_heavy_kaggle_transform.py
import pandas as pd
from io import StringIO
from instructor_workout.etl.utils.aws_client import get_s3

BUCKET = "instructor-workout-datas"

BRONZE_KEY = "bronze/raw/hevy/depara_manual_heavy_kaggle.csv"
SILVER_KEY = "silver/depara/depara_heavy_kaggle.parquet"


def silver_depara_heavy_kaggle():
    print("\n=== 🥈 SILVER - De/Para Heavy ↔ Kaggle ===\n")

    s3 = get_s3()

    # Lê o CSV do bronze
    obj = s3.get_object(Bucket=BUCKET, Key=BRONZE_KEY)
    body = obj["Body"].read().decode("utf-8")

    df = pd.read_csv(StringIO(body))

    print(f"➡️ Linhas originais: {len(df)}")
    print(f"➡️ Colunas originais: {list(df.columns)}")

    # Padroniza nomes de colunas
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("ç", "c")
        .str.replace("ã", "a")
        .str.replace("á", "a")
        .str.replace("é", "e")
    )

    # Esperado algo como:
    # exercise_title_heavy, exercise_title_kaggle, score
    # Vamos só garantir alguns aliases comuns:
    rename_map = {
        "exercise_title_hevy": "exercise_title_heavy",
        "exercise_kaggle": "exercise_title_kaggle",
    }
    df = df.rename(columns=rename_map)

    # Remove duplicados
    df = df.drop_duplicates()

    # Se existir score, podemos filtrar lixo (ex: >= 5 ou >= 10)
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
        df = df[df["score"].fillna(0) >= 5]

    print(f"✅ Linhas após limpeza: {len(df)}")

    # Salva temporário em parquet
    tmp_path = "/tmp/depara_heavy_kaggle_silver.parquet"
    df.to_parquet(tmp_path, index=False)

    # Sobe para S3
    s3.upload_file(tmp_path, BUCKET, SILVER_KEY)

    print(f"✅ Silver salvo em: s3://{BUCKET}/{SILVER_KEY}")


if __name__ == "__main__":
    silver_depara_heavy_kaggle()
