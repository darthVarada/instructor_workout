import pandas as pd
from io import BytesIO
from datetime import datetime
from instructor_workout.etl.utils.aws_client import get_s3

BUCKET = "instructor-workout-datas"

BRONZE_PREFIX = "bronze/raw/kaggle/"
SILVER_PREFIX = "silver/kaggle/"

def get_latest_bronze_key(s3):
    """Busca o arquivo mais recente da bronze do Kaggle"""
    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=BRONZE_PREFIX
    )

    contents = response.get("Contents", [])
    if not contents:
        raise RuntimeError("❌ Nenhum arquivo encontrado na Bronze/Kaggle")

    latest = sorted(contents, key=lambda x: x["LastModified"])[-1]
    return latest["Key"]

def silver_kaggle():
    print("\n=== 🥈 SILVER - Kaggle Gym Exercises ===")

    s3 = get_s3()

    bronze_key = get_latest_bronze_key(s3)
    print(f"➡️ Lendo Bronze: {bronze_key}")

    obj = s3.get_object(Bucket=BUCKET, Key=bronze_key)

    buffer = BytesIO(obj["Body"].read())
    df = pd.read_parquet(buffer)

    print(f"✅ Linhas carregadas: {len(df)}")

    # ===============================
    # ✅ LIMPEZA BÁSICA DOS TEXTOS
    # ===============================
    for col in df.select_dtypes(include="object").columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"\\u200b", "", regex=True)
            .str.replace("\n", " ", regex=False)
            .str.replace("\r", " ", regex=False)
            .str.strip()
        )

    today = datetime.today().strftime("%Y%m%d")
    silver_key = f"{SILVER_PREFIX}gym_exercises_silver_{today}.parquet"

    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)

    s3.put_object(
        Bucket=BUCKET,
        Key=silver_key,
        Body=out_buffer.getvalue(),
        ContentType="application/parquet"
    )

    print(f"✅ Silver salva em: s3://{BUCKET}/{silver_key}")

if __name__ == "__main__":
    silver_kaggle()
