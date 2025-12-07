import pandas as pd
from io import BytesIO
from datetime import datetime
from instructor_workout.etl.utils.aws_client import get_s3

BUCKET = "instructor-workout-datas"

BRONZE_PREFIX = "bronze/raw/users_form_log_birthdate/"
SILVER_PREFIX = "silver/users/"

def get_latest_bronze_key(s3):
    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=BRONZE_PREFIX
    )

    contents = response.get("Contents", [])
    if not contents:
        raise RuntimeError("❌ Nenhum arquivo encontrado em bronze/raw/users_form/")

    latest = sorted(contents, key=lambda x: x["LastModified"])[-1]
    return latest["Key"]

def silver_users():
    print("\n=== 🥈 SILVER - Users Form ===")

    s3 = get_s3()

    bronze_key = get_latest_bronze_key(s3)
    print(f"➡️ Lendo Bronze: {bronze_key}")

    obj = s3.get_object(Bucket=BUCKET, Key=bronze_key)
    buffer = BytesIO(obj["Body"].read())

    # Detecta automaticamente CSV ou Parquet
    if bronze_key.endswith(".csv"):
        df = pd.read_csv(buffer)
    else:
        df = pd.read_parquet(buffer)

    print(f"✅ Linhas carregadas: {len(df)}")

    # ===============================
    # 🧹 LIMPEZAS E PADRONIZAÇÃO
    # ===============================
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    if "data_nascimento" in df.columns:
        df["data_nascimento"] = pd.to_datetime(df["data_nascimento"], errors="coerce")

    df = df.drop_duplicates()

    today = datetime.today().strftime("%Y%m%d")
    silver_key = f"{SILVER_PREFIX}users_silver_{today}.parquet"

    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)

    s3.put_object(
        Bucket=BUCKET,
        Key=silver_key,
        Body=out_buffer.getvalue(),
        ContentType="application/parquet"
    )

    print(f"✅ Silver Users salvo em: s3://{BUCKET}/{silver_key}")

if __name__ == "__main__":
    silver_users()
