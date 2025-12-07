import os
from datetime import datetime
import pandas as pd

from pathlib import Path
from instructor_workout.etl.utils.aws_client import get_s3

BUCKET = "instructor-workout-datas"

# Caminho ABSOLUTO dentro do seu projeto local
# (pois o container monta ../src e também monta a raiz do projeto)
LOCAL_CSV_PATH = (
    "/opt/airflow/../data/silver/users_form_log_birthdate.csv"
)

def ingest_users_form():

    print("\n=== 📥 Ingestão do Users Form → S3 Bronze ===\n")
    print(f"📌 Lendo arquivo local: {LOCAL_CSV_PATH}")

    if not os.path.exists(LOCAL_CSV_PATH):
        raise FileNotFoundError(
            f"❌ Arquivo não encontrado: {LOCAL_CSV_PATH}. "
            "Verifique se está montado corretamente no Docker."
        )

    df = pd.read_csv(LOCAL_CSV_PATH)

    # Nome único p/ evitar sobrescrita
    today = datetime.today().strftime("%Y%m%d")
    parquet_filename = f"users_form_{today}.parquet"
    tmp_parquet = f"/tmp/{parquet_filename}"

    print("➡️ Convertendo CSV → Parquet...")
    df.to_parquet(tmp_parquet, index=False)

    # Prefixo bronze/raw
    s3_key = f"bronze/raw/users_form_log_birthdate/{parquet_filename}"

    print(f"⬆️ Upload para s3://{BUCKET}/{s3_key}")
    s3 = get_s3()

    try:
        s3.upload_file(tmp_parquet, BUCKET, s3_key)
        print("✔️ Upload concluído com sucesso!")
    except Exception as e:
        print(f"❌ Erro no upload para o S3: {e}")
        raise

    print("\n=== ✔ Finalizado: Users Form → Bronze ===\n")


if __name__ == "__main__":
    ingest_users_form()
