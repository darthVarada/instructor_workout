import os
import pandas as pd
from datetime import datetime
from instructor_workout.etl.utils.aws_client import get_s3

# ✅ Configurações
BUCKET = "instructor-workout-datas"

LOCAL_CSV = "/opt/data/silver/synthetic_realistic_workout.csv"
# <-- Ajuste se ele estiver em outro diretório montado

def ingest_synthetic_base():
    print("=== 🚀 Ingestão Synthetic Base → Bronze ===")

    if not os.path.exists(LOCAL_CSV):
        raise FileNotFoundError(f"❌ CSV não encontrado: {LOCAL_CSV}")

    df = pd.read_csv(LOCAL_CSV)

    print(f"➡️ Linhas carregadas: {len(df)}")

    parquet_path = "/tmp/synthetic_base.parquet"
    df.to_parquet(parquet_path, index=False)

    s3_key = "bronze/raw/synthetic_realistic_workout_base/base_full.parquet"

    s3 = get_s3()

    print(f"⬆️ Enviando para s3://{BUCKET}/{s3_key}")
    s3.upload_file(parquet_path, BUCKET, s3_key)

    print("✅ Bronze Base carregada com sucesso!")

if __name__ == "__main__":
    ingest_synthetic_base()
