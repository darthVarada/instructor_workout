# src/instructor_workout/etl/ingestion/kaggle_ingest_minio.py
import os
import zipfile
import tempfile
from datetime import datetime

import pandas as pd

from instructor_workout.etl.ingestion.minio_client import get_s3_client


def ingest_kaggle_gym_exercises() -> None:
    print("=== 🚀 Ingestão Kaggle Gym Exercises → Bronze (MinIO) ===")

    kaggle_dataset = "rishitmurarka/gym-exercises-dataset"

    # Diretório temporário seguro (portável entre Windows/Linux)
    tmp_dir = tempfile.mkdtemp(prefix="kaggle_gym_")
    print(f"➡️ Usando diretório temporário: {tmp_dir}")

    print(f"➡️ Baixando dataset do Kaggle: {kaggle_dataset}")

    # Requer kaggle CLI instalado e token configurado (~/.kaggle/kaggle.json)
    exit_code = os.system(
        f'kaggle datasets download -d {kaggle_dataset} -p "{tmp_dir}" --force'
    )

    if exit_code != 0:
        raise RuntimeError("❌ Erro ao baixar dataset do Kaggle. Verifique o kaggle CLI e o token.")

    # Procurar ZIP
    zip_path = None
    for f in os.listdir(tmp_dir):
        if f.endswith(".zip"):
            zip_path = os.path.join(tmp_dir, f)
            break

    if not zip_path:
        raise RuntimeError("❌ Nenhum arquivo ZIP encontrado após o download.")

    print(f"➡️ ZIP encontrado: {zip_path}")

    # Extrair ZIP
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(tmp_dir)

    # Procurar CSV
    csv_path = None
    for f in os.listdir(tmp_dir):
        if f.endswith(".csv"):
            csv_path = os.path.join(tmp_dir, f)
            break

    if not csv_path:
        raise RuntimeError("❌ Nenhum CSV encontrado no ZIP.")

    print(f"➡️ CSV encontrado: {csv_path}")

    # Carregar CSV
    df = pd.read_csv(csv_path)
    print(f"➡️ Total de linhas carregadas: {len(df)}")

    # Criar parquet temporário
    today = datetime.today()
    partition = f"{today.year}/{today.month:02}/{today.day:02}"

    parquet_path = os.path.join(tmp_dir, "gym_exercises.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"➡️ Parquet gerado: {parquet_path}")

    # Nome final no Bronze
    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET")
    if not bronze_bucket:
        raise RuntimeError("❌ Variável de ambiente MINIO_BRONZE_BUCKET não está configurada.")

    object_name = f"gym_exercises/{partition}/data.parquet"
    print(f"➡️ Enviando para MinIO → {bronze_bucket}/{object_name}")

    client = get_s3_client()

    client.upload_file(
        Filename=parquet_path,
        Bucket=bronze_bucket,
        Key=object_name,
    )

    print("✔️ Ingestão Kaggle Gym → Bronze concluída com sucesso!")


if __name__ == "__main__":
    ingest_kaggle_gym_exercises()
