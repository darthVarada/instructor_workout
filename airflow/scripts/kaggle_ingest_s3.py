import os
import requests
import zipfile
import tempfile
from datetime import datetime
import pandas as pd
from instructor_workout.etl.utils.aws_client import get_s3


BUCKET = "instructor-workout-datas"


def ingest_kaggle_to_s3():
    print("=== 🚀 Ingestão Kaggle → bronze/raw/kaggle ===")

    KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
    KAGGLE_KEY = os.getenv("KAGGLE_KEY")

    if not KAGGLE_USERNAME or not KAGGLE_KEY:
        raise RuntimeError("❌ Variáveis KAGGLE_USERNAME e KAGGLE_KEY não definidas no ambiente!")

    dataset = "rishitmurarka/gym-exercises-dataset"

    url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset}"

    tmp_dir = tempfile.mkdtemp(prefix="kaggle_dl_")
    zip_path = os.path.join(tmp_dir, "dataset.zip")

    print(f"➡️ Baixando via Kaggle REST API para: {zip_path}")

    response = requests.get(url, auth=(KAGGLE_USERNAME, KAGGLE_KEY), stream=True)

    if response.status_code != 200:
        raise RuntimeError(f"❌ Erro ao acessar Kaggle API: {response.status_code} → {response.text}")

    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print("✔️ Download concluído!")

    print("➡️ Extraindo ZIP...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(tmp_dir)

    csv_path = next(
        (os.path.join(tmp_dir, f) for f in os.listdir(tmp_dir) if f.endswith(".csv")),
        None
    )

    if not csv_path:
        raise RuntimeError("❌ CSV não encontrado dentro do arquivo baixado!")

    print(f"✔️ CSV localizado: {csv_path}")

    df = pd.read_csv(csv_path)

    today = datetime.today().strftime("%Y%m%d")
    parquet_filename = f"gym_exercises_{today}.parquet"
    parquet_path = os.path.join(tmp_dir, parquet_filename)

    print("➡️ Convertendo para Parquet...")
    df.to_parquet(parquet_path, index=False)

    s3_key = f"bronze/raw/kaggle/{parquet_filename}"

    s3 = get_s3()
    print(f"⬆️ Enviando para S3 → s3://{BUCKET}/{s3_key}")
    s3.upload_file(parquet_path, BUCKET, s3_key)

    print("🎉 Kaggle → S3 finalizado com sucesso!")


if __name__ == "__main__":
    ingest_kaggle_to_s3()
