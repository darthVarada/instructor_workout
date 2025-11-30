from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from pymongo import MongoClient

from instructor_workout.etl.ingestion.minio_client import get_s3_client


def ingest_user_profiles_mongo_to_minio() -> None:
    """
    Full load de perfis de usuário a partir do MongoDB para o Bronze (MinIO) em Parquet.

    Mongo:
      - MONGO_URI (ex: mongodb://root:root@localhost:27017)
      - MONGO_DB  (ex: instructor_workout)
      - MONGO_USERS_COLLECTION (ex: users)

    Bronze (MinIO):
      - MINIO_BRONZE_BUCKET
      - Key: user_profiles/YYYY/MM/DD/data.parquet
    """
    print("=== 🚀 Ingestão User Profiles (Mongo) → Bronze (MinIO) ===")

    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://root:root@localhost:27017/?authSource=admin",
    )
    print(f"➡️ Conectando no Mongo em: {mongo_uri}")

    mongo_db = os.getenv("MONGO_DB", "instructor_workout")
    mongo_coll = os.getenv("MONGO_USERS_COLLECTION", "users")
    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET")

    if not bronze_bucket:
        raise RuntimeError("❌ MINIO_BRONZE_BUCKET não configurado.")

    # 1) Ler dados do Mongo
    client_mongo = MongoClient(mongo_uri)
    coll = client_mongo[mongo_db][mongo_coll]

    print(f"➡️ Lendo documentos do MongoDB {mongo_db}.{mongo_coll} ...")
    docs: List[Dict[str, Any]] = list(coll.find({}))  # full load

    if not docs:
        print("⚠️ Nenhum documento encontrado em Mongo. Nada a escrever no Bronze.")
        return

    # Normalizar estrutura: garantir user_id e remover _id cru
    for d in docs:
        if "_id" in d and "user_id" not in d:
            d["user_id"] = str(d["_id"])
        d.pop("_id", None)

    df = pd.DataFrame(docs)
    print(f"➡️ Linhas carregadas do Mongo: {len(df)}")

    # 2) Gerar parquet temporário
    tmp_dir = Path(tempfile.mkdtemp(prefix="user_profiles_mongo_"))
    parquet_path = tmp_dir / "user_profiles.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"➡️ Parquet gerado: {parquet_path}")

    # 3) Enviar para MinIO (partição por data de ingestão)
    now = datetime.now(timezone.utc)
    partition = now.strftime("%Y/%m/%d")
    object_key = f"user_profiles/{partition}/data.parquet"

    print(f"➡️ Enviando para MinIO → {bronze_bucket}/{object_key}")
    s3 = get_s3_client()
    s3.upload_file(
        Filename=str(parquet_path),
        Bucket=bronze_bucket,
        Key=object_key,
    )

    print("✔️ Ingestão User Profiles (Mongo) → Bronze concluída com sucesso!")


if __name__ == "__main__":
    ingest_user_profiles_mongo_to_minio()
