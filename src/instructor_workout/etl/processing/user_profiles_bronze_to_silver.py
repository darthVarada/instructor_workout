from __future__ import annotations

import io
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd

from instructor_workout.etl.ingestion.minio_client import get_s3_client


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nomes de colunas e corrige possíveis problemas de encoding.
    """
    # Corrigir encoding quebrado eventualmente vindo de CSVs antigos
    rename_map: Dict[str, str] = {
        "nÃ­vel_treinamento": "nivel_treinamento",
        "restriÃ§Ãµes_fÃ­sicas": "restricoes_fisicas",
        "percentual_gordura(%)": "percentual_gordura",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Normalizar snake_case geral
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    return df


def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Faz cast básico dos tipos mais importantes.
    Garante que datas fiquem tz-aware em UTC.
    """
    # Datas
    for col in ["data_registro", "data_nascimento"]:
        if col in df.columns:
            # Converte para datetime, forçando UTC e mantendo tz-aware
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Numéricos
    numeric_cols = [
        "peso",
        "altura",
        "percentual_gordura",
        "frequencia_semanal",
        "horas_sono",
        "nutricional_score",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df



def _enrich_features(df: pd.DataFrame) -> pd.DataFrame:
    print("Dtypes em _enrich_features:")
    print(df.dtypes)
    """
    Cria features derivadas, como IMC, idade, faixas, etc.
    """
    # IMC
    if {"peso", "altura"}.issubset(df.columns):
        df["imc"] = df["peso"] / (df["altura"] ** 2)

    # Idade em anos
    if "data_nascimento" in df.columns:
    # now já tz-aware em UTC
        now = pd.Timestamp.now(tz="UTC")
        df["idade_anos"] = (now - df["data_nascimento"]).dt.days / 365.25




    # Faixa de IMC
    def classificar_imc(imc: float | None) -> str | None:
        if imc is None or pd.isna(imc):
            return None
        if imc < 18.5:
            return "baixo_peso"
        if imc < 25:
            return "normal"
        if imc < 30:
            return "sobrepeso"
        return "obesidade"

    if "imc" in df.columns:
        df["faixa_imc"] = df["imc"].apply(classificar_imc)

    # Normalizar algumas colunas de texto
    for col in ["sexo", "objetivo", "nivel_treinamento"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.lower()

    # Metadata
    df["silver_ingestion_ts_utc"] = datetime.now(timezone.utc)

    return df


def bronze_to_silver_user_profiles() -> None:
    """
    Lê o parquet mais recente de user_profiles no Bronze,
    aplica limpeza / enriquecimento e escreve Silver consolidado.
    """
    print("=== 🚀 Transformação Bronze → Silver (User Profiles) ===")

    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET")
    silver_bucket = os.getenv("MINIO_SILVER_BUCKET")

    if not bronze_bucket or not silver_bucket:
        raise RuntimeError("❌ Buckets MINIO_BRONZE_BUCKET / MINIO_SILVER_BUCKET não configurados.")

    s3 = get_s3_client()

    print("➡️ Buscando arquivos de user_profiles no Bronze...")
    resp = s3.list_objects_v2(
        Bucket=bronze_bucket,
        Prefix="user_profiles/",
    )

    contents: List[Dict[str, Any]] = resp.get("Contents", [])
    if not contents:
        raise RuntimeError("❌ Nenhum arquivo encontrado em Bronze/user_profiles.")

    # Pega o mais recente (LastModified)
    latest_obj = sorted(contents, key=lambda x: x["LastModified"], reverse=True)[0]
    bronze_key = latest_obj["Key"]

    print(f"➡️ Último arquivo encontrado no Bronze: {bronze_key}")

    # Download parquet
    print("➡️ Baixando parquet do Bronze...")
    obj = s3.get_object(Bucket=bronze_bucket, Key=bronze_key)
    parquet_bytes = obj["Body"].read()

    df = pd.read_parquet(io.BytesIO(parquet_bytes))
    print(f"➡️ Linhas carregadas: {len(df)}")

    # Transformações
    df = _normalize_columns(df)
    df = _cast_types(df)
    df = _enrich_features(df)

    # Garantir user_id
    if "user_id" not in df.columns:
        # fallback se por acaso veio só algum id genérico
        if "id" in df.columns:
            df = df.rename(columns={"id": "user_id"})
        else:
            df["user_id"] = df.index.astype(str)

    # Remover duplicados por user_id, mantendo registro mais recente
    if "data_registro" in df.columns:
        df = df.sort_values("data_registro").drop_duplicates("user_id", keep="last")
    else:
        df = df.drop_duplicates("user_id")

    print("✔️ Dados estruturados para Silver (User Profiles)!")

    # Gerar parquet em memória
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    silver_key = "user_profiles/profiles.parquet"
    print(f"➡️ Salvando Silver consolidado em: {silver_bucket}/{silver_key}")

    s3.upload_fileobj(
        Fileobj=buffer,
        Bucket=silver_bucket,
        Key=silver_key,
    )

    print("✔️ Silver de User Profiles atualizado com sucesso!")


if __name__ == "__main__":
    bronze_to_silver_user_profiles()
