import io
from typing import Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import streamlit as st


def get_s3():
    """
    Cria cliente S3 usando credenciais do secrets.toml
    """
    return boto3.client(
        "s3",
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["AWS_SECRET_KEY"],
        region_name=st.secrets["AWS_REGION"],
    )


def read_parquet_folder(bucket: str, prefix: str) -> pd.DataFrame:
    """
    Lê todos os arquivos Parquet de um prefix no S3 e concatena em um DataFrame.
    Se não houver arquivos, retorna DataFrame vazio.
    """
    s3 = get_s3()
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in resp:
        return pd.DataFrame()

    dfs = []
    for obj in resp["Contents"]:
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        df = pd.read_parquet(io.BytesIO(body))
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def write_parquet(df: pd.DataFrame, bucket: str, key: str) -> None:
    """
    Escreve um DataFrame como Parquet no S3.
    """
    s3 = get_s3()

    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
